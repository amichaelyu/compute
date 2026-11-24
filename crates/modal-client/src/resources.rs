//! Apps, registry images and secrets. Mirrors `go/app.go`, `go/image.go`, `go/secret.go` and
//! `go/environment.go` in modal-labs/modal-client.

use std::collections::BTreeMap;

use anyhow::{Context, Result, anyhow, bail};
use tonic::Code;

use crate::client::{CallOpts, Client};
use crate::proto::client as pb;
use crate::proto::client::generic_result::GenericStatus;

/// Server-side timeout of one `ImageJoinStreaming` call, in seconds (as in the Go SDK).
const IMAGE_JOIN_TIMEOUT_SECS: f32 = 55.0;
/// Consecutive transient `ImageJoinStreaming` failures tolerated while waiting for a build.
const IMAGE_JOIN_MAX_FAILURES: u32 = 10;

impl Client {
    /// Looks up the app `name` in `environment` (`None` = the workspace's default environment),
    /// creating it if missing, and returns its app ID.
    pub async fn app_get_or_create(&self, name: &str, environment: Option<&str>) -> Result<String> {
        let req = pb::AppGetOrCreateRequest {
            app_name: name.to_string(),
            environment_name: environment.unwrap_or_default().to_string(),
            object_creation_type: pb::ObjectCreationType::CreateIfMissing as i32,
        };
        let resp = self
            .call(
                "AppGetOrCreate",
                req,
                CallOpts::default(),
                |mut c, r| async move { c.app_get_or_create(r).await },
            )
            .await
            .with_context(|| format!("AppGetOrCreate {name:?}"))?;
        tracing::debug!(app_id = resp.app_id, name, "modal: got app");
        Ok(resp.app_id)
    }

    /// Builds (or reuses) an image from a registry reference such as `python:3.12-slim` or
    /// `ghcr.io/org/img:tag`, waits until it is ready, and returns its image ID.
    ///
    /// For a private registry, pass the ID of a secret holding `REGISTRY_USERNAME` and
    /// `REGISTRY_PASSWORD` (or AWS ECR / GCP Artifact Registry credentials); the auth type is
    /// inferred from the secret's keys, as in the Go SDK's `Images.FromRegistry`. The image
    /// builder version comes from the default environment; see [`Client::image_from_registry_in`].
    pub async fn image_from_registry(
        &self,
        app_id: &str,
        image_ref: &str,
        registry_secret_id: Option<&str>,
    ) -> Result<String> {
        self.image_from_registry_in(app_id, image_ref, registry_secret_id, None)
            .await
    }

    /// Like [`Client::image_from_registry`], with the environment used to look up the image
    /// builder version (`None` = default environment).
    pub async fn image_from_registry_in(
        &self,
        app_id: &str,
        image_ref: &str,
        registry_secret_id: Option<&str>,
        environment: Option<&str>,
    ) -> Result<String> {
        let builder_version = self
            .image_builder_version(environment.unwrap_or_default())
            .await?;
        let image_registry_config = match registry_secret_id {
            Some(secret_id) => {
                let keys = self.secret_keys(secret_id).await?;
                Some(pb::ImageRegistryConfig {
                    registry_auth_type: infer_registry_auth_type(&keys)? as i32,
                    secret_id: secret_id.to_string(),
                })
            }
            None => None,
        };
        let req = pb::ImageGetOrCreateRequest {
            app_id: app_id.to_string(),
            image: Some(pb::Image {
                dockerfile_commands: vec![format!("FROM {image_ref}")],
                image_registry_config,
                ..Default::default()
            }),
            builder_version,
            ..Default::default()
        };
        let resp = self
            .call(
                "ImageGetOrCreate",
                req,
                CallOpts::default(),
                |mut c, r| async move { c.image_get_or_create(r).await },
            )
            .await
            .with_context(|| format!("ImageGetOrCreate {image_ref:?}"))?;
        let image_id = resp.image_id;

        let mut result = resp
            .result
            .filter(|r| r.status() != GenericStatus::Unspecified);
        let mut last_entry_id = String::new();
        let mut failures = 0;
        while result.is_none() {
            tracing::debug!(image_id, image_ref, "modal: waiting for image build");
            match self.image_join_once(&image_id, &mut last_entry_id).await {
                Ok(r) => {
                    result = r;
                    failures = 0;
                }
                // The Go SDK gives up on the first stream error; tolerate a few transient ones,
                // since builds can take many minutes.
                Err(err)
                    if crate::client::is_retryable(err.code())
                        && failures < IMAGE_JOIN_MAX_FAILURES =>
                {
                    failures += 1;
                    tracing::debug!(image_id, error = %err, "modal: image build stream failed; rejoining");
                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                }
                Err(err) => {
                    return Err(
                        anyhow::Error::new(err).context(format!("ImageJoinStreaming {image_id}"))
                    );
                }
            }
        }
        let result = result.expect("loop ends with a result");
        match result.status() {
            GenericStatus::Success => Ok(image_id),
            GenericStatus::Failure | GenericStatus::MemoryManagerEviction => {
                bail!(
                    "image build for {image_ref} ({image_id}) failed with the exception:\n{}",
                    result.exception
                )
            }
            GenericStatus::Terminated => bail!(
                "image build for {image_ref} ({image_id}) terminated due to external shut-down; try again"
            ),
            GenericStatus::Timeout => bail!("image build for {image_ref} ({image_id}) timed out"),
            other => bail!(
                "image build for {image_ref} ({image_id}) failed with unknown status {}",
                other.as_str_name()
            ),
        }
    }

    /// One `ImageJoinStreaming` call: returns the final build result if it arrived before the
    /// stream ended (`waitForBuildIteration` in Go). Log lines are ignored.
    async fn image_join_once(
        &self,
        image_id: &str,
        last_entry_id: &mut String,
    ) -> Result<Option<pb::GenericResult>, tonic::Status> {
        let req = pb::ImageJoinStreamingRequest {
            image_id: image_id.to_string(),
            timeout: IMAGE_JOIN_TIMEOUT_SECS,
            last_entry_id: last_entry_id.clone(),
            include_logs_for_finished: false,
        };
        let mut stream = self
            .stub()
            .image_join_streaming(self.stream_request(req))
            .await?
            .into_inner();
        while let Some(item) = stream.message().await? {
            if !item.entry_id.is_empty() {
                *last_entry_id = item.entry_id;
            }
            if let Some(result) = item.result
                && result.status() != GenericStatus::Unspecified
            {
                return Ok(Some(result));
            }
        }
        Ok(None)
    }

    /// The environment's image builder version, cached per environment (`environmentManager` in Go).
    async fn image_builder_version(&self, environment: &str) -> Result<String> {
        if let Some(v) = self
            .inner
            .builder_versions
            .lock()
            .expect("builder version lock")
            .get(environment)
        {
            return Ok(v.clone());
        }
        let req = pb::EnvironmentGetOrCreateRequest {
            deployment_name: environment.to_string(),
            object_creation_type: pb::ObjectCreationType::Unspecified as i32,
        };
        let resp = self
            .call(
                "EnvironmentGetOrCreate",
                req,
                CallOpts::default(),
                |mut c, r| async move { c.environment_get_or_create(r).await },
            )
            .await
            .context("failed to get environment for image builder version")?;
        let version = resp
            .metadata
            .and_then(|m| m.settings)
            .map(|s| s.image_builder_version)
            .unwrap_or_default();
        self.inner
            .builder_versions
            .lock()
            .expect("builder version lock")
            .insert(environment.to_string(), version.clone());
        Ok(version)
    }

    async fn secret_keys(&self, secret_id: &str) -> Result<Vec<String>> {
        let req = pb::SecretGetInfoRequest {
            secret_id: secret_id.to_string(),
        };
        let resp = self
            .call(
                "SecretGetInfo",
                req,
                CallOpts::default(),
                |mut c, r| async move { c.secret_get_info(r).await },
            )
            .await
            .with_context(|| format!("SecretGetInfo {secret_id}"))?;
        Ok(resp.metadata.map(|m| m.keys).unwrap_or_default())
    }

    /// Creates a secret holding `env` as environment variables and returns its secret ID.
    ///
    /// With `name: None` the secret is ephemeral (`OBJECT_CREATION_TYPE_EPHEMERAL`, as the Go
    /// SDK's `Secrets.FromMap` does): it is unnamed and garbage-collected by Modal. With a name it
    /// is a named secret in `environment`, overwritten if it exists (like the Python SDK's
    /// `Secret.objects.create(..., overwrite=True)`); delete it with [`Client::secret_delete`].
    pub async fn secret_from_env(
        &self,
        name: Option<&str>,
        env: &BTreeMap<String, String>,
        environment: Option<&str>,
    ) -> Result<String> {
        for key in env.keys() {
            validate_env_var_name(key)?;
        }
        let (deployment_name, creation_type) = match name {
            Some(name) => (
                name.to_string(),
                pb::ObjectCreationType::CreateOverwriteIfExists,
            ),
            None => (String::new(), pb::ObjectCreationType::Ephemeral),
        };
        let req = pb::SecretGetOrCreateRequest {
            deployment_name,
            environment_name: environment.unwrap_or_default().to_string(),
            object_creation_type: creation_type as i32,
            env_dict: env.iter().map(|(k, v)| (k.clone(), v.clone())).collect(),
            ..Default::default()
        };
        let resp = self
            .call(
                "SecretGetOrCreate",
                req,
                CallOpts::default(),
                |mut c, r| async move { c.secret_get_or_create(r).await },
            )
            .await
            .context("SecretGetOrCreate")?;
        Ok(resp.secret_id)
    }

    /// Deletes a secret by ID. A secret that no longer exists is not an error. Ephemeral secrets
    /// are cleaned up by Modal on their own, so failures for them can be ignored.
    pub async fn secret_delete(&self, secret_id: &str) -> Result<()> {
        let req = pb::SecretDeleteRequest {
            secret_id: secret_id.to_string(),
        };
        match self
            .call(
                "SecretDelete",
                req,
                CallOpts::default(),
                |mut c, r| async move { c.secret_delete(r).await },
            )
            .await
        {
            Ok(()) => Ok(()),
            Err(err) if err.code() == Code::NotFound => Ok(()),
            Err(err) => Err(anyhow::Error::new(err).context(format!("SecretDelete {secret_id}"))),
        }
    }
}

/// Registry auth type implied by a secret's keys (`inferRegistryAuthType` in Go).
fn infer_registry_auth_type(keys: &[String]) -> Result<pb::RegistryAuthType> {
    let has_all = |required: &[&str]| required.iter().all(|k| keys.iter().any(|key| key == k));
    let candidates: [(pb::RegistryAuthType, &str, &[&[&str]]); 3] = [
        (
            pb::RegistryAuthType::Aws,
            "AWS",
            &[
                &["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_REGION"],
                &["AWS_ROLE_ARN", "AWS_REGION"],
            ],
        ),
        (
            pb::RegistryAuthType::Gcp,
            "GCP",
            &[&["SERVICE_ACCOUNT_JSON"]],
        ),
        (
            pb::RegistryAuthType::StaticCreds,
            "username and password",
            &[&["REGISTRY_USERNAME", "REGISTRY_PASSWORD"]],
        ),
    ];
    let matches: Vec<_> = candidates
        .iter()
        .filter(|(_, _, sets)| sets.iter().any(|set| has_all(set)))
        .collect();
    const HELP: &str = "Registry secret must include exactly one of: AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY and AWS_REGION \
        (or AWS_ROLE_ARN and AWS_REGION) for AWS ECR; SERVICE_ACCOUNT_JSON for GCP Artifact Registry; \
        or REGISTRY_USERNAME and REGISTRY_PASSWORD for other registries.";
    match matches.as_slice() {
        [(auth_type, _, _)] => Ok(*auth_type),
        [] => {
            let mut found: Vec<_> = keys.to_vec();
            found.sort();
            let found = if found.is_empty() {
                "none".to_string()
            } else {
                found.join(", ")
            };
            Err(anyhow!("{HELP} Found keys: {found}."))
        }
        many => {
            let names: Vec<_> = many.iter().map(|(_, name, _)| *name).collect();
            Err(anyhow!(
                "Registry secret has keys for more than one kind of authentication ({}). {HELP}",
                names.join(", ")
            ))
        }
    }
}

/// Secret keys become environment variables, so they must be valid variable names.
fn validate_env_var_name(key: &str) -> Result<()> {
    let mut chars = key.chars();
    let valid = matches!(chars.next(), Some(c) if c.is_ascii_alphabetic() || c == '_')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_');
    if !valid {
        bail!(
            "secret key name {key:?} is invalid for environment variables: only letters, numbers and underscores are allowed, \
             and the name cannot start with a number"
        );
    }
    Ok(())
}
