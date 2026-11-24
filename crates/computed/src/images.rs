//! Container images. A job runs in a registry image, or in an image built from a
//! Dockerfile in its snapshot. Builds run on the builder backend with
//! `docker buildx build --platform ... --push`, are tagged `<prefix>:<hash>` where the hash
//! covers the Dockerfile and every file in the build context, and are skipped when the registry
//! already has that tag. Code still comes from the snapshot at run time, so editing code outside
//! the build context doesn't trigger a rebuild.

use crate::backend::Backend;
use crate::error::ApiError;
use crate::model::{BuildSpec, ContainerReq, ContainerSpec, Job};
use crate::scheduler::Scheduler;
use crate::store::ImageRow;
use crate::transport::shell_quote;
use anyhow::{Context, Result, bail};
use async_compression::tokio::bufread::ZstdDecoder;
use chrono::Utc;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::io::Read;
use std::sync::Arc;
use tokio::io::{AsyncWriteExt, BufReader};

/// Where a job stands with its image.
pub enum ImageStatus {
    Ready,
    Building(String),
    Failed(String),
}

impl Scheduler {
    /// Turns a request's `container` into the stored spec, working out the build tag.
    pub async fn container_spec(&self, snapshot: &str, req: &ContainerReq) -> Result<ContainerSpec, ApiError> {
        if let Some(image) = &req.image {
            return Ok(ContainerSpec { image: image.clone(), build: None });
        }
        let dockerfile = req.build.clone().unwrap_or_default();
        let context = req.context.clone().unwrap_or_else(|| match dockerfile.rsplit_once('/') {
            Some((dir, _)) => dir.to_string(),
            None => ".".to_string(),
        });
        let prefix = self.registry.prefix().ok_or_else(|| {
            ApiError::bad_request(
                "image builds are not set up on this daemon (no registry prefix)",
                "use a prebuilt image with {\"image\": \"...\"}, or ask the user to configure [registry]",
            )
        })?;
        let path = self.snapshots.path(snapshot);
        let (df, ctx) = (dockerfile.clone(), context.clone());
        let hash = tokio::task::spawn_blocking(move || build_hash(&path, &df, &ctx))
            .await
            .map_err(anyhow::Error::from)??;
        Ok(ContainerSpec {
            image: format!("{prefix}:{}", &hash[..16]),
            build: Some(BuildSpec { dockerfile, context }),
        })
    }

    /// For a job with a build: ready, building (started now if needed), or failed.
    pub fn image_status(self: &Arc<Self>, job: &Job) -> Result<ImageStatus> {
        let Some(ContainerSpec { image, build: Some(build) }) = &job.spec.container else {
            return Ok(ImageStatus::Ready);
        };
        match self.store.image(image)? {
            Some(row) if row.state == "ready" => return Ok(ImageStatus::Ready),
            Some(row) if row.state == "failed" && row.snapshot == job.snapshot => {
                return Ok(ImageStatus::Failed(row.error.unwrap_or_else(|| "build failed".into())));
            }
            Some(row) if row.state == "building" && self.builds_in_flight(image) => {
                return Ok(ImageStatus::Building(image.clone()));
            }
            _ => {}
        }
        // Not built yet (or a stale/failed attempt from another snapshot): build now.
        self.store.put_image(&ImageRow {
            tag: image.clone(),
            dockerfile: build.dockerfile.clone(),
            context: build.context.clone(),
            snapshot: job.snapshot.clone(),
            state: "building".into(),
            builder: None,
            error: None,
            created_at: Utc::now(),
            finished_at: None,
        })?;
        self.start_build(image.clone(), build.clone(), job.snapshot.clone());
        Ok(ImageStatus::Building(image.clone()))
    }

    fn start_build(self: &Arc<Self>, tag: String, build: BuildSpec, snapshot: String) {
        if !self.mark_build(&tag, true) {
            return;
        }
        let me = self.clone();
        tokio::spawn(async move {
            let result = me.build(&tag, &build, &snapshot).await;
            let (state, error) = match &result {
                Ok(()) => ("ready", None),
                Err(e) => ("failed", Some(crate::backend::tail(&format!("{e:#}"), 1500))),
            };
            match &result {
                Ok(()) => tracing::info!("image {tag} ready"),
                Err(e) => tracing::warn!("image build {tag} failed: {e:#}"),
            }
            if let Ok(Some(mut row)) = me.store.image(&tag) {
                row.state = state.into();
                row.error = error;
                row.finished_at = Some(Utc::now());
                let _ = me.store.put_image(&row);
            }
            me.mark_build(&tag, false);
            me.kick();
        });
    }

    /// The backend that builds images: `[registry] builder`, else the first reachable local
    /// backend with Docker.
    fn builder(&self) -> Result<Arc<dyn Backend>> {
        if let Some(name) = &self.registry.builder {
            return self.backend(name).with_context(|| format!("registry builder {name:?} is not a backend"));
        }
        self.backends
            .iter()
            .find(|b| {
                b.config().kind == crate::config::BackendKind::Local
                    && self.runtime(b.name()).reachable
                    && b.runtimes().iter().any(|r| r == "docker")
            })
            .cloned()
            .context("no reachable backend with Docker to build images on")
    }

    pub fn build_log_path(&self, tag: &str) -> std::path::PathBuf {
        let key = tag.rsplit(':').next().unwrap_or(tag);
        self.paths.builds_dir().join(format!("{key}.log"))
    }

    async fn build(&self, tag: &str, build: &BuildSpec, snapshot: &str) -> Result<()> {
        let builder = self.builder()?;
        if let Ok(Some(mut row)) = self.store.image(tag) {
            row.builder = Some(builder.name().to_string());
            self.store.put_image(&row)?;
        }
        let dir = builder.unpack_snapshot(snapshot, &self.snapshots.path(snapshot)).await?;
        let (shell, _) = builder.shell_for(None).await?;
        self.registry_login(builder.as_ref()).await?;
        tokio::fs::create_dir_all(self.paths.builds_dir()).await?;
        let log_path = self.build_log_path(tag);
        let t = shell_quote(tag);
        let exists = shell
            .run(&format!("docker buildx imagetools inspect {t} >/dev/null 2>&1 && echo exists || true"))
            .await?
            .text();
        if exists.trim() == "exists" {
            tokio::fs::write(&log_path, format!("{tag} is already in the registry; build skipped\n")).await?;
            return Ok(());
        }
        let platforms = self.registry.platforms.join(",");
        let script = format!(
            "cd {dir} || exit 1\n\
             docker buildx inspect compute >/dev/null 2>&1 || docker buildx create --name compute --driver docker-container >/dev/null 2>&1\n\
             docker buildx build --builder compute --progress plain --platform {platforms} -f {df} -t {t} --push {ctx} 2>&1\n\
             echo \"__compute_exit $?\"",
            dir = shell_quote(&dir),
            platforms = shell_quote(&platforms),
            df = shell_quote(&build.dockerfile),
            ctx = shell_quote(&build.context),
        );
        let mut out = shell.pipe_out(&script).await?;
        let mut file = tokio::fs::File::create(&log_path).await?;
        file.write_all(format!("building {tag} on {} for {platforms}\n", builder.name()).as_bytes()).await?;
        tokio::io::copy(&mut out, &mut file).await?;
        file.flush().await?;
        let log = tokio::fs::read_to_string(&log_path).await.unwrap_or_default();
        let code = log
            .lines()
            .rev()
            .find_map(|l| l.strip_prefix("__compute_exit "))
            .and_then(|c| c.trim().parse::<i32>().ok());
        match code {
            Some(0) => Ok(()),
            Some(c) => bail!("docker buildx exited with {c}: {}", crate::backend::tail(&log, 800)),
            None => bail!("build output ended early: {}", crate::backend::tail(&log, 800)),
        }
    }

    /// Logs `backend`'s Docker into the registry once per daemon run (for pushes and private
    /// pulls). The password goes over stdin, never on a command line.
    pub async fn registry_login(&self, backend: &dyn Backend) -> Result<()> {
        let (Some(secret), Some(host)) = (&self.registry.secret, self.registry.host()) else {
            return Ok(());
        };
        if self.logged_in(backend.name()) {
            return Ok(());
        }
        let (Some(user), Some(pass)) = (self.secrets.value(secret, "REGISTRY_USERNAME")?, self.secrets.value(secret, "REGISTRY_PASSWORD")?) else {
            tracing::warn!("secret {secret} lacks REGISTRY_USERNAME/REGISTRY_PASSWORD; skipping docker login on {}", backend.name());
            return Ok(());
        };
        let (shell, _) = backend.shell_for(None).await?;
        shell
            .run_input(
                &format!("IFS= read -r u\nIFS= read -r p\nprintf '%s' \"$p\" | docker login {} -u \"$u\" --password-stdin >/dev/null", shell_quote(&host)),
                Some(format!("{user}\n{pass}\n").into_bytes()),
            )
            .await
            .context("docker login")?;
        self.set_logged_in(backend.name());
        Ok(())
    }

    /// `.registry.env` for Apptainer pulls of a private image from the configured registry.
    pub fn registry_env(&self, image: &str) -> Result<Option<String>> {
        let (Some(secret), Some(host)) = (&self.registry.secret, self.registry.host()) else {
            return Ok(None);
        };
        if !image.starts_with(&format!("{host}/")) {
            return Ok(None);
        }
        let user = self.secrets.value(secret, "REGISTRY_USERNAME")?.unwrap_or_default();
        let pass = self.secrets.value(secret, "REGISTRY_PASSWORD")?.unwrap_or_default();
        Ok(Some(format!(
            "APPTAINER_DOCKER_USERNAME={}\nAPPTAINER_DOCKER_PASSWORD={}\n",
            shell_quote(&user),
            shell_quote(&pass)
        )))
    }
}

/// SHA-256 over the Dockerfile and every file under the context, read from the snapshot.
fn build_hash(snapshot: &std::path::Path, dockerfile: &str, context: &str) -> Result<String, ApiError> {
    let file = std::fs::File::open(snapshot).map_err(|e| anyhow::anyhow!("opening snapshot: {e}"))?;
    let rt = tokio::runtime::Handle::current();
    let reader = tokio_util::io::SyncIoBridge::new_with_handle(
        ZstdDecoder::new(BufReader::new(tokio::fs::File::from_std(file))),
        rt,
    );
    let mut archive = tar::Archive::new(reader);
    let ctx = context.trim_end_matches('/').trim_start_matches("./");
    let in_ctx = |p: &str| ctx == "." || ctx.is_empty() || p == ctx || p.starts_with(&format!("{ctx}/"));
    let mut files: BTreeMap<String, [u8; 32]> = BTreeMap::new();
    let mut found_dockerfile = false;
    for entry in archive.entries().map_err(anyhow::Error::from)? {
        let mut entry = entry.map_err(anyhow::Error::from)?;
        if !entry.header().entry_type().is_file() {
            continue;
        }
        let path = entry.path().map_err(anyhow::Error::from)?.to_string_lossy().trim_start_matches("./").to_string();
        let is_df = path == dockerfile.trim_start_matches("./");
        if !is_df && !in_ctx(&path) {
            continue;
        }
        found_dockerfile |= is_df;
        let mut h = Sha256::new();
        let mut buf = [0u8; 65536];
        loop {
            let n = entry.read(&mut buf).map_err(anyhow::Error::from)?;
            if n == 0 {
                break;
            }
            h.update(&buf[..n]);
        }
        files.insert(path, h.finalize().into());
    }
    if !found_dockerfile {
        return Err(ApiError::bad_request(format!("{dockerfile} is not in the snapshot"), "give the Dockerfile's path relative to the repo root"));
    }
    if files.len() > 20_000 {
        return Err(ApiError::bad_request(
            format!("build context {context:?} has {} files", files.len()),
            "point `container.context` at a small directory (the Dockerfile's own, by default)",
        ));
    }
    let mut h = Sha256::new();
    h.update(format!("dockerfile {dockerfile}\ncontext {ctx}\n"));
    for (p, d) in &files {
        h.update(p.as_bytes());
        h.update([0]);
        h.update(d);
    }
    Ok(hex::encode(h.finalize()))
}
