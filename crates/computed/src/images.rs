//! Container images. A job runs in a registry image, or in an image built from a
//! Dockerfile in its snapshot. Builds run on the builder backend with
//! `docker buildx build --platform ... --push`, are tagged `<prefix>:<hash>` where the hash
//! covers the Dockerfile and every file in the build context, and are skipped when the registry
//! already has that tag. Code still comes from the snapshot at run time, so editing code outside
//! the build context doesn't trigger a rebuild.
//!
//! Registry images are public: only the builder logs in (to push), every backend pulls
//! anonymously, and jobs get credentials at run time from secrets. After a build the daemon
//! checks that the tag pulls without a login, and fails the build with how to fix it if not
//! (GHCR creates new packages private, and has no API to change that).

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
    /// Turns a request's `container` into the stored spec, working out the build tag. A guest's
    /// build is always local (built on the host that runs the job), never on the registry
    /// builder: placement keeps those jobs to cloud instances of their own.
    pub async fn container_spec(&self, snapshot: &str, req: &ContainerReq, guest: bool) -> Result<ContainerSpec, ApiError> {
        if let Some(image) = &req.image {
            return Ok(ContainerSpec { image: image.clone(), build: None });
        }
        let dockerfile = req.build.clone().unwrap_or_default();
        let context = req.context.clone().unwrap_or_else(|| match dockerfile.rsplit_once('/') {
            Some((dir, _)) => dir.to_string(),
            None => ".".to_string(),
        });
        let prefix = self.registry.prefix().filter(|_| !guest);
        let path = self.snapshots.path(snapshot);
        let (df, ctx) = (dockerfile.clone(), context.clone());
        let hash = tokio::task::spawn_blocking(move || build_hash(&path, &df, &ctx))
            .await
            .map_err(anyhow::Error::from)??;
        Ok(match prefix {
            Some(prefix) => ContainerSpec { image: format!("{prefix}:{}", &hash[..16]), build: Some(BuildSpec { dockerfile, context, local: false }) },
            // No registry: build where the job runs, under a tag no other build can reuse.
            None => ContainerSpec { image: format!("compute-local:{}", &hash[..16]), build: Some(BuildSpec { dockerfile, context, local: true }) },
        })
    }

    /// For a job with a build: ready, building (started now if needed), or failed.
    pub fn image_status(self: &Arc<Self>, job: &Job) -> Result<ImageStatus> {
        let Some(ContainerSpec { image, build: Some(build) }) = &job.spec.container else {
            return Ok(ImageStatus::Ready);
        };
        if build.local {
            return Ok(ImageStatus::Ready); // built while staging, on the job's host
        }
        match self.store.image(image)? {
            Some(row) if row.state == "ready" => return Ok(ImageStatus::Ready),
            // A failed build fails the jobs of its snapshot. "Not public" is different: it is
            // fixed outside compute, so it fails the jobs submitted before the check and a job
            // submitted after it checks again.
            Some(row)
                if row.state == "failed"
                    && if row.error.as_deref().unwrap_or("").starts_with(NOT_PUBLIC) {
                        row.finished_at.is_some_and(|f| job.created_at <= f)
                    } else {
                        row.snapshot == job.snapshot
                    } =>
            {
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

    /// Builds a job's local image on its Docker host (once per tag; output goes to the job's
    /// log). Called while staging, after the snapshot is unpacked.
    pub async fn local_build(&self, backend: &Arc<dyn Backend>, job: &Job) -> Result<()> {
        let Some(ContainerSpec { image, build: Some(b) }) = &job.spec.container else { return Ok(()) };
        if !b.local {
            return Ok(());
        }
        // A shared host builds each tag once; every cloud job has an instance of its own.
        let lock = if backend.is_paid() {
            self.dataset_lock(&format!("build:{}:{}", backend.name(), job.id))
        } else {
            self.dataset_lock(&format!("build:{}:{image}", backend.name()))
        };
        let _guard = lock.lock().await;
        let (shell, root) = backend.shell_for(Some(job)).await?;
        let snap = shell_quote(&crate::backend::Remote::snapshot_dir(&root, &job.snapshot));
        let log = shell_quote(&format!("{}/log", crate::backend::Remote::jobdir(&root, job.id)));
        let t = shell_quote(image);
        let script = format!(
            "cd {snap} || exit 1\n\
             if docker image inspect {t} >/dev/null 2>&1; then echo \"compute: image {image} is already built here\" >> {log}; echo '__compute_exit 0'; exit 0; fi\n\
             echo \"compute: building {image} from {df} (context {ctx})\" >> {log}\n\
             docker build --progress plain -f {dfq} -t {t} {ctxq} >> {log} 2>&1\n\
             echo \"__compute_exit $?\"",
            df = b.dockerfile,
            ctx = b.context,
            dfq = shell_quote(&b.dockerfile),
            ctxq = shell_quote(&b.context),
        );
        let mut out = shell.pipe_out(&script).await?;
        let mut text = String::new();
        tokio::io::AsyncReadExt::read_to_string(&mut out, &mut text).await?;
        match text.lines().rev().find_map(|l| l.strip_prefix("__compute_exit ")).and_then(|c| c.trim().parse::<i32>().ok()) {
            Some(0) => Ok(()),
            Some(c) => bail!("build_failed: docker build exited with {c}; the build output is in the job's log"),
            None => bail!("build_failed: the build stopped early; see the job's log"),
        }
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
            return require_public(tag).await;
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
            Some(0) => require_public(tag).await,
            Some(c) => bail!("docker buildx exited with {c}: {}", crate::backend::tail(&log, 800)),
            None => bail!("build output ended early: {}", crate::backend::tail(&log, 800)),
        }
    }

    /// Logs the builder's Docker into the registry once per daemon run, to push. The password
    /// goes over stdin, never on a command line.
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
}

/// Error prefix for a pushed image that can't be pulled anonymously. It fails the jobs waiting
/// on the build; a job submitted later checks again, so making the package public and
/// resubmitting is enough.
pub const NOT_PUBLIC: &str = "not_public:";

/// Fails if `image` (`host/repo:tag`) can't be pulled without credentials. When the registry
/// can't be asked (network trouble, 5xx), it lets the build through with a warning rather than
/// blame the package.
async fn require_public(image: &str) -> Result<()> {
    match anonymous_pull_ok(image).await {
        Ok(Some(true)) => return Ok(()),
        Ok(Some(false)) => {}
        Ok(None) => {
            tracing::warn!("couldn't check that {image} is public (the registry didn't answer clearly); assuming it is");
            return Ok(());
        }
        Err(e) => {
            tracing::warn!("couldn't check that {image} is public: {e:#}; assuming it is");
            return Ok(());
        }
    }
    let (host, path) = image.split_once('/').unwrap_or(("", image));
    let repo = path.rsplit_once(':').map_or(path, |(r, _)| r);
    let fix = match (host, repo.split_once('/')) {
        ("ghcr.io", Some((owner, name))) => format!(
            "open https://github.com/users/{owner}/packages/container/{}/settings (for an organization: \
             https://github.com/orgs/{owner}/packages/container/{}/settings), choose Change visibility, \
             then Public. This is needed once per package; then resubmit",
            name.replace('/', "%2F"),
            name.replace('/', "%2F"),
        ),
        _ => format!("make the repository {repo} on {host} public, then resubmit"),
    };
    bail!("{NOT_PUBLIC} {image} was pushed, but it can't be pulled without a login, and backends pull anonymously: {fix}")
}

/// Whether the registry serves the manifest of `image` to an anonymous client, following the
/// registry's bearer-token challenge: Some(true) yes, Some(false) refused, None unclear (a
/// server error, rate limit or unexpected answer).
async fn anonymous_pull_ok(image: &str) -> Result<Option<bool>> {
    let (host, path) = image.split_once('/').context("image has no registry host")?;
    let (repo, tag) = path.rsplit_once(':').context("image has no tag")?;
    // Docker Hub names (docker.io/x) are served by registry-1.docker.io, with "library/" for
    // official images.
    let (api_host, repo) = match host {
        "docker.io" | "index.docker.io" | "registry-1.docker.io" => {
            ("registry-1.docker.io", if repo.contains('/') { repo.to_string() } else { format!("library/{repo}") })
        }
        h => (h, repo.to_string()),
    };
    let url = format!("https://{api_host}/v2/{repo}/manifests/{tag}");
    let accept = "Accept: application/vnd.oci.image.index.v1+json, application/vnd.docker.distribution.manifest.list.v2+json, \
                  application/vnd.oci.image.manifest.v1+json, application/vnd.docker.distribution.manifest.v2+json";
    let curl = |args: Vec<String>| async move {
        let out = tokio::time::timeout(std::time::Duration::from_secs(40), tokio::process::Command::new("curl").args(&args).output())
            .await
            .context("registry check timed out")??;
        Ok::<String, anyhow::Error>(String::from_utf8_lossy(&out.stdout).into_owned())
    };
    let head = |auth: Option<String>| {
        let mut a: Vec<String> = ["-sS", "--max-time", "30", "-o", "/dev/null", "-D", "-", "-I", "-H", accept].iter().map(|s| s.to_string()).collect();
        if let Some(t) = auth {
            a.extend(["-H".into(), format!("Authorization: Bearer {t}")]);
        }
        a.push(url.clone());
        curl(a)
    };
    let status = |headers: &str| headers.lines().rfind(|l| l.starts_with("HTTP/")).and_then(|l| l.split_whitespace().nth(1)).and_then(|c| c.parse::<u16>().ok());
    let refused = |code: Option<u16>| match code {
        Some(200) => Some(true),
        Some(401 | 403 | 404) => Some(false),
        _ => None,
    };
    let first = head(None).await?;
    match status(&first) {
        Some(401) => {}
        code => return Ok(refused(code)),
    }
    // WWW-Authenticate: Bearer realm="https://ghcr.io/token",service="ghcr.io",scope="repository:x:pull"
    let Some(challenge) = first.lines().find_map(|l| {
        let (k, v) = l.split_once(':')?;
        k.trim().eq_ignore_ascii_case("www-authenticate").then(|| v.trim().to_string())
    }) else {
        return Ok(None);
    };
    let param = |name: &str| {
        challenge.split(',').find_map(|kv| {
            let (k, v) = kv.trim().trim_start_matches("Bearer ").split_once('=')?;
            (k.trim() == name).then(|| v.trim().trim_matches('"').to_string())
        })
    };
    let Some(realm) = param("realm") else { return Ok(None) };
    let service = param("service").unwrap_or_else(|| api_host.to_string());
    let scope = format!("repository:{repo}:pull");
    let out = curl(vec![
        "-sS".into(), "--max-time".into(), "30".into(), "-w".into(), "\n%{http_code}".into(), "-G".into(), realm,
        "--data-urlencode".into(), format!("service={service}"),
        "--data-urlencode".into(), format!("scope={scope}"),
    ])
    .await?;
    let (body, code) = out.rsplit_once('\n').unwrap_or(("", out.as_str()));
    let token = serde_json::from_str::<serde_json::Value>(body)
        .ok()
        .and_then(|v| v["token"].as_str().or(v["access_token"].as_str()).map(str::to_string));
    let Some(token) = token else {
        // GHCR refuses an anonymous token for a private package (403 DENIED).
        return Ok(match code.trim().parse::<u16>().ok() {
            Some(401 | 403 | 404) => Some(false),
            _ => None,
        });
    };
    Ok(refused(status(&head(Some(token)).await?)))
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
