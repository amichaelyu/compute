//! Datasets: named, versioned data. A version never changes once registered. The daemon
//! tracks which backends hold a copy of which version, copies a dataset to a job's backend
//! before it starts, and evicts least-recently-used copies to stay under each backend's
//! `data_cap_gb`.
//!
//! Copies between hosts stream `tar -c | tar -x` through the coordinator without touching its
//! disk. Hugging Face and rclone sources are fetched on the target when it can reach them.

use crate::backend::{Backend, tar_dir_script};
use crate::error::ApiError;
use crate::model::{DatasetUse, JobState, valid_dir_name, valid_glob};
use crate::scheduler::Scheduler;
use crate::store::{CopyRow, DatasetRow};
use crate::transport::{LocalShell, Reader, Shell, shell_quote};
use anyhow::{Context, Result, bail};
use async_compression::tokio::bufread::ZstdDecoder;
use axum::http::StatusCode;
use chrono::Utc;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::sync::Arc;
use std::time::Instant;
use tokio::io::BufReader;

/// Transfer rate assumed for a backend before one has been measured.
const DEFAULT_RATE: f64 = 30.0 * 1024.0 * 1024.0;

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum Source {
    /// A directory on one of the backends.
    HostPath { backend: String, path: String },
    /// A finished job's outputs (all of them, or the files matching `glob`).
    JobOutputs { job: i64, #[serde(default)] glob: Option<String> },
    /// A tar.zst uploaded to the daemon (`PUT /v1/datasets/{name}/upload`).
    Upload { sha256: String },
    /// A Hugging Face repo at a revision; `secret` names a secret with HF_TOKEN for private repos.
    Hf {
        repo: String,
        #[serde(default = "main_rev")]
        revision: String,
        #[serde(default)]
        repo_type: Option<String>,
        #[serde(default)]
        secret: Option<String>,
        /// Filled in at registration: the commit the revision resolved to.
        #[serde(default)]
        commit: Option<String>,
    },
    /// An rclone remote path such as `gdrive:datasets/raw`, fetched on a backend that has the
    /// remote configured (`via`, or any backend with it).
    Rclone {
        remote: String,
        #[serde(default)]
        via: Option<String>,
    },
}

fn main_rev() -> String {
    "main".into()
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RegisterReq {
    pub name: String,
    #[serde(default)]
    pub description: String,
    pub source: Source,
}

/// A path on a host as written by a user: absolute or under `$HOME`/`$USER`, no shell syntax
/// beyond `$VAR`.
fn valid_host_path(p: &str) -> bool {
    (p.starts_with('/') || p.starts_with("$HOME") || p.starts_with("${HOME}"))
        && !p.contains("..")
        && p.chars().all(|c| c.is_ascii_alphanumeric() || "_./-${}".contains(c))
}

fn short(hash: &[u8]) -> String {
    hex::encode(hash)[..12].to_string()
}

/// Where a dataset version's data comes from when a backend needs a copy.
enum Origin {
    /// `tar -c` of a directory (filtered by glob) through a shell.
    Dir { shell: Arc<dyn Shell>, path: String, glob: Option<String>, backend: Option<String> },
    /// The uploaded archive on the coordinator.
    Upload(std::path::PathBuf),
    Hf { repo: String, repo_type: Option<String>, commit: String, secret: Option<String> },
    Rclone { remote: String },
}

impl Scheduler {
    fn backend_or_err(&self, name: &str) -> Result<Arc<dyn Backend>, ApiError> {
        self.backend(name).ok_or_else(|| ApiError::bad_request(format!("unknown backend {name:?}"), "see GET /v1/backends"))
    }

    /// Registers a dataset version. Returns the row and whether it was new.
    pub async fn register_dataset(&self, req: RegisterReq) -> Result<(DatasetRow, bool), ApiError> {
        if !valid_dir_name(&req.name) {
            return Err(ApiError::bad_request("dataset name is invalid", "use letters, digits, _ . and -, up to 64 characters"));
        }
        if req.description.chars().count() > 500 {
            return Err(ApiError::bad_request("`description` is longer than 500 characters", ""));
        }
        let (version, size, files, source) = match req.source {
            Source::HostPath { backend, path } => {
                if !valid_host_path(&path) {
                    return Err(ApiError::bad_request(
                        format!("path {path:?} isn't allowed"),
                        "give an absolute path (or one under $HOME); letters, digits, _ . / - and $VAR only",
                    ));
                }
                let b = self.backend_or_err(&backend)?;
                let (shell, _) = b.shell_for(None).await.map_err(|e| unreachable(&backend, e))?;
                let real = shell
                    .run(&format!("cd \"{path}\" && pwd -P"))
                    .await
                    .map_err(|e| ApiError::bad_request(format!("{path} is not a directory on {backend}: {e:#}"), "check the path"))?
                    .text()
                    .trim()
                    .to_string();
                let (hash, size, files) = listing(shell.as_ref(), &real, None).await?;
                let version = short(&hash);
                // The source directory itself serves as that backend's copy (never deleted).
                self.store.put_copy(&CopyRow {
                    name: req.name.clone(),
                    version: version.clone(),
                    backend: backend.clone(),
                    path: real.clone(),
                    size,
                    owned: false,
                    created_at: Utc::now(),
                    last_used: Utc::now(),
                })?;
                (version, size, files, Source::HostPath { backend, path: real })
            }
            Source::JobOutputs { job, glob } => {
                if glob.as_deref().is_some_and(|g| !valid_glob(g)) {
                    return Err(ApiError::bad_request("glob has characters that aren't allowed", "use letters, digits and _ . * ? / - [ ] only"));
                }
                let j = self.store.get_job(job)?.ok_or_else(|| ApiError::not_found(format!("job {job} not found")))?;
                if j.state != JobState::Succeeded {
                    return Err(ApiError::bad_request(format!("job {job} is {}", j.state.as_str()), "register outputs of a job that succeeded"));
                }
                let (shell, path, _) = self.outputs_location(&j).await?;
                let (_, size, files) = listing(shell.as_ref(), &path, glob.as_deref()).await?;
                let version = short(&Sha256::digest(format!("job {job} glob {}", glob.clone().unwrap_or_default())));
                (version, size, files, Source::JobOutputs { job, glob })
            }
            Source::Upload { .. } => {
                return Err(ApiError::bad_request("upload sources are created with PUT /v1/datasets/{name}/upload", "send the tar.zst there"));
            }
            Source::Hf { repo, revision, repo_type, secret, .. } => {
                let ok = |s: &str| !s.is_empty() && s.chars().all(|c| c.is_ascii_alphanumeric() || "_.-/".contains(c));
                if !ok(&repo) || !ok(&revision) || repo_type.as_deref().is_some_and(|t| !["model", "dataset", "space"].contains(&t)) {
                    return Err(ApiError::bad_request("bad Hugging Face repo, revision or repo_type", "e.g. {\"repo\": \"org/name\", \"repo_type\": \"dataset\"}"));
                }
                if let Some(s) = &secret {
                    self.secrets.check(std::slice::from_ref(s))?;
                }
                let token = match &secret {
                    Some(s) => self.secrets.value(s, "HF_TOKEN")?,
                    None => None,
                };
                let (commit, size, files) = hf_resolve(&repo, repo_type.as_deref(), &revision, token.as_deref()).await?;
                let version = commit[..12.min(commit.len())].to_string();
                (version, size, files, Source::Hf { repo, revision, repo_type, secret, commit: Some(commit) })
            }
            Source::Rclone { remote, via } => {
                if remote.is_empty()
                    || !remote.contains(':')
                    || !remote.chars().all(|c| c.is_ascii_alphanumeric() || "_.-/: ".contains(c))
                {
                    return Err(ApiError::bad_request("bad rclone remote", "e.g. gdrive:datasets/raw"));
                }
                let via = match via {
                    Some(v) => v,
                    None => self.rclone_host(&remote).await?,
                };
                let b = self.backend_or_err(&via)?;
                let (shell, _) = b.shell_for(None).await.map_err(|e| unreachable(&via, e))?;
                let r = shell_quote(&remote);
                let out = shell
                    .run(&format!(
                        "set -o pipefail\nrclone lsf -R --files-only --format pst {r} | LC_ALL=C sort | {{ sha256sum 2>/dev/null || shasum -a 256; }} | cut -c1-64\n\
                         rclone size --json {r}"
                    ))
                    .await
                    .map_err(|e| ApiError::bad_request(format!("rclone on {via} failed: {e:#}"), "check the remote name and that it's configured there"))?
                    .text();
                let mut lines = out.lines();
                let hash = lines.next().unwrap_or_default().trim().to_string();
                let sizes: serde_json::Value = serde_json::from_str(lines.next().unwrap_or("{}")).unwrap_or_default();
                (hash[..12.min(hash.len())].to_string(), sizes["bytes"].as_u64().unwrap_or(0), sizes["count"].as_u64().unwrap_or(0), Source::Rclone { remote, via: Some(via) })
            }
        };
        let row = DatasetRow {
            name: req.name,
            version,
            source: serde_json::to_value(&source).map_err(anyhow::Error::from)?,
            size,
            files,
            description: req.description,
            created_at: Utc::now(),
        };
        let new = self.store.insert_dataset(&row)?;
        Ok((row, new))
    }

    /// Stores an uploaded tar.zst as a new version of `name`.
    pub async fn upload_dataset(&self, name: &str, description: String, body: axum::body::Body) -> Result<(DatasetRow, bool), ApiError> {
        if !valid_dir_name(name) {
            return Err(ApiError::bad_request("dataset name is invalid", "use letters, digits, _ . and -"));
        }
        let (sha, size) = crate::snapshots::store_blob(&self.paths.datasets_dir(), body, 512 * 1024 * 1024 * 1024).await?;
        let row = DatasetRow {
            name: name.to_string(),
            version: sha[..12].to_string(),
            source: serde_json::to_value(Source::Upload { sha256: sha }).map_err(anyhow::Error::from)?,
            size,
            files: 0,
            description,
            created_at: Utc::now(),
        };
        let new = self.store.insert_dataset(&row)?;
        Ok((row, new))
    }

    /// Resolves `name` (latest) or `name@version-prefix`.
    pub fn resolve_dataset(&self, spec: &str) -> Result<DatasetUse, ApiError> {
        let (name, ver) = match spec.split_once('@') {
            Some((n, v)) => (n, Some(v)),
            None => (spec, None),
        };
        let versions = self.store.dataset_versions(Some(name))?;
        let found = match ver {
            None => versions.first(),
            Some(v) => versions.iter().find(|d| d.version.starts_with(v)),
        };
        let d = found.ok_or_else(|| {
            ApiError::new(
                StatusCode::BAD_REQUEST,
                "dataset_missing",
                format!("dataset {spec:?} is not registered"),
                "register it with POST /v1/datasets (see GET /v1/datasets)",
            )
        })?;
        Ok(DatasetUse { name: d.name.clone(), version: d.version.clone() })
    }

    /// Where a finished job's outputs are: its backend's run dir, or the coordinator for cloud
    /// jobs (copied off before teardown). Returns `(shell, path, backend)`.
    pub async fn outputs_location(&self, job: &crate::model::Job) -> Result<(Arc<dyn Shell>, String, Option<String>), ApiError> {
        let name = job.backend.clone().ok_or_else(|| ApiError::bad_request(format!("job {} never ran", job.id), ""))?;
        let b = self.backend_or_err(&name)?;
        if b.config().kind.is_cloud() {
            let dir = self.paths.outputs_dir().join(job.id.to_string()).join("out");
            return Ok((Arc::new(LocalShell), dir.to_string_lossy().into_owned(), None));
        }
        let (shell, root) = b.shell_for(None).await.map_err(|e| unreachable(&name, e))?;
        Ok((shell, format!("{}/out", crate::backend::Remote::jobdir(&root, job.id)), Some(name)))
    }

    async fn rclone_host(&self, remote: &str) -> Result<String, ApiError> {
        let rname = remote.split(':').next().unwrap_or_default();
        for b in &self.backends {
            if b.config().kind.is_cloud() || !self.runtime(b.name()).reachable {
                continue;
            }
            let Ok((shell, _)) = b.shell_for(None).await else { continue };
            if let Ok(out) = shell.run("rclone listremotes 2>/dev/null || true").await
                && out.text().lines().any(|l| l.trim().trim_end_matches(':') == rname)
            {
                return Ok(b.name().to_string());
            }
        }
        Err(ApiError::bad_request(
            format!("no backend has an rclone remote named {rname:?}"),
            "configure it on one backend (rclone config) or pass \"via\"",
        ))
    }

    /// Seconds to copy the job's missing datasets to `backend`, for placement.
    pub fn staging_seconds(&self, job: &crate::model::Job, backend: &str) -> f64 {
        let mut secs = 0.0;
        for d in &job.spec.datasets {
            let have = self
                .store
                .dataset_copies(Some(backend), Some(&d.name))
                .map(|c| c.iter().any(|c| c.version == d.version))
                .unwrap_or(false);
            if have {
                continue;
            }
            let size = self
                .store
                .dataset_versions(Some(&d.name))
                .ok()
                .and_then(|v| v.into_iter().find(|x| x.version == d.version))
                .map(|x| x.size)
                .unwrap_or(0);
            let rate = self.transfer_rate(backend);
            secs += size as f64 / rate;
        }
        secs
    }

    fn transfer_rate(&self, backend: &str) -> f64 {
        self.runtime(backend).transfer_rate.unwrap_or(DEFAULT_RATE)
    }

    /// Makes sure `backend` has a copy of the dataset version and returns its path there. For
    /// cloud backends the copy goes into the job's own instance and isn't tracked.
    pub async fn ensure_dataset(&self, d: &DatasetUse, backend: &Arc<dyn Backend>, job: &crate::model::Job) -> Result<String> {
        let cloud = backend.config().kind.is_cloud();
        let bname = backend.name().to_string();
        let lock = self.dataset_lock(&format!("{}@{}@{}", d.name, d.version, if cloud { format!("{bname}#{}", job.id) } else { bname.clone() }));
        let _guard = lock.lock().await;
        let row = self
            .store
            .dataset_versions(Some(&d.name))?
            .into_iter()
            .find(|x| x.version == d.version)
            .with_context(|| format!("dataset {}@{} is no longer registered", d.name, d.version))?;
        let (shell, root) = backend.shell_for(Some(job)).await?;
        if !cloud
            && let Some(copy) = self.store.dataset_copies(Some(&bname), Some(&d.name))?.into_iter().find(|c| c.version == d.version)
        {
            let p = shell_quote(&copy.path);
            let check = if copy.owned { format!("test -f {p}/.compute-ready && echo ok") } else { format!("test -d {p} && echo ok") };
            if shell.run(&format!("{check} || true")).await?.text().trim() == "ok" {
                self.store.touch_copy(&d.name, &d.version, &bname)?;
                return Ok(copy.path);
            }
            tracing::warn!("dataset {}@{} copy on {bname} is gone (purged?); restaging", d.name, d.version);
            self.store.delete_copy(&d.name, &d.version, &bname)?;
        }
        let dest = format!("{root}/data/{}/{}", d.name, d.version);
        if !cloud {
            self.make_room(backend, &bname, row.size).await?;
        }
        let started = Instant::now();
        let origin = self.origin(&row).await?;
        let q = shell_quote(&dest);
        let prep = format!("set -e\nd={q}\nchmod -R u+w \"$d\" 2>/dev/null || true\nrm -rf \"$d\" \"$d.tmp\"\nmkdir -p \"$d.tmp\"\n");
        let finish = "\ntouch \"$d.tmp/.compute-ready\"\nmv \"$d.tmp\" \"$d\"\nchmod -R a-w \"$d\" 2>/dev/null || true\n";
        match origin {
            Origin::Dir { shell: src, path, glob, backend: src_backend } => {
                if !cloud && src_backend.as_deref() == Some(bname.as_str()) && glob.is_none() {
                    // The data is already on this backend; use it in place.
                    self.store.put_copy(&CopyRow {
                        name: d.name.clone(),
                        version: d.version.clone(),
                        backend: bname.clone(),
                        path: path.clone(),
                        size: row.size,
                        owned: false,
                        created_at: Utc::now(),
                        last_used: Utc::now(),
                    })?;
                    return Ok(path);
                }
                let data = src.pipe_out(&tar_dir_script(&path, glob.as_deref())).await?;
                shell.pipe_in(&format!("{prep}tar -x --exclude='._*' -C \"$d.tmp\"{finish}"), data).await?;
            }
            Origin::Upload(file) => {
                let f = tokio::fs::File::open(&file).await.with_context(|| format!("opening {}", file.display()))?;
                let data: Reader = Box::new(ZstdDecoder::new(BufReader::new(f)));
                shell.pipe_in(&format!("{prep}tar -x --exclude='._*' -C \"$d.tmp\"{finish}"), data).await?;
            }
            Origin::Hf { repo, repo_type, commit, secret } => {
                let token = match &secret {
                    Some(s) => self.secrets.value(s, "HF_TOKEN")?.unwrap_or_default(),
                    None => String::new(),
                };
                let rt = repo_type.map(|t| format!(" --repo-type {t}")).unwrap_or_default();
                let script = format!(
                    "{prep}IFS= read -r HF_TOKEN || true\nif [ -n \"$HF_TOKEN\" ]; then export HF_TOKEN; else unset HF_TOKEN; fi\n\
                     export HF_HOME={hf}\nexport UV_CACHE_DIR={uvc}\n\
                     uvx --from huggingface_hub hf download {repo}{rt} --revision {commit} --local-dir \"$d.tmp\" >/dev/null\n\
                     rm -rf \"$d.tmp/.cache\"{finish}",
                    hf = shell_quote(&format!("{root}/cache/hf")),
                    uvc = shell_quote(&format!("{root}/cache/uv")),
                    repo = shell_quote(&repo),
                    commit = shell_quote(&commit),
                );
                shell.run_input(&script, Some(format!("{token}\n").into_bytes())).await?;
            }
            Origin::Rclone { remote } => {
                let rname = remote.split(':').next().unwrap_or_default().to_string();
                let has = shell.run("rclone listremotes 2>/dev/null || true").await?.text().lines().any(|l| l.trim().trim_end_matches(':') == rname);
                if has {
                    shell.run(&format!("{prep}rclone copy {} \"$d.tmp\"{finish}", shell_quote(&remote))).await?;
                } else {
                    // Fetch on the host that has the remote, then copy from there.
                    let via = row.source["rclone"]["via"].as_str().unwrap_or_default().to_string();
                    let vb = self.backend(&via).with_context(|| format!("rclone host {via} is gone"))?;
                    if vb.name() == bname {
                        bail!("{bname} lost its rclone remote {rname}");
                    }
                    let path = Box::pin(self.ensure_dataset(d, &vb, job)).await?;
                    let (src, _) = vb.shell_for(None).await?;
                    let data = src.pipe_out(&tar_dir_script(&path, None)).await?;
                    shell.pipe_in(&format!("{prep}tar -x --exclude='._*' -C \"$d.tmp\"{finish}"), data).await?;
                }
            }
        }
        let elapsed = started.elapsed().as_secs_f64().max(1.0);
        if row.size > 50 * 1024 * 1024 {
            let rate = row.size as f64 / elapsed;
            self.set_transfer_rate(&bname, rate);
        }
        if !cloud {
            let size = shell
                .run(&format!("du -sk {q} 2>/dev/null | cut -f1"))
                .await
                .ok()
                .and_then(|o| o.text().trim().parse::<u64>().ok())
                .map(|k| k * 1024)
                .unwrap_or(row.size);
            self.store.put_copy(&CopyRow {
                name: d.name.clone(),
                version: d.version.clone(),
                backend: bname.clone(),
                path: dest.clone(),
                size,
                owned: true,
                created_at: Utc::now(),
                last_used: Utc::now(),
            })?;
        }
        tracing::info!("dataset {}@{} staged to {bname} in {:.0} s", d.name, d.version, elapsed);
        Ok(dest)
    }

    async fn origin(&self, row: &DatasetRow) -> Result<Origin> {
        let source: Source = serde_json::from_value(row.source.clone()).context("dataset source")?;
        Ok(match source {
            Source::HostPath { backend, path } => {
                // Prefer the source; fall back to any copy (the source host may be down).
                match self.backend(&backend) {
                    Some(b) if self.runtime(&backend).reachable => {
                        let (shell, _) = b.shell_for(None).await?;
                        Origin::Dir { shell, path, glob: None, backend: Some(backend) }
                    }
                    _ => self.any_copy(row).await?,
                }
            }
            Source::JobOutputs { job, glob } => {
                let j = self.store.get_job(job)?.with_context(|| format!("job {job} is gone"))?;
                let (shell, path, backend) = self.outputs_location(&j).await.map_err(|e| anyhow::anyhow!(e.message))?;
                Origin::Dir { shell, path, glob, backend }
            }
            Source::Upload { sha256 } => Origin::Upload(crate::snapshots::blob_path(&self.paths.datasets_dir(), &sha256)),
            Source::Hf { repo, repo_type, commit, secret, .. } => Origin::Hf {
                repo,
                repo_type,
                commit: commit.context("dataset has no resolved commit")?,
                secret,
            },
            Source::Rclone { remote, .. } => Origin::Rclone { remote },
        })
    }

    async fn any_copy(&self, row: &DatasetRow) -> Result<Origin> {
        for c in self.store.dataset_copies(None, Some(&row.name))? {
            if c.version != row.version || !self.runtime(&c.backend).reachable {
                continue;
            }
            if let Some(b) = self.backend(&c.backend) {
                let (shell, _) = b.shell_for(None).await?;
                return Ok(Origin::Dir { shell, path: c.path, glob: None, backend: Some(c.backend) });
            }
        }
        bail!("no reachable copy of dataset {}@{}", row.name, row.version)
    }

    /// Deletes least-recently-used copies on `backend` that no unfinished job needs, until
    /// `needed` more bytes fit under its `data_cap_gb`.
    async fn make_room(&self, backend: &Arc<dyn Backend>, bname: &str, needed: u64) -> Result<()> {
        let Some(cap_gb) = backend.config().data_cap_gb else { return Ok(()) };
        let cap = cap_gb * 1024 * 1024 * 1024;
        if needed > cap {
            bail!("dataset is {} GB, more than {bname}'s data_cap_gb of {cap_gb}", needed / (1 << 30));
        }
        let copies: Vec<CopyRow> = self.store.dataset_copies(Some(bname), None)?.into_iter().filter(|c| c.owned).collect();
        let mut used: u64 = copies.iter().map(|c| c.size).sum();
        if used + needed <= cap {
            return Ok(());
        }
        let busy: std::collections::HashSet<(String, String)> = self
            .store
            .jobs_in_states(&[JobState::Waiting, JobState::Queued, JobState::Staging, JobState::Pending, JobState::Running])?
            .into_iter()
            .flat_map(|j| j.spec.datasets.into_iter().map(|d| (d.name, d.version)))
            .collect();
        let (shell, _) = backend.shell_for(None).await?;
        for c in copies {
            if used + needed <= cap {
                break;
            }
            if busy.contains(&(c.name.clone(), c.version.clone())) {
                continue;
            }
            let p = shell_quote(&c.path);
            shell.run(&format!("chmod -R u+w {p} 2>/dev/null || true; rm -rf {p}")).await?;
            self.store.delete_copy(&c.name, &c.version, bname)?;
            used = used.saturating_sub(c.size);
            tracing::info!("evicted dataset {}@{} from {bname} ({} MB)", c.name, c.version, c.size >> 20);
        }
        if used + needed > cap {
            bail!("{bname} has no room under data_cap_gb = {cap_gb}: the datasets there are in use by other jobs");
        }
        Ok(())
    }

    /// Deletes one backend's copy (API). The source directory of a host_path dataset is only
    /// forgotten, never deleted.
    pub async fn delete_dataset_copy(&self, name: &str, backend: &str) -> Result<usize, ApiError> {
        let b = self.backend_or_err(backend)?;
        let copies: Vec<CopyRow> = self.store.dataset_copies(Some(backend), Some(name))?;
        if copies.is_empty() {
            return Err(ApiError::not_found(format!("{backend} has no copy of {name}")));
        }
        let (shell, _) = b.shell_for(None).await.map_err(|e| unreachable(backend, e))?;
        for c in &copies {
            if c.owned {
                let p = shell_quote(&c.path);
                shell.run(&format!("chmod -R u+w {p} 2>/dev/null || true; rm -rf {p}")).await?;
            }
            self.store.delete_copy(&c.name, &c.version, backend)?;
        }
        Ok(copies.len())
    }
}

fn unreachable(name: &str, e: anyhow::Error) -> ApiError {
    ApiError::new(StatusCode::SERVICE_UNAVAILABLE, "backend_unreachable", format!("{name}: {e:#}"), "try again when it's back (GET /v1/backends)")
}

/// Hash, total size and file count of a directory's file list (names, sizes, mtimes).
async fn listing(shell: &dyn Shell, path: &str, glob: Option<&str>) -> Result<([u8; 32], u64, u64), ApiError> {
    let filter = match glob {
        Some(g) => format!("-path {}", shell_quote(&format!("./{g}"))),
        None => String::new(),
    };
    let script = format!(
        "set -o pipefail\ncd {} || exit 3\n\
         if find . -maxdepth 0 -printf '' >/dev/null 2>&1; then find . -type f {filter} -printf '%P|%s|%T@\\n'; \
         else find . -type f {filter} -exec stat -f '%N|%z|%m' {{}} +; fi | LC_ALL=C sort",
        shell_quote(path)
    );
    let out = shell
        .run(&script)
        .await
        .map_err(|e| ApiError::bad_request(format!("listing {path} failed: {e:#}"), "check the path"))?
        .stdout;
    let text = String::from_utf8_lossy(&out);
    let mut size = 0u64;
    let mut files = 0u64;
    for line in text.lines() {
        files += 1;
        size += line.split('|').nth(1).and_then(|s| s.parse::<u64>().ok()).unwrap_or(0);
    }
    if files == 0 {
        return Err(ApiError::bad_request(format!("{path} has no files{}", glob.map(|g| format!(" matching {g}")).unwrap_or_default()), ""));
    }
    let hash: [u8; 32] = Sha256::digest(&out).into();
    Ok((hash, size, files))
}

/// Resolves a Hugging Face revision to a commit, with total size and file count.
async fn hf_resolve(repo: &str, repo_type: Option<&str>, revision: &str, token: Option<&str>) -> Result<(String, u64, u64), ApiError> {
    let kind = match repo_type.unwrap_or("model") {
        "dataset" => "datasets",
        "space" => "spaces",
        _ => "models",
    };
    let url = format!("https://huggingface.co/api/{kind}/{repo}/revision/{revision}?blobs=true");
    let mut cmd = tokio::process::Command::new("curl");
    cmd.args(["-fsS", "--max-time", "60", "-H", "@-", &url])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped());
    let mut child = cmd.spawn().map_err(anyhow::Error::from)?;
    {
        use tokio::io::AsyncWriteExt;
        let mut stdin = child.stdin.take().context("curl stdin")?;
        let header = token.map(|t| format!("Authorization: Bearer {t}\n")).unwrap_or_else(|| "Accept: application/json\n".into());
        stdin.write_all(header.as_bytes()).await.map_err(anyhow::Error::from)?;
    }
    let out = child.wait_with_output().await.map_err(anyhow::Error::from)?;
    if !out.status.success() {
        return Err(ApiError::bad_request(
            format!("Hugging Face lookup of {repo}@{revision} failed: {}", String::from_utf8_lossy(&out.stderr).trim()),
            "check repo, repo_type and revision; private repos need `secret` naming a secret with HF_TOKEN",
        ));
    }
    let v: serde_json::Value = serde_json::from_slice(&out.stdout).map_err(anyhow::Error::from)?;
    let commit = v["sha"].as_str().context("no sha in the Hugging Face response")?.to_string();
    let siblings = v["siblings"].as_array().cloned().unwrap_or_default();
    let size = siblings.iter().filter_map(|s| s["size"].as_u64()).sum();
    Ok((commit, size, siblings.len() as u64))
}
