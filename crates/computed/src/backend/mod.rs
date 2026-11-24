//! The `Backend` trait and the remote-host plumbing shared by the local and Slurm backends.
//!
//! Layout under a backend's root:
//!   bin/compute-run            the runner
//!   snapshots/<sha256 hex>/    unpacked code (shared by jobs)
//!   runs/<job id>/             job.env, command, log, status.json, heartbeat, cancel
//!   runs/<job id>/out/         $JOB_OUTPUT_DIR
//!   data/                      $JOB_DATA_DIR
//!   cache/                     uv / HF caches where the backend sets them

pub mod local;
pub mod slurm;

use crate::config::{BackendConfig, Transport};
use crate::model::{Job, JobState};
use crate::transport::{Ssh, Tunnels, shell_quote};
use anyhow::{Context, Result, bail};
use async_compression::tokio::bufread::ZstdDecoder;
use async_trait::async_trait;
use chrono::{DateTime, TimeZone, Utc};
use serde::Serialize;
use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::process::Stdio;
use std::sync::{Arc, RwLock};
use tokio::io::{AsyncWriteExt, BufReader};
use tokio::sync::Mutex;

pub const RUNNER: &str = include_str!("../../../../runner/compute-run");

/// The resources a job is launched with on a backend (Slurm may round CPUs up).
#[derive(Clone, Debug, Serialize, PartialEq)]
pub struct Shape {
    pub cpus: u32,
    pub mem_mb: u64,
    pub gpus: u32,
    pub partition: Option<String>,
    pub qos: Option<String>,
}

#[derive(Clone, Debug)]
pub enum Quote {
    Ok { start: DateTime<Utc>, source: String, reason: String, shape: Shape },
    /// `permanent` means this backend can never run the job as asked (not just "busy" or "down").
    No { reason: String, permanent: bool },
}

impl Quote {
    pub fn no(reason: impl Into<String>, permanent: bool) -> Quote {
        Quote::No { reason: reason.into(), permanent }
    }
}

pub struct Probe {
    pub root: String,
    pub hardware: serde_json::Value,
}

#[derive(Debug, Default)]
pub struct PollResult {
    pub id: i64,
    pub state: Option<JobState>,
    pub exit_code: Option<i32>,
    pub heartbeat: Option<DateTime<Utc>>,
    pub started: Option<DateTime<Utc>>,
    pub ended: Option<DateTime<Utc>>,
    pub est_start: Option<DateTime<Utc>>,
    pub note: Option<String>,
}

/// What a local backend offers the scheduler, resolved from config and the probe.
#[derive(Clone, Debug, Serialize)]
pub struct Capacity {
    pub cpus: u32,
    pub mem_gb: u32,
    pub os: String,
    pub arch: String,
}

/// Resources held by an active job on a local backend.
#[derive(Clone, Debug)]
pub struct Alloc {
    pub ends_at: DateTime<Utc>,
    pub cpus: u32,
    pub mem_mb: u64,
}

#[async_trait]
pub trait Backend: Send + Sync {
    fn name(&self) -> &str;
    fn config(&self) -> &BackendConfig;
    /// Root directory on the backend, known after a successful probe.
    fn root(&self) -> Option<String>;
    /// Schedulable CPUs/memory and OS of a local backend (None for Slurm or before a probe).
    fn capacity(&self) -> Option<Capacity> {
        None
    }
    async fn probe(&self) -> Result<Probe>;
    async fn quote(&self, job: &Job, active: &[Alloc]) -> Quote;
    /// Puts the snapshot, runner and job files on the backend.
    async fn stage(&self, job: &Job, snapshot: &Path, secrets_env: Option<String>) -> Result<()>;
    /// Starts staged jobs (several only for a Slurm array batch) and returns one handle per job.
    async fn launch(&self, jobs: &[Job], shape: &Shape) -> Result<Vec<serde_json::Value>>;
    async fn poll(&self, jobs: &[Job]) -> Result<Vec<PollResult>>;
    async fn logs(&self, job: &Job, offset: u64, max: u64) -> Result<(Vec<u8>, u64)>;
    /// Starts a remote `tar -c` of the job's outputs; the caller streams its stdout.
    async fn fetch(&self, job: &Job, glob: Option<&str>) -> Result<tokio::process::Child>;
    async fn cancel(&self, job: &Job) -> Result<()>;
    async fn availability(&self) -> Result<serde_json::Value>;
}

/// How a backend's host is reached, plus what has already been put there.
pub struct Remote {
    pub cfg: BackendConfig,
    tunnels: Arc<Tunnels>,
    root: RwLock<Option<String>>,
    rex_user: Mutex<Option<String>>,
    staged: Mutex<HashSet<String>>,
    snapshot_locks: Mutex<HashMap<String, Arc<Mutex<()>>>>,
}

impl Remote {
    pub fn new(cfg: BackendConfig, tunnels: Arc<Tunnels>) -> Remote {
        Remote {
            cfg,
            tunnels,
            root: RwLock::new(None),
            rex_user: Mutex::new(None),
            staged: Mutex::new(HashSet::new()),
            snapshot_locks: Mutex::new(HashMap::new()),
        }
    }

    /// An ssh handle for the host: through a `rex forward` tunnel (transport `rex`) or direct.
    pub async fn ssh(&self) -> Result<Ssh> {
        match self.cfg.transport() {
            Transport::Rex => {
                let label = self.cfg.rex_label.as_deref().context("transport rex without rex_label")?;
                let port = self.cfg.tunnel_port.context("transport rex without a tunnel port")?;
                self.tunnels.ensure(label, port).await?;
                let user = match &self.cfg.ssh_user {
                    Some(u) => u.clone(),
                    None => {
                        let mut cached = self.rex_user.lock().await;
                        if cached.is_none() {
                            *cached = Some(crate::transport::rex_whoami(label).await?);
                        }
                        cached.clone().unwrap_or_default()
                    }
                };
                Ok(Ssh {
                    host: "127.0.0.1".into(),
                    user: Some(user),
                    port: Some(port),
                    tunnel: true,
                    alias: self.cfg.name.clone(),
                })
            }
            Transport::Ssh => Ok(Ssh {
                host: self.cfg.ssh_host.clone().context("transport ssh without ssh_host")?,
                user: self.cfg.ssh_user.clone(),
                port: self.cfg.ssh_port,
                tunnel: false,
                alias: self.cfg.name.clone(),
            }),
        }
    }

    pub fn root(&self) -> Option<String> {
        self.root.read().unwrap_or_else(|e| e.into_inner()).clone()
    }

    pub fn root_or_err(&self) -> Result<String> {
        self.root().with_context(|| format!("backend {} has not been probed yet", self.cfg.name))
    }

    /// Expands the root, creates the layout, installs the runner and runs `extra` (hardware
    /// detection). Returns the root and `extra`'s stdout.
    pub async fn probe_root(&self, extra: &str) -> Result<(String, String)> {
        let ssh = self.ssh().await?;
        let script = format!(
            "set -e\nROOT=\"{}\"\nmkdir -p \"$ROOT/bin\" \"$ROOT/snapshots\" \"$ROOT/runs\" \"$ROOT/data\" \"$ROOT/cache\"\n\
             cat > \"$ROOT/bin/compute-run.tmp\"\nchmod 755 \"$ROOT/bin/compute-run.tmp\"\n\
             mv -f \"$ROOT/bin/compute-run.tmp\" \"$ROOT/bin/compute-run\"\nprintf 'ROOT=%s\\n' \"$ROOT\"\n{extra}",
            self.cfg.root()
        );
        let out = ssh.run_input(&script, Some(RUNNER.as_bytes().to_vec())).await?.text();
        let (first, rest) = out.split_once('\n').unwrap_or((out.as_str(), ""));
        let root = first.strip_prefix("ROOT=").context("probe did not print ROOT")?.trim().to_string();
        if !root.starts_with('/') {
            bail!("root {root:?} on {} is not absolute", self.cfg.name);
        }
        *self.root.write().unwrap_or_else(|e| e.into_inner()) = Some(root.clone());
        Ok((root, rest.to_string()))
    }

    pub fn jobdir(root: &str, id: i64) -> String {
        format!("{root}/runs/{id}")
    }

    pub fn snapshot_dir(root: &str, snapshot: &str) -> String {
        format!("{root}/snapshots/{}", snapshot.trim_start_matches("sha256:"))
    }

    /// Unpacks a snapshot on the host once, then runs `uv sync` if it's a uv project.
    pub async fn stage_snapshot(&self, snapshot: &str, path: &Path, env_prelude: &str) -> Result<()> {
        if self.staged.lock().await.contains(snapshot) {
            return Ok(());
        }
        let lock = self
            .snapshot_locks
            .lock()
            .await
            .entry(snapshot.to_string())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone();
        let _guard = lock.lock().await;
        if self.staged.lock().await.contains(snapshot) {
            return Ok(());
        }
        let root = self.root_or_err()?;
        let dir = shell_quote(&Self::snapshot_dir(&root, snapshot));
        let ssh = self.ssh().await?;
        let present = ssh
            .run(&format!("test -f {dir}/.compute-ready && echo yes || echo no"))
            .await?
            .text();
        if present.trim() != "yes" {
            let script = format!(
                "set -e\n{env_prelude}\nd={dir}\nt=\"$d.tmp.$$\"\nrm -rf \"$t\" \"$d\"\nmkdir -p \"$t\"\n\
                 tar -x -C \"$t\"\nmv \"$t\" \"$d\"\ncd \"$d\"\n\
                 if [ -f pyproject.toml ]; then\n  if [ -f uv.lock ]; then uv sync --frozen; else uv sync; fi\nfi\n\
                 touch .compute-ready"
            );
            let mut child = ssh.spawn(&script, Stdio::piped(), Stdio::null())?;
            let mut stdin = child.stdin.take().context("ssh stdin")?;
            let file = tokio::fs::File::open(path)
                .await
                .with_context(|| format!("opening snapshot {}", path.display()))?;
            let mut decoder = ZstdDecoder::new(BufReader::new(file));
            let copy = tokio::io::copy(&mut decoder, &mut stdin).await;
            drop(stdin);
            let out = child.wait_with_output().await?;
            copy.context("streaming snapshot")?;
            if !out.status.success() {
                bail!(
                    "unpacking snapshot on {} failed: {}",
                    self.cfg.name,
                    tail(&String::from_utf8_lossy(&out.stderr), 2000)
                );
            }
        }
        self.staged.lock().await.insert(snapshot.to_string());
        Ok(())
    }

    /// Writes job.env, command and .secrets.env (0600) into a fresh job directory.
    pub async fn write_job_files(&self, id: i64, job_env: &str, command: &str, secrets_env: Option<&str>) -> Result<()> {
        let root = self.root_or_err()?;
        let dir = shell_quote(&Self::jobdir(&root, id));
        let mut builder = tar::Builder::new(Vec::new());
        let mut add = |name: &str, data: &[u8], mode: u32| -> Result<()> {
            let mut h = tar::Header::new_gnu();
            h.set_size(data.len() as u64);
            h.set_mode(mode);
            h.set_mtime(Utc::now().timestamp() as u64);
            h.set_cksum();
            builder.append_data(&mut h, name, data)?;
            Ok(())
        };
        add("job.env", job_env.as_bytes(), 0o644)?;
        add("command", command.as_bytes(), 0o644)?;
        if let Some(s) = secrets_env {
            add(".secrets.env", s.as_bytes(), 0o600)?;
        }
        let data = builder.into_inner()?;
        let script = format!(
            "set -e\numask 077\nd={dir}\nrm -rf \"$d\"\n\
             mkdir -p \"$d/out\"\ntar -x -C \"$d\""
        );
        self.ssh().await?.run_input(&script, Some(data)).await?;
        Ok(())
    }

    /// Reads `status.json` and the heartbeat time of each job directory in one round trip.
    pub async fn read_statuses(&self, ids: &[i64]) -> Result<HashMap<i64, (serde_json::Value, Option<DateTime<Utc>>)>> {
        if ids.is_empty() {
            return Ok(HashMap::new());
        }
        let root = self.root_or_err()?;
        let mut script = format!("cd {} || exit 0\n", shell_quote(&format!("{root}/runs")));
        script.push_str(&format!(
            "for d in {}; do\n  printf '%s\\t' \"$d\"\n  if [ -f \"$d/status.json\" ]; then tr -d '\\n' < \"$d/status.json\"; else printf '{{}}'; fi\n  printf '\\t'\n  \
             stat -c %Y \"$d/heartbeat\" 2>/dev/null || stat -f %m \"$d/heartbeat\" 2>/dev/null || echo 0\ndone\n",
            ids.iter().map(|i| i.to_string()).collect::<Vec<_>>().join(" ")
        ));
        let out = self.ssh().await?.run(&script).await?.text();
        let mut map = HashMap::new();
        for line in out.lines() {
            let mut parts = line.splitn(3, '\t');
            let (Some(id), Some(status), Some(beat)) = (parts.next(), parts.next(), parts.next()) else {
                continue;
            };
            let Ok(id) = id.parse::<i64>() else { continue };
            let status = serde_json::from_str(status).unwrap_or(serde_json::Value::Null);
            let beat = beat.trim().parse::<i64>().ok().filter(|t| *t > 0).and_then(|t| Utc.timestamp_opt(t, 0).single());
            map.insert(id, (status, beat));
        }
        Ok(map)
    }

    pub async fn logs(&self, id: i64, offset: u64, max: u64) -> Result<(Vec<u8>, u64)> {
        let root = self.root_or_err()?;
        let f = shell_quote(&format!("{}/log", Self::jobdir(&root, id)));
        let script = format!(
            "f={f}\nif [ ! -f \"$f\" ]; then echo 0; exit 0; fi\nwc -c < \"$f\" | tr -d ' '\n\
             tail -c +{} \"$f\" | head -c {max}",
            offset + 1
        );
        let out = self.ssh().await?.run(&script).await?.stdout;
        let nl = out.iter().position(|b| *b == b'\n').unwrap_or(out.len());
        let size: u64 = String::from_utf8_lossy(&out[..nl]).trim().parse().unwrap_or(0);
        let data = if nl < out.len() { out[nl + 1..].to_vec() } else { Vec::new() };
        Ok((data, size))
    }

    /// `glob` has already been validated to contain only safe characters.
    pub async fn fetch(&self, id: i64, glob: Option<&str>) -> Result<tokio::process::Child> {
        let root = self.root_or_err()?;
        let out = shell_quote(&format!("{}/out", Self::jobdir(&root, id)));
        let script = match glob {
            None => format!("cd {out} && tar -c ."),
            Some(g) => format!(
                "cd {out} && find . -type f -path {} -print0 | tar -c --null -T -",
                shell_quote(&format!("./{g}"))
            ),
        };
        self.ssh().await?.spawn(&script, Stdio::null(), Stdio::piped())
    }

    pub async fn touch_cancel(&self, id: i64) -> Result<()> {
        let root = self.root_or_err()?;
        let d = shell_quote(&Self::jobdir(&root, id));
        self.ssh().await?.run(&format!("mkdir -p {d} && touch {d}/cancel")).await?;
        Ok(())
    }
}

/// Exports for job.env: the runner's variables plus the job's own env.
pub fn job_env(root: &str, job: &Job, timeout_s: u64, extra: &[(String, String)]) -> String {
    let jobdir = Remote::jobdir(root, job.id);
    let mut lines = vec![
        format!("export JOB_ID={}", job.id),
        format!("export JOB_OUTPUT_DIR={}", shell_quote(&format!("{jobdir}/out"))),
        format!("export JOB_DATA_DIR={}", shell_quote(&format!("{root}/data"))),
        format!("export JOB_SNAPSHOT_DIR={}", shell_quote(&Remote::snapshot_dir(root, &job.snapshot))),
        format!("export JOB_TIMEOUT_S={timeout_s}"),
        format!("export ARRAY_INDEX={}", job.array_index.map(|i| i.to_string()).unwrap_or_default()),
    ];
    let pairs = extra.iter().map(|(k, v)| (k.as_str(), v.as_str()));
    let user = job.env.iter().map(|(k, v)| (k.as_str(), v.as_str()));
    for (k, v) in pairs.chain(user) {
        lines.push(format!("export {k}={}", shell_quote(v)));
    }
    lines.push(String::new());
    lines.join("\n")
}

/// Turns a runner `status.json` into a poll result.
pub fn parse_status(id: i64, status: &serde_json::Value, beat: Option<DateTime<Utc>>) -> PollResult {
    let ts = |k: &str| status[k].as_i64().and_then(|t| Utc.timestamp_opt(t, 0).single());
    PollResult {
        id,
        state: status["state"].as_str().and_then(JobState::parse),
        exit_code: status["exit_code"].as_i64().map(|c| c as i32),
        heartbeat: beat,
        started: ts("started"),
        ended: ts("ended"),
        est_start: None,
        note: None,
    }
}

pub fn tail(s: &str, max: usize) -> String {
    let s = s.trim();
    if s.len() <= max {
        return s.to_string();
    }
    let mut start = s.len() - max;
    while !s.is_char_boundary(start) {
        start += 1;
    }
    format!("…{}", &s[start..])
}

/// Writes a small file to a host (used for sbatch scripts and task lists).
pub async fn write_remote_file(ssh: &Ssh, path: &str, data: &str) -> Result<()> {
    let q = shell_quote(path);
    let mut child = ssh.spawn(&format!("cat > {q}"), Stdio::piped(), Stdio::null())?;
    let mut stdin = child.stdin.take().context("ssh stdin")?;
    stdin.write_all(data.as_bytes()).await?;
    drop(stdin);
    let out = child.wait_with_output().await?;
    if !out.status.success() {
        bail!("writing {path} failed: {}", String::from_utf8_lossy(&out.stderr).trim());
    }
    Ok(())
}
