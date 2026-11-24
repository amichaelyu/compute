//! The `Backend` trait and the remote-host plumbing shared by every backend.
//!
//! Layout under a backend's root:
//!   bin/compute-run            the runner
//!   snapshots/<sha256 hex>/    unpacked code (shared by jobs)
//!   runs/<job id>/             job.env, command, datasets, inputs, log, status.json, heartbeat, cancel
//!   runs/<job id>/out/         $JOB_OUTPUT_DIR
//!   runs/<job id>/in/          $JOB_INPUT_DIR (other jobs' outputs)
//!   runs/<job id>/data/        $JOB_DATA_DIR (links to dataset copies)
//!   data/<name>/<version>/     dataset copies
//!   cache/                     uv / HF / Apptainer caches where the backend sets them

pub mod cloud;
pub mod local;
pub mod slurm;

use crate::config::{BackendConfig, Transport};
use crate::model::{Job, JobState};
use crate::transport::{Reader, Shell, Ssh, Tunnels, shell_quote};
use anyhow::{Context, Result, bail};
use async_compression::tokio::bufread::ZstdDecoder;
use async_trait::async_trait;
use chrono::{DateTime, TimeZone, Utc};
use serde::Serialize;
use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::{Arc, RwLock};
use tokio::io::BufReader;
use tokio::sync::Mutex;

pub const RUNNER: &str = include_str!("../../../../runner/compute-run");

/// The resources a job is launched with on a backend (Slurm may round CPUs up; cloud backends
/// pick an offer).
#[derive(Clone, Debug, Serialize, PartialEq, Default)]
pub struct Shape {
    pub cpus: u32,
    pub mem_mb: u64,
    pub gpus: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub partition: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub qos: Option<String>,
    /// Slurm `--dependency`, when a child is submitted while its parents are still queued.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dependency: Option<String>,
    #[serde(skip_serializing_if = "is_one")]
    pub nodes: u32,
    /// Cloud: the provider offer chosen (GPU type, instance type, price).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub offer: Option<serde_json::Value>,
    /// Cloud: price per hour for the whole job (all nodes).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub usd_per_hour: Option<f64>,
}

fn is_one(n: &u32) -> bool {
    *n <= 1
}

impl Shape {
    pub fn simple(cpus: u32, mem_mb: u64, gpus: u32) -> Shape {
        Shape { cpus, mem_mb, gpus, nodes: 1, ..Default::default() }
    }
}

#[derive(Clone, Debug)]
pub enum Quote {
    Ok { start: DateTime<Utc>, source: String, reason: String, shape: Shape, cost: Option<f64> },
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
    /// Free disk at the backend's root when last measured.
    pub disk_free_mb: Option<u64>,
    /// Memory Docker containers can use in total (a VM's limit on macOS).
    pub docker_mem_mb: Option<u64>,
}

/// Resources held by an active job on a local backend.
#[derive(Clone, Debug)]
pub struct Alloc {
    pub ends_at: DateTime<Utc>,
    pub cpus: u32,
    pub mem_mb: u64,
    /// Disk the job said it needs (resources.disk_gb).
    pub disk_mb: u64,
    /// Runs in a Docker container (counts against the Docker VM's memory).
    pub container: bool,
}

/// Extra files and settings for staging a job, worked out by the scheduler.
#[derive(Clone, Debug, Default)]
pub struct StageFiles {
    /// `.secrets.env` contents (KEY='value' lines).
    pub secrets_env: Option<String>,
    /// `(name, path on the backend)` for each dataset.
    pub datasets: Vec<(String, String)>,
    /// `(source dir on the backend, glob, as)` for inputs whose parent ran on the same backend;
    /// the runner copies them when the job starts.
    pub inputs: Vec<(String, String, String)>,
    /// `docker` or `apptainer` for container jobs.
    pub runtime: Option<String>,
    /// Extra exports for job.env (multi-node rendezvous, caches).
    pub env: Vec<(String, String)>,
    /// A sandboxed job's own env (`user.env`), which only the container reads: the runner
    /// never sources it, so it can't set PATH, LD_PRELOAD and the like for host commands.
    pub user_env: Option<String>,
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
    /// Runtimes: `native`, `docker`, `apptainer` (configured or detected by the probe).
    fn runtimes(&self) -> Vec<String>;
    /// Paid backends need `max_cost_usd` and count against the budget.
    fn is_paid(&self) -> bool {
        false
    }
    async fn probe(&self) -> Result<Probe>;
    async fn quote(&self, job: &Job, active: &[Alloc]) -> Quote;
    /// Puts the snapshot, runner and job files on the backend.
    async fn stage(&self, job: &Job, snapshot: &Path, files: &StageFiles) -> Result<()>;
    /// Starts staged jobs (several only for a Slurm array batch) and returns one handle per job.
    async fn launch(&self, jobs: &[Job], shape: &Shape) -> Result<Vec<serde_json::Value>>;
    async fn poll(&self, jobs: &[Job]) -> Result<Vec<PollResult>>;
    async fn logs(&self, job: &Job, offset: u64, max: u64) -> Result<(Vec<u8>, u64)>;
    /// A tar stream of the job's outputs (optionally only files matching `glob`).
    async fn fetch(&self, job: &Job, glob: Option<&str>) -> Result<Reader>;
    async fn cancel(&self, job: &Job) -> Result<()>;
    async fn availability(&self) -> Result<serde_json::Value>;
    /// A shell and root for the backend's host (`None`) or for a job's own instance (cloud).
    async fn shell_for(&self, job: Option<&Job>) -> Result<(Arc<dyn Shell>, String)>;
    /// Unpacks a snapshot (no `uv sync`) and returns its directory; used for image builds.
    async fn unpack_snapshot(&self, _snapshot: &str, _path: &Path) -> Result<String> {
        bail!("{} can't build images", self.name())
    }
    /// Unpacks a tar stream into `runs/<id>/<sub>` for a staged job.
    async fn receive(&self, job: &Job, sub: &str, data: Reader) -> Result<()> {
        let (shell, root) = self.shell_for(Some(job)).await?;
        let dir = shell_quote(&format!("{}/{sub}", Remote::jobdir(&root, job.id)));
        shell.pipe_in(&format!("set -e\nmkdir -p {dir}\ntar -x --exclude='._*' -C {dir}"), data).await
    }
    /// A batch scheduler's queue of the user's jobs (Slurm), including ones submitted outside
    /// compute; None for other backends.
    async fn queue(&self) -> Result<Option<serde_json::Value>> {
        Ok(None)
    }
    /// Resource samples written by the runner (`{"latest", "history"}`).
    async fn metrics(&self, job: &Job, history: u32) -> Result<serde_json::Value> {
        let (shell, root) = self.shell_for(None).await?;
        let d = shell_quote(&Remote::jobdir(&root, job.id));
        let script = format!("cat {d}/metrics.json 2>/dev/null || echo null\necho ---\ntail -n {history} {d}/metrics.jsonl 2>/dev/null || true");
        Ok(parse_metrics(&shell.run(&script).await?.text()))
    }
    /// Cloud backends: tear down instances with no live job behind them. `live` maps each live
    /// job to its stored handle.
    async fn sweep(&self, _live: &HashMap<i64, Option<serde_json::Value>>) -> Result<()> {
        Ok(())
    }
    /// Cloud backends: prices per GPU type, for `GET /v1/backends/{name}`.
    fn pricing(&self) -> Option<serde_json::Value> {
        None
    }
    /// Re-measures what changes between probes (a local host's free disk and Docker memory).
    async fn refresh(&self) -> Result<()> {
        Ok(())
    }
    /// The job's output files under `out/` (matching `glob`) with their sizes in bytes.
    async fn list_outputs(&self, job: &Job, glob: Option<&str>) -> Result<Vec<(String, u64)>> {
        let (shell, root) = self.shell_for(Some(job)).await?;
        let out = shell.run(&list_dir_script(&format!("{}/out", Remote::jobdir(&root, job.id)), glob)).await?.text();
        Ok(parse_listing(&out))
    }
    /// Deletes a finished job's linked inputs and dataset view (they're only for the run).
    async fn delete_inputs(&self, job: &Job) -> Result<()> {
        let (shell, root) = self.shell_for(Some(job)).await?;
        let d = shell_quote(&Remote::jobdir(&root, job.id));
        shell.run(&format!("rm -rf {d}/in {d}/data")).await.map(|_| ())
    }
    /// Deletes a finished job's outputs and copied inputs (keeping its log, status and
    /// metrics), to free disk.
    async fn delete_outputs(&self, job: &Job) -> Result<()> {
        let (shell, root) = self.shell_for(Some(job)).await?;
        let d = shell_quote(&Remote::jobdir(&root, job.id));
        shell.run(&format!("rm -rf {d}/out {d}/in {d}/data")).await.map(|_| ())
    }
}

/// Lists the files under `dir` (optionally matching `glob`, as in `tar_dir_script`) as
/// `size<TAB>./path` lines; works with GNU and BSD stat.
pub fn list_dir_script(dir: &str, glob: Option<&str>) -> String {
    let d = shell_quote(dir);
    let filter = glob.map(|g| format!(" -path {}", shell_quote(&format!("./{g}")))).unwrap_or_default();
    // `%s<TAB>%n` (GNU) or `%z<TAB>%N` (BSD), with a real tab, quoted so it stays one word.
    format!(
        "cd {d} 2>/dev/null || exit 0\n\
         t=$(printf '\\t')\n\
         if stat -c %s . >/dev/null 2>&1; then\n\
           find . -type f{filter} -print0 | xargs -0 stat -c \"%s$t%n\" 2>/dev/null | head -n 100000\n\
         else\n\
           find . -type f{filter} -print0 | xargs -0 stat -f \"%z$t%N\" 2>/dev/null | head -n 100000\n\
         fi"
    )
}

/// Parses `size<TAB>./path` lines.
pub fn parse_listing(out: &str) -> Vec<(String, u64)> {
    out.lines()
        .filter_map(|l| {
            let (size, path) = l.split_once('\t')?;
            Some((path.trim_start_matches("./").to_string(), size.trim().parse().ok()?))
        })
        .collect()
}

/// How a backend's host is reached: the configured host, or a fixed shell (a cloud instance).
enum Conn {
    Host,
    Fixed(Arc<dyn Shell>),
}

/// How a backend's host is reached, plus what has already been put there.
pub struct Remote {
    pub cfg: BackendConfig,
    tunnels: Arc<Tunnels>,
    conn: Conn,
    root: RwLock<Option<String>>,
    detected_runtimes: RwLock<Vec<String>>,
    rex_user: Mutex<Option<String>>,
    staged: Mutex<HashSet<String>>,
    snapshot_locks: Mutex<HashMap<String, Arc<Mutex<()>>>>,
}

impl Remote {
    pub fn new(cfg: BackendConfig, tunnels: Arc<Tunnels>) -> Remote {
        Remote {
            cfg,
            tunnels,
            conn: Conn::Host,
            root: RwLock::new(None),
            detected_runtimes: RwLock::new(Vec::new()),
            rex_user: Mutex::new(None),
            staged: Mutex::new(HashSet::new()),
            snapshot_locks: Mutex::new(HashMap::new()),
        }
    }

    /// A remote for one cloud instance, reached through `shell`, with a known root.
    pub fn with_shell(cfg: BackendConfig, shell: Arc<dyn Shell>, root: String) -> Remote {
        Remote {
            cfg,
            tunnels: Arc::new(Tunnels::default()),
            conn: Conn::Fixed(shell),
            root: RwLock::new(Some(root)),
            detected_runtimes: RwLock::new(Vec::new()),
            rex_user: Mutex::new(None),
            staged: Mutex::new(HashSet::new()),
            snapshot_locks: Mutex::new(HashMap::new()),
        }
    }

    /// The shell for this remote: ssh to the configured host, or the fixed instance shell.
    pub async fn shell(&self) -> Result<Arc<dyn Shell>> {
        match &self.conn {
            Conn::Host => Ok(Arc::new(self.ssh().await?)),
            Conn::Fixed(s) => Ok(s.clone()),
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
                    known_hosts: None,
                })
            }
            Transport::Ssh => Ok(Ssh {
                host: self.cfg.ssh_host.clone().context("transport ssh without ssh_host")?,
                user: self.cfg.ssh_user.clone(),
                port: self.cfg.ssh_port,
                tunnel: false,
                alias: self.cfg.name.clone(),
                known_hosts: None,
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
        let ssh = self.shell().await?;
        let root_expr = match self.root() {
            Some(r) if matches!(self.conn, Conn::Fixed(_)) => r,
            _ => self.cfg.root(),
        };
        let script = format!(
            "set -e\nROOT=\"{root_expr}\"\nmkdir -p \"$ROOT/bin\" \"$ROOT/snapshots\" \"$ROOT/runs\" \"$ROOT/data\" \"$ROOT/cache\"\n\
             cat > \"$ROOT/bin/compute-run.tmp\"\nchmod 755 \"$ROOT/bin/compute-run.tmp\"\n\
             mv -f \"$ROOT/bin/compute-run.tmp\" \"$ROOT/bin/compute-run\"\nprintf 'ROOT=%s\\n' \"$ROOT\"\n\
             printf 'RUNTIMES=native'\nif command -v docker >/dev/null 2>&1 && docker info >/dev/null 2>&1; then printf ',docker'; fi\n\
             if command -v apptainer >/dev/null 2>&1 || {{ type module >/dev/null 2>&1 && module load apptainer >/dev/null 2>&1 && command -v apptainer >/dev/null 2>&1; }}; then printf ',apptainer'; fi\necho\n{extra}"
        );
        let out = ssh.run_input(&script, Some(RUNNER.as_bytes().to_vec())).await?.text();
        let (first, rest) = out.split_once('\n').unwrap_or((out.as_str(), ""));
        let root = first.strip_prefix("ROOT=").context("probe did not print ROOT")?.trim().to_string();
        let (rt_line, rest) = rest.split_once('\n').unwrap_or((rest, ""));
        if let Some(list) = rt_line.strip_prefix("RUNTIMES=") {
            *self.detected_runtimes.write().unwrap_or_else(|e| e.into_inner()) =
                list.split(',').map(|r| r.trim().to_string()).filter(|r| !r.is_empty()).collect();
        }
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

    /// Runtimes from the config, or detected by the last probe.
    pub fn runtimes(&self) -> Vec<String> {
        if let Some(list) = &self.cfg.runtimes {
            return list.clone();
        }
        let detected = self.detected_runtimes.read().unwrap_or_else(|e| e.into_inner()).clone();
        if detected.is_empty() { vec!["native".into()] } else { detected }
    }

    /// Unpacks a snapshot on the host once. For native jobs (`sync`), also runs `uv sync` if
    /// it's a uv project; container jobs bring their own environment.
    pub async fn stage_snapshot(&self, snapshot: &str, path: &Path, env_prelude: &str, sync: bool) -> Result<()> {
        let want = if sync { format!("{snapshot}:ready") } else { format!("{snapshot}:unpacked") };
        if self.staged.lock().await.contains(&want) {
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
        if self.staged.lock().await.contains(&want) {
            return Ok(());
        }
        let root = self.root_or_err()?;
        let dir = shell_quote(&Self::snapshot_dir(&root, snapshot));
        let shell = self.shell().await?;
        let probe = shell
            .run(&format!(
                "if [ -f {dir}/.compute-ready ]; then echo ready; elif [ -f {dir}/.compute-unpacked ]; then echo unpacked; else echo none; fi\n\
                 command -v zstd >/dev/null 2>&1 && echo zstd || echo nozstd"
            ))
            .await?
            .text();
        let state = probe.lines().next().unwrap_or("none").trim().to_string();
        let state = state.as_str();
        if state == "none" {
            // Send the compressed snapshot when the host can decompress it: several times fewer
            // bytes over slow links (Rex tunnels).
            let zstd = probe.lines().any(|l| l.trim() == "zstd");
            let unpack = if zstd { "zstd -dcq | tar -x --exclude='._*' -C \"$t\"" } else { "tar -x --exclude='._*' -C \"$t\"" };
            let script = format!(
                "set -e\nset -o pipefail\nd={dir}\nt=\"$d.tmp.$$\"\nrm -rf \"$t\" \"$d\"\nmkdir -p \"$t\"\n\
                 {unpack}\ntouch \"$t/.compute-unpacked\"\nmv \"$t\" \"$d\""
            );
            let file = tokio::fs::File::open(path)
                .await
                .with_context(|| format!("opening snapshot {}", path.display()))?;
            let data: Reader = if zstd { Box::new(file) } else { Box::new(ZstdDecoder::new(BufReader::new(file))) };
            shell
                .pipe_in(&script, data)
                .await
                .with_context(|| format!("unpacking snapshot on {}", self.cfg.name))?;
        }
        self.staged.lock().await.insert(format!("{snapshot}:unpacked"));
        if sync && state != "ready" {
            let script = format!(
                "set -e\n{env_prelude}\ncd {dir}\n\
                 if [ -f pyproject.toml ] && command -v uv >/dev/null 2>&1; then\n  if [ -f uv.lock ]; then uv sync --frozen; else uv sync; fi\nfi\n\
                 touch .compute-ready"
            );
            shell
                .run(&script)
                .await
                .map_err(|e| anyhow::anyhow!("uv sync on {} failed: {}", self.cfg.name, tail(&format!("{e:#}"), 2000)))?;
        }
        if sync {
            self.staged.lock().await.insert(want);
        }
        Ok(())
    }

    /// Stages a job: its snapshot (synced for native jobs) and its job directory.
    pub async fn stage_job(&self, job: &Job, snapshot: &Path, files: &StageFiles, env_prelude: &str, timeout_s: u64) -> Result<()> {
        let root = self.root_or_err()?;
        self.stage_snapshot(&job.snapshot, snapshot, env_prelude, !job.is_container()).await?;
        let env = job_env(&root, job, timeout_s, files);
        if job.spec.sandbox {
            let mut files = files.clone();
            files.user_env = Some(user_env(job));
            return self.write_job_files(job.id, &env, &job.command, &files).await;
        }
        self.write_job_files(job.id, &env, &job.command, files).await
    }

    /// Writes job.env, command, datasets, inputs and .secrets.env (0600) into a fresh job
    /// directory.
    pub async fn write_job_files(&self, id: i64, job_env: &str, command: &str, files: &StageFiles) -> Result<()> {
        let root = self.root_or_err()?;
        let dir = shell_quote(&Self::jobdir(&root, id));
        let data = job_files_tar(job_env, command, files)?;
        let script = format!(
            "set -e\numask 077\nd={dir}\nrm -rf \"$d\"\n\
             mkdir -p \"$d/out\" \"$d/in\" \"$d/data\"\ntar -x -C \"$d\""
        );
        self.shell().await?.run_input(&script, Some(data)).await?;
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
        let out = self.shell().await?.run(&script).await?.text();
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
        // BSD head (macOS) refuses `-c 0`.
        let max = max.max(1);
        let root = self.root_or_err()?;
        let f = shell_quote(&format!("{}/log", Self::jobdir(&root, id)));
        let script = format!(
            "f={f}\nif [ ! -f \"$f\" ]; then echo 0; exit 0; fi\nwc -c < \"$f\" | tr -d ' '\n\
             tail -c +{} \"$f\" | head -c {max}",
            offset + 1
        );
        let out = self.shell().await?.run(&script).await?.stdout;
        let nl = out.iter().position(|b| *b == b'\n').unwrap_or(out.len());
        let size: u64 = String::from_utf8_lossy(&out[..nl]).trim().parse().unwrap_or(0);
        let data = if nl < out.len() { out[nl + 1..].to_vec() } else { Vec::new() };
        Ok((data, size))
    }

    /// `glob` has already been validated to contain only safe characters.
    pub async fn fetch(&self, id: i64, glob: Option<&str>) -> Result<Reader> {
        let root = self.root_or_err()?;
        let out = format!("{}/out", Self::jobdir(&root, id));
        self.shell().await?.pipe_out(&tar_dir_script(&out, glob)).await
    }

    /// The latest resource sample and the last `history` samples of a job.
    pub async fn metrics(&self, id: i64, history: u32) -> Result<serde_json::Value> {
        let root = self.root_or_err()?;
        let d = shell_quote(&Self::jobdir(&root, id));
        let script = format!("cat {d}/metrics.json 2>/dev/null || echo null\necho ---\ntail -n {history} {d}/metrics.jsonl 2>/dev/null || true");
        let out = self.shell().await?.run(&script).await?.text();
        Ok(parse_metrics(&out))
    }

    pub async fn touch_cancel(&self, id: i64) -> Result<()> {
        let root = self.root_or_err()?;
        let d = shell_quote(&Self::jobdir(&root, id));
        self.shell().await?.run(&format!("mkdir -p {d} && touch {d}/cancel")).await?;
        Ok(())
    }
}

/// Parses `latest\n---\nhistory lines` into `{"latest": ..., "history": [...]}`.
pub fn parse_metrics(out: &str) -> serde_json::Value {
    let (latest, hist) = out.split_once("---").unwrap_or((out, ""));
    let latest: serde_json::Value = serde_json::from_str(latest.trim()).unwrap_or(serde_json::Value::Null);
    let history: Vec<serde_json::Value> = hist.lines().filter_map(|l| serde_json::from_str(l.trim()).ok()).collect();
    serde_json::json!({"latest": latest, "history": history})
}

/// A script that writes a tar of `dir` (or the files in it matching `glob`) to stdout.
/// `COPYFILE_DISABLE` keeps macOS tar from adding `._*` metadata files.
pub fn tar_dir_script(dir: &str, glob: Option<&str>) -> String {
    let d = shell_quote(dir);
    match glob {
        None => format!("export COPYFILE_DISABLE=1; cd {d} && tar -c ."),
        Some(g) => format!(
            "export COPYFILE_DISABLE=1; cd {d} && find . -type f -path {} -print0 | tar -c --null -T -",
            shell_quote(&format!("./{g}"))
        ),
    }
}

/// The job directory's files as a tar archive.
pub fn job_files_tar(job_env: &str, command: &str, files: &StageFiles) -> Result<Vec<u8>> {
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
    if !files.datasets.is_empty() {
        let lines: String = files.datasets.iter().map(|(n, p)| format!("{n}\t{p}\n")).collect();
        add("datasets", lines.as_bytes(), 0o644)?;
    }
    if !files.inputs.is_empty() {
        let lines: String = files.inputs.iter().map(|(src, g, a)| format!("{src}\t{g}\t{a}\n")).collect();
        add("inputs", lines.as_bytes(), 0o644)?;
    }
    if let Some(s) = &files.secrets_env {
        add(".secrets.env", s.as_bytes(), 0o600)?;
    }
    if let Some(u) = &files.user_env {
        add("user.env", u.as_bytes(), 0o600)?;
    }
    Ok(builder.into_inner()?)
}

/// Exports for job.env: the runner's variables plus the job's own env.
pub fn job_env(root: &str, job: &Job, timeout_s: u64, files: &StageFiles) -> String {
    let jobdir = Remote::jobdir(root, job.id);
    let mut lines = vec![
        format!("export JOB_ID={}", job.id),
        format!("export JOB_ROOT={}", shell_quote(root)),
        format!("export JOB_OUTPUT_DIR={}", shell_quote(&format!("{jobdir}/out"))),
        format!("export JOB_INPUT_DIR={}", shell_quote(&format!("{jobdir}/in"))),
        format!("export JOB_DATA_DIR={}", shell_quote(&format!("{jobdir}/data"))),
        format!("export JOB_SNAPSHOT_DIR={}", shell_quote(&Remote::snapshot_dir(root, &job.snapshot))),
        format!("export JOB_TIMEOUT_S={timeout_s}"),
        format!("export ARRAY_INDEX={}", job.array_index.map(|i| i.to_string()).unwrap_or_default()),
        format!("export JOB_CPUS={}", job.resources.cpus),
        format!("export JOB_MEM_MB={}", job.resources.mem_mb),
        format!("export JOB_DISK_MB={}", job.resources.disk_mb),
        format!("export JOB_GPUS={}", job.resources.gpus),
    ];
    if let (Some(c), Some(rt)) = (&job.spec.container, &files.runtime) {
        lines.push(format!("export JOB_RUNTIME={}", shell_quote(rt)));
        lines.push(format!("export JOB_IMAGE={}", shell_quote(&c.image)));
    }
    if job.spec.sandbox {
        lines.push("export JOB_SANDBOX=1".into());
    }
    let pairs = files.env.iter().map(|(k, v)| (k.as_str(), v.as_str()));
    // A sandboxed job's own env goes to user.env instead (see StageFiles::user_env).
    let user = job.env.iter().filter(|_| !job.spec.sandbox).map(|(k, v)| (k.as_str(), v.as_str()));
    for (k, v) in pairs.chain(user) {
        lines.push(format!("export {k}={}", shell_quote(v)));
    }
    lines.push(String::new());
    lines.join("\n")
}

/// `export KEY='value'` lines of the job's own env.
pub fn user_env(job: &Job) -> String {
    let mut lines: Vec<String> = job.env.iter().map(|(k, v)| format!("export {k}={}", shell_quote(v))).collect();
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
        // The runner's diagnosis of a failure (out of memory, out of disk).
        note: status["note"].as_str().filter(|n| !n.is_empty()).map(str::to_string),
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
pub async fn write_remote_file(shell: &dyn Shell, path: &str, data: &str) -> Result<()> {
    let q = shell_quote(path);
    shell
        .run_input(&format!("cat > {q}"), Some(data.as_bytes().to_vec()))
        .await
        .with_context(|| format!("writing {path}"))?;
    Ok(())
}
