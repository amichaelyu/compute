//! Local hosts (any Mac or Linux machine): with transport `rex`, jobs start as Rex blocks
//! in the host's `compute` session and files go over ssh through a `rex forward` tunnel; with
//! transport `ssh`, everything goes over direct ssh and jobs start detached with nohup.

use super::{Alloc, Backend, Capacity, PollResult, Probe, Quote, Remote, Shape, StageFiles, parse_status};
use crate::config::{BackendConfig, Transport};
use crate::model::Job;
use crate::transport::{Reader, Shell, Tunnels, rex_run, shell_quote};
use anyhow::{Context, Result};
use async_trait::async_trait;
use chrono::Utc;
use serde_json::json;
use std::path::Path;
use std::sync::{Arc, RwLock};

pub struct LocalBackend {
    remote: Remote,
    capacity: RwLock<Option<Capacity>>,
}

impl LocalBackend {
    pub fn new(cfg: BackendConfig, tunnels: Arc<Tunnels>) -> LocalBackend {
        LocalBackend { remote: Remote::new(cfg, tunnels), capacity: RwLock::new(None) }
    }
    fn cfg(&self) -> &BackendConfig {
        &self.remote.cfg
    }
    /// The configured Docker memory, else what `docker info` reported (bytes).
    fn docker_mem(&self, detected: Option<i64>) -> Option<u64> {
        self.cfg().docker_mem_gb.map(|g| u64::from(g) * 1024).or(detected.filter(|b| *b > 0).map(|b| b as u64 / 1_048_576))
    }
}

const HARDWARE: &str = r#"
if [ "$(uname -s)" = Darwin ]; then
  printf 'os=macos\narch=%s\ncpus=%s\nmem_bytes=%s\ncpu_model=%s\n' "$(uname -m)" \
    "$(sysctl -n hw.ncpu)" "$(sysctl -n hw.memsize)" "$(sysctl -n machdep.cpu.brand_string)"
else
  printf 'os=linux\narch=%s\ncpus=%s\nmem_bytes=%s\ncpu_model=%s\n' "$(uname -m)" "$(nproc)" \
    "$(awk '/MemTotal/{print $2*1024}' /proc/meminfo)" \
    "$(grep -m1 'model name' /proc/cpuinfo | cut -d: -f2 | sed 's/^ *//')"
fi
printf 'disk_free_kb=%s\n' "$(df -k "$ROOT" | awk 'NR==2{print $4}')"
if command -v docker >/dev/null 2>&1 && docker info >/dev/null 2>&1; then
  printf 'docker_mem_bytes=%s\n' "$(docker info --format '{{.MemTotal}}' 2>/dev/null)"
fi
"#;

/// What changes between probes: free disk and the memory Docker can use.
const RESOURCES: &str = r#"printf 'disk_free_kb=%s\n' "$(df -k "$ROOT" | awk 'NR==2{print $4}')"
if command -v docker >/dev/null 2>&1 && docker info >/dev/null 2>&1; then
  printf 'docker_mem_bytes=%s\n' "$(docker info --format '{{.MemTotal}}' 2>/dev/null)"
fi
"#;

pub fn parse_kv(text: &str) -> serde_json::Map<String, serde_json::Value> {
    let mut m = serde_json::Map::new();
    for line in text.lines() {
        if let Some((k, v)) = line.split_once('=') {
            let v = v.trim();
            let val = v.parse::<i64>().map(serde_json::Value::from).unwrap_or_else(|_| v.into());
            m.insert(k.trim().to_string(), val);
        }
    }
    m
}

fn normalize_arch(a: &str) -> String {
    match a {
        "x86_64" => "amd64".into(),
        "aarch64" => "arm64".into(),
        other => other.into(),
    }
}

#[async_trait]
impl Backend for LocalBackend {
    fn name(&self) -> &str {
        &self.cfg().name
    }
    fn config(&self) -> &BackendConfig {
        self.cfg()
    }
    fn root(&self) -> Option<String> {
        self.remote.root()
    }
    fn capacity(&self) -> Option<Capacity> {
        self.capacity.read().unwrap_or_else(|e| e.into_inner()).clone()
    }
    fn runtimes(&self) -> Vec<String> {
        self.remote.runtimes()
    }

    async fn probe(&self) -> Result<Probe> {
        let (root, out) = self.remote.probe_root(HARDWARE).await?;
        let mut hw = parse_kv(&out);
        let c = self.cfg();
        let total_cpus = hw.get("cpus").and_then(|v| v.as_i64()).unwrap_or(1) as u32;
        let total_mem_gb = hw.get("mem_bytes").and_then(|v| v.as_i64()).map(|b| (b as f64 / 1_073_741_824.0).round() as u32).unwrap_or(1);
        hw.insert("mem_gb".into(), json!(total_mem_gb));
        let cap = Capacity {
            cpus: c.cpus.unwrap_or_else(|| total_cpus.saturating_sub(c.reserve_cpus).max(1)),
            mem_gb: c.mem_gb.unwrap_or_else(|| total_mem_gb.saturating_sub(c.reserve_mem_gb).max(1)),
            os: c.os.clone().unwrap_or_else(|| hw.get("os").and_then(|v| v.as_str()).unwrap_or("unknown").to_string()),
            arch: c.arch.clone().unwrap_or_else(|| normalize_arch(hw.get("arch").and_then(|v| v.as_str()).unwrap_or("unknown"))),
            disk_free_mb: hw.get("disk_free_kb").and_then(|v| v.as_i64()).map(|k| k.max(0) as u64 / 1024),
            docker_mem_mb: self.docker_mem(hw.get("docker_mem_bytes").and_then(|v| v.as_i64())),
        };
        hw.insert("schedulable".into(), json!({"cpus": cap.cpus, "mem_gb": cap.mem_gb, "gpus": 0}));
        hw.insert("runtimes".into(), json!(self.remote.runtimes()));
        *self.capacity.write().unwrap_or_else(|e| e.into_inner()) = Some(cap);
        Ok(Probe { root, hardware: serde_json::Value::Object(hw) })
    }

    async fn quote(&self, job: &Job, active: &[Alloc]) -> Quote {
        let c = self.cfg();
        let Some(cap) = self.capacity() else {
            return Quote::no(format!("{} has not been probed yet", c.name), false);
        };
        let r = &job.resources;
        if !r.regions.is_empty() {
            return Quote::no(format!("resources.regions is set, and {} isn't a cloud backend", c.name), true);
        }
        if r.nodes > c.max_nodes {
            return Quote::no(format!("nodes={}; {} runs single-node jobs", r.nodes, c.name), true);
        }
        if r.gpus > 0 {
            return Quote::no(format!("gpus={}, {} has no schedulable GPU", r.gpus, c.name), true);
        }
        if job.spec.sandbox && job.local_build() {
            return Quote::no(format!("a guest's image is built on a cloud instance of the job's own (Lambda), not on {}", c.name), true);
        }
        if job.is_container() && !self.remote.runtimes().iter().any(|x| x == "docker") {
            return Quote::no(format!("container job; {} has no Docker", c.name), true);
        }
        match job.os.as_deref() {
            Some("linux") if cap.os != "linux" => return Quote::no(format!("os=linux, {} is {}", c.name, cap.os), true),
            Some("mac") if cap.os != "macos" => return Quote::no(format!("os=mac, {} is {}", c.name, cap.os), true),
            _ => {}
        }
        let cap_mem = u64::from(cap.mem_gb) * 1024;
        if r.cpus > cap.cpus || r.mem_mb > cap_mem {
            return Quote::no(
                format!("needs {} CPU / {} MB; {} offers {} CPU / {} MB", r.cpus, r.mem_mb, c.name, cap.cpus, cap_mem),
                true,
            );
        }
        // Containers share the Docker VM's memory (OrbStack / Docker Desktop on macOS), which
        // can be well below the host's. Keep 1 GB of it for Docker itself.
        let container = job.is_container();
        let docker_cap = cap.docker_mem_mb.map(|m| m.saturating_sub(1024)).filter(|_| container);
        if let Some(dc) = docker_cap
            && r.mem_mb > dc
        {
            return Quote::no(format!("needs {} MB in a container; {}'s Docker VM has {} MB for containers (raise the VM's memory)", r.mem_mb, c.name, dc), true);
        }
        // Disk: what jobs said they need, against what was free when last measured, keeping
        // disk_reserve_gb spare.
        let reserve = u64::from(c.disk_reserve_gb) * 1024;
        let disk_free = cap.disk_free_mb;
        let fits = |cpus: u32, mem: u64, cmem: u64, disk: u64| {
            cpus + r.cpus <= cap.cpus
                && mem + r.mem_mb <= cap_mem
                && docker_cap.is_none_or(|dc| cmem + r.mem_mb <= dc)
                && disk_free.is_none_or(|f| disk + r.disk_mb + reserve <= f)
        };
        let shape = Shape::simple(r.cpus, r.mem_mb, 0);
        let used_cpus: u32 = active.iter().map(|a| a.cpus).sum();
        let used_mem: u64 = active.iter().map(|a| a.mem_mb).sum();
        let used_cmem: u64 = active.iter().filter(|a| a.container).map(|a| a.mem_mb).sum();
        let used_disk: u64 = active.iter().map(|a| a.disk_mb).sum();
        let now = Utc::now();
        if fits(used_cpus, used_mem, used_cmem, used_disk) {
            return Quote::Ok { start: now, source: "now".into(), reason: format!("{} has room now", c.name), shape, cost: None };
        }
        if let Some(f) = disk_free
            && r.disk_mb + reserve > f
        {
            // Not enough disk even with nothing else running: wait for space to be freed.
            return Quote::no(
                format!(
                    "{} has {:.0} GB free (keeping {} GB spare); the job needs {:.0} GB (resources.disk_gb). Old outputs are deleted after keep_outputs_days, or with DELETE /v1/jobs/<id>/outputs",
                    c.name,
                    f as f64 / 1024.0,
                    c.disk_reserve_gb,
                    r.disk_mb as f64 / 1024.0
                ),
                false,
            );
        }
        // Free running jobs in order of their expected end until this one fits.
        let mut ends: Vec<&Alloc> = active.iter().collect();
        ends.sort_by_key(|a| a.ends_at);
        let (mut cpus, mut mem, mut cmem, mut disk) = (used_cpus, used_mem, used_cmem, used_disk);
        let mut start = now;
        for a in ends {
            cpus -= a.cpus;
            mem -= a.mem_mb;
            if a.container {
                cmem -= a.mem_mb;
            }
            disk -= a.disk_mb;
            start = a.ends_at.max(now + chrono::Duration::minutes(1));
            if fits(cpus, mem, cmem, disk) {
                break;
            }
        }
        Quote::Ok {
            start,
            source: "daemon".into(),
            reason: format!("{} is busy; room expected when running jobs finish (estimated from similar past jobs, else their time limits)", c.name),
            shape,
            cost: None,
        }
    }

    async fn stage(&self, job: &Job, snapshot: &Path, files: &StageFiles) -> Result<()> {
        self.remote.stage_job(job, snapshot, files, "", job.resources.time_s).await
    }

    async fn launch(&self, jobs: &[Job], _shape: &Shape) -> Result<Vec<serde_json::Value>> {
        let root = self.remote.root_or_err()?;
        let runner = format!("{root}/bin/compute-run");
        let mut handles = Vec::new();
        for job in jobs {
            let dir = Remote::jobdir(&root, job.id);
            match self.cfg().transport() {
                Transport::Rex => {
                    let label = self.cfg().rex_label.clone().context("transport rex without rex_label")?;
                    let title: String = format!("#{} {}", job.id, job.title).chars().take(60).collect();
                    let block = rex_run(&label, &title, &["bash".to_string(), runner.clone(), dir]).await?;
                    handles.push(json!({"block": block}));
                }
                Transport::Ssh => {
                    let script = format!(
                        "nohup bash {} {} </dev/null >/dev/null 2>&1 &\necho $!",
                        shell_quote(&runner),
                        shell_quote(&dir)
                    );
                    let out = self.remote.shell().await?.run(&script).await?.text();
                    handles.push(json!({"pid": out.trim().parse::<i64>().ok()}));
                }
            }
        }
        Ok(handles)
    }

    async fn poll(&self, jobs: &[Job]) -> Result<Vec<PollResult>> {
        let ids: Vec<i64> = jobs.iter().map(|j| j.id).collect();
        let statuses = self.remote.read_statuses(&ids).await?;
        Ok(jobs
            .iter()
            .map(|j| match statuses.get(&j.id) {
                Some((status, beat)) => parse_status(j.id, status, *beat),
                None => PollResult { id: j.id, ..Default::default() },
            })
            .collect())
    }

    async fn logs(&self, job: &Job, offset: u64, max: u64) -> Result<(Vec<u8>, u64)> {
        self.remote.logs(job.id, offset, max).await
    }

    async fn fetch(&self, job: &Job, glob: Option<&str>) -> Result<Reader> {
        self.remote.fetch(job.id, glob).await
    }

    async fn cancel(&self, job: &Job) -> Result<()> {
        self.remote.touch_cancel(job.id).await
    }

    async fn set_time_limit(&self, job: &Job, time_s: u64) -> Result<()> {
        self.remote.set_time_limit(job.id, time_s).await
    }

    async fn refresh(&self) -> Result<()> {
        let root = self.remote.root_or_err()?;
        let out = self.remote.shell().await?.run(&format!("ROOT={}\n{RESOURCES}", shell_quote(&root))).await?.text();
        let kv = parse_kv(&out);
        let disk = kv.get("disk_free_kb").and_then(|v| v.as_i64()).map(|k| k.max(0) as u64 / 1024);
        let docker = self.docker_mem(kv.get("docker_mem_bytes").and_then(|v| v.as_i64()));
        if let Some(cap) = self.capacity.write().unwrap_or_else(|e| e.into_inner()).as_mut() {
            cap.disk_free_mb = disk.or(cap.disk_free_mb);
            cap.docker_mem_mb = docker.or(cap.docker_mem_mb);
        }
        Ok(())
    }

    async fn availability(&self) -> Result<serde_json::Value> {
        let root = shell_quote(&self.remote.root_or_err()?);
        let out = self
            .remote
            .shell()
            .await?
            .run(&format!(
                "uptime | sed 's/.*load average[s]*: //'\n\
                 (nproc 2>/dev/null || sysctl -n hw.ncpu)\n\
                 df -k {root} | awk 'NR==2 {{print $2, $4}}'"
            ))
            .await?
            .text();
        let mut lines = out.lines();
        let load = lines.next().unwrap_or("").trim().to_string();
        let cores: u32 = lines.next().and_then(|l| l.trim().parse().ok()).unwrap_or(0);
        let disk: Vec<u64> = lines.next().unwrap_or("").split_whitespace().filter_map(|x| x.parse().ok()).collect();
        let load1: f64 = load.split([',', ' ']).find(|x| !x.is_empty()).and_then(|x| x.parse().ok()).unwrap_or(0.0);
        Ok(json!({
            "load_average": load,
            "load_1m": load1,
            "cores": cores,
            "disk_total_gb": disk.first().map(|k| *k as f64 / 1_048_576.0),
            "disk_free_gb": disk.get(1).map(|k| *k as f64 / 1_048_576.0),
        }))
    }

    async fn unpack_snapshot(&self, snapshot: &str, path: &Path) -> Result<String> {
        self.remote.stage_snapshot(snapshot, path, "", false).await?;
        Ok(Remote::snapshot_dir(&self.remote.root_or_err()?, snapshot))
    }

    async fn shell_for(&self, _job: Option<&Job>) -> Result<(Arc<dyn Shell>, String)> {
        Ok((self.remote.shell().await?, self.remote.root_or_err()?))
    }
}
