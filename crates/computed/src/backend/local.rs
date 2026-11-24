//! Local hosts (Mac Mini, MacBook, any Linux box): with transport `rex`, jobs start as Rex blocks
//! in the host's `compute` session and files go over ssh through a `rex forward` tunnel; with
//! transport `ssh`, everything goes over direct ssh and jobs start detached with nohup.

use super::{Alloc, Backend, Capacity, PollResult, Probe, Quote, Remote, Shape, job_env, parse_status};
use crate::config::{BackendConfig, Transport};
use crate::model::Job;
use crate::transport::{Tunnels, rex_run, shell_quote};
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
        };
        hw.insert("schedulable".into(), json!({"cpus": cap.cpus, "mem_gb": cap.mem_gb, "gpus": 0}));
        *self.capacity.write().unwrap_or_else(|e| e.into_inner()) = Some(cap);
        Ok(Probe { root, hardware: serde_json::Value::Object(hw) })
    }

    async fn quote(&self, job: &Job, active: &[Alloc]) -> Quote {
        let c = self.cfg();
        let Some(cap) = self.capacity() else {
            return Quote::no(format!("{} has not been probed yet", c.name), false);
        };
        let r = &job.resources;
        if r.gpus > 0 {
            return Quote::no(format!("gpus={}, {} has no schedulable GPU", r.gpus, c.name), true);
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
        let shape = Shape { cpus: r.cpus, mem_mb: r.mem_mb, gpus: 0, partition: None, qos: None };
        let used_cpus: u32 = active.iter().map(|a| a.cpus).sum();
        let used_mem: u64 = active.iter().map(|a| a.mem_mb).sum();
        let now = Utc::now();
        if used_cpus + r.cpus <= cap.cpus && used_mem + r.mem_mb <= cap_mem {
            return Quote::Ok { start: now, source: "now".into(), reason: format!("{} has room now", c.name), shape };
        }
        // Free running jobs in order of their expected end until this one fits.
        let mut ends: Vec<&Alloc> = active.iter().collect();
        ends.sort_by_key(|a| a.ends_at);
        let (mut cpus, mut mem) = (used_cpus, used_mem);
        let mut start = now;
        for a in ends {
            cpus -= a.cpus;
            mem -= a.mem_mb;
            start = a.ends_at.max(now + chrono::Duration::minutes(1));
            if cpus + r.cpus <= cap.cpus && mem + r.mem_mb <= cap_mem {
                break;
            }
        }
        Quote::Ok {
            start,
            source: "daemon".into(),
            reason: format!("{} is busy; room expected when running jobs reach their time limits", c.name),
            shape,
        }
    }

    async fn stage(&self, job: &Job, snapshot: &Path, secrets_env: Option<String>) -> Result<()> {
        let root = self.remote.root_or_err()?;
        self.remote.stage_snapshot(&job.snapshot, snapshot, "").await?;
        let env = job_env(&root, job, job.resources.time_s, &[]);
        self.remote.write_job_files(job.id, &env, &job.command, secrets_env.as_deref()).await
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
                    let out = self.remote.ssh().await?.run(&script).await?.text();
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

    async fn fetch(&self, job: &Job, glob: Option<&str>) -> Result<tokio::process::Child> {
        self.remote.fetch(job.id, glob).await
    }

    async fn cancel(&self, job: &Job) -> Result<()> {
        self.remote.touch_cancel(job.id).await
    }

    async fn availability(&self) -> Result<serde_json::Value> {
        let out = self
            .remote
            .ssh()
            .await?
            .run("uptime | sed 's/.*load average[s]*: //'")
            .await?
            .text();
        Ok(json!({"load_average": out.trim()}))
    }
}
