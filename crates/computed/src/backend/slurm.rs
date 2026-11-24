//! FarmShare (Slurm) over `ssh farm`. Request sizing follows the FarmShare rules: memory is
//! cpus x 4000 MB, a 4-GPU job takes the whole node, and 1-3 GPU jobs compare a few CPU/memory
//! shapes with `sbatch --test-only` and take the earliest start.

use super::{Alloc, Backend, PollResult, Probe, Quote, Remote, Shape, job_env, parse_status, write_remote_file};
use crate::config::{BackendConfig, SlurmConfig};
use crate::model::{Job, JobState};
use crate::transport::{Tunnels, shell_quote};
use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use chrono::{DateTime, NaiveDateTime, TimeZone, Utc};
use chrono_tz::Tz;
use serde_json::json;
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;

const QUOTE_TTL: Duration = Duration::from_secs(60);

pub struct SlurmBackend {
    remote: Remote,
    scfg: SlurmConfig,
    tz: Tz,
    quotes: Mutex<HashMap<String, (Instant, Quote)>>,
}

impl SlurmBackend {
    pub fn new(cfg: BackendConfig, tz: Tz, tunnels: Arc<Tunnels>) -> SlurmBackend {
        let scfg = cfg.slurm.clone().expect("filled in by Config::load");
        SlurmBackend { remote: Remote::new(cfg, tunnels), scfg, tz, quotes: Mutex::new(HashMap::new()) }
    }
    fn cfg(&self) -> &BackendConfig {
        &self.remote.cfg
    }

    /// uv/HF/pip caches live under the root (scratch), never in home.
    fn cache_env(&self, root: &str) -> Vec<(String, String)> {
        vec![
            ("UV_CACHE_DIR".into(), format!("{root}/cache/uv")),
            ("UV_PYTHON_INSTALL_DIR".into(), format!("{root}/cache/uv-python")),
            ("HF_HOME".into(), format!("{root}/cache/hf")),
            ("PIP_CACHE_DIR".into(), format!("{root}/cache/pip")),
        ]
    }

    fn parse_local(&self, s: &str) -> Option<DateTime<Utc>> {
        let naive = NaiveDateTime::parse_from_str(s.trim(), "%Y-%m-%dT%H:%M:%S").ok()?;
        self.tz.from_local_datetime(&naive).earliest().map(|t| t.with_timezone(&Utc))
    }

    /// Candidate request shapes for a job, smallest first.
    fn shapes(&self, job: &Job) -> Result<Vec<Shape>, String> {
        let r = &job.resources;
        let s = &self.scfg;
        let per_cpu = u64::from(s.mem_per_cpu_mb);
        let cpus_min = r.cpus.max(r.mem_mb.div_ceil(per_cpu) as u32);
        let shape = |cpus: u32, part: &str, qos: Option<&str>| Shape {
            cpus,
            mem_mb: u64::from(cpus) * per_cpu,
            gpus: r.gpus,
            partition: Some(part.to_string()),
            qos: qos.map(str::to_string),
        };
        if r.gpus == 0 {
            if cpus_min > s.max_cpu_task_cpus {
                return Err(format!(
                    "needs {cpus_min} CPUs (memory {} MB at {} MB/CPU); farm allows {} per CPU task",
                    r.mem_mb, s.mem_per_cpu_mb, s.max_cpu_task_cpus
                ));
            }
            return Ok(vec![shape(cpus_min, &s.cpu_partition, Some(&s.cpu_qos))]);
        }
        if cpus_min > s.node_cpus {
            return Err(format!(
                "needs {cpus_min} CPUs (memory {} MB at {} MB/CPU); a gpu node has {}",
                r.mem_mb, s.mem_per_cpu_mb, s.node_cpus
            ));
        }
        if r.gpus >= s.max_gpus {
            let mut whole = shape(s.node_cpus, &s.gpu_partition, None);
            whole.mem_mb = u64::from(s.node_mem_mb);
            return Ok(vec![whole]);
        }
        let mut cpus: Vec<u32> = vec![cpus_min, 8 * r.gpus, 16 * r.gpus]
            .into_iter()
            .filter(|c| *c >= cpus_min && *c <= s.node_cpus)
            .collect();
        cpus.sort_unstable();
        cpus.dedup();
        Ok(cpus.into_iter().map(|c| shape(c, &s.gpu_partition, None)).collect())
    }

    /// sbatch options for a shape, used both for `--test-only` and as `#SBATCH` lines.
    fn sbatch_args(shape: &Shape, time_s: u64) -> Vec<String> {
        let mut a = vec![
            format!("--partition={}", shape.partition.as_deref().unwrap_or("normal")),
            format!("--cpus-per-task={}", shape.cpus),
            format!("--mem={}M", shape.mem_mb),
            format!("--time={}", time_s.div_ceil(60).max(1)),
        ];
        if let Some(q) = &shape.qos {
            a.push(format!("--qos={q}"));
        }
        if shape.gpus > 0 {
            a.push(format!("--gres=gpu:{}", shape.gpus));
        }
        a
    }

    fn sbatch_opts(shape: &Shape, time_s: u64) -> String {
        Self::sbatch_args(shape, time_s).iter().map(|a| shell_quote(a)).collect::<Vec<_>>().join(" ")
    }

    async fn compute_quote(&self, job: &Job) -> Quote {
        let c = self.cfg();
        let r = &job.resources;
        if r.gpus > self.scfg.max_gpus {
            return Quote::no(format!("gpus={}; farm allows at most {} GPUs per user", r.gpus, self.scfg.max_gpus), true);
        }
        if job.os.as_deref() == Some("mac") {
            return Quote::no("os=mac; farm is linux", true);
        }
        if let (Some(want), Some(have)) = (&r.gpu_type, &c.gpu_type)
            && r.gpus > 0 && !want.eq_ignore_ascii_case(have) {
                return Quote::no(format!("gpu_type={want}; farm has {have}"), true);
            }
        let shapes = match self.shapes(job) {
            Ok(s) => s,
            Err(e) => return Quote::no(e, true),
        };
        let ssh = match self.remote.ssh().await {
            Ok(s) => s,
            Err(e) => return Quote::no(format!("unreachable: {e:#}"), false),
        };
        let mut script = String::new();
        for (i, s) in shapes.iter().enumerate() {
            script.push_str(&format!(
                "printf 'SHAPE {i} '; sbatch --test-only {} --wrap true 2>&1 | tr '\\n' ' '; echo\n",
                Self::sbatch_opts(s, r.time_s)
            ));
        }
        let partition = shapes[0].partition.clone().unwrap_or_default();
        script.push_str(&format!("echo LEFT; squeue -h -p {} -t R -o '%L'\n", shell_quote(&partition)));
        let out = match ssh.run(&script).await {
            Ok(o) => o.text(),
            Err(e) => return Quote::no(format!("unreachable: {e:#}"), false),
        };
        let now = Utc::now();
        let mut best: Option<(DateTime<Utc>, Shape)> = None;
        let mut errors = Vec::new();
        let (shape_part, left_part) = out.split_once("LEFT").unwrap_or((&out, ""));
        for line in shape_part.lines() {
            let Some(rest) = line.strip_prefix("SHAPE ") else { continue };
            let (idx, text) = rest.split_once(' ').unwrap_or((rest, ""));
            let Ok(idx) = idx.parse::<usize>() else { continue };
            let start = text
                .split("to start at ")
                .nth(1)
                .and_then(|t| t.split_whitespace().next())
                .and_then(|t| self.parse_local(t));
            match start {
                Some(t) => {
                    let t = t.max(now);
                    let better = match &best {
                        None => true,
                        Some((bt, bs)) => t < *bt || (t == *bt && shapes[idx].cpus > bs.cpus),
                    };
                    if better {
                        best = Some((t, shapes[idx].clone()));
                    }
                }
                None => errors.push(text.trim().to_string()),
            }
        }
        let describe = |s: &Shape| {
            let gpu = if s.gpus > 0 { format!("{} GPU, ", s.gpus) } else { String::new() };
            format!("{gpu}{} CPU / {} MB on {}", s.cpus, s.mem_mb, s.partition.as_deref().unwrap_or(""))
        };
        if let Some((start, shape)) = best {
            let reason = if shapes.len() > 1 {
                format!("{} starts earliest of {} shapes compared with --test-only", describe(&shape), shapes.len())
            } else {
                describe(&shape)
            };
            return Quote::Ok { start, source: "slurm".into(), reason, shape };
        }
        if errors.iter().any(|e| e.contains("error")) {
            // Slurm refused every shape (e.g. a QOS or partition limit): not placeable here now.
            return Quote::no(format!("sbatch --test-only refused: {}", errors.join(" | ")), false);
        }
        // Slurm gave no estimate: use the soonest time limit among running jobs.
        let soonest = left_part.lines().filter_map(parse_slurm_duration).min().unwrap_or_default();
        let shape = shapes.into_iter().next_back().expect("at least one shape");
        Quote::Ok {
            start: now + chrono::Duration::from_std(soonest).unwrap_or_default(),
            source: "daemon".into(),
            reason: format!("{}; Slurm gave no start time, estimated from running jobs' time left", describe(&shape)),
            shape,
        }
    }

    fn slurm_key(handle: &serde_json::Value) -> Option<String> {
        let id = handle["slurm_id"].as_str()?;
        Some(match handle["task"].as_u64() {
            Some(t) => format!("{id}_{t}"),
            None => id.to_string(),
        })
    }
}

/// Parses Slurm durations: `D-HH:MM:SS`, `HH:MM:SS`, `MM:SS`.
pub fn parse_slurm_duration(s: &str) -> Option<Duration> {
    let s = s.trim();
    let (days, rest) = match s.split_once('-') {
        Some((d, r)) => (d.parse::<u64>().ok()?, r),
        None => (0, s),
    };
    let parts: Vec<u64> = rest.split(':').map(|p| p.parse().ok()).collect::<Option<_>>()?;
    let secs = match parts.as_slice() {
        [h, m, s] => h * 3600 + m * 60 + s,
        [m, s] => m * 60 + s,
        [s] => *s,
        _ => return None,
    };
    Some(Duration::from_secs(days * 86_400 + secs))
}

/// Total and used GPUs from sinfo's Gres/GresUsed fields, e.g. `gpu:l40s:4(S:0-1)`.
fn gres_gpus(field: &str) -> u32 {
    field
        .split(',')
        .filter(|t| t.starts_with("gpu"))
        .map(|t| t.split('(').next().unwrap_or(t))
        .filter_map(|t| t.rsplit(':').next()?.parse::<u32>().ok())
        .sum()
}

fn title_for_slurm(id: i64, title: &str) -> String {
    let clean: String = title
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() || " _.-".contains(c) { c } else { '_' })
        .collect();
    let s = format!("c{id} {}", clean.trim()).replace(' ', "_");
    s.chars().take(60).collect()
}

#[async_trait]
impl Backend for SlurmBackend {
    fn name(&self) -> &str {
        &self.cfg().name
    }
    fn config(&self) -> &BackendConfig {
        self.cfg()
    }
    fn root(&self) -> Option<String> {
        self.remote.root()
    }

    async fn probe(&self) -> Result<Probe> {
        let s = &self.scfg;
        let extra = format!(
            "echo GPU; sinfo -h -p {} -o '%D|%c|%m|%G'\necho CPU; sinfo -h -p {} -o '%D|%c|%m|%N'\n",
            shell_quote(&s.gpu_partition),
            shell_quote(&s.cpu_partition)
        );
        let (root, out) = self.remote.probe_root(&extra).await?;
        let (gpu, cpu) = out.split_once("CPU").unwrap_or((&out, ""));
        let rows = |text: &str| -> Vec<serde_json::Value> {
            text.lines()
                .filter(|l| l.contains('|'))
                .map(|l| {
                    let f: Vec<&str> = l.split('|').collect();
                    json!({
                        "nodes": f.first().and_then(|v| v.trim().parse::<u32>().ok()),
                        // Mixed partitions report "80+" (the smallest node, plus); keep it as text.
                        "cpus": f.get(1).map(|v| v.trim()),
                        "mem_gb": f.get(2).and_then(|v| {
                            let v = v.trim();
                            let plus = if v.ends_with('+') { "+" } else { "" };
                            v.trim_end_matches('+').parse::<u64>().ok().map(|m| format!("{}{plus}", m / 1024))
                        }),
                        "gres_or_nodes": f.get(3).map(|v| v.trim()),
                    })
                })
                .collect()
        };
        let hardware = json!({
            "partitions": {
                s.gpu_partition.clone(): {
                    "groups": rows(gpu.trim_start_matches("GPU")),
                    "gpu_type": self.cfg().gpu_type,
                    "gpu_mem_gb": 48,
                    "arch": self.cfg().arch.clone().unwrap_or_else(|| "amd64".into()),
                },
                s.cpu_partition.clone(): {"groups": rows(cpu)},
            },
            "limits": {
                "max_gpus_per_user": s.max_gpus,
                "mem_per_cpu_mb": s.mem_per_cpu_mb,
                "gpu_node": {"cpus": s.node_cpus, "mem_mb": s.node_mem_mb},
            },
        });
        Ok(Probe { root, hardware })
    }

    async fn quote(&self, job: &Job, _active: &[Alloc]) -> Quote {
        let key = format!("{}:{:?}:{:?}", job.group_key(), job.resources, job.os);
        if let Some((at, q)) = self.quotes.lock().await.get(&key)
            && at.elapsed() < QUOTE_TTL {
                return q.clone();
            }
        let q = self.compute_quote(job).await;
        let mut cache = self.quotes.lock().await;
        cache.retain(|_, (at, _)| at.elapsed() < QUOTE_TTL);
        cache.insert(key, (Instant::now(), q.clone()));
        q
    }

    async fn stage(&self, job: &Job, snapshot: &Path, secrets_env: Option<String>) -> Result<()> {
        let root = self.remote.root_or_err()?;
        let cache = self.cache_env(&root);
        let prelude: String = cache
            .iter()
            .map(|(k, v)| format!("export {k}={}\n", shell_quote(v)))
            .collect();
        self.remote.stage_snapshot(&job.snapshot, snapshot, &prelude).await?;
        // Slurm enforces --time, so the runner's own timeout is off (0).
        let env = job_env(&root, job, 0, &cache);
        self.remote.write_job_files(job.id, &env, &job.command, secrets_env.as_deref()).await
    }

    async fn launch(&self, jobs: &[Job], shape: &Shape) -> Result<Vec<serde_json::Value>> {
        let first = jobs.first().context("launch with no jobs")?;
        let root = self.remote.root_or_err()?;
        let ssh = self.remote.ssh().await?;
        let mut script = format!(
            "#!/bin/bash\n#SBATCH --job-name={}\n",
            title_for_slurm(first.id, &first.title)
        );
        for arg in Self::sbatch_args(shape, first.resources.time_s) {
            script.push_str(&format!("#SBATCH {arg}\n"));
        }
        
        let file = if jobs.len() == 1 {
            let dir = Remote::jobdir(&root, first.id);
            script.push_str(&format!("#SBATCH --output={dir}/slurm.out\n"));
            script.push_str(&format!("exec bash {} {}\n", shell_quote(&format!("{root}/bin/compute-run")), shell_quote(&dir)));
            format!("{dir}/job.sbatch")
        } else {
            let tasks: String = jobs.iter().map(|j| format!("{}\n", Remote::jobdir(&root, j.id))).collect();
            let tasks_file = format!("{root}/runs/batch-{}.tasks", first.id);
            write_remote_file(&ssh, &tasks_file, &tasks).await?;
            script.push_str(&format!("#SBATCH --array=0-{}\n", jobs.len() - 1));
            script.push_str(&format!("#SBATCH --output={root}/runs/slurm-%A_%a.out\n"));
            script.push_str(&format!(
                "exec bash {} --tasks {}\n",
                shell_quote(&format!("{root}/bin/compute-run")),
                shell_quote(&tasks_file)
            ));
            format!("{root}/runs/batch-{}.sbatch", first.id)
        };
        write_remote_file(&ssh, &file, &script).await?;
        let out = ssh.run(&format!("sbatch --parsable {}", shell_quote(&file))).await?.text();
        let slurm_id = out.trim().split(';').next().unwrap_or("").trim().to_string();
        if slurm_id.is_empty() || !slurm_id.chars().all(|c| c.is_ascii_digit()) {
            bail!("sbatch returned {out:?}");
        }
        Ok(if jobs.len() == 1 {
            vec![json!({"slurm_id": slurm_id})]
        } else {
            (0..jobs.len()).map(|i| json!({"slurm_id": slurm_id, "task": i})).collect()
        })
    }

    async fn poll(&self, jobs: &[Job]) -> Result<Vec<PollResult>> {
        if jobs.is_empty() {
            return Ok(Vec::new());
        }
        let ids: Vec<i64> = jobs.iter().map(|j| j.id).collect();
        let statuses = self.remote.read_statuses(&ids).await?;
        let mut slurm_ids: Vec<String> = jobs
            .iter()
            .filter_map(|j| j.handle.as_ref().and_then(|h| h["slurm_id"].as_str()).map(str::to_string))
            .collect();
        slurm_ids.sort();
        slurm_ids.dedup();
        let mut queue: HashMap<String, (String, String)> = HashMap::new();
        let mut acct: HashMap<String, (String, String)> = HashMap::new();
        if !slurm_ids.is_empty() {
            let list = slurm_ids.join(",");
            let script = format!(
                "squeue -h -r -j {list} -o '%i|%T|%S' 2>/dev/null\necho ACCT\n\
                 sacct -n -P -X -j {list} -o JobID,State,ExitCode 2>/dev/null"
            );
            let out = self.remote.ssh().await?.run(&script).await?.text();
            let (q, a) = out.split_once("ACCT").unwrap_or((&out, ""));
            for line in q.lines() {
                let f: Vec<&str> = line.split('|').collect();
                if f.len() == 3 {
                    queue.insert(f[0].trim().to_string(), (f[1].trim().to_string(), f[2].trim().to_string()));
                }
            }
            for line in a.lines() {
                let f: Vec<&str> = line.split('|').collect();
                if f.len() == 3 {
                    acct.insert(f[0].trim().to_string(), (f[1].trim().to_string(), f[2].trim().to_string()));
                }
            }
        }
        let mut results = Vec::new();
        for job in jobs {
            let status = statuses.get(&job.id);
            let mut r = match status {
                Some((s, beat)) => parse_status(job.id, s, *beat),
                None => PollResult { id: job.id, ..Default::default() },
            };
            if r.state.is_some_and(JobState::is_terminal) {
                results.push(r);
                continue;
            }
            let key = job.handle.as_ref().and_then(Self::slurm_key);
            if let Some((state, start)) = key.as_ref().and_then(|k| queue.get(k)) {
                match state.as_str() {
                    "PENDING" | "CONFIGURING" => {
                        r.state = Some(JobState::Pending);
                        r.est_start = self.parse_local(start);
                    }
                    _ => {
                        r.state = Some(JobState::Running);
                        if r.started.is_none() {
                            r.started = self.parse_local(start);
                        }
                    }
                }
                results.push(r);
                continue;
            }
            // Not in the queue and no final status from the runner: ask sacct.
            if let Some((state, exit)) = key.as_ref().and_then(|k| acct.get(k)) {
                let code = exit.split(':').next().and_then(|c| c.parse::<i32>().ok());
                let mapped = if state.starts_with("CANCELLED") {
                    Some(JobState::Cancelled)
                } else {
                    match state.as_str() {
                        "COMPLETED" => Some(if code == Some(0) { JobState::Succeeded } else { JobState::Failed }),
                        "TIMEOUT" => Some(JobState::Timeout),
                        "FAILED" | "NODE_FAIL" | "OUT_OF_MEMORY" | "BOOT_FAIL" | "DEADLINE" | "PREEMPTED" => {
                            r.note = Some(format!("slurm state {state}"));
                            Some(JobState::Failed)
                        }
                        _ => None,
                    }
                };
                if let Some(m) = mapped {
                    r.state = Some(m);
                    r.exit_code = r.exit_code.or(code);
                    r.ended = r.ended.or(Some(Utc::now()));
                }
            }
            results.push(r);
        }
        Ok(results)
    }

    async fn logs(&self, job: &Job, offset: u64, max: u64) -> Result<(Vec<u8>, u64)> {
        self.remote.logs(job.id, offset, max).await
    }

    async fn fetch(&self, job: &Job, glob: Option<&str>) -> Result<tokio::process::Child> {
        self.remote.fetch(job.id, glob).await
    }

    async fn cancel(&self, job: &Job) -> Result<()> {
        if let Some(key) = job.handle.as_ref().and_then(Self::slurm_key) {
            self.remote.ssh().await?.run(&format!("scancel {}", shell_quote(&key))).await?;
        }
        Ok(())
    }

    async fn availability(&self) -> Result<serde_json::Value> {
        let s = &self.scfg;
        let script = format!(
            "sinfo -h -p {gp} -N -O 'NodeHost:30,Gres:60,GresUsed:80,StateCompact:20'\necho MINE\n\
             squeue -h --me -o '%T|%P'\necho LEFT\nsqueue -h -p {gp} -t R -o '%L'\n",
            gp = shell_quote(&s.gpu_partition)
        );
        let out = self.remote.ssh().await?.run(&script).await?.text();
        let (nodes, rest) = out.split_once("MINE").unwrap_or((&out, ""));
        let (mine, left) = rest.split_once("LEFT").unwrap_or((rest, ""));
        let mut free = 0;
        let mut per_node = serde_json::Map::new();
        for line in nodes.lines() {
            let f: Vec<&str> = line.split_whitespace().collect();
            if f.len() < 4 {
                continue;
            }
            let (total, used) = (gres_gpus(f[1]), gres_gpus(f[2]));
            let usable = !["down", "drain", "drng", "fail", "maint"].iter().any(|x| f[3].contains(x));
            let n = if usable { total.saturating_sub(used) } else { 0 };
            free += n;
            per_node.insert(f[0].to_string(), json!({"gpus_free": n, "state": f[3]}));
        }
        let (mut running, mut pending) = (0, 0);
        for line in mine.lines() {
            if line.starts_with("RUNNING") {
                running += 1;
            } else if line.starts_with("PENDING") {
                pending += 1;
            }
        }
        let next_free = if free > 0 {
            Some(Utc::now())
        } else {
            left.lines()
                .filter_map(parse_slurm_duration)
                .min()
                .map(|d| Utc::now() + chrono::Duration::from_std(d).unwrap_or_default())
        };
        Ok(json!({
            "gpus_free_now": free,
            "nodes": per_node,
            "my_running": running,
            "my_pending": pending,
            "next_gpu_free_estimate": next_free.map(|t| t.with_timezone(&self.tz).to_rfc3339()),
        }))
    }
}
