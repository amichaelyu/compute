//! The scheduler loop: probe backends, resolve dependencies, poll live jobs, place queued jobs
//! (earliest estimated start wins; free beats paid, local beats Slurm on a tie), then stage and
//! launch them.

use crate::backend::{Alloc, Backend, Quote, Shape, StageFiles};
use crate::config::{BackendKind, BudgetConfig, Paths, RegistryConfig};
use crate::images::ImageStatus;
use crate::model::{DepRef, Job, JobState};
use crate::secrets::Secrets;
use crate::snapshots::Snapshots;
use crate::store::{JobUpdate, Store};
use anyhow::{Context, Result};
use chrono::{DateTime, Datelike, TimeZone, Utc};
use chrono_tz::Tz;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::{Notify, watch};

const TICK: Duration = Duration::from_secs(3);
const PROBE_RETRY: Duration = Duration::from_secs(60);
const PROBE_REFRESH: Duration = Duration::from_secs(24 * 3600);
const SWEEP_EVERY: Duration = Duration::from_secs(300);
const LOST_AFTER: i64 = 5 * 60;
const NO_STATUS_AFTER: i64 = 10 * 60;
const STAGE_ATTEMPTS: u32 = 3;

#[derive(Clone, Default)]
pub struct BackendRuntime {
    pub reachable: bool,
    pub last_probe: Option<Instant>,
    pub probed_at: Option<DateTime<Utc>>,
    pub hardware: Option<serde_json::Value>,
    pub availability: Option<serde_json::Value>,
    pub availability_at: Option<DateTime<Utc>>,
    pub last_error: Option<String>,
    /// Measured bytes per second for dataset copies to this backend.
    pub transfer_rate: Option<f64>,
}

pub struct Scheduler {
    pub store: Arc<Store>,
    pub backends: Vec<Arc<dyn Backend>>,
    pub snapshots: Arc<Snapshots>,
    pub secrets: Arc<Secrets>,
    pub paths: Paths,
    pub registry: RegistryConfig,
    pub budget: BudgetConfig,
    pub tz: Tz,
    notify: Notify,
    version: watch::Sender<u64>,
    runtime: Mutex<HashMap<String, BackendRuntime>>,
    in_flight: Mutex<HashSet<i64>>,
    dataset_locks: Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
    builds: Mutex<HashSet<String>>,
    logged_in: Mutex<HashSet<String>>,
}

/// The chosen backend for a job.
#[derive(Clone)]
pub struct Best {
    pub backend: String,
    pub start: DateTime<Utc>,
    pub source: String,
    pub reason: String,
    pub shape: Shape,
    pub cost: Option<f64>,
}

/// Every backend's answer for a job, for dry runs and the submit response.
pub struct Placement {
    pub quotes: Vec<(String, Quote)>,
    pub best: Option<Best>,
}

/// Where a job's dependencies stand.
enum Deps {
    Met,
    Failed(String),
    /// Unfinished parents, and the latest estimated finish among them.
    Pending(Vec<Job>, Option<DateTime<Utc>>),
}

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

pub struct SchedulerConfig {
    pub paths: Paths,
    pub registry: RegistryConfig,
    pub budget: BudgetConfig,
    pub tz: Tz,
}

impl Scheduler {
    pub fn new(
        store: Arc<Store>,
        backends: Vec<Arc<dyn Backend>>,
        snapshots: Arc<Snapshots>,
        secrets: Arc<Secrets>,
        cfg: SchedulerConfig,
    ) -> Arc<Scheduler> {
        let (version, _) = watch::channel(0);
        Arc::new(Scheduler {
            store,
            backends,
            snapshots,
            secrets,
            paths: cfg.paths,
            registry: cfg.registry,
            budget: cfg.budget,
            tz: cfg.tz,
            notify: Notify::new(),
            version,
            runtime: Mutex::new(HashMap::new()),
            in_flight: Mutex::new(HashSet::new()),
            dataset_locks: Mutex::new(HashMap::new()),
            builds: Mutex::new(HashSet::new()),
            logged_in: Mutex::new(HashSet::new()),
        })
    }

    pub fn backend(&self, name: &str) -> Option<Arc<dyn Backend>> {
        self.backends.iter().find(|b| b.name() == name).cloned()
    }

    pub fn kick(&self) {
        self.notify.notify_one();
    }

    pub fn subscribe(&self) -> watch::Receiver<u64> {
        self.version.subscribe()
    }

    fn bump(&self) {
        self.version.send_modify(|v| *v += 1);
    }

    pub fn runtime(&self, name: &str) -> BackendRuntime {
        lock(&self.runtime).get(name).cloned().unwrap_or_default()
    }

    fn set_runtime(&self, name: &str, f: impl FnOnce(&mut BackendRuntime)) {
        f(lock(&self.runtime).entry(name.to_string()).or_default());
    }

    pub fn set_transfer_rate(&self, name: &str, rate: f64) {
        self.set_runtime(name, |r| {
            r.transfer_rate = Some(match r.transfer_rate {
                Some(old) => 0.5 * old + 0.5 * rate,
                None => rate,
            })
        });
    }

    pub fn dataset_lock(&self, key: &str) -> Arc<tokio::sync::Mutex<()>> {
        lock(&self.dataset_locks).entry(key.to_string()).or_default().clone()
    }

    pub fn builds_in_flight(&self, tag: &str) -> bool {
        lock(&self.builds).contains(tag)
    }

    /// Marks a build as running (`true`, returns false if it already was) or finished.
    pub fn mark_build(&self, tag: &str, running: bool) -> bool {
        let mut b = lock(&self.builds);
        if running { b.insert(tag.to_string()) } else { b.remove(tag) }
    }

    pub fn logged_in(&self, backend: &str) -> bool {
        lock(&self.logged_in).contains(backend)
    }

    pub fn set_logged_in(&self, backend: &str) {
        lock(&self.logged_in).insert(backend.to_string());
    }

    /// Whether a backend takes new jobs, and until when an override lasts.
    pub fn enabled(&self, name: &str) -> Result<(bool, Option<DateTime<Utc>>)> {
        let default = self.backend(name).map(|b| b.config().enabled).unwrap_or(false);
        match self.store.backend_override(name)? {
            Some((_, Some(until))) if until <= Utc::now() => {
                self.store.clear_backend_override(name)?;
                Ok((default, None))
            }
            Some((on, until)) => Ok((on, until)),
            None => Ok((default, None)),
        }
    }

    pub fn update(&self, id: i64, u: JobUpdate) -> Result<()> {
        self.store.update_job(id, u)?;
        self.bump();
        Ok(())
    }

    /// Startup: jobs caught mid-staging go back to the queue; running jobs get a fresh grace
    /// period so a long daemon outage doesn't mark them lost before the first poll.
    pub fn reconcile(&self) -> Result<()> {
        for j in self.store.jobs_in_states(&[JobState::Staging])? {
            self.store.update_job(
                j.id,
                JobUpdate {
                    state: Some(if j.spec.has_deps() { JobState::Waiting } else { JobState::Queued }),
                    backend: Some(None),
                    handle: Some(None),
                    reason: Some(Some("requeued: daemon restarted during staging".into())),
                    ..Default::default()
                },
            )?;
        }
        for j in self.store.jobs_in_states(&[JobState::Running])? {
            self.store.update_job(j.id, JobUpdate { last_seen: Some(Some(Utc::now())), ..Default::default() })?;
        }
        Ok(())
    }

    pub async fn run(self: Arc<Self>) {
        let mut last_gc = Instant::now() - Duration::from_secs(86_400);
        let mut last_sweep = Instant::now() - SWEEP_EVERY;
        loop {
            if let Err(e) = self.tick().await {
                tracing::error!("scheduler tick: {e:#}");
            }
            if last_gc.elapsed() > Duration::from_secs(86_400) {
                last_gc = Instant::now();
                match self.snapshots.gc() {
                    Ok(n) if n > 0 => tracing::info!("removed {n} stale snapshots"),
                    Ok(_) => {}
                    Err(e) => tracing::warn!("snapshot gc: {e:#}"),
                }
            }
            if last_sweep.elapsed() > SWEEP_EVERY {
                last_sweep = Instant::now();
                self.sweep().await;
            }
            tokio::select! {
                _ = self.notify.notified() => {}
                _ = tokio::time::sleep(TICK) => {}
            }
        }
    }

    async fn tick(self: &Arc<Self>) -> Result<()> {
        self.probe_due().await;
        self.resolve_deps().await?;
        self.poll_all().await?;
        self.place_queued().await
    }

    /// Cloud backends: tear down instances no live job owns.
    async fn sweep(&self) {
        let mut live: HashMap<i64, Option<serde_json::Value>> = match self.store.jobs_in_states(&[JobState::Staging, JobState::Pending, JobState::Running]) {
            Ok(jobs) => jobs.into_iter().map(|j| (j.id, j.handle)).collect(),
            Err(e) => {
                tracing::warn!("sweep: {e:#}");
                return;
            }
        };
        for id in lock(&self.in_flight).iter() {
            live.entry(*id).or_insert(None);
        }
        for b in self.backends.iter().filter(|b| b.is_paid()) {
            if let Err(e) = tokio::time::timeout(Duration::from_secs(120), b.sweep(&live)).await.unwrap_or_else(|_| Err(anyhow::anyhow!("timed out"))) {
                tracing::warn!("sweep {}: {e:#}", b.name());
            }
        }
    }

    async fn probe_due(self: &Arc<Self>) {
        let mut due = Vec::new();
        for b in &self.backends {
            let name = b.name().to_string();
            if !self.enabled(&name).map(|(on, _)| on).unwrap_or(false) {
                continue;
            }
            let rt = self.runtime(&name);
            let wait = if rt.reachable { PROBE_REFRESH } else { PROBE_RETRY };
            if rt.last_probe.is_none_or(|t| t.elapsed() >= wait) {
                due.push(b.clone());
            }
        }
        let probes = due.into_iter().map(|b| {
            let me = self.clone();
            async move {
                let name = b.name().to_string();
                me.set_runtime(&name, |r| r.last_probe = Some(Instant::now()));
                match tokio::time::timeout(Duration::from_secs(90), b.probe()).await {
                    Ok(Ok(p)) => {
                        tracing::info!("backend {name} reachable, root {}", p.root);
                        me.set_runtime(&name, |r| {
                            r.reachable = true;
                            r.probed_at = Some(Utc::now());
                            r.hardware = Some(p.hardware);
                            r.last_error = None;
                        });
                    }
                    Ok(Err(e)) => me.mark_unreachable(&name, format!("{e:#}")),
                    Err(_) => me.mark_unreachable(&name, "probe timed out after 90 s".into()),
                }
            }
        });
        futures_util::future::join_all(probes).await;
    }

    fn mark_unreachable(&self, name: &str, err: String) {
        tracing::warn!("backend {name} unreachable: {err}");
        self.set_runtime(name, |r| {
            r.reachable = false;
            r.last_error = Some(err);
        });
    }

    /// Re-probes a backend now (used after enabling one).
    pub async fn probe_now(self: &Arc<Self>, name: &str) {
        self.set_runtime(name, |r| r.last_probe = None);
        self.probe_due().await;
    }

    // ---------------------------------------------------------------- dependencies

    /// The jobs a reference names (an array expands to all its tasks).
    fn dep_jobs(&self, r: &DepRef) -> Result<Vec<Job>> {
        Ok(match r {
            DepRef::Job(id) => self.store.get_job(*id)?.into_iter().collect(),
            DepRef::Array { array } => self.store.jobs_in_array(*array)?,
        })
    }

    fn deps(&self, job: &Job) -> Result<Deps> {
        let mut unfinished = Vec::new();
        let mut latest: Option<DateTime<Utc>> = None;
        let mut track = |p: &Job, unfinished: &mut Vec<Job>| {
            unfinished.push(p.clone());
            let fin = p.est_finish.or(p.started_at.map(|s| s + chrono::Duration::seconds(p.resources.time_s as i64)));
            latest = match (latest, fin) {
                (Some(a), Some(b)) => Some(a.max(b)),
                (a, b) => a.or(b),
            };
        };
        for r in &job.spec.after_ok {
            let parents = self.dep_jobs(r)?;
            if parents.is_empty() {
                return Ok(Deps::Failed(format!("{r} no longer exists")));
            }
            for p in parents {
                if p.state.is_terminal() {
                    if p.state != JobState::Succeeded {
                        return Ok(Deps::Failed(format!("job {} ({}) {}", p.id, p.title, p.state.as_str())));
                    }
                } else {
                    track(&p, &mut unfinished);
                }
            }
        }
        for r in &job.spec.after_any {
            for p in self.dep_jobs(r)? {
                if !p.state.is_terminal() {
                    track(&p, &mut unfinished);
                }
            }
        }
        Ok(if unfinished.is_empty() { Deps::Met } else { Deps::Pending(unfinished, latest) })
    }

    async fn resolve_deps(self: &Arc<Self>) -> Result<()> {
        let in_flight = lock(&self.in_flight).clone();
        let mut native: HashMap<(String, String), (Shape, Vec<Job>)> = HashMap::new();
        for job in self.store.jobs_in_states(&[JobState::Waiting])? {
            if in_flight.contains(&job.id) {
                continue;
            }
            match self.deps(&job)? {
                Deps::Met => {
                    self.update(job.id, JobUpdate { state: Some(JobState::Queued), reason: Some(Some("dependencies met".into())), ..Default::default() })?;
                    self.kick();
                }
                Deps::Failed(why) => self.fail_dependency(&job, &why)?,
                Deps::Pending(parents, latest) => {
                    if let Some((name, shape, start)) = self.native_dependency(&job, &parents, latest).await? {
                        let finish = start + chrono::Duration::seconds(job.resources.time_s as i64);
                        self.update(
                            job.id,
                            JobUpdate {
                                state: Some(JobState::Staging),
                                backend: Some(Some(name.clone())),
                                reason: Some(Some(format!("submitted to {name} with --dependency={}", shape.dependency.clone().unwrap_or_default()))),
                                est_start: Some(Some(start)),
                                est_finish: Some(Some(finish)),
                                est_source: Some(Some("daemon".into())),
                                ..Default::default()
                            },
                        )?;
                        let mut job = job;
                        job.state = JobState::Staging;
                        job.backend = Some(name.clone());
                        native.entry((name, job.group_key())).or_insert_with(|| (shape, Vec::new())).1.push(job);
                        continue;
                    }
                    let names: Vec<String> = parents.iter().take(3).map(|p| format!("{} ({})", p.id, p.state.as_str())).collect();
                    let more = if parents.len() > 3 { format!(" and {} more", parents.len() - 3) } else { String::new() };
                    let reason = Some(format!("waiting for job {}{more}", names.join(", ")));
                    let est = latest.map(|t| t.max(Utc::now()));
                    if job.reason != reason || job.est_start.map(|t| t.timestamp() / 60) != est.map(|t| t.timestamp() / 60) {
                        self.update(
                            job.id,
                            JobUpdate {
                                reason: Some(reason),
                                est_start: Some(est),
                                est_finish: Some(est.map(|t| t + chrono::Duration::seconds(job.resources.time_s as i64))),
                                est_source: Some(est.map(|_| "dependencies".into())),
                                ..Default::default()
                            },
                        )?;
                    }
                }
            }
        }
        for ((name, _), (shape, jobs)) in native {
            if let Some(b) = self.backend(&name) {
                self.spawn_launch(b, jobs, shape);
            }
        }
        // Children submitted with native Slurm dependencies: stop them if a parent failed.
        for job in self.store.jobs_in_states(&[JobState::Pending])? {
            if job.handle.as_ref().is_some_and(|h| h["native_deps"].as_bool() == Some(true))
                && let Deps::Failed(why) = self.deps(&job)?
            {
                if let Some(b) = job.backend.as_deref().and_then(|n| self.backend(n)) {
                    let _ = b.cancel(&job).await;
                }
                self.fail_dependency(&job, &why)?;
            }
        }
        Ok(())
    }

    fn fail_dependency(&self, job: &Job, why: &str) -> Result<()> {
        self.update(
            job.id,
            JobUpdate {
                state: Some(JobState::DependencyFailed),
                ended_at: Some(Some(Utc::now())),
                reason: Some(Some(format!("dependency_failed: {why}"))),
                est_start: Some(None),
                est_finish: Some(None),
                ..Default::default()
            },
        )
    }

    /// When every unfinished parent is queued or running on one Slurm backend and the child
    /// can only run there, the child is submitted right away with `--dependency`, so it ages
    /// in Slurm's queue while its parents run.
    async fn native_dependency(&self, job: &Job, parents: &[Job], latest: Option<DateTime<Utc>>) -> Result<Option<(String, Shape, DateTime<Utc>)>> {
        let Some(first) = parents.first().and_then(|p| p.backend.clone()) else { return Ok(None) };
        let Some(b) = self.backend(&first) else { return Ok(None) };
        if b.config().kind != BackendKind::Slurm {
            return Ok(None);
        }
        let mut ok_ids = Vec::new();
        let mut any_ids = Vec::new();
        let after_ok: HashSet<i64> = job
            .spec
            .after_ok
            .iter()
            .flat_map(|r| self.dep_jobs(r).unwrap_or_default().into_iter().map(|j| j.id))
            .collect();
        for p in parents {
            if p.backend.as_deref() != Some(first.as_str()) || !matches!(p.state, JobState::Pending | JobState::Running) {
                return Ok(None);
            }
            let Some(h) = &p.handle else { return Ok(None) };
            let Some(id) = h["slurm_id"].as_str() else { return Ok(None) };
            let key = match h["task"].as_u64() {
                Some(t) => format!("{id}_{t}"),
                None => id.to_string(),
            };
            if after_ok.contains(&p.id) { ok_ids.push(key) } else { any_ids.push(key) }
        }
        // Inputs from parents elsewhere must already exist; same-backend ones are copied by the runner.
        for inp in &job.spec.inputs {
            if let Some(p) = self.store.get_job(inp.from_job)?
                && !p.state.is_terminal()
                && p.backend.as_deref() != Some(first.as_str())
            {
                return Ok(None);
            }
        }
        if job.spec.container.as_ref().is_some_and(|c| c.build.is_some()) {
            match self.store.image(&job.spec.container.as_ref().map(|c| c.image.clone()).unwrap_or_default())? {
                Some(row) if row.state == "ready" => {}
                _ => return Ok(None),
            }
        }
        let plan = self.plan(job).await?;
        let only_here = plan.quotes.iter().all(|(n, q)| n == &first || matches!(q, Quote::No { permanent: true, .. }));
        let Some(best) = plan.best.filter(|b| b.backend == first) else { return Ok(None) };
        if !only_here {
            return Ok(None);
        }
        let mut dep = Vec::new();
        if !ok_ids.is_empty() {
            dep.push(format!("afterok:{}", ok_ids.join(":")));
        }
        if !any_ids.is_empty() {
            dep.push(format!("afterany:{}", any_ids.join(":")));
        }
        let mut shape = best.shape;
        shape.dependency = Some(dep.join(","));
        let start = match latest {
            Some(l) => best.start.max(l),
            None => best.start,
        };
        Ok(Some((first, shape, start)))
    }

    // ---------------------------------------------------------------- polling

    async fn poll_all(self: &Arc<Self>) -> Result<()> {
        let live = self.store.jobs_in_states(&[JobState::Pending, JobState::Running])?;
        let in_flight = lock(&self.in_flight).clone();
        let mut groups: HashMap<String, Vec<Job>> = HashMap::new();
        for j in live {
            if in_flight.contains(&j.id) {
                continue;
            }
            if let Some(b) = &j.backend {
                groups.entry(b.clone()).or_default().push(j);
            }
        }
        for (name, jobs) in groups {
            let Some(backend) = self.backend(&name) else { continue };
            if backend.root().is_none() {
                // Not probed yet in this daemon run; wait for the probe.
                continue;
            }
            match tokio::time::timeout(Duration::from_secs(60), backend.poll(&jobs)).await {
                Ok(Ok(results)) => {
                    self.set_runtime(&name, |r| r.reachable = true);
                    let by_id: HashMap<i64, Job> = jobs.into_iter().map(|j| (j.id, j)).collect();
                    for r in results {
                        if let Some(job) = by_id.get(&r.id) {
                            self.apply_poll(job, r).await?;
                        }
                    }
                }
                Ok(Err(e)) => {
                    self.mark_unreachable(&name, format!("{e:#}"));
                    self.check_lost(&jobs, None).await?;
                }
                Err(_) => {
                    self.mark_unreachable(&name, "poll timed out".into());
                    self.check_lost(&jobs, None).await?;
                }
            }
        }
        Ok(())
    }

    async fn apply_poll(&self, job: &Job, r: crate::backend::PollResult) -> Result<()> {
        let now = Utc::now();
        match r.state {
            Some(s) if s.is_terminal() => {
                // Slurm kills a job whose --dependency can no longer be met; record why.
                if s == JobState::Cancelled
                    && job.handle.as_ref().is_some_and(|h| h["native_deps"].as_bool() == Some(true))
                    && let Deps::Failed(why) = self.deps(job)?
                {
                    return self.fail_dependency(job, &why);
                }
                let mut reason = job.reason.clone();
                if let Some(n) = r.note {
                    reason = Some(n);
                }
                let ended = r.ended.unwrap_or(now);
                if let Some(cost) = self.end_cost(job, ended)? {
                    reason = Some(format!("{} (cost ${cost:.2})", reason.unwrap_or_default()).trim_start().to_string());
                }
                self.update(
                    job.id,
                    JobUpdate {
                        state: Some(s),
                        exit_code: Some(r.exit_code),
                        started_at: Some(job.started_at.or(r.started)),
                        ended_at: Some(Some(ended)),
                        last_seen: Some(Some(now)),
                        reason: Some(reason),
                        ..Default::default()
                    },
                )?;
            }
            Some(JobState::Running) => {
                let seen = [r.heartbeat, r.started, job.started_at].into_iter().flatten().max();
                let mut u = JobUpdate { last_seen: Some(seen.or(job.last_seen)), ..Default::default() };
                if job.state != JobState::Running {
                    let started = r.started.unwrap_or(now);
                    u.state = Some(JobState::Running);
                    u.started_at = Some(Some(started));
                    u.est_start = Some(Some(started));
                    u.est_finish = Some(Some(started + chrono::Duration::seconds(job.resources.time_s as i64)));
                    u.est_source = Some(Some("actual".into()));
                }
                self.update(job.id, u)?;
                if let Some(seen) = seen
                    && (now - seen).num_seconds() > LOST_AFTER
                    && r.heartbeat.is_some()
                {
                    self.mark_lost(job, "no heartbeat for 5 minutes").await?;
                }
            }
            Some(JobState::Pending) => {
                if let Some(start) = r.est_start
                    && job.est_start != Some(start)
                {
                    self.update(
                        job.id,
                        JobUpdate {
                            est_start: Some(Some(start)),
                            est_finish: Some(Some(start + chrono::Duration::seconds(job.resources.time_s as i64))),
                            est_source: Some(Some("slurm".into())),
                            ..Default::default()
                        },
                    )?;
                }
            }
            _ => {
                // No status yet. A local job whose runner never wrote status.json, or a Slurm job
                // that vanished from squeue and sacct, is lost after a grace period.
                let since = job.started_at.unwrap_or(job.created_at);
                if (now - since).num_seconds() > NO_STATUS_AFTER && job.state == JobState::Running {
                    self.mark_lost(job, "no status from the runner for 10 minutes").await?;
                }
            }
        }
        Ok(())
    }

    /// Closes a paid job's cost record; returns its cost.
    fn end_cost(&self, job: &Job, ended: DateTime<Utc>) -> Result<Option<f64>> {
        match job.backend.as_deref().and_then(|n| self.backend(n)) {
            Some(b) if b.is_paid() => self.store.cost_end(job.id, ended),
            _ => Ok(None),
        }
    }

    async fn check_lost(&self, jobs: &[Job], _err: Option<String>) -> Result<()> {
        let now = Utc::now();
        for j in jobs.iter().filter(|j| j.state == JobState::Running) {
            let seen = j.last_seen.or(j.started_at).unwrap_or(j.created_at);
            if (now - seen).num_seconds() > LOST_AFTER {
                self.mark_lost(j, "backend unreachable for 5 minutes").await?;
            }
        }
        Ok(())
    }

    async fn mark_lost(&self, job: &Job, why: &str) -> Result<()> {
        let backend = job.backend.clone().unwrap_or_default();
        if let Some(b) = self.backend(&backend) {
            let _ = tokio::time::timeout(Duration::from_secs(60), b.cancel(job)).await;
        }
        let _ = self.end_cost(job, Utc::now());
        if job.retries_left > 0 {
            tracing::warn!("job {} lost on {backend} ({why}); requeueing", job.id);
            self.update(
                job.id,
                JobUpdate {
                    state: Some(JobState::Queued),
                    backend: Some(None),
                    handle: Some(None),
                    started_at: Some(None),
                    retries_left: Some(job.retries_left - 1),
                    reason: Some(Some(format!("requeued: lost on {backend} ({why})"))),
                    ..Default::default()
                },
            )
        } else {
            tracing::warn!("job {} lost on {backend} ({why})", job.id);
            self.update(
                job.id,
                JobUpdate {
                    state: Some(JobState::Lost),
                    ended_at: Some(Some(Utc::now())),
                    reason: Some(Some(format!("lost on {backend}: {why}"))),
                    ..Default::default()
                },
            )
        }
    }

    // ---------------------------------------------------------------- budget

    fn month_start(&self) -> DateTime<Utc> {
        let now = Utc::now().with_timezone(&self.tz);
        self.tz
            .with_ymd_and_hms(now.year(), now.month(), 1, 0, 0, 0)
            .earliest()
            .map(|t| t.with_timezone(&Utc))
            .unwrap_or_else(Utc::now)
    }

    /// `(spent this month, committed by running cloud jobs until their time limits)`.
    pub fn spend(&self) -> Result<(f64, f64)> {
        let now = Utc::now();
        let since = self.month_start();
        let mut spent = 0.0;
        let mut committed = 0.0;
        for (job_id, _, rate, started, ended, cost) in self.store.costs_since(since)? {
            match (ended, cost) {
                (Some(_), Some(c)) => spent += c,
                _ => {
                    let from = started.max(since);
                    let hours = (now - from).num_seconds().max(0) as f64 / 3600.0;
                    spent += rate * hours;
                    if let Ok(Some(j)) = self.store.get_job(job_id) {
                        let limit = started + chrono::Duration::seconds(j.resources.time_s as i64 + 600);
                        committed += rate * ((limit - now).num_seconds().max(0) as f64 / 3600.0);
                    }
                }
            }
        }
        Ok((spent, committed))
    }

    /// Bytes used on the coordinator by outputs copied off cloud instances.
    pub fn outputs_used(&self) -> u64 {
        fn walk(p: &std::path::Path) -> u64 {
            let Ok(rd) = std::fs::read_dir(p) else { return 0 };
            rd.flatten()
                .map(|e| match e.file_type() {
                    Ok(t) if t.is_dir() => walk(&e.path()),
                    Ok(_) => e.metadata().map(|m| m.len()).unwrap_or(0),
                    Err(_) => 0,
                })
                .sum()
        }
        walk(&self.paths.outputs_dir())
    }

    // ---------------------------------------------------------------- placement

    /// Resources held on a local backend by its staging and running jobs.
    fn allocs(&self, backend: &str) -> Result<Vec<Alloc>> {
        let now = Utc::now();
        Ok(self
            .store
            .jobs_in_states(&[JobState::Staging, JobState::Running])?
            .into_iter()
            .filter(|j| j.backend.as_deref() == Some(backend))
            .map(|j| Alloc {
                ends_at: j.started_at.unwrap_or(now) + chrono::Duration::seconds(j.resources.time_s as i64),
                cpus: j.resources.cpus,
                mem_mb: j.resources.mem_mb,
            })
            .collect())
    }

    /// Quotes from every backend, with the reason each one can't take the job.
    pub async fn plan(&self, job: &Job) -> Result<Placement> {
        let mut quotes = Vec::new();
        let (spent, committed) = self.spend()?;
        let remaining = self.budget.monthly_usd - spent - committed;
        for b in &self.backends {
            let name = b.name().to_string();
            let q = if job.backends.as_ref().is_some_and(|l| !l.contains(&name)) {
                Quote::no("not in the job's `backends` list", true)
            } else if b.is_paid() && job.spec.max_cost_usd.is_none() {
                Quote::no("paid backend; set max_cost_usd to allow it", true)
            } else if !self.enabled(&name)?.0 {
                Quote::no(format!("{name} is disabled"), false)
            } else if !self.runtime(&name).reachable || b.root().is_none() {
                let err = self.runtime(&name).last_error.unwrap_or_else(|| "not probed yet".into());
                Quote::no(format!("unreachable: {err}"), false)
            } else {
                match b.quote(job, &self.allocs(&name)?).await {
                    Quote::Ok { start, source, mut reason, shape, cost } => {
                        if let Some(c) = cost
                            && b.is_paid()
                            && c > remaining
                        {
                            Quote::no(
                                format!("estimated ${c:.2} is over the monthly budget (${remaining:.2} left of ${:.2})", self.budget.monthly_usd),
                                false,
                            )
                        } else {
                            let stage = self.staging_seconds(job, &name);
                            let mut start = start;
                            if stage >= 60.0 {
                                start = start.max(Utc::now() + chrono::Duration::seconds(stage as i64));
                                reason = format!("{reason}; ~{:.0} min to copy datasets", stage / 60.0);
                            }
                            Quote::Ok { start, source, reason, shape, cost }
                        }
                    }
                    no => no,
                }
            };
            quotes.push((name, q));
        }
        // Prefer a backend that already holds the job's inputs.
        let mut local_inputs: HashMap<String, usize> = HashMap::new();
        for inp in &job.spec.inputs {
            if let Ok(Some(p)) = self.store.get_job(inp.from_job)
                && let Some(b) = p.backend
            {
                *local_inputs.entry(b).or_default() += 1;
            }
        }
        let rank = |name: &str| -> (u8, usize) {
            let kind = match self.backend(name).map(|b| b.config().kind) {
                Some(BackendKind::Local) => 0,
                Some(BackendKind::Slurm) => 1,
                _ => 2,
            };
            (kind, usize::MAX - local_inputs.get(name).copied().unwrap_or(0))
        };
        let best = quotes
            .iter()
            .filter_map(|(n, q)| match q {
                Quote::Ok { start, source, reason, shape, cost } => Some(Best {
                    backend: n.clone(),
                    start: *start,
                    source: source.clone(),
                    reason: reason.clone(),
                    shape: shape.clone(),
                    cost: *cost,
                }),
                Quote::No { .. } => None,
            })
            // Compare starts to the minute so "now" on two backends is a tie; then free before
            // paid, inputs already there, local before Slurm, and cheaper first.
            .min_by(|a, b| {
                let ka = (a.start.timestamp() / 60, a.cost.is_some(), rank(&a.backend));
                let kb = (b.start.timestamp() / 60, b.cost.is_some(), rank(&b.backend));
                ka.cmp(&kb).then(a.cost.unwrap_or(0.0).total_cmp(&b.cost.unwrap_or(0.0)))
            });
        Ok(Placement { quotes, best })
    }

    async fn place_queued(self: &Arc<Self>) -> Result<()> {
        let mut queued = self.store.jobs_in_states(&[JobState::Queued])?;
        queued.sort_by_key(|j| (-j.priority, j.id));
        let in_flight = lock(&self.in_flight).clone();
        let mut batches: HashMap<(String, String), (Shape, Vec<Job>)> = HashMap::new();
        let now = Utc::now();
        for job in queued {
            if in_flight.contains(&job.id) {
                continue;
            }
            match self.image_status(&job)? {
                ImageStatus::Ready => {}
                ImageStatus::Building(tag) => {
                    let reason = Some(format!("waiting for the image build {tag} (GET /v1/images)"));
                    if job.reason != reason {
                        self.update(job.id, JobUpdate { reason: Some(reason), ..Default::default() })?;
                    }
                    continue;
                }
                ImageStatus::Failed(err) => {
                    self.update(
                        job.id,
                        JobUpdate {
                            state: Some(JobState::Failed),
                            ended_at: Some(Some(Utc::now())),
                            reason: Some(Some(format!("build_failed: {err}"))),
                            ..Default::default()
                        },
                    )?;
                    continue;
                }
            }
            let plan = self.plan(&job).await?;
            let Some(best) = plan.best else {
                let why: Vec<String> = plan
                    .quotes
                    .iter()
                    .filter_map(|(n, q)| match q {
                        Quote::No { reason, .. } => Some(format!("{n}: {reason}")),
                        Quote::Ok { .. } => None,
                    })
                    .collect();
                let reason = Some(format!("waiting: {}", why.join("; ")));
                if job.reason != reason {
                    self.update(job.id, JobUpdate { reason: Some(reason), est_start: Some(None), est_finish: Some(None), est_source: Some(None), ..Default::default() })?;
                }
                continue;
            };
            let Best { backend: name, start, source, reason, shape, cost } = best;
            let Some(backend) = self.backend(&name) else { continue };
            let finish = start + chrono::Duration::seconds(job.resources.time_s as i64);
            let kind = backend.config().kind;
            if kind == BackendKind::Local && start > now + chrono::Duration::seconds(5) {
                // Best option is a busy local host: keep waiting here, and keep the estimate fresh.
                if job.est_start.map(|t| t.timestamp() / 60) != Some(start.timestamp() / 60) || job.backend.is_some() {
                    self.update(
                        job.id,
                        JobUpdate {
                            reason: Some(Some(format!("queued for {name}: {reason}"))),
                            est_start: Some(Some(start)),
                            est_finish: Some(Some(finish)),
                            est_source: Some(Some(source)),
                            ..Default::default()
                        },
                    )?;
                }
                continue;
            }
            let reason = match cost {
                Some(c) => format!("{reason} (estimated ${c:.2})"),
                None => reason,
            };
            self.update(
                job.id,
                JobUpdate {
                    state: Some(JobState::Staging),
                    backend: Some(Some(name.clone())),
                    reason: Some(Some(reason)),
                    est_start: Some(Some(start)),
                    est_finish: Some(Some(finish)),
                    est_source: Some(Some(source)),
                    ..Default::default()
                },
            )?;
            let mut job = job;
            job.state = JobState::Staging;
            job.backend = Some(name.clone());
            if kind == BackendKind::Slurm {
                batches.entry((name, job.group_key())).or_insert_with(|| (shape, Vec::new())).1.push(job);
            } else {
                self.spawn_launch(backend, vec![job], shape);
            }
        }
        for ((name, _), (shape, jobs)) in batches {
            if let Some(b) = self.backend(&name) {
                self.spawn_launch(b, jobs, shape);
            }
        }
        Ok(())
    }

    fn spawn_launch(self: &Arc<Self>, backend: Arc<dyn Backend>, jobs: Vec<Job>, shape: Shape) {
        {
            let mut f = lock(&self.in_flight);
            for j in &jobs {
                f.insert(j.id);
            }
        }
        let me = self.clone();
        tokio::spawn(async move {
            let ids: Vec<i64> = jobs.iter().map(|j| j.id).collect();
            let mut last_err = String::new();
            let mut done = false;
            // A paid launch is not retried blindly: one attempt, then the job fails with the reason.
            let attempts = if backend.is_paid() { 1 } else { STAGE_ATTEMPTS };
            for attempt in 1..=attempts {
                match me.stage_and_launch(&backend, &jobs, &shape).await {
                    Ok(()) => {
                        done = true;
                        break;
                    }
                    Err(e) => {
                        last_err = format!("{e:#}");
                        tracing::warn!("staging {:?} on {} (attempt {attempt}): {last_err}", ids, backend.name());
                        if attempt < attempts {
                            tokio::time::sleep(Duration::from_secs(5 * u64::from(attempt))).await;
                        }
                    }
                }
            }
            if !done {
                for id in &ids {
                    if backend.is_paid()
                        && let Ok(Some(j)) = me.store.get_job(*id)
                    {
                        let _ = backend.cancel(&j).await;
                    }
                    let _ = me.update(
                        *id,
                        JobUpdate {
                            state: Some(JobState::Failed),
                            ended_at: Some(Some(Utc::now())),
                            reason: Some(Some(format!("staging_failed: {}", crate::backend::tail(&last_err, 1500)))),
                            ..Default::default()
                        },
                    );
                }
            }
            {
                let mut f = lock(&me.in_flight);
                for id in &ids {
                    f.remove(id);
                }
            }
            me.kick();
        });
    }

    /// Files and settings for staging `job` on `backend`: secrets, registry credentials,
    /// dataset copies and same-backend inputs.
    async fn stage_files(&self, backend: &Arc<dyn Backend>, job: &Job) -> Result<StageFiles> {
        let mut files = StageFiles { secrets_env: self.secrets.env_file(&job.secrets)?, ..Default::default() };
        if let Some(c) = &job.spec.container {
            let runtimes = backend.runtimes();
            let rt = if runtimes.iter().any(|r| r == "docker") {
                "docker"
            } else if runtimes.iter().any(|r| r == "apptainer") {
                "apptainer"
            } else {
                "native" // cloud backends that run the image itself
            };
            let from_registry = self.registry.host().is_some_and(|h| c.image.starts_with(&format!("{h}/")));
            match rt {
                "docker" if from_registry && !backend.config().kind.is_cloud() => self.registry_login(backend.as_ref()).await?,
                "apptainer" => files.registry_env = self.registry_env(&c.image)?,
                _ => {}
            }
            if rt != "native" {
                files.runtime = Some(rt.into());
            }
        }
        for d in &job.spec.datasets {
            let path = self.ensure_dataset(d, backend, job).await.with_context(|| format!("staging dataset {}@{}", d.name, d.version))?;
            files.datasets.push((d.name.clone(), path));
        }
        if !backend.config().kind.is_cloud() {
            let (_, root) = backend.shell_for(None).await?;
            for inp in &job.spec.inputs {
                let parent = self.store.get_job(inp.from_job)?.with_context(|| format!("input job {} is gone", inp.from_job))?;
                if parent.backend.as_deref() == Some(backend.name()) {
                    files.inputs.push((
                        format!("{}/out", crate::backend::Remote::jobdir(&root, parent.id)),
                        inp.glob.clone().unwrap_or_else(|| "*".into()),
                        inp.as_.clone(),
                    ));
                }
            }
        }
        Ok(files)
    }

    /// Copies inputs whose parent ran on another backend (or in the cloud) into the job's
    /// `in/<as>` directory.
    async fn copy_inputs(&self, backend: &Arc<dyn Backend>, job: &Job) -> Result<()> {
        for inp in &job.spec.inputs {
            let parent = self.store.get_job(inp.from_job)?.with_context(|| format!("input job {} is gone", inp.from_job))?;
            let same = parent.backend.as_deref() == Some(backend.name()) && !backend.config().kind.is_cloud();
            if same {
                continue;
            }
            let pb = parent
                .backend
                .as_deref()
                .and_then(|n| self.backend(n))
                .with_context(|| format!("input job {} never ran", parent.id))?;
            let data = pb.fetch(&parent, inp.glob.as_deref()).await.with_context(|| format!("reading outputs of job {}", parent.id))?;
            backend
                .receive(job, &format!("in/{}", inp.as_), data)
                .await
                .with_context(|| format!("copying outputs of job {} to {}", parent.id, backend.name()))?;
        }
        Ok(())
    }

    async fn stage_and_launch(&self, backend: &Arc<dyn Backend>, jobs: &[Job], shape: &Shape) -> Result<()> {
        // Skip jobs cancelled while waiting to stage.
        let mut live = Vec::new();
        for j in jobs {
            match self.store.get_job(j.id)? {
                Some(cur) if cur.state == JobState::Staging => live.push(cur),
                _ => {}
            }
        }
        if live.is_empty() {
            return Ok(());
        }
        for j in &live {
            let path = self.snapshots.path(&j.snapshot);
            self.snapshots.touch(&j.snapshot)?;
            let files = self.stage_files(backend, j).await?;
            backend.stage(j, &path, &files).await.with_context(|| format!("staging job {}", j.id))?;
            self.copy_inputs(backend, j).await?;
        }
        let handles = backend.launch(&live, shape).await?;
        let kind = backend.config().kind;
        let now = Utc::now();
        for (j, mut h) in live.iter().zip(handles) {
            if shape.dependency.is_some() {
                h["native_deps"] = true.into();
            }
            let mut u = JobUpdate { handle: Some(Some(h)), ..Default::default() };
            if kind == BackendKind::Local {
                u.state = Some(JobState::Running);
                u.started_at = Some(Some(now));
                u.last_seen = Some(Some(now));
            } else {
                u.state = Some(JobState::Pending);
            }
            if backend.is_paid()
                && let Some(rate) = shape.usd_per_hour
            {
                self.store.cost_start(j.id, backend.name(), rate, now)?;
            }
            self.update(j.id, u)?;
            // Cancelled between the check above and the launch: stop it now.
            if self.store.get_job(j.id)?.is_some_and(|c| c.state == JobState::Cancelled) {
                let mut launched = j.clone();
                launched.handle = self.store.get_job(j.id)?.and_then(|c| c.handle);
                let _ = backend.cancel(&launched).await;
                let _ = self.end_cost(j, Utc::now());
            }
        }
        Ok(())
    }

    pub async fn cancel(&self, job: &Job) -> Result<()> {
        if job.state.is_terminal() {
            return Ok(());
        }
        if matches!(job.state, JobState::Pending | JobState::Running)
            && let Some(b) = job.backend.as_deref().and_then(|n| self.backend(n))
        {
            b.cancel(job).await?;
        }
        let _ = self.end_cost(job, Utc::now());
        self.update(
            job.id,
            JobUpdate {
                state: Some(JobState::Cancelled),
                ended_at: Some(Some(Utc::now())),
                reason: Some(Some("cancelled by request".into())),
                ..Default::default()
            },
        )
    }

    /// Live availability for a backend, refreshed at most once a minute.
    pub async fn availability(&self, name: &str) -> Option<serde_json::Value> {
        let rt = self.runtime(name);
        if let (Some(a), Some(at)) = (&rt.availability, rt.availability_at)
            && (Utc::now() - at).num_seconds() < 60
        {
            return Some(a.clone());
        }
        let b = self.backend(name)?;
        if !rt.reachable || b.root().is_none() {
            return rt.availability;
        }
        match tokio::time::timeout(Duration::from_secs(30), b.availability()).await {
            Ok(Ok(mut a)) => {
                if b.config().kind == BackendKind::Local {
                    let allocs = self.allocs(name).unwrap_or_default();
                    let cap_cpus = b.capacity().map(|c| c.cpus).unwrap_or(0);
                    a["cpus_in_use"] = allocs.iter().map(|x| x.cpus).sum::<u32>().into();
                    a["mem_gb_in_use"] = (allocs.iter().map(|x| x.mem_mb).sum::<u64>() as f64 / 1024.0).into();
                    a["cpus_free"] = cap_cpus.saturating_sub(allocs.iter().map(|x| x.cpus).sum::<u32>()).into();
                    a["running_jobs"] = allocs.len().into();
                }
                self.set_runtime(name, |r| {
                    r.availability = Some(a.clone());
                    r.availability_at = Some(Utc::now());
                });
                Some(a)
            }
            _ => rt.availability,
        }
    }
}
