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
/// States a job can still leave on its own; writes that end or move a job check for these.
const LIVE: &[JobState] = &[JobState::Waiting, JobState::Queued, JobState::Staging, JobState::Pending, JobState::Running];
/// States of a job the backend has.
const LAUNCHED: &[JobState] = &[JobState::Pending, JobState::Running];
const NO_STATUS_AFTER: i64 = 10 * 60;
const STAGE_ATTEMPTS: u32 = 3;

/// Room a queued job holds on a busy local backend from its estimated start.
pub struct Reserved {
    job: i64,
    start: DateTime<Utc>,
    alloc: Alloc,
}

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
    /// Hourly rates of paid jobs placed but not launched yet (launch records the real cost row).
    reserved: Mutex<HashMap<i64, f64>>,
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
            reserved: Mutex::new(HashMap::new()),
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

    /// Updates a job only while the job is in one of `states`; false when it has moved on (e.g.
    /// it was cancelled since it was read).
    pub fn update_if(&self, id: i64, states: &[JobState], u: JobUpdate) -> Result<bool> {
        let applied = self.store.update_job_if(id, states, u)?;
        if applied {
            self.bump();
        }
        Ok(applied)
    }

    /// Startup: jobs caught mid-staging go back to the queue, except cloud jobs that already
    /// have an instance, which resume staging on it (see `resume_staging`); running jobs get a
    /// fresh grace period so a long daemon outage doesn't mark them lost before the first poll.
    pub fn reconcile(&self) -> Result<()> {
        for j in self.store.jobs_in_states(&[JobState::Staging])? {
            if self.resumable(&j) {
                continue;
            }
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

    /// A cloud job whose instance was recorded while staging.
    fn resumable(&self, j: &Job) -> bool {
        j.backend.as_deref().and_then(|n| self.backend(n)).is_some_and(|b| b.is_paid())
            && j.handle.as_ref().is_some_and(|h| h["staging"].as_bool() == Some(true) && h["instance"].is_object())
    }

    /// After a restart: finish staging the cloud jobs `reconcile` left in Staging, once their
    /// backend has been probed. (Jobs staging in this run are in flight and skipped.)
    fn resume_staging(self: &Arc<Self>) {
        let Ok(jobs) = self.store.jobs_in_states(&[JobState::Staging]) else { return };
        let in_flight = lock(&self.in_flight).clone();
        for j in jobs.into_iter().filter(|j| !in_flight.contains(&j.id) && self.resumable(j)) {
            if let Some(b) = j.backend.as_deref().and_then(|n| self.backend(n))
                && b.root().is_some()
            {
                tracing::info!("job {}: resuming staging on {}", j.id, b.name());
                self.spawn_launch(b, vec![j], Shape::default());
            }
        }
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
        self.resume_staging();
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
        self.tidy_hosts().await;
    }

    /// Local and Slurm backends: re-measure free disk and Docker memory, and delete outputs
    /// older than the backend's keep_outputs_days (keeping any a live job still needs as inputs)
    /// along with stale entries of the shared input cache.
    async fn tidy_hosts(&self) {
        let needed: HashSet<i64> = self
            .store
            .jobs_in_states(LIVE)
            .map(|js| js.iter().flat_map(|j| j.spec.inputs.iter().map(|i| i.from_job)).collect())
            .unwrap_or_default();
        for b in self.backends.iter().filter(|b| !b.is_paid() && b.root().is_some()) {
            let name = b.name().to_string();
            if b.config().kind == BackendKind::Local
                && let Ok(Err(e)) = tokio::time::timeout(Duration::from_secs(60), b.refresh()).await
            {
                tracing::warn!("measuring {name}: {e:#}");
            }
            let days = b.config().keep_outputs_days;
            let before = Utc::now() - chrono::Duration::days(i64::from(days));
            let Ok(ids) = self.store.outputs_to_expire(&name, before, 200) else { continue };
            for id in ids.into_iter().filter(|id| !needed.contains(id)) {
                let Ok(Some(j)) = self.store.get_job(id) else { continue };
                match tokio::time::timeout(Duration::from_secs(60), b.delete_outputs(&j)).await {
                    Ok(Ok(())) => {
                        let _ = self.store.mark_outputs_deleted(id, &format!("kept {days} days (keep_outputs_days on {name})"));
                    }
                    Ok(Err(e)) => {
                        tracing::warn!("deleting old outputs of job {id} on {name}: {e:#}");
                        break;
                    }
                    Err(_) => break,
                }
            }
            if let Ok((shell, root)) = b.shell_for(None).await {
                let cache = crate::transport::shell_quote(&format!("{root}/cache/inputs"));
                // Entries are named <parent job>-<glob hash>; keep the ones live jobs need.
                let keep: String = needed.iter().map(|id| format!(" ! -name '{id}-*'")).collect();
                let _ = shell.run(&format!("[ -d {cache} ] && find {cache} -mindepth 1 -maxdepth 1 -mtime +{days}{keep} -exec rm -rf {{}} + ; true")).await;
            }
        }
    }

    async fn probe_due(self: &Arc<Self>) {
        // A disabled backend takes no new jobs, but the jobs it has still need polling.
        let busy: HashSet<String> = self
            .store
            .jobs_in_states(&[JobState::Staging, JobState::Pending, JobState::Running])
            .map(|js| js.into_iter().filter_map(|j| j.backend).collect())
            .unwrap_or_default();
        let mut due = Vec::new();
        for b in &self.backends {
            let name = b.name().to_string();
            if !self.enabled(&name).map(|(on, _)| on).unwrap_or(false) && !busy.contains(&name) {
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
                        let why = p.reason.as_deref().map(|r| format!(": {}", crate::backend::tail(r, 200))).unwrap_or_default();
                        return Ok(Deps::Failed(format!(
                            "job {} ({}) {}{why}. Resubmit that job, then this one with the new id in after_ok/inputs",
                            p.id,
                            p.title,
                            p.state.as_str()
                        )));
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
                    self.update_if(job.id, &[JobState::Waiting], JobUpdate { state: Some(JobState::Queued), reason: Some(Some("dependencies met".into())), ..Default::default() })?;
                    self.kick();
                }
                Deps::Failed(why) => self.fail_dependency(&job, &why)?,
                Deps::Pending(parents, latest) => {
                    if let Some((name, shape, start)) = self.native_dependency(&job, &parents, latest).await? {
                        let finish = start + chrono::Duration::seconds(job.resources.time_s as i64);
                        if !self.update_if(
                            job.id,
                            &[JobState::Waiting],
                            JobUpdate {
                                state: Some(JobState::Staging),
                                backend: Some(Some(name.clone())),
                                reason: Some(Some(format!("submitted to {name} with --dependency={}", shape.dependency.clone().unwrap_or_default()))),
                                est_start: Some(Some(start)),
                                est_finish: Some(Some(finish)),
                                est_source: Some(Some("daemon".into())),
                                ..Default::default()
                            },
                        )? {
                            continue;
                        }
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
                        self.update_if(
                            job.id,
                            &[JobState::Waiting],
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
        self.update_if(
            job.id,
            LIVE,
            JobUpdate {
                state: Some(JobState::DependencyFailed),
                ended_at: Some(Some(Utc::now())),
                reason: Some(Some(format!("dependency_failed: {why}"))),
                est_start: Some(None),
                est_finish: Some(None),
                ..Default::default()
            },
        )
        .map(|_| ())
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
            let Some(backend) = self.backend(&name) else {
                // The backend was removed from hosts.toml: nothing can reach these jobs now.
                for j in &jobs {
                    let _ = self.end_cost(j, Utc::now());
                    self.update_if(
                        j.id,
                        LAUNCHED,
                        JobUpdate {
                            state: Some(JobState::Lost),
                            ended_at: Some(Some(Utc::now())),
                            reason: Some(Some(format!(
                                "lost: backend {name} is no longer configured; if the job (or a cloud instance) still runs there, stop it by hand"
                            ))),
                            ..Default::default()
                        },
                    )?;
                }
                continue;
            };
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
                // Its linked inputs and dataset view were only for the run.
                if let Some(b) = job.backend.as_deref().and_then(|n| self.backend(n)) {
                    let j = job.clone();
                    tokio::spawn(async move {
                        if let Ok(Err(e)) = tokio::time::timeout(Duration::from_secs(120), b.delete_inputs(&j)).await {
                            tracing::warn!("removing the inputs of job {}: {e:#}", j.id);
                        }
                    });
                }
                // A cloud instance bills until teardown, which is done by now.
                if let Some(cost) = self.end_cost(job, now)? {
                    reason = Some(format!("{} (cost ${cost:.2})", reason.unwrap_or_default()).trim_start().to_string());
                }
                self.update_if(
                    job.id,
                    LAUNCHED,
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
                if !self.update_if(job.id, LAUNCHED, u)? {
                    return Ok(());
                }
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
                    self.update_if(
                        job.id,
                        &[JobState::Pending],
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
                // No status. A local job whose runner never wrote status.json, a Slurm job that
                // vanished from squeue and sacct, or a cloud instance that stopped answering is
                // lost after a grace period, counted from when it was last seen.
                let since = job.last_seen.or(job.started_at).unwrap_or(job.created_at);
                if (now - since).num_seconds() > NO_STATUS_AFTER && job.state == JobState::Running {
                    self.mark_lost(job, "no status from the runner for 10 minutes").await?;
                }
            }
        }
        Ok(())
    }

    /// Closes a paid job's cost record; returns its cost.
    /// Closes the job's open cost record, if it has one (by job id, so it works even if the
    /// backend is gone from the config).
    fn end_cost(&self, job: &Job, ended: DateTime<Utc>) -> Result<Option<f64>> {
        self.store.cost_end(job.id, ended)
    }

    /// The backend didn't answer. Its jobs may well still be running there, and they can't be
    /// cancelled now, so they stay as they are (with a note) until it answers again; requeueing
    /// them would run them twice.
    async fn check_lost(&self, jobs: &[Job], _err: Option<String>) -> Result<()> {
        let now = Utc::now();
        for j in jobs.iter().filter(|j| j.state == JobState::Running) {
            let seen = j.last_seen.or(j.started_at).unwrap_or(j.created_at);
            let note = format!("backend {} unreachable; status unknown until it answers", j.backend.as_deref().unwrap_or("?"));
            if (now - seen).num_seconds() > LOST_AFTER && j.reason.as_deref() != Some(note.as_str()) {
                self.update_if(j.id, LAUNCHED, JobUpdate { reason: Some(Some(note)), ..Default::default() })?;
            }
        }
        Ok(())
    }

    async fn mark_lost(&self, job: &Job, why: &str) -> Result<()> {
        let backend = job.backend.clone().unwrap_or_default();
        if let Some(b) = self.backend(&backend) {
            // Only end or requeue a job once it's known to be stopped; otherwise retry next tick.
            match tokio::time::timeout(Duration::from_secs(60), b.cancel(job)).await {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    tracing::warn!("job {} looks lost on {backend} ({why}), but cancelling it failed: {e:#}", job.id);
                    return Ok(());
                }
                Err(_) => {
                    tracing::warn!("job {} looks lost on {backend} ({why}), but cancelling it timed out", job.id);
                    return Ok(());
                }
            }
        }
        let _ = self.end_cost(job, Utc::now());
        if job.retries_left > 0 {
            tracing::warn!("job {} lost on {backend} ({why}); requeueing", job.id);
            self.update_if(
                job.id,
                LAUNCHED,
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
            .map(|_| ())
        } else {
            tracing::warn!("job {} lost on {backend} ({why})", job.id);
            self.update_if(
                job.id,
                LAUNCHED,
                JobUpdate {
                    state: Some(JobState::Lost),
                    ended_at: Some(Some(Utc::now())),
                    reason: Some(Some(format!("lost on {backend}: {why}"))),
                    ..Default::default()
                },
            )
            .map(|_| ())
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

    /// Cloud spend so far this calendar month (in the daemon's time zone), for reporting.
    pub fn spent_this_month(&self) -> Result<f64> {
        let now = Utc::now();
        let since = self.month_start();
        let mut spent = 0.0;
        for (_, _, rate, started, ended, cost) in self.store.costs_since(since)? {
            match (ended, cost) {
                (Some(_), Some(c)) => spent += c,
                // Open: earlier attempts plus this one so far.
                (_, prior) => spent += prior.unwrap_or(0.0) + rate * (now - started.max(since)).num_seconds().max(0) as f64 / 3600.0,
            }
        }
        Ok(spent)
    }

    /// USD per hour of the cloud instances running or about to start, which counts against
    /// `budget.hourly_usd`.
    pub fn hourly_in_use(&self) -> Result<f64> {
        let open: HashMap<i64, f64> =
            self.store.costs_since(Utc::now())?.into_iter().filter(|r| r.4.is_none()).map(|r| (r.0, r.2)).collect();
        let staging: HashSet<i64> = self.store.jobs_in_states(&[JobState::Staging])?.iter().map(|j| j.id).collect();
        let mut reserved = lock(&self.reserved);
        reserved.retain(|id, _| staging.contains(id) && !open.contains_key(id));
        // + 0.0: an empty float sum is -0.0.
        Ok(open.values().sum::<f64>() + reserved.values().sum::<f64>() + 0.0)
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

    /// How long a job is expected to run: the median of recent successful runs with the same
    /// title (with 20% slack), capped by its time limit; the time limit when there are none.
    pub fn expected_secs(&self, job: &Job) -> i64 {
        let limit = job.resources.time_s as i64;
        match self.store.typical_secs(&job.title) {
            Ok(Some(t)) => (t + t / 5).clamp(60, limit.max(60)),
            _ => limit,
        }
    }

    /// Resources held on a local backend by its staging and running jobs.
    fn allocs(&self, backend: &str) -> Result<Vec<Alloc>> {
        let now = Utc::now();
        Ok(self
            .store
            .jobs_in_states(&[JobState::Staging, JobState::Running])?
            .into_iter()
            .filter(|j| j.backend.as_deref() == Some(backend))
            .map(|j| Alloc {
                // A job past its expected end is assumed to finish soon, not at its limit.
                ends_at: (j.started_at.unwrap_or(now) + chrono::Duration::seconds(self.expected_secs(&j))).max(now + chrono::Duration::minutes(1)),
                cpus: j.resources.cpus,
                mem_mb: j.resources.mem_mb,
                disk_mb: j.resources.disk_mb,
                container: j.is_container(),
            })
            .collect())
    }

    /// Quotes from every backend, with the reason each one can't take the job.
    pub async fn plan(&self, job: &Job) -> Result<Placement> {
        self.plan_with(job, &HashMap::new()).await
    }

    /// A local backend's quote for `job` while queued jobs ahead of it hold reservations
    /// there (backfill): it may start now only if it fits beside every reservation, or if it
    /// is sure to end (its time limit) before the first reservation starts. Otherwise it
    /// waits until there is room beside them.
    async fn quote_around(&self, b: &Arc<dyn Backend>, job: &Job, active: Vec<Alloc>, held: &[Reserved]) -> Quote {
        if held.is_empty() {
            return b.quote(job, &active).await;
        }
        let now = Utc::now();
        let starts_now = |q: &Quote| matches!(q, Quote::Ok { start, .. } if *start <= now + chrono::Duration::seconds(5));
        let mut beside = active.clone();
        beside.extend(held.iter().map(|r| r.alloc.clone()));
        let around = b.quote(job, &beside).await;
        if starts_now(&around) {
            return around;
        }
        let first = held.iter().map(|r| r.start).min().unwrap_or(now);
        let ends = now + chrono::Duration::seconds(job.resources.time_s as i64);
        if ends <= first {
            let alone = b.quote(job, &active).await;
            if starts_now(&alone) {
                return alone;
            }
        }
        match around {
            Quote::Ok { start, source, reason, shape, cost } => {
                let ids: Vec<String> = held.iter().map(|r| r.job.to_string()).collect();
                Quote::Ok {
                    start,
                    source,
                    reason: format!("{reason}; room is held for queued job(s) {} ahead of it", ids.join(", ")),
                    shape,
                    cost,
                }
            }
            no => no,
        }
    }

    /// `plan`, with the reservations queued jobs ahead of `job` hold on local backends.
    async fn plan_with(&self, job: &Job, reserved: &HashMap<String, Vec<Reserved>>) -> Result<Placement> {
        let mut quotes = Vec::new();
        let limit = self.budget.hourly_usd;
        let in_use = self.hourly_in_use()?;
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
                let held = reserved.get(&name).map(Vec::as_slice).unwrap_or_default();
                match self.quote_around(b, job, self.allocs(&name)?, held).await {
                    Quote::Ok { start, source, mut reason, shape, cost } => {
                        let rate = shape.usd_per_hour.unwrap_or(0.0);
                        if b.is_paid() && rate > limit {
                            Quote::no(format!("${rate:.2}/h is over the hourly limit of ${limit:.2}/h"), false)
                        } else if b.is_paid() && in_use + rate > limit {
                            Quote::no(format!("${rate:.2}/h doesn't fit the hourly limit yet: ${in_use:.2}/h of ${limit:.2}/h in use"), false)
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
        // Room on busy local hosts held for queued jobs (in priority order) that wait there, so
        // smaller jobs behind them can't take every gap that opens and starve them.
        let mut reserved: HashMap<String, Vec<Reserved>> = HashMap::new();
        let now = Utc::now();
        // Batches collected so far are launched even if a later job errors.
        let placed: Result<()> = async {
        for job in queued {
            if in_flight.contains(&job.id) {
                continue;
            }
            match self.image_status(&job)? {
                ImageStatus::Ready => {}
                ImageStatus::Building(tag) => {
                    let reason = Some(format!("waiting for the image build {tag} (GET /v1/images)"));
                    if job.reason != reason {
                        self.update_if(job.id, &[JobState::Queued], JobUpdate { reason: Some(reason), ..Default::default() })?;
                    }
                    continue;
                }
                ImageStatus::Failed(err) => {
                    self.update_if(
                        job.id,
                        &[JobState::Queued],
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
            let plan = self.plan_with(&job, &reserved).await?;
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
                    self.update_if(job.id, &[JobState::Queued], JobUpdate { reason: Some(reason), est_start: Some(None), est_finish: Some(None), est_source: Some(None), ..Default::default() })?;
                }
                continue;
            };
            let Best { backend: name, start, source, reason, shape, cost } = best;
            let Some(backend) = self.backend(&name) else { continue };
            let finish = start + chrono::Duration::seconds(self.expected_secs(&job));
            let kind = backend.config().kind;
            if kind == BackendKind::Local && start > now + chrono::Duration::seconds(5) {
                // Best option is a busy local host: keep waiting here, and keep the estimate fresh.
                reserved.entry(name.clone()).or_default().push(Reserved {
                    job: job.id,
                    start,
                    alloc: Alloc {
                        ends_at: finish,
                        cpus: job.resources.cpus,
                        mem_mb: job.resources.mem_mb,
                        disk_mb: job.resources.disk_mb,
                        container: job.is_container(),
                    },
                });
                if job.est_start.map(|t| t.timestamp() / 60) != Some(start.timestamp() / 60) || job.backend.is_some() {
                    self.update_if(
                        job.id,
                        &[JobState::Queued],
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
            // Cancelled while it was being planned: leave it.
            if !self.update_if(
                job.id,
                &[JobState::Queued],
                JobUpdate {
                    state: Some(JobState::Staging),
                    backend: Some(Some(name.clone())),
                    reason: Some(Some(reason)),
                    est_start: Some(Some(start)),
                    est_finish: Some(Some(finish)),
                    est_source: Some(Some(source)),
                    ..Default::default()
                },
            )? {
                continue;
            }
            if backend.is_paid()
                && let Some(rate) = shape.usd_per_hour
            {
                lock(&self.reserved).insert(job.id, rate);
            }
            let mut job = job;
            job.state = JobState::Staging;
            job.backend = Some(name.clone());
            if kind == BackendKind::Slurm {
                batches.entry((name, job.group_key())).or_insert_with(|| (shape, Vec::new())).1.push(job);
            } else {
                self.spawn_launch(backend, vec![job], shape);
            }
        }
        Ok(())
        }
        .await;
        for ((name, _), (shape, jobs)) in batches {
            if let Some(b) = self.backend(&name) {
                self.spawn_launch(b, jobs, shape);
            }
        }
        placed
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
                    if let Ok(Some(j)) = me.store.get_job(*id) {
                        if backend.is_paid() {
                            let _ = backend.cancel(&j).await;
                        }
                        let _ = me.end_cost(&j, Utc::now());
                    }
                    let _ = me.update_if(
                        *id,
                        &[JobState::Staging],
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

    /// Files and settings for staging `job` on `backend`: secrets, dataset copies and
    /// same-backend inputs. Images are public, so pulls need no registry credentials.
    async fn stage_files(&self, backend: &Arc<dyn Backend>, job: &Job) -> Result<StageFiles> {
        let mut files = StageFiles { secrets_env: self.secrets.env_file(&job.secrets)?, ..Default::default() };
        if job.spec.container.is_some() {
            let runtimes = backend.runtimes();
            let rt = if runtimes.iter().any(|r| r == "docker") {
                "docker"
            } else if runtimes.iter().any(|r| r == "apptainer") {
                "apptainer"
            } else {
                "native" // cloud backends that run the image itself
            };
            if rt != "native" {
                files.runtime = Some(rt.into());
            }
        }
        for d in &job.spec.datasets {
            let path = self.ensure_dataset(d, backend, job).await.with_context(|| format!("staging dataset {}@{}", d.name, d.version))?;
            files.datasets.push((d.name.clone(), path));
        }
        if !backend.config().kind.is_cloud() {
            let (shell, root) = backend.shell_for(None).await?;
            for inp in &job.spec.inputs {
                let parent = self.store.get_job(inp.from_job)?.with_context(|| format!("input job {} is gone", inp.from_job))?;
                let glob = inp.glob.clone().unwrap_or_else(|| "*".into());
                if parent.backend.as_deref() == Some(backend.name()) {
                    files.inputs.push((format!("{}/out", crate::backend::Remote::jobdir(&root, parent.id)), glob, inp.as_.clone()));
                    continue;
                }
                // From another backend: fetch once into this host's input cache, shared by every
                // job (e.g. all array tasks) that needs the same outputs; the runner links them.
                let tag = hex::encode(<sha2::Sha256 as sha2::Digest>::digest(glob.as_bytes()));
                let dir = format!("{root}/cache/inputs/{}-{}", parent.id, &tag[..8]);
                let q = crate::transport::shell_quote(&dir);
                let lock = self.dataset_lock(&format!("input:{}:{dir}", backend.name()));
                let _guard = lock.lock().await;
                let have = shell.run(&format!("test -f {q}.complete && echo yes || true")).await?.text();
                if have.trim() != "yes" {
                    let pb = parent
                        .backend
                        .as_deref()
                        .and_then(|n| self.backend(n))
                        .with_context(|| format!("input job {} never ran", parent.id))?;
                    let data = pb.fetch(&parent, inp.glob.as_deref()).await.with_context(|| format!("reading outputs of job {}", parent.id))?;
                    shell
                        .pipe_in(
                            &format!("set -e\nrm -rf {q}.tmp\nmkdir -p {q}.tmp\ntar -x --exclude='._*' -C {q}.tmp\nrm -rf {q}\nmv {q}.tmp {q}\ntouch {q}.complete"),
                            data,
                        )
                        .await
                        .with_context(|| format!("copying outputs of job {} to {}", parent.id, backend.name()))?;
                }
                files.inputs.push((dir, "*".into(), inp.as_.clone()));
            }
        }
        Ok(files)
    }

    /// Cloud jobs: copies their inputs into the instance's `in/<as>` directory. (Local and
    /// Slurm backends link them from the parent's outputs or the input cache instead; see
    /// `stage_files`.)
    async fn copy_inputs(&self, backend: &Arc<dyn Backend>, job: &Job) -> Result<()> {
        if !backend.config().kind.is_cloud() {
            return Ok(());
        }
        for inp in &job.spec.inputs {
            let parent = self.store.get_job(inp.from_job)?.with_context(|| format!("input job {} is gone", inp.from_job))?;
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
            self.local_build(backend, j).await?;
        }
        // Cancelled while staging: stop here, and tear down what staging set up (a cloud
        // instance) instead of launching.
        let mut still = Vec::new();
        for j in live {
            match self.store.get_job(j.id)? {
                Some(cur) if cur.state == JobState::Staging => still.push(j),
                Some(cur) => {
                    if backend.is_paid() {
                        let _ = backend.cancel(&cur).await;
                    }
                    let _ = self.end_cost(&cur, Utc::now());
                }
                None => {}
            }
        }
        let live = still;
        if live.is_empty() {
            return Ok(());
        }
        let handles = backend.launch(&live, shape).await?;
        let kind = backend.config().kind;
        let now = Utc::now();
        for (j, mut h) in live.iter().zip(handles) {
            if shape.dependency.is_some() {
                h["native_deps"] = true.into();
            }
            let mut u = JobUpdate { handle: Some(Some(h.clone())), ..Default::default() };
            if kind == BackendKind::Local {
                u.state = Some(JobState::Running);
                u.started_at = Some(Some(now));
                u.last_seen = Some(Some(now));
            } else {
                u.state = Some(JobState::Pending);
            }
            // (A cloud job's cost record was opened when its instance was created.)
            if !self.update_if(j.id, &[JobState::Staging], u)? {
                // Cancelled while launching: stop what was just started.
                let mut launched = j.clone();
                launched.handle = Some(h);
                let _ = backend.cancel(&launched).await;
                let _ = self.end_cost(j, Utc::now());
            }
        }
        Ok(())
    }

    /// Cancels a job. A job still staging is stopped by its launch task, which sees the state
    /// change. If the backend can't be reached the job is still marked cancelled, with a note
    /// that it may be running there.
    pub async fn cancel(&self, job: &Job, by: &str) -> Result<()> {
        if job.state.is_terminal() {
            return Ok(());
        }
        let mut reason = format!("cancelled by request from {by}");
        if matches!(job.state, JobState::Pending | JobState::Running)
            && let Some(b) = job.backend.as_deref().and_then(|n| self.backend(n))
            && let Err(e) = b.cancel(job).await
        {
            tracing::warn!("cancelling job {} on {}: {e:#}", job.id, b.name());
            reason = format!("cancelled by request from {by}; {} could not be reached, so it may still be running there", b.name());
        }
        // A job being staged right now has its cost closed by its launch task, after teardown.
        // One left in Staging by a restart (not resumed yet) is closed here; the sweep then
        // tears its instance down.
        if !(job.state == JobState::Staging && lock(&self.in_flight).contains(&job.id)) {
            let _ = self.end_cost(job, Utc::now());
        }
        self.update_if(
            job.id,
            LIVE,
            JobUpdate {
                state: Some(JobState::Cancelled),
                ended_at: Some(Some(Utc::now())),
                reason: Some(Some(reason)),
                ..Default::default()
            },
        )
        .map(|_| ())
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
