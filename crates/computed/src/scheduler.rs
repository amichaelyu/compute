//! The scheduler loop: probe backends, poll live jobs, place queued jobs (earliest estimated
//! start wins; local beats Slurm on a tie), then stage and launch them.

use crate::backend::{Alloc, Backend, Quote, Shape};
use crate::config::BackendKind;
use crate::model::{Job, JobState};
use crate::secrets::Secrets;
use crate::snapshots::Snapshots;
use crate::store::{JobUpdate, Store};
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::{Notify, watch};

const TICK: Duration = Duration::from_secs(3);
const PROBE_RETRY: Duration = Duration::from_secs(60);
const PROBE_REFRESH: Duration = Duration::from_secs(24 * 3600);
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
}

pub struct Scheduler {
    pub store: Arc<Store>,
    pub backends: Vec<Arc<dyn Backend>>,
    pub snapshots: Arc<Snapshots>,
    pub secrets: Arc<Secrets>,
    notify: Notify,
    version: watch::Sender<u64>,
    runtime: Mutex<HashMap<String, BackendRuntime>>,
    in_flight: Mutex<HashSet<i64>>,
}

/// One backend's answer for a job, for dry runs and the submit response.
pub struct Placement {
    pub quotes: Vec<(String, Quote)>,
    pub best: Option<(String, DateTime<Utc>, String, String, Shape)>,
}

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

impl Scheduler {
    pub fn new(
        store: Arc<Store>,
        backends: Vec<Arc<dyn Backend>>,
        snapshots: Arc<Snapshots>,
        secrets: Arc<Secrets>,
    ) -> Arc<Scheduler> {
        let (version, _) = watch::channel(0);
        Arc::new(Scheduler {
            store,
            backends,
            snapshots,
            secrets,
            notify: Notify::new(),
            version,
            runtime: Mutex::new(HashMap::new()),
            in_flight: Mutex::new(HashSet::new()),
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
                    state: Some(JobState::Queued),
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
            tokio::select! {
                _ = self.notify.notified() => {}
                _ = tokio::time::sleep(TICK) => {}
            }
        }
    }

    async fn tick(self: &Arc<Self>) -> Result<()> {
        self.probe_due().await;
        self.poll_all().await?;
        self.place_queued().await
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
                let mut reason = job.reason.clone();
                if let Some(n) = r.note {
                    reason = Some(n);
                }
                self.update(
                    job.id,
                    JobUpdate {
                        state: Some(s),
                        exit_code: Some(r.exit_code),
                        started_at: Some(job.started_at.or(r.started)),
                        ended_at: Some(Some(r.ended.unwrap_or(now))),
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
                    && (now - seen).num_seconds() > LOST_AFTER && r.heartbeat.is_some() {
                        self.mark_lost(job, "no heartbeat for 5 minutes").await?;
                    }
            }
            Some(JobState::Pending) => {
                if let Some(start) = r.est_start
                    && job.est_start != Some(start) {
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
                if (now - since).num_seconds() > NO_STATUS_AFTER {
                    self.mark_lost(job, "no status from the runner for 10 minutes").await?;
                }
            }
        }
        Ok(())
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
            let _ = tokio::time::timeout(Duration::from_secs(20), b.cancel(job)).await;
        }
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
        for b in &self.backends {
            let name = b.name().to_string();
            let q = if job.backends.as_ref().is_some_and(|l| !l.contains(&name)) {
                Quote::no("not in the job's `backends` list", true)
            } else if !self.enabled(&name)?.0 {
                Quote::no(format!("{name} is disabled"), false)
            } else if !self.runtime(&name).reachable || b.root().is_none() {
                let err = self.runtime(&name).last_error.unwrap_or_else(|| "not probed yet".into());
                Quote::no(format!("unreachable: {err}"), false)
            } else {
                b.quote(job, &self.allocs(&name)?).await
            };
            quotes.push((name, q));
        }
        let rank = |name: &str| match self.backend(name).map(|b| b.config().kind) {
            Some(BackendKind::Local) => 0,
            _ => 1,
        };
        let best = quotes
            .iter()
            .filter_map(|(n, q)| match q {
                Quote::Ok { start, source, reason, shape } => {
                    Some((n.clone(), *start, source.clone(), reason.clone(), shape.clone()))
                }
                Quote::No { .. } => None,
            })
            // Compare starts to the minute so "now" on two backends is a tie.
            .min_by_key(|(n, start, ..)| (start.timestamp() / 60, rank(n)));
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
            let plan = self.plan(&job).await?;
            let Some((name, start, source, reason, shape)) = plan.best else {
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
            let Some(backend) = self.backend(&name) else { continue };
            let finish = start + chrono::Duration::seconds(job.resources.time_s as i64);
            let local = backend.config().kind == BackendKind::Local;
            if local && start > now + chrono::Duration::seconds(5) {
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
            if local {
                self.spawn_launch(backend, vec![job], shape);
            } else {
                batches.entry((name, job.group_key())).or_insert_with(|| (shape, Vec::new())).1.push(job);
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
            for attempt in 1..=STAGE_ATTEMPTS {
                match me.stage_and_launch(&backend, &jobs, &shape).await {
                    Ok(()) => {
                        done = true;
                        break;
                    }
                    Err(e) => {
                        last_err = format!("{e:#}");
                        tracing::warn!("staging {:?} on {} (attempt {attempt}): {last_err}", ids, backend.name());
                        if attempt < STAGE_ATTEMPTS {
                            tokio::time::sleep(Duration::from_secs(5 * u64::from(attempt))).await;
                        }
                    }
                }
            }
            if !done {
                for id in &ids {
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
            let secrets = self.secrets.env_file(&j.secrets)?;
            backend.stage(j, &path, secrets).await.with_context(|| format!("staging job {}", j.id))?;
        }
        let handles = backend.launch(&live, shape).await?;
        let local = backend.config().kind == BackendKind::Local;
        let now = Utc::now();
        for (j, h) in live.iter().zip(handles) {
            let mut u = JobUpdate { handle: Some(Some(h)), ..Default::default() };
            if local {
                u.state = Some(JobState::Running);
                u.started_at = Some(Some(now));
                u.last_seen = Some(Some(now));
            } else {
                u.state = Some(JobState::Pending);
            }
            self.update(j.id, u)?;
            // Cancelled between the check above and the launch: stop it now.
            if self.store.get_job(j.id)?.is_some_and(|c| c.state == JobState::Cancelled) {
                let mut launched = j.clone();
                launched.handle = self.store.get_job(j.id)?.and_then(|c| c.handle);
                let _ = backend.cancel(&launched).await;
            }
        }
        Ok(())
    }

    pub async fn cancel(&self, job: &Job) -> Result<()> {
        if job.state.is_terminal() {
            return Ok(());
        }
        if matches!(job.state, JobState::Pending | JobState::Running)
            && let Some(b) = job.backend.as_deref().and_then(|n| self.backend(n)) {
                b.cancel(job).await?;
            }
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
            && (Utc::now() - at).num_seconds() < 60 {
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
