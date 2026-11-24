//! The `/v1/` HTTP API.

use crate::auth::Auth;
use crate::backend::Quote;
use crate::config::BackendKind;
use crate::error::{ApiError, ApiResult};
use crate::model::{Job, JobRequest, JobState, NewJob};
use crate::scheduler::Scheduler;
use crate::store::JobFilter;
use async_compression::tokio::bufread::ZstdEncoder;
use axum::body::Body;
use axum::extract::rejection::JsonRejection;
use axum::extract::{DefaultBodyLimit, FromRequestParts, Path, Query, State};
use axum::http::request::Parts;
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post, put};
use axum::{Json, Router};
use chrono::{DateTime, NaiveTime, SecondsFormat, TimeZone, Utc};
use chrono_tz::Tz;
use serde::Deserialize;
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::BufReader;
use tokio_util::io::ReaderStream;

#[derive(Clone)]
pub struct AppState {
    pub sched: Arc<Scheduler>,
    pub auth: Arc<Auth>,
    pub tz: Tz,
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/v1/", get(root))
        .route("/v1", get(root))
        .route("/v1/auth/challenge", post(challenge))
        .route("/v1/auth/token", post(token))
        .route("/v1/snapshots", post(upload_snapshot).layer(DefaultBodyLimit::disable()))
        .route("/v1/snapshots/{id}", axum::routing::head(head_snapshot))
        .route("/v1/jobs", post(submit).get(list_jobs))
        .route("/v1/jobs/{id}", get(get_job))
        .route("/v1/jobs/{id}/wait", get(wait_job))
        .route("/v1/jobs/{id}/logs", get(job_logs))
        .route("/v1/jobs/{id}/outputs", get(job_outputs))
        .route("/v1/jobs/{id}/cancel", post(cancel_job))
        .route("/v1/backends", get(list_backends))
        .route("/v1/backends/{name}", get(get_backend))
        .route("/v1/backends/{name}/enable", post(enable_backend))
        .route("/v1/backends/{name}/disable", post(disable_backend))
        .route("/v1/secrets", get(list_secrets))
        .route("/v1/secrets/{name}", put(put_secret).delete(delete_secret))
        .with_state(state)
}

// ------------------------------------------------------------------ auth

/// An authenticated request; holds the fingerprint of the key that signed the session.
pub struct Session {
    pub fingerprint: String,
}

impl FromRequestParts<AppState> for Session {
    type Rejection = ApiError;
    async fn from_request_parts(parts: &mut Parts, state: &AppState) -> Result<Self, Self::Rejection> {
        let bearer = parts
            .headers
            .get(header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.strip_prefix("Bearer "))
            .ok_or_else(|| ApiError::unauthorized("missing `Authorization: Bearer <session>`"))?;
        match state.auth.session(bearer.trim())? {
            Some(fingerprint) => Ok(Session { fingerprint }),
            None => Err(ApiError::unauthorized("session expired or unknown")),
        }
    }
}

async fn root() -> Json<Value> {
    Json(json!({"name": "computed", "build": env!("CARGO_PKG_VERSION"), "api_versions": ["v1"]}))
}

async fn challenge(State(s): State<AppState>) -> Json<Value> {
    let (nonce, expires) = s.auth.challenge();
    Json(json!({"nonce": nonce, "namespace": crate::auth::NAMESPACE, "expires_at": fmt(&s.tz, expires)}))
}

#[derive(Deserialize)]
struct TokenReq {
    nonce: String,
    signature: String,
    public_key: String,
}

async fn token(State(s): State<AppState>, body: Result<Json<TokenReq>, JsonRejection>) -> ApiResult<Json<Value>> {
    let Json(req) = body.map_err(json_err)?;
    let (token, fp, expires) = s.auth.token(&req.nonce, &req.signature, &req.public_key)?;
    Ok(Json(json!({"token": token, "fingerprint": fp, "expires_at": fmt(&s.tz, expires)})))
}

fn json_err(e: JsonRejection) -> ApiError {
    ApiError::bad_request(format!("invalid JSON body: {}", e.body_text()), "check field names and types against the skill file")
}

fn fmt(tz: &Tz, t: DateTime<Utc>) -> String {
    t.with_timezone(tz).to_rfc3339_opts(SecondsFormat::Secs, false)
}

fn fmt_opt(tz: &Tz, t: Option<DateTime<Utc>>) -> Value {
    t.map(|t| Value::String(fmt(tz, t))).unwrap_or(Value::Null)
}

// ------------------------------------------------------------------ snapshots

async fn upload_snapshot(State(s): State<AppState>, _sess: Session, body: Body) -> ApiResult<Json<Value>> {
    let (id, existed) = s.sched.snapshots.store(body).await?;
    Ok(Json(json!({"id": id, "existed": existed})))
}

async fn head_snapshot(State(s): State<AppState>, _sess: Session, Path(id): Path<String>) -> StatusCode {
    if s.sched.snapshots.exists(&id) { StatusCode::OK } else { StatusCode::NOT_FOUND }
}

// ------------------------------------------------------------------ jobs

fn quote_line(tz: &Tz, q: &Quote) -> String {
    match q {
        Quote::Ok { start, source, reason, .. } => {
            let when = if (*start - Utc::now()).num_seconds() < 60 {
                "now".to_string()
            } else {
                start.with_timezone(tz).format("%a %H:%M").to_string()
            };
            format!("ok: starts {when} ({source}); {reason}")
        }
        Quote::No { reason, .. } => format!("no: {reason}"),
    }
}

async fn submit(State(s): State<AppState>, sess: Session, body: Result<Json<JobRequest>, JsonRejection>) -> ApiResult<Response> {
    let Json(req) = body.map_err(json_err)?;
    let res = req.validate()?;
    if let Some(list) = &req.backends {
        for name in list {
            if s.sched.backend(name).is_none() {
                let known: Vec<&str> = s.sched.backends.iter().map(|b| b.name()).collect();
                return Err(ApiError::bad_request(format!("unknown backend {name:?}"), format!("known backends: {}", known.join(", "))));
            }
        }
    }
    if !s.sched.snapshots.exists(&req.snapshot) {
        return Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            "snapshot_missing",
            format!("snapshot {} is not stored", req.snapshot),
            "upload it with POST /v1/snapshots first",
        ));
    }
    s.sched.secrets.check(&req.secrets)?;

    if let Some(key) = &req.idempotency_key {
        let existing = s.sched.store.jobs_by_idempotency_key(key)?;
        if !existing.is_empty() && !req.dry_run {
            let first = &existing[0];
            let mut body = submit_body(&s.tz, first, None);
            body["idempotent_replay"] = true.into();
            if let Some(a) = first.array_id {
                body = json!({"array_id": a, "tasks": existing.len(), "idempotent_replay": true});
            }
            return Ok(Json(body).into_response());
        }
    }

    let preview = Job::preview(&req, res.clone());
    let plan = s.sched.plan(&preview).await?;
    let per_backend: BTreeMap<String, String> =
        plan.quotes.iter().map(|(n, q)| (n.clone(), quote_line(&s.tz, q))).collect();
    let all_permanent = plan.quotes.iter().all(|(_, q)| matches!(q, Quote::No { permanent: true, .. }));
    if all_permanent {
        let reasons: Vec<String> = per_backend.iter().map(|(n, l)| format!("{n}: {}", l.trim_start_matches("no: "))).collect();
        return Err(ApiError::new(
            StatusCode::UNPROCESSABLE_ENTITY,
            "unplaceable",
            "no backend can ever run this job as requested",
            reasons.join("; "),
        ));
    }
    if req.dry_run {
        let best = plan.best.as_ref();
        return Ok(Json(json!({
            "would_place": best.is_some(),
            "backend": best.map(|b| b.0.clone()),
            "reason": best.map(|b| b.3.clone()),
            "estimated_start": best.map(|b| fmt(&s.tz, b.1)),
            "estimated_start_source": best.map(|b| b.2.clone()),
            "estimated_finish": best.map(|b| fmt(&s.tz, b.1 + chrono::Duration::seconds(res.time_s as i64))),
            "shape": best.map(|b| serde_json::to_value(&b.4).unwrap_or_default()),
            "per_backend": per_backend,
        }))
        .into_response());
    }

    let count = req.array.as_ref().map(|a| a.count).unwrap_or(1);
    let array_id = if req.array.is_some() { Some(s.sched.store.new_array()?) } else { None };
    let jobs: Vec<NewJob> = (0..count)
        .map(|i| NewJob {
            array_id,
            array_index: array_id.map(|_| i),
            title: req.title.clone(),
            description: req.description.clone(),
            command: if array_id.is_some() { req.command.replace("{index}", &i.to_string()) } else { req.command.clone() },
            snapshot: req.snapshot.clone(),
            resources: res.clone(),
            backends: req.backends.clone(),
            os: req.os.clone(),
            env: req.env.clone(),
            secrets: req.secrets.clone(),
            retries: req.retries,
            priority: req.priority,
            idempotency_key: req.idempotency_key.clone(),
            key_fingerprint: sess.fingerprint.clone(),
        })
        .collect();
    let ids = s.sched.store.insert_jobs(&jobs)?;
    s.sched.snapshots.touch(&req.snapshot)?;
    s.sched.kick();

    let best = plan.best.as_ref();
    let estimate = json!({
        "backend": best.map(|b| b.0.clone()),
        "reason": best.map(|b| b.3.clone()).unwrap_or_else(|| "waiting: no backend can start it yet".into()),
        "estimated_start": best.map(|b| fmt(&s.tz, b.1)),
        "estimated_start_source": best.map(|b| b.2.clone()),
        "estimated_finish": best.map(|b| fmt(&s.tz, b.1 + chrono::Duration::seconds(res.time_s as i64))),
    });
    let body = if let Some(a) = array_id {
        json!({
            "array_id": a,
            "tasks": count,
            "job_ids": {"first": ids.first(), "last": ids.last()},
            "state": "queued",
            "first_task_estimate": estimate,
            "per_backend": per_backend,
            "note": "each task is placed when it can start; see GET /v1/jobs?array=ID",
        })
    } else {
        let mut b = estimate;
        b["id"] = ids[0].into();
        b["state"] = "queued".into();
        b["per_backend"] = serde_json::to_value(&per_backend).unwrap_or_default();
        b
    };
    Ok((StatusCode::CREATED, Json(body)).into_response())
}

fn submit_body(tz: &Tz, j: &Job, per_backend: Option<Value>) -> Value {
    let mut v = json!({
        "id": j.id,
        "state": j.state,
        "backend": j.backend,
        "reason": j.reason,
        "estimated_start": fmt_opt(tz, j.est_start),
        "estimated_start_source": j.est_source,
        "estimated_finish": fmt_opt(tz, j.est_finish),
    });
    if let Some(p) = per_backend {
        v["per_backend"] = p;
    }
    v
}

fn job_short(tz: &Tz, j: &Job) -> Value {
    json!({
        "id": j.id,
        "array_id": j.array_id,
        "array_index": j.array_index,
        "title": j.title,
        "state": j.state,
        "backend": j.backend,
        "created_at": fmt(tz, j.created_at),
        "started_at": fmt_opt(tz, j.started_at),
        "ended_at": fmt_opt(tz, j.ended_at),
        "exit_code": j.exit_code,
    })
}

fn job_full(tz: &Tz, j: &Job, verbose: bool) -> Value {
    let mut v = json!({
        "id": j.id,
        "array_id": j.array_id,
        "array_index": j.array_index,
        "title": j.title,
        "description": j.description,
        "state": j.state,
        "backend": j.backend,
        "reason": j.reason,
        "estimated_start": fmt_opt(tz, j.est_start),
        "estimated_start_source": j.est_source,
        "estimated_finish": fmt_opt(tz, j.est_finish),
        "resources": {
            "nodes": j.resources.nodes, "cpus": j.resources.cpus,
            "mem_gb": (j.resources.mem_mb as f64 / 1024.0), "gpus": j.resources.gpus,
            "gpu_type": j.resources.gpu_type, "time_s": j.resources.time_s,
        },
        "secrets": j.secrets,
        "retries_left": j.retries_left,
        "priority": j.priority,
        "submitted_by": j.key_fingerprint,
        "exit_code": j.exit_code,
        "created_at": fmt(tz, j.created_at),
        "started_at": fmt_opt(tz, j.started_at),
        "ended_at": fmt_opt(tz, j.ended_at),
    });
    if verbose {
        v["command"] = j.command.clone().into();
        v["env"] = serde_json::to_value(&j.env).unwrap_or_default();
        v["snapshot"] = j.snapshot.clone().into();
        v["handle"] = j.handle.clone().unwrap_or(Value::Null);
        v["backends_allowed"] = serde_json::to_value(&j.backends).unwrap_or_default();
        v["os"] = serde_json::to_value(&j.os).unwrap_or_default();
        v["idempotency_key"] = serde_json::to_value(&j.idempotency_key).unwrap_or_default();
    }
    v
}

#[derive(Deserialize)]
struct ListQuery {
    state: Option<String>,
    q: Option<String>,
    limit: Option<u32>,
    array: Option<i64>,
}

async fn list_jobs(State(s): State<AppState>, _sess: Session, Query(q): Query<ListQuery>) -> ApiResult<Json<Value>> {
    let mut states = Vec::new();
    if let Some(st) = &q.state {
        for part in st.split(',').map(str::trim).filter(|p| !p.is_empty()) {
            states.push(JobState::parse(part).ok_or_else(|| {
                ApiError::bad_request(format!("unknown state {part:?}"), "states: queued, staging, pending, running, succeeded, failed, cancelled, timeout, lost")
            })?);
        }
    }
    let jobs = s.sched.store.list_jobs(&JobFilter {
        states,
        query: q.q.clone(),
        array_id: q.array,
        limit: Some(q.limit.unwrap_or(50).min(1000)),
    })?;
    Ok(Json(json!({"jobs": jobs.iter().map(|j| job_short(&s.tz, j)).collect::<Vec<_>>()})))
}

#[derive(Deserialize)]
struct VerboseQuery {
    verbose: Option<u8>,
}

fn load_job(s: &AppState, id: i64) -> ApiResult<Job> {
    s.sched.store.get_job(id)?.ok_or_else(|| ApiError::not_found(format!("job {id} not found")))
}

async fn get_job(State(s): State<AppState>, _sess: Session, Path(id): Path<i64>, Query(q): Query<VerboseQuery>) -> ApiResult<Json<Value>> {
    let j = load_job(&s, id)?;
    Ok(Json(job_full(&s.tz, &j, q.verbose.unwrap_or(0) > 0)))
}

#[derive(Deserialize)]
struct WaitQuery {
    until: Option<String>,
    timeout: Option<u64>,
}

async fn wait_job(State(s): State<AppState>, _sess: Session, Path(id): Path<i64>, Query(q): Query<WaitQuery>) -> ApiResult<Json<Value>> {
    let until = q.until.as_deref().unwrap_or("done");
    if until != "done" && until != "running" {
        return Err(ApiError::bad_request("`until` must be running or done", ""));
    }
    let timeout = Duration::from_secs(q.timeout.unwrap_or(600).min(900));
    let deadline = tokio::time::Instant::now() + timeout;
    let mut rx = s.sched.subscribe();
    loop {
        let j = load_job(&s, id)?;
        let reached = j.state.is_terminal() || (until == "running" && j.state == JobState::Running);
        if reached {
            let mut v = job_full(&s.tz, &j, false);
            v["timed_out"] = false.into();
            return Ok(Json(v));
        }
        let changed = tokio::time::timeout_at(deadline, rx.changed()).await;
        if changed.is_err() {
            let mut v = job_full(&s.tz, &j, false);
            v["timed_out"] = true.into();
            return Ok(Json(v));
        }
    }
}

#[derive(Deserialize)]
struct LogQuery {
    offset: Option<u64>,
    max_bytes: Option<u64>,
}

async fn job_logs(State(s): State<AppState>, _sess: Session, Path(id): Path<i64>, Query(q): Query<LogQuery>) -> ApiResult<Json<Value>> {
    let j = load_job(&s, id)?;
    let offset = q.offset.unwrap_or(0);
    let max = q.max_bytes.unwrap_or(65_536).clamp(1, 1_048_576);
    let backend = j.backend.as_deref().and_then(|n| s.sched.backend(n));
    let Some(backend) = backend.filter(|b| b.root().is_some()) else {
        return Ok(Json(json!({"data": "", "next_offset": offset, "eof": j.state.is_terminal(), "state": j.state})));
    };
    if matches!(j.state, JobState::Queued | JobState::Staging) {
        return Ok(Json(json!({"data": "", "next_offset": offset, "eof": false, "state": j.state})));
    }
    let (data, size) = backend.logs(&j, offset, max).await?;
    let next = offset + data.len() as u64;
    let mut text = String::from_utf8_lossy(&data).into_owned();
    for secret in s.sched.secrets.values(&j.secrets) {
        text = text.replace(&secret, "***");
    }
    Ok(Json(json!({
        "data": text,
        "next_offset": next,
        "size": size,
        "eof": j.state.is_terminal() && next >= size,
        "state": j.state,
    })))
}

#[derive(Deserialize)]
struct OutputsQuery {
    glob: Option<String>,
}

fn valid_glob(g: &str) -> bool {
    !g.is_empty()
        && !g.contains("..")
        && !g.starts_with('/')
        && g.chars().all(|c| c.is_ascii_alphanumeric() || "_.*?/-[]".contains(c))
}

async fn job_outputs(State(s): State<AppState>, _sess: Session, Path(id): Path<i64>, Query(q): Query<OutputsQuery>) -> ApiResult<Response> {
    let j = load_job(&s, id)?;
    if let Some(g) = &q.glob
        && !valid_glob(g) {
            return Err(ApiError::bad_request(
                format!("glob {g:?} has characters that aren't allowed"),
                "use letters, digits and _ . * ? / - [ ] only; * also matches /",
            ));
        }
    let backend = j
        .backend
        .as_deref()
        .and_then(|n| s.sched.backend(n))
        .filter(|b| b.root().is_some())
        .ok_or_else(|| ApiError::bad_request(format!("job {id} has not run anywhere yet"), "wait until it is running or done"))?;
    let mut child = backend.fetch(&j, q.glob.as_deref()).await?;
    let stdout = child.stdout.take().ok_or_else(|| anyhow::anyhow!("no stdout from fetch"))?;
    tokio::spawn(async move {
        let _ = child.wait().await;
    });
    let stream = ReaderStream::new(ZstdEncoder::new(BufReader::new(stdout)));
    Ok((
        [
            (header::CONTENT_TYPE, "application/zstd".to_string()),
            (header::CONTENT_DISPOSITION, format!("attachment; filename=\"job-{id}-outputs.tar.zst\"")),
        ],
        Body::from_stream(stream),
    )
        .into_response())
}

#[derive(Deserialize)]
struct CancelQuery {
    array: Option<u8>,
}

async fn cancel_job(State(s): State<AppState>, _sess: Session, Path(id): Path<i64>, Query(q): Query<CancelQuery>) -> ApiResult<Json<Value>> {
    let jobs = if q.array.unwrap_or(0) > 0 {
        let all = s.sched.store.list_jobs(&JobFilter { array_id: Some(id), ..Default::default() })?;
        if all.is_empty() {
            return Err(ApiError::not_found(format!("array {id} not found")));
        }
        all
    } else {
        vec![load_job(&s, id)?]
    };
    let mut cancelled = 0;
    let mut errors = Vec::new();
    for j in jobs.iter().filter(|j| !j.state.is_terminal()) {
        match s.sched.cancel(j).await {
            Ok(()) => cancelled += 1,
            Err(e) => errors.push(format!("job {}: {e:#}", j.id)),
        }
    }
    s.sched.kick();
    Ok(Json(json!({"cancelled": cancelled, "errors": errors})))
}

// ------------------------------------------------------------------ backends

async fn backend_summary(s: &AppState, name: &str) -> ApiResult<Value> {
    let b = s.sched.backend(name).ok_or_else(|| ApiError::not_found(format!("backend {name} not found")))?;
    let c = b.config();
    let rt = s.sched.runtime(name);
    let (enabled, until) = s.sched.enabled(name)?;
    let queued = s
        .sched
        .store
        .jobs_in_states(&[JobState::Staging, JobState::Pending, JobState::Running])?
        .into_iter()
        .filter(|j| j.backend.as_deref() == Some(name))
        .count();
    let hardware = match (c.kind, b.capacity()) {
        (BackendKind::Local, Some(cap)) => format!("{} CPU / {} GB schedulable, {} {}", cap.cpus, cap.mem_gb, cap.os, cap.arch),
        (BackendKind::Local, None) => "not probed yet".to_string(),
        (BackendKind::Slurm, _) => format!(
            "Slurm; up to {} x {} GPUs per user, CPU partition {}",
            c.gpus,
            c.gpu_type.as_deref().unwrap_or("GPU"),
            c.slurm.as_ref().map(|s| s.cpu_partition.as_str()).unwrap_or("")
        ),
    };
    Ok(json!({
        "name": name,
        "kind": c.kind.as_str(),
        "transport": c.transport().as_str(),
        "enabled": enabled,
        "enabled_until": fmt_opt(&s.tz, until),
        "reachable": rt.reachable,
        "hardware": hardware,
        "active_jobs": queued,
        "pricing": null,
    }))
}

async fn list_backends(State(s): State<AppState>, _sess: Session) -> ApiResult<Json<Value>> {
    let mut out = Vec::new();
    for b in &s.sched.backends {
        out.push(backend_summary(&s, b.name()).await?);
    }
    Ok(Json(json!({"backends": out})))
}

async fn get_backend(State(s): State<AppState>, _sess: Session, Path(name): Path<String>) -> ApiResult<Json<Value>> {
    let b = s.sched.backend(&name).ok_or_else(|| ApiError::not_found(format!("backend {name} not found")))?;
    let c = b.config();
    let rt = s.sched.runtime(&name);
    let (enabled, until) = s.sched.enabled(&name)?;
    let mut restrictions: Vec<String> = Vec::new();
    match c.kind {
        BackendKind::Local => match b.capacity() {
            Some(cap) => {
                restrictions.push(format!("scheduler may use {} CPUs / {} GB", cap.cpus, cap.mem_gb));
                restrictions.push(format!("os {} / {}", cap.os, cap.arch));
            }
            None => restrictions.push("not probed yet: capacity unknown".into()),
        },
        BackendKind::Slurm => {
            if let Some(sc) = &c.slurm {
                restrictions.push(format!(
                    "GPU jobs: partition {}, at most {} GPUs; a {}-GPU job takes a whole node ({} CPUs / {} MB)",
                    sc.gpu_partition, sc.max_gpus, sc.max_gpus, sc.node_cpus, sc.node_mem_mb
                ));
                restrictions.push(format!("CPU jobs: partition {} (qos {}), at most {} CPUs per task", sc.cpu_partition, sc.cpu_qos, sc.max_cpu_task_cpus));
                restrictions.push(format!("memory is requested as cpus x {} MB", sc.mem_per_cpu_mb));
            }
        }
    }
    restrictions.push(format!("max_nodes = {}", c.max_nodes));
    restrictions.push(format!("runtimes: {}", c.runtimes.join(", ")));
    restrictions.extend(c.restrictions.iter().cloned());
    if !enabled {
        restrictions.push("currently disabled: takes no new jobs".into());
    }
    let availability = if enabled { s.sched.availability(&name).await } else { None };
    Ok(Json(json!({
        "name": name,
        "kind": c.kind.as_str(),
        "transport": c.transport().as_str(),
        "enabled": enabled,
        "enabled_until": fmt_opt(&s.tz, until),
        "reachable": rt.reachable,
        "last_error": rt.last_error,
        "hardware": rt.hardware,
        "availability": availability,
        "restrictions": restrictions,
        "pricing": null,
        "refreshed_at": {
            "hardware": fmt_opt(&s.tz, rt.probed_at),
            "availability": fmt_opt(&s.tz, rt.availability_at),
        },
    })))
}

#[derive(Deserialize, Default)]
struct EnableReq {
    until: Option<String>,
}

/// Parses `until` as `HH:MM` (next occurrence, local time) or RFC 3339.
fn parse_until(tz: &Tz, s: &str) -> Option<DateTime<Utc>> {
    if let Ok(t) = DateTime::parse_from_rfc3339(s) {
        return Some(t.with_timezone(&Utc));
    }
    let time = NaiveTime::parse_from_str(s, "%H:%M").ok()?;
    let now = Utc::now().with_timezone(tz);
    let mut day = now.date_naive();
    if now.time() >= time {
        day = day.succ_opt()?;
    }
    tz.from_local_datetime(&day.and_time(time)).earliest().map(|t| t.with_timezone(&Utc))
}

async fn enable_backend(
    State(s): State<AppState>,
    _sess: Session,
    Path(name): Path<String>,
    body: Option<Json<EnableReq>>,
) -> ApiResult<Json<Value>> {
    if s.sched.backend(&name).is_none() {
        return Err(ApiError::not_found(format!("backend {name} not found")));
    }
    let until = match body.and_then(|Json(b)| b.until) {
        Some(u) => Some(parse_until(&s.tz, &u).ok_or_else(|| ApiError::bad_request(format!("bad `until` {u:?}"), "use \"18:00\" or an RFC 3339 time"))?),
        None => None,
    };
    s.sched.store.set_backend_override(&name, true, until)?;
    s.sched.probe_now(&name).await;
    s.sched.kick();
    backend_summary(&s, &name).await.map(Json)
}

async fn disable_backend(State(s): State<AppState>, _sess: Session, Path(name): Path<String>) -> ApiResult<Json<Value>> {
    if s.sched.backend(&name).is_none() {
        return Err(ApiError::not_found(format!("backend {name} not found")));
    }
    s.sched.store.set_backend_override(&name, false, None)?;
    let mut v = backend_summary(&s, &name).await?;
    v["note"] = "draining: running jobs finish, no new jobs start".into();
    Ok(Json(v))
}

// ------------------------------------------------------------------ secrets

async fn list_secrets(State(s): State<AppState>, _sess: Session) -> ApiResult<Json<Value>> {
    let list = s.sched.secrets.list()?;
    Ok(Json(json!({"secrets": list.iter().map(|i| json!({
        "name": i.name, "keys": i.keys, "updated_at": fmt(&s.tz, i.updated_at)
    })).collect::<Vec<_>>()})))
}

#[derive(Deserialize)]
struct SecretReq {
    env: BTreeMap<String, String>,
}

async fn put_secret(
    State(s): State<AppState>,
    _sess: Session,
    Path(name): Path<String>,
    body: Result<Json<SecretReq>, JsonRejection>,
) -> ApiResult<Json<Value>> {
    let Json(req) = body.map_err(json_err)?;
    let keys: Vec<String> = req.env.keys().cloned().collect();
    s.sched.secrets.put(&name, req.env)?;
    Ok(Json(json!({"name": name, "keys": keys})))
}

async fn delete_secret(State(s): State<AppState>, _sess: Session, Path(name): Path<String>) -> ApiResult<Json<Value>> {
    if s.sched.secrets.delete(&name)? {
        Ok(Json(json!({"deleted": name})))
    } else {
        Err(ApiError::not_found(format!("secret {name} not found")))
    }
}
