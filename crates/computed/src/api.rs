//! The `/v1/` HTTP API.

use crate::auth::Auth;
use crate::backend::Quote;
use crate::config::BackendKind;
use crate::error::{ApiError, ApiResult};
use crate::model::{DepRef, Job, JobRequest, JobSpec, JobState, NewJob};
use crate::scheduler::{Best, Scheduler};
use crate::store::JobFilter;
use async_compression::tokio::bufread::ZstdEncoder;
use axum::body::Body;
use axum::extract::rejection::JsonRejection;
use axum::extract::{DefaultBodyLimit, FromRequestParts, Path, Query, State};
use axum::http::request::Parts;
use axum::http::{HeaderMap, StatusCode, header};
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
    pub access: Option<Arc<crate::access::Access>>,
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
        .route("/v1/jobs/{id}/metrics", get(job_metrics))
        .route("/v1/jobs/{id}/outputs", get(job_outputs).delete(delete_outputs))
        .route("/v1/jobs/{id}/outputs/list", get(list_outputs))
        .route("/v1/jobs/{id}/cancel", post(cancel_job))
        .route("/v1/backends", get(list_backends))
        .route("/v1/backends/{name}", get(get_backend))
        .route("/v1/backends/{name}/queue", get(backend_queue))
        .route("/v1/backends/{name}/enable", post(enable_backend))
        .route("/v1/backends/{name}/disable", post(disable_backend))
        .route("/v1/secrets", get(list_secrets))
        .route("/v1/secrets/{name}", put(put_secret).delete(delete_secret))
        .route("/v1/datasets", post(register_dataset).get(list_datasets))
        .route("/v1/datasets/{name}", get(get_dataset))
        .route("/v1/datasets/{name}/upload", put(upload_dataset).layer(DefaultBodyLimit::disable()))
        .route("/v1/datasets/{name}/copies/{backend}", axum::routing::delete(delete_dataset_copy))
        .route("/v1/images", get(list_images))
        .route("/v1/images/{key}/logs", get(image_logs))
        .route("/v1/budget", get(budget))
        .route("/v1/whoami", get(whoami))
        .fallback(crate::ui::serve)
        .with_state(state)
}

// ------------------------------------------------------------------ auth

/// An authenticated request. `fingerprint` is the SSH key that signed an API session, or
/// `cf:<email>` for a dashboard request that passed Cloudflare Access.
pub struct Session {
    pub fingerprint: String,
    /// SSH-key sessions have full access; Access (browser) sessions only view and do safe actions.
    pub full: bool,
}

impl Session {
    /// Submitting jobs, uploading code and changing secrets need an SSH-key session.
    pub fn require_full(&self) -> ApiResult<()> {
        if self.full {
            return Ok(());
        }
        Err(ApiError::new(
            StatusCode::FORBIDDEN,
            "forbidden",
            "the dashboard can view jobs, cancel them and turn backends on or off, nothing more",
            "use an SSH-key session (the compute skill) for this",
        ))
    }
}

const ACCESS_HEADER: &str = "cf-access-jwt-assertion";

impl FromRequestParts<AppState> for Session {
    type Rejection = ApiError;
    async fn from_request_parts(parts: &mut Parts, state: &AppState) -> Result<Self, Self::Rejection> {
        let bearer = parts
            .headers
            .get(header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.strip_prefix("Bearer "));
        if let Some(bearer) = bearer {
            return match state.auth.session(bearer.trim())? {
                Some(fingerprint) => Ok(Session { fingerprint, full: true }),
                None => Err(ApiError::unauthorized("session expired or unknown")),
            };
        }
        let jwt = parts.headers.get(ACCESS_HEADER).and_then(|v| v.to_str().ok());
        match (jwt, &state.access) {
            (Some(token), Some(access)) => match access.verify(token).await {
                Ok(email) => Ok(Session { fingerprint: format!("cf:{email}"), full: false }),
                Err(e) => {
                    tracing::warn!("Access token rejected: {e:#}");
                    Err(ApiError::unauthorized(format!("Cloudflare Access login rejected: {e}")))
                }
            },
            _ => Err(ApiError::unauthorized("missing `Authorization: Bearer <session>`")),
        }
    }
}

async fn whoami(sess: Session) -> Json<Value> {
    Json(json!({
        "identity": sess.fingerprint,
        "scope": if sess.full { "full" } else { "dashboard" },
    }))
}

/// Changes within v1 that a running agent should know about, newest first.
const CHANGES: &[(&str, &str)] = &[
    ("2026-11-24", "Job JSON always has command, env, snapshot and snapshot_created_at; list items have resources. GET /v1/jobs/{id}/outputs/list lists output files with sizes; /outputs answers 404 no_outputs when nothing matches and 410 once outputs are deleted; DELETE /v1/jobs/{id}/outputs frees a finished job's disk. /logs?tail=N gives the last N bytes. A cancel's reason says who cancelled. Long titles are shortened, not refused. retries defaults to 1."),
    ("2026-11-24", "Through the public Cloudflare hostname, wait returns after at most 90 s and uploads over 100 MB are refused. POST /v1/auth/challenge can return 429 rate_limited."),
    ("2026-11-24", "resources.disk_gb: free disk the job needs; the runner checks it before starting, and cloud offers must have it. Failed jobs say in their reason when they ran out of memory or disk."),
    ("2026-11-24", "Images are public: backends pull without credentials, so images must hold no secrets (pass them with `secrets`). A build whose registry package is private fails with build_failed: not_public: and a link to make it public."),
    ("2026-11-24", "The cloud budget is an hourly limit (budget.hourly_usd): a paid job starts only if its $/h fits beside the instances already running. /v1/budget reports hourly_usd, in_use_usd_per_hour, remaining_usd_per_hour and spent_this_month_usd."),
    ("2026-11-24", "GET /v1/backends/{name}/queue: your jobs in a Slurm queue, including ones submitted outside compute."),
    ("2026-11-24", "GET /v1/jobs/{id}/metrics: CPU, memory, GPU, power and disk samples every 10 s."),
    ("2026-11-24", "inputs[].allow_failed and job_outputs.allow_failed reuse outputs of a job that didn't succeed."),
    ("2026-11-24", "container.build works without a registry: built with docker on the Docker host that runs the job."),
    ("2026-11-24", "Datasets in $JOB_DATA_DIR are real directories (hard links or clones), not symlinks."),
    ("2026-11-24", "$JOB_DATA_DIR holds only the job's declared datasets, one directory each; the backend's shared data directory is no longer exposed."),
    ("2026-11-24", "New: after_ok/after_any/inputs, datasets, container, max_cost_usd/quote_id/keep_outputs, nodes > 1; /v1/datasets, /v1/images, /v1/budget."),
];

async fn root() -> Json<Value> {
    Json(json!({
        "name": "computed",
        "build": env!("CARGO_PKG_VERSION"),
        "api_versions": ["v1"],
        "changes": CHANGES.iter().map(|(d, c)| json!({"date": d, "change": c})).collect::<Vec<_>>(),
    }))
}

async fn challenge(State(s): State<AppState>) -> ApiResult<Json<Value>> {
    let (nonce, expires) = s.auth.challenge()?;
    Ok(Json(json!({"nonce": nonce, "namespace": crate::auth::NAMESPACE, "expires_at": fmt(&s.tz, expires)})))
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

async fn upload_snapshot(State(s): State<AppState>, sess: Session, body: Body) -> ApiResult<Json<Value>> {
    sess.require_full()?;
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

/// The request minus `dry_run` and `quote_id`, hashed, so a quote matches only the same request.
fn request_hash(raw: &Value) -> String {
    let mut v = raw.clone();
    if let Some(m) = v.as_object_mut() {
        m.remove("dry_run");
        m.remove("quote_id");
    }
    hex::encode(<sha2::Sha256 as sha2::Digest>::digest(v.to_string().as_bytes()))
}

/// Builds the stored spec: dependencies, inputs, datasets, container and cloud fields.
async fn build_spec(s: &AppState, req: &JobRequest) -> ApiResult<JobSpec> {
    let mut spec = JobSpec {
        after_ok: req.after_ok.clone(),
        after_any: req.after_any.clone(),
        inputs: req.inputs.clone(),
        max_cost_usd: req.max_cost_usd,
        keep_outputs: req.keep_outputs.clone(),
        ..Default::default()
    };
    for r in req.after_ok.iter().chain(&req.after_any) {
        let exists = match r {
            DepRef::Job(id) => s.sched.store.get_job(*id)?.is_some(),
            DepRef::Array { array } => !s.sched.store.jobs_in_array(*array)?.is_empty(),
        };
        if !exists {
            return Err(ApiError::bad_request(format!("dependency {r} does not exist"), "use ids from GET /v1/jobs (arrays as {\"array\": id})"));
        }
    }
    for inp in &req.inputs {
        if s.sched.store.get_job(inp.from_job)?.is_none() {
            return Err(ApiError::bad_request(format!("input job {} does not exist", inp.from_job), "check the id"));
        }
        let r = DepRef::Job(inp.from_job);
        if inp.allow_failed {
            // Any end state is fine; only wait for it to finish.
            spec.after_ok.retain(|x| x != &r);
            if !spec.after_any.contains(&r) {
                spec.after_any.push(r);
            }
        } else if !spec.after_ok.contains(&r) && !spec.after_any.contains(&r) {
            spec.after_ok.push(r);
        }
    }
    let mut names = std::collections::HashSet::new();
    for d in &req.datasets {
        let u = s.sched.resolve_dataset(d)?;
        if !names.insert(u.name.clone()) {
            return Err(ApiError::bad_request(format!("dataset {} is listed twice", u.name), "use one version per name"));
        }
        spec.datasets.push(u);
    }
    if let Some(c) = &req.container {
        spec.container = Some(s.sched.container_spec(&req.snapshot, c).await?);
    }
    Ok(spec)
}

async fn submit(State(s): State<AppState>, sess: Session, body: Result<Json<Value>, JsonRejection>) -> ApiResult<Response> {
    sess.require_full()?;
    let Json(raw) = body.map_err(json_err)?;
    let mut req: JobRequest = serde_json::from_value(raw.clone())
        .map_err(|e| ApiError::bad_request(format!("invalid job request: {e}"), "check field names and types against the skill file"))?;
    // A long title is shortened rather than refused.
    if req.title.chars().count() > 80 {
        req.title = format!("{}…", req.title.chars().take(79).collect::<String>().trim_end());
    }
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

    let spec = build_spec(&s, &req).await?;
    let hash = request_hash(&raw);
    let quoted = if req.max_cost_usd.is_some() && !req.dry_run {
        let Some(qid) = &req.quote_id else {
            return Err(ApiError::new(
                StatusCode::BAD_REQUEST,
                "quote_required",
                "jobs that may run on paid backends need a quote first",
                "send the same request with \"dry_run\": true, tell the user the backend and estimated cost, then submit with the returned quote_id",
            ));
        };
        let Some((qhash, _, cost, expires)) = s.sched.store.quote(qid)? else {
            return Err(ApiError::new(StatusCode::BAD_REQUEST, "quote_expired", format!("quote {qid} is unknown"), "get a new quote with a dry run"));
        };
        if expires < Utc::now() {
            return Err(ApiError::new(StatusCode::BAD_REQUEST, "quote_expired", format!("quote {qid} expired"), "quotes last 15 minutes; get a new one with a dry run"));
        }
        if qhash != hash {
            return Err(ApiError::new(
                StatusCode::BAD_REQUEST,
                "quote_mismatch",
                "the request changed since the quote",
                "submit exactly the request you dry-ran (apart from dry_run and quote_id), or get a new quote",
            ));
        }
        cost
    } else {
        None
    };

    let preview = Job::preview(&req, res.clone(), spec.clone());
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
    let over_budget = plan.best.is_none()
        && plan.quotes.iter().any(|(_, q)| matches!(q, Quote::No { reason, .. } if reason.contains("over the hourly limit")))
        && plan.quotes.iter().all(|(_, q)| matches!(q, Quote::No { permanent: true, .. }) || matches!(q, Quote::No { reason, .. } if reason.contains("over the hourly limit")));
    if over_budget {
        return Err(ApiError::new(
            StatusCode::UNPROCESSABLE_ENTITY,
            "over_budget",
            "the only backends that can run this job cost more per hour than the hourly cloud limit",
            "see GET /v1/budget; ask for fewer or cheaper GPUs, or ask the user to raise budget.hourly_usd",
        ));
    }
    let best = plan.best.as_ref();
    let estimate = |b: Option<&Best>| {
        json!({
            "backend": b.map(|b| b.backend.clone()),
            "reason": b.map(|b| b.reason.clone()),
            "estimated_start": b.map(|b| fmt(&s.tz, b.start)),
            "estimated_start_source": b.map(|b| b.source.clone()),
            "estimated_finish": b.map(|b| fmt(&s.tz, b.start + chrono::Duration::seconds(res.time_s as i64))),
            "estimated_cost_usd": b.and_then(|b| b.cost),
        })
    };
    let waiting = spec.has_deps();
    if req.dry_run {
        let mut v = estimate(best);
        v["would_place"] = best.is_some().into();
        v["shape"] = best.map(|b| serde_json::to_value(&b.shape).unwrap_or_default()).unwrap_or(Value::Null);
        v["per_backend"] = serde_json::to_value(&per_backend).unwrap_or_default();
        if let Some(c) = &spec.container {
            v["image"] = c.image.clone().into();
        }
        if waiting {
            v["note"] = "the job will wait for its dependencies first; placement is decided when they finish".into();
        }
        if req.max_cost_usd.is_some() {
            // The cheapest paid option, so the user sees what the cloud would cost even when a
            // free backend wins right now.
            let paid = plan
                .quotes
                .iter()
                .filter_map(|(n, q)| match q {
                    Quote::Ok { cost: Some(c), .. } => Some((n.clone(), *c)),
                    _ => None,
                })
                .min_by(|a, b| a.1.total_cmp(&b.1));
            let qid = format!("q_{}", hex::encode(rand::random::<[u8; 8]>()));
            let expires = Utc::now() + chrono::Duration::minutes(15);
            let cost = best.and_then(|b| b.cost).or(paid.as_ref().map(|p| p.1));
            s.sched.store.put_quote(&qid, &hash, best.map(|b| b.backend.as_str()), cost, expires)?;
            v["quote_id"] = qid.into();
            v["quote_expires_at"] = fmt(&s.tz, expires).into();
            v["cheapest_paid_option"] = paid.map(|(n, c)| json!({"backend": n, "estimated_cost_usd": c})).unwrap_or(Value::Null);
            v["budget"] = json!({
                "hourly_usd": s.sched.budget.hourly_usd,
                "in_use_usd_per_hour": round2(s.sched.hourly_in_use()?),
                "spent_this_month_usd": round2(s.sched.spent_this_month()?),
            });
        }
        return Ok(Json(v).into_response());
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
            spec: spec.clone(),
        })
        .collect();
    let ids = s.sched.store.insert_jobs(&jobs)?;
    s.sched.snapshots.touch(&req.snapshot)?;
    s.sched.kick();

    let mut estimate = estimate(best);
    if best.is_none() {
        estimate["reason"] = "waiting: no backend can start it yet".into();
    }
    if waiting {
        estimate["note"] = "waiting for dependencies; placement is decided when they finish".into();
    }
    if let Some(q) = quoted {
        estimate["quoted_cost_usd"] = q.into();
    }
    if let Some(c) = &spec.container {
        estimate["image"] = c.image.clone().into();
    }
    if req.max_cost_usd.is_some() {
        let used = s.sched.outputs_used();
        let cap = s.sched.budget.outputs_cap_gb * 1024 * 1024 * 1024;
        if used > cap {
            estimate["warning"] = format!(
                "the coordinator holds {} GB of cloud outputs, over outputs_cap_gb = {}; set keep_outputs to only what you need",
                used >> 30,
                s.sched.budget.outputs_cap_gb
            )
            .into();
        }
    }
    let state = if waiting { "waiting" } else { "queued" };
    let body = if let Some(a) = array_id {
        json!({
            "array_id": a,
            "tasks": count,
            "job_ids": {"first": ids.first(), "last": ids.last()},
            "state": state,
            "first_task_estimate": estimate,
            "per_backend": per_backend,
            "note": "each task is placed when it can start; see GET /v1/jobs?array=ID",
        })
    } else {
        let mut b = estimate;
        b["id"] = ids[0].into();
        b["state"] = state.into();
        b["per_backend"] = serde_json::to_value(&per_backend).unwrap_or_default();
        b
    };
    Ok((StatusCode::CREATED, Json(body)).into_response())
}

fn round2(x: f64) -> f64 {
    (x * 100.0).round() / 100.0
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
        "reason": j.reason,
        "created_at": fmt(tz, j.created_at),
        "started_at": fmt_opt(tz, j.started_at),
        "ended_at": fmt_opt(tz, j.ended_at),
        "estimated_start": fmt_opt(tz, j.est_start),
        "estimated_finish": fmt_opt(tz, j.est_finish),
        "time_s": j.resources.time_s,
        "resources": resources_json(&j.resources),
        "exit_code": j.exit_code,
    })
}

fn resources_json(r: &crate::model::Resources) -> Value {
    json!({
        "nodes": r.nodes, "cpus": r.cpus, "mem_gb": (r.mem_mb as f64 / 1024.0), "gpus": r.gpus,
        "gpu_type": r.gpu_type, "time_s": r.time_s,
        "disk_gb": (r.disk_mb > 0).then(|| r.disk_mb as f64 / 1024.0),
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
        "resources": resources_json(&j.resources),
        // What the job runs: always shown, so it can be checked after the fact.
        "command": j.command,
        "env": j.env,
        "snapshot": j.snapshot,
        "secrets": j.secrets,
        "after_ok": j.spec.after_ok,
        "after_any": j.spec.after_any,
        "inputs": j.spec.inputs,
        "datasets": j.spec.datasets.iter().map(|d| format!("{}@{}", d.name, d.version)).collect::<Vec<_>>(),
        "image": j.spec.container.as_ref().map(|c| c.image.clone()),
        "max_cost_usd": j.spec.max_cost_usd,
        "retries_left": j.retries_left,
        "priority": j.priority,
        "submitted_by": j.key_fingerprint,
        "exit_code": j.exit_code,
        "created_at": fmt(tz, j.created_at),
        "started_at": fmt_opt(tz, j.started_at),
        "ended_at": fmt_opt(tz, j.ended_at),
    });
    if verbose {
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
                ApiError::bad_request(
                    format!("unknown state {part:?}"),
                    "states: waiting, queued, staging, pending, running, succeeded, failed, cancelled, timeout, lost, dependency_failed",
                )
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
    let mut v = job_full(&s.tz, &j, q.verbose.unwrap_or(0) > 0);
    if let Some((rate, cost)) = s.sched.store.cost_of(id)? {
        v["usd_per_hour"] = rate.into();
        v["cost_usd"] = cost.map(round2).into();
    }
    v["snapshot_created_at"] = s.sched.store.snapshot_created(&j.snapshot)?.map(|t| fmt(&s.tz, t)).into();
    if let Some((at, why)) = s.sched.store.outputs_deleted(id)? {
        v["outputs_deleted"] = json!({"at": fmt(&s.tz, at), "why": why});
    }
    Ok(Json(v))
}

#[derive(Deserialize)]
struct WaitQuery {
    until: Option<String>,
    timeout: Option<u64>,
}

/// Longest `wait` through Cloudflare, which drops a response that takes over 100 s (error 524).
const PROXIED_WAIT_SECS: u64 = 90;

async fn wait_job(
    State(s): State<AppState>,
    _sess: Session,
    Path(id): Path<i64>,
    Query(q): Query<WaitQuery>,
    headers: HeaderMap,
) -> ApiResult<Json<Value>> {
    let until = q.until.as_deref().unwrap_or("done");
    if until != "done" && until != "running" {
        return Err(ApiError::bad_request("`until` must be running or done", ""));
    }
    // Cloudflare adds cf-ray to every request it proxies. Faking it only shortens the caller's
    // own wait, and callers already repeat a wait that returns `timed_out: true`.
    let max = if headers.contains_key("cf-ray") { PROXIED_WAIT_SECS } else { 900 };
    let timeout = Duration::from_secs(q.timeout.unwrap_or(600).min(max));
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
    /// Only the last N bytes (overrides offset).
    tail: Option<u64>,
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
    let (offset, max) = match q.tail {
        Some(t) => {
            let (_, size) = backend.logs(&j, 0, 0).await?;
            let t = t.clamp(1, 1_048_576);
            (size.saturating_sub(t), t)
        }
        None => (offset, max),
    };
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
struct MetricsQuery {
    history: Option<u32>,
}

/// Resource usage sampled by the runner every 10 s: CPU (percent of one core, summed), memory
/// (MB resident), per-GPU utilization, memory and power, and free disk where the job runs.
async fn job_metrics(State(s): State<AppState>, _sess: Session, Path(id): Path<i64>, Query(q): Query<MetricsQuery>) -> ApiResult<Json<Value>> {
    let j = load_job(&s, id)?;
    let requested = json!({"cpus": j.resources.cpus, "mem_mb": j.resources.mem_mb, "gpus": j.resources.gpus});
    let backend = j.backend.as_deref().and_then(|n| s.sched.backend(n)).filter(|b| b.root().is_some());
    let empty = json!({"latest": null, "history": [], "requested": requested, "state": j.state});
    let Some(backend) = backend else { return Ok(Json(empty)) };
    if matches!(j.state, JobState::Waiting | JobState::Queued | JobState::Staging) {
        return Ok(Json(empty));
    }
    let mut v = match tokio::time::timeout(Duration::from_secs(20), backend.metrics(&j, q.history.unwrap_or(0).min(2000))).await {
        Ok(Ok(v)) => v,
        Ok(Err(e)) => return Err(ApiError::new(StatusCode::SERVICE_UNAVAILABLE, "backend_unreachable", format!("{e:#}"), "try again shortly")),
        Err(_) => return Err(ApiError::new(StatusCode::SERVICE_UNAVAILABLE, "backend_unreachable", "reading metrics timed out", "try again shortly")),
    };
    v["requested"] = requested;
    v["state"] = serde_json::to_value(j.state).unwrap_or_default();
    Ok(Json(v))
}

#[derive(Deserialize)]
struct OutputsQuery {
    glob: Option<String>,
}

use crate::model::valid_glob;

async fn job_outputs(State(s): State<AppState>, _sess: Session, Path(id): Path<i64>, Query(q): Query<OutputsQuery>) -> ApiResult<Response> {
    let j = load_job(&s, id)?;
    if let Some(g) = &q.glob
        && !valid_glob(g) {
            return Err(ApiError::bad_request(
                format!("glob {g:?} has characters that aren't allowed"),
                "use letters, digits and _ . * ? / - [ ] only; * also matches /",
            ));
        }
    let backend = outputs_backend(&s, &j)?;
    // Say "nothing matches" plainly instead of sending an empty archive.
    let files = backend.list_outputs(&j, q.glob.as_deref()).await?;
    if files.is_empty() {
        return Err(ApiError::new(
            StatusCode::NOT_FOUND,
            "no_outputs",
            match &q.glob {
                Some(g) => format!("job {id} has no output files matching {g:?}"),
                None => format!("job {id} has no output files"),
            },
            format!("GET /v1/jobs/{id}/outputs/list shows what there is"),
        ));
    }
    let stdout = backend.fetch(&j, q.glob.as_deref()).await?;
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

/// The backend holding a job's outputs; 410 once they've been deleted.
fn outputs_backend(s: &AppState, j: &Job) -> ApiResult<Arc<dyn crate::backend::Backend>> {
    if let Some((at, why)) = s.sched.store.outputs_deleted(j.id)? {
        return Err(ApiError::new(
            StatusCode::GONE,
            "outputs_deleted",
            format!("job {}'s outputs were deleted at {} ({why})", j.id, fmt(&s.tz, at)),
            "its log is still available; rerun the job to get them again",
        ));
    }
    j.backend
        .as_deref()
        .and_then(|n| s.sched.backend(n))
        .filter(|b| b.root().is_some())
        .ok_or_else(|| ApiError::bad_request(format!("job {} has not run anywhere yet", j.id), "wait until it is running or done"))
}

/// The job's output files with their sizes (optionally only those matching `glob`).
async fn list_outputs(State(s): State<AppState>, _sess: Session, Path(id): Path<i64>, Query(q): Query<OutputsQuery>) -> ApiResult<Json<Value>> {
    let j = load_job(&s, id)?;
    if let Some(g) = &q.glob
        && !valid_glob(g)
    {
        return Err(ApiError::bad_request(format!("glob {g:?} has characters that aren't allowed"), "use letters, digits and _ . * ? / - [ ] only; * also matches /"));
    }
    let backend = outputs_backend(&s, &j)?;
    let files = backend.list_outputs(&j, q.glob.as_deref()).await?;
    let total: u64 = files.iter().map(|(_, n)| n).sum();
    Ok(Json(json!({
        "files": files.iter().map(|(p, n)| json!({"path": p, "bytes": n})).collect::<Vec<_>>(),
        "count": files.len(),
        "total_bytes": total,
    })))
}

/// Frees a finished job's disk: deletes its outputs and inputs on the backend (or the copy on
/// the coordinator, for cloud jobs). Its log stays.
async fn delete_outputs(State(s): State<AppState>, sess: Session, Path(id): Path<i64>) -> ApiResult<Json<Value>> {
    sess.require_full()?;
    let j = load_job(&s, id)?;
    if !j.state.is_terminal() {
        return Err(ApiError::bad_request(format!("job {id} is {}", j.state.as_str()), "only a finished job's outputs can be deleted; cancel it first"));
    }
    let needed_by: Vec<i64> = s
        .sched
        .store
        .jobs_in_states(&[JobState::Waiting, JobState::Queued, JobState::Staging, JobState::Pending, JobState::Running])?
        .iter()
        .filter(|c| c.spec.inputs.iter().any(|i| i.from_job == id))
        .map(|c| c.id)
        .collect();
    if !needed_by.is_empty() {
        return Err(ApiError::new(
            StatusCode::CONFLICT,
            "outputs_in_use",
            format!("job {id}'s outputs are inputs of unfinished jobs {needed_by:?}"),
            "wait for those, or cancel them, first",
        ));
    }
    let backend = outputs_backend(&s, &j)?;
    backend.delete_outputs(&j).await?;
    s.sched.store.mark_outputs_deleted(id, &format!("deleted on request by {}", sess.fingerprint))?;
    Ok(Json(json!({"deleted": true})))
}

#[derive(Deserialize)]
struct CancelQuery {
    array: Option<u8>,
}

async fn cancel_job(State(s): State<AppState>, sess: Session, Path(id): Path<i64>, Query(q): Query<CancelQuery>) -> ApiResult<Json<Value>> {
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
        match s.sched.cancel(j, &sess.fingerprint).await {
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
        (k, _) => format!("{} GPU cloud (paid, per job), up to {} node(s)", k.as_str(), c.max_nodes),
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
        "paid": b.is_paid(),
        "runtimes": b.runtimes(),
        "pricing": if b.is_paid() { b.pricing().map(|p| json!({"cheapest_per_gpu_hour": p.as_array().and_then(|a| a.iter().filter(|o| o["available"] == true).map(|o| o["usd_per_hour_per_gpu"].as_f64().unwrap_or(f64::INFINITY)).reduce(f64::min))})) } else { None },
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
        _ => {
            restrictions.push("paid: only for jobs that set max_cost_usd, after a dry-run quote".into());
            restrictions.push("GPU jobs only; runs a container image (container.image or container.build)".into());
            restrictions.push(format!("budget: at most ${:.2}/h across all cloud backends at once (GET /v1/budget)", s.sched.budget.hourly_usd));
            restrictions.push("outputs matching keep_outputs (default all) are copied to the coordinator, then the instance is deleted".into());
            if let Some(cc) = &c.cloud {
                if !cc.gpu_types.is_empty() {
                    restrictions.push(format!("GPU types allowed: {}", cc.gpu_types.join(", ")));
                }
                if c.kind == BackendKind::Modal {
                    restrictions.push(format!("prices from the rate table in hosts.toml ({})", cc.rates_date.clone().unwrap_or_else(|| "undated".into())));
                }
            }
        }
    }
    restrictions.push(format!("max_nodes = {}", c.max_nodes));
    restrictions.push(format!("runtimes: {}", b.runtimes().join(", ")));
    if let Some(cap) = c.data_cap_gb {
        restrictions.push(format!("dataset copies capped at {cap} GB (least recently used are evicted)"));
    }
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
        "pricing": b.pricing(),
        "refreshed_at": {
            "hardware": fmt_opt(&s.tz, rt.probed_at),
            "availability": fmt_opt(&s.tz, rt.availability_at),
        },
    })))
}

/// The user's jobs in a Slurm backend's queue, with their start estimates; jobs compute
/// submitted carry `compute_job`.
async fn backend_queue(State(s): State<AppState>, _sess: Session, Path(name): Path<String>) -> ApiResult<Json<Value>> {
    let b = s.sched.backend(&name).ok_or_else(|| ApiError::not_found(format!("backend {name} not found")))?;
    if b.root().is_none() {
        return Ok(Json(json!({"jobs": [], "note": "not probed yet"})));
    }
    match tokio::time::timeout(Duration::from_secs(30), b.queue()).await {
        Ok(Ok(Some(v))) => Ok(Json(v)),
        Ok(Ok(None)) => Err(ApiError::bad_request(format!("{name} has no batch queue"), "only Slurm backends have one")),
        Ok(Err(e)) => Err(ApiError::new(StatusCode::SERVICE_UNAVAILABLE, "backend_unreachable", format!("{e:#}"), "try again shortly")),
        Err(_) => Err(ApiError::new(StatusCode::SERVICE_UNAVAILABLE, "backend_unreachable", "squeue timed out", "try again shortly")),
    }
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
    sess: Session,
    Path(name): Path<String>,
    body: Result<Json<SecretReq>, JsonRejection>,
) -> ApiResult<Json<Value>> {
    sess.require_full()?;
    let Json(req) = body.map_err(json_err)?;
    let keys: Vec<String> = req.env.keys().cloned().collect();
    s.sched.secrets.put(&name, req.env)?;
    Ok(Json(json!({"name": name, "keys": keys})))
}

async fn delete_secret(State(s): State<AppState>, sess: Session, Path(name): Path<String>) -> ApiResult<Json<Value>> {
    sess.require_full()?;
    if s.sched.secrets.delete(&name)? {
        Ok(Json(json!({"deleted": name})))
    } else {
        Err(ApiError::not_found(format!("secret {name} not found")))
    }
}

// ------------------------------------------------------------------ datasets

fn dataset_json(tz: &Tz, d: &crate::store::DatasetRow) -> Value {
    json!({
        "name": d.name,
        "version": d.version,
        "source": d.source,
        "size_bytes": d.size,
        "files": d.files,
        "description": d.description,
        "created_at": fmt(tz, d.created_at),
    })
}

async fn register_dataset(
    State(s): State<AppState>,
    sess: Session,
    body: Result<Json<crate::datasets::RegisterReq>, JsonRejection>,
) -> ApiResult<Response> {
    sess.require_full()?;
    let Json(req) = body.map_err(json_err)?;
    let (row, new) = s.sched.register_dataset(req).await?;
    let mut v = dataset_json(&s.tz, &row);
    v["new_version"] = new.into();
    Ok((if new { StatusCode::CREATED } else { StatusCode::OK }, Json(v)).into_response())
}

#[derive(Deserialize)]
struct UploadQuery {
    description: Option<String>,
}

async fn upload_dataset(
    State(s): State<AppState>,
    sess: Session,
    Path(name): Path<String>,
    Query(q): Query<UploadQuery>,
    body: Body,
) -> ApiResult<Response> {
    sess.require_full()?;
    let (row, new) = s.sched.upload_dataset(&name, q.description.unwrap_or_default(), body).await?;
    let mut v = dataset_json(&s.tz, &row);
    v["new_version"] = new.into();
    Ok((if new { StatusCode::CREATED } else { StatusCode::OK }, Json(v)).into_response())
}

async fn list_datasets(State(s): State<AppState>, _sess: Session) -> ApiResult<Json<Value>> {
    let versions = s.sched.store.dataset_versions(None)?;
    let copies = s.sched.store.dataset_copies(None, None)?;
    let mut out: Vec<Value> = Vec::new();
    let mut seen = std::collections::HashSet::new();
    for d in &versions {
        if !seen.insert(d.name.clone()) {
            continue; // versions are newest first; list each name once
        }
        let count = versions.iter().filter(|x| x.name == d.name).count();
        let on: Vec<&str> = copies.iter().filter(|c| c.name == d.name && c.version == d.version).map(|c| c.backend.as_str()).collect();
        out.push(json!({
            "name": d.name,
            "latest": d.version,
            "versions": count,
            "size_bytes": d.size,
            "description": d.description,
            "copies_of_latest": on,
            "created_at": fmt(&s.tz, d.created_at),
        }));
    }
    Ok(Json(json!({"datasets": out})))
}

async fn get_dataset(State(s): State<AppState>, _sess: Session, Path(name): Path<String>) -> ApiResult<Json<Value>> {
    let versions = s.sched.store.dataset_versions(Some(&name))?;
    if versions.is_empty() {
        return Err(ApiError::not_found(format!("dataset {name} not found")));
    }
    let copies = s.sched.store.dataset_copies(None, Some(&name))?;
    Ok(Json(json!({
        "name": name,
        "versions": versions.iter().map(|d| {
            let mut v = dataset_json(&s.tz, d);
            v["copies"] = copies.iter().filter(|c| c.version == d.version).map(|c| json!({
                "backend": c.backend, "path": c.path, "size_bytes": c.size,
                "managed": c.owned, "last_used": fmt(&s.tz, c.last_used),
            })).collect::<Vec<_>>().into();
            v
        }).collect::<Vec<_>>(),
    })))
}

async fn delete_dataset_copy(State(s): State<AppState>, sess: Session, Path((name, backend)): Path<(String, String)>) -> ApiResult<Json<Value>> {
    sess.require_full()?;
    let n = s.sched.delete_dataset_copy(&name, &backend).await?;
    Ok(Json(json!({"deleted_copies": n, "backend": backend, "name": name})))
}

// ------------------------------------------------------------------ images

async fn list_images(State(s): State<AppState>, _sess: Session) -> ApiResult<Json<Value>> {
    let rows = s.sched.store.images(100)?;
    Ok(Json(json!({
        "registry": s.sched.registry.prefix(),
        "images": rows.iter().map(|i| json!({
            "tag": i.tag,
            "key": i.tag.rsplit(':').next(),
            "state": i.state,
            "dockerfile": i.dockerfile,
            "context": i.context,
            "builder": i.builder,
            "error": i.error,
            "created_at": fmt(&s.tz, i.created_at),
            "finished_at": fmt_opt(&s.tz, i.finished_at),
        })).collect::<Vec<_>>(),
    })))
}

async fn image_logs(State(s): State<AppState>, _sess: Session, Path(key): Path<String>, Query(q): Query<LogQuery>) -> ApiResult<Json<Value>> {
    if !key.chars().all(|c| c.is_ascii_hexdigit()) || key.is_empty() {
        return Err(ApiError::bad_request("bad image key", "use the `key` from GET /v1/images"));
    }
    let path = s.sched.paths.builds_dir().join(format!("{key}.log"));
    let data = tokio::fs::read(&path).await.unwrap_or_default();
    let offset = (q.offset.unwrap_or(0) as usize).min(data.len());
    let max = q.max_bytes.unwrap_or(65_536).clamp(1, 1_048_576) as usize;
    let end = (offset + max).min(data.len());
    let state = s.sched.store.images(500)?.into_iter().find(|i| i.tag.ends_with(&format!(":{key}"))).map(|i| i.state);
    Ok(Json(json!({
        "data": String::from_utf8_lossy(&data[offset..end]),
        "next_offset": end,
        "size": data.len(),
        "eof": state.as_deref() != Some("building") && end >= data.len(),
        "state": state,
    })))
}

// ------------------------------------------------------------------ budget

async fn budget(State(s): State<AppState>, _sess: Session) -> ApiResult<Json<Value>> {
    let limit = s.sched.budget.hourly_usd;
    let in_use = s.sched.hourly_in_use()?;
    let since = Utc::now() - chrono::Duration::days(40);
    let rows = s.sched.store.costs_since(since)?;
    Ok(Json(json!({
        "hourly_usd": limit,
        "in_use_usd_per_hour": round2(in_use),
        "remaining_usd_per_hour": round2(limit - in_use),
        "spent_this_month_usd": round2(s.sched.spent_this_month()?),
        "jobs": rows.iter().rev().take(50).map(|(id, b, rate, st, en, cost)| json!({
            "job": id, "backend": b, "usd_per_hour": rate, "started_at": fmt(&s.tz, *st),
            "ended_at": fmt_opt(&s.tz, *en), "cost_usd": cost.map(round2),
        })).collect::<Vec<_>>(),
    })))
}
