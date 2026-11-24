//! Job requests, validated resources and job records.

use crate::error::ApiError;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::time::Duration;

#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum JobState {
    Queued,
    Staging,
    Pending,
    Running,
    Succeeded,
    Failed,
    Cancelled,
    Timeout,
    Lost,
}

impl JobState {
    pub const ALL: [JobState; 9] = [
        JobState::Queued,
        JobState::Staging,
        JobState::Pending,
        JobState::Running,
        JobState::Succeeded,
        JobState::Failed,
        JobState::Cancelled,
        JobState::Timeout,
        JobState::Lost,
    ];
    pub fn as_str(self) -> &'static str {
        match self {
            JobState::Queued => "queued",
            JobState::Staging => "staging",
            JobState::Pending => "pending",
            JobState::Running => "running",
            JobState::Succeeded => "succeeded",
            JobState::Failed => "failed",
            JobState::Cancelled => "cancelled",
            JobState::Timeout => "timeout",
            JobState::Lost => "lost",
        }
    }
    pub fn parse(s: &str) -> Option<JobState> {
        JobState::ALL.into_iter().find(|st| st.as_str() == s)
    }
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            JobState::Succeeded
                | JobState::Failed
                | JobState::Cancelled
                | JobState::Timeout
                | JobState::Lost
        )
    }
}

#[derive(Deserialize, Clone, Debug)]
#[serde(deny_unknown_fields)]
pub struct ResourcesReq {
    #[serde(default = "one")]
    pub nodes: u32,
    #[serde(default = "one")]
    pub cpus: u32,
    #[serde(default = "four")]
    pub mem_gb: f64,
    #[serde(default)]
    pub gpus: u32,
    #[serde(default)]
    pub gpu_type: Option<String>,
    pub time: String,
}

fn one() -> u32 {
    1
}
fn four() -> f64 {
    4.0
}

#[derive(Deserialize, Clone, Debug)]
#[serde(deny_unknown_fields)]
pub struct ArrayReq {
    pub count: u32,
}

/// The body of `POST /v1/jobs`. Fields for later milestones are accepted by the parser so the
/// daemon can reject them with a clear message rather than a generic "unknown field".
#[derive(Deserialize, Clone, Debug)]
#[serde(deny_unknown_fields)]
pub struct JobRequest {
    pub title: String,
    pub description: String,
    pub command: String,
    pub snapshot: String,
    pub resources: ResourcesReq,
    #[serde(default)]
    pub backends: Option<Vec<String>>,
    #[serde(default)]
    pub os: Option<String>,
    #[serde(default)]
    pub container: Option<serde_json::Value>,
    #[serde(default)]
    pub datasets: Option<serde_json::Value>,
    #[serde(default)]
    pub inputs: Option<serde_json::Value>,
    #[serde(default)]
    pub after_ok: Option<serde_json::Value>,
    #[serde(default)]
    pub after_any: Option<serde_json::Value>,
    #[serde(default)]
    pub array: Option<ArrayReq>,
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    #[serde(default)]
    pub secrets: Vec<String>,
    #[serde(default)]
    pub max_cost_usd: Option<f64>,
    #[serde(default)]
    pub quote_id: Option<String>,
    #[serde(default)]
    pub keep_outputs: Option<String>,
    #[serde(default)]
    pub retries: u32,
    #[serde(default)]
    pub priority: i64,
    #[serde(default)]
    pub idempotency_key: Option<String>,
    #[serde(default)]
    pub dry_run: bool,
}

/// Validated, normalised resources stored with each job.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct Resources {
    pub nodes: u32,
    pub cpus: u32,
    pub mem_mb: u64,
    pub gpus: u32,
    pub gpu_type: Option<String>,
    pub time_s: u64,
}

/// Parses `2h`, `90m`, `1h30m`, `45s`, `1d` (and combinations) into a duration.
pub fn parse_duration(s: &str) -> Option<Duration> {
    let s = s.trim();
    if s.is_empty() {
        return None;
    }
    let mut total: u64 = 0;
    let mut num = String::new();
    for c in s.chars() {
        if c.is_ascii_digit() {
            num.push(c);
            continue;
        }
        let n: u64 = num.parse().ok()?;
        num.clear();
        let mult = match c {
            'd' => 86_400,
            'h' => 3_600,
            'm' => 60,
            's' => 1,
            _ => return None,
        };
        total = total.checked_add(n.checked_mul(mult)?)?;
    }
    if !num.is_empty() {
        return None; // a bare number has no unit
    }
    (total > 0).then(|| Duration::from_secs(total))
}

fn valid_env_key(k: &str) -> bool {
    let mut chars = k.chars();
    matches!(chars.next(), Some(c) if c == '_' || c.is_ascii_alphabetic())
        && chars.all(|c| c == '_' || c.is_ascii_alphanumeric())
}

pub fn valid_env_name(k: &str) -> bool {
    valid_env_key(k)
}

pub fn is_snapshot_id(s: &str) -> bool {
    s.strip_prefix("sha256:")
        .is_some_and(|h| h.len() == 64 && h.chars().all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase()))
}

impl JobRequest {
    /// Checks everything that doesn't need the store or the backends.
    pub fn validate(&self) -> Result<Resources, ApiError> {
        let later = |field: &str, m: &str| {
            Err(ApiError::bad_request(
                format!("`{field}` is not available yet"),
                format!("it arrives in milestone {m}; remove the field for now"),
            ))
        };
        if self.container.is_some() {
            return later("container", "M4");
        }
        if self.datasets.is_some() {
            return later("datasets", "M3");
        }
        if self.inputs.is_some() {
            return later("inputs", "M2");
        }
        if self.after_ok.is_some() || self.after_any.is_some() {
            return later("after_ok/after_any", "M2");
        }
        if self.max_cost_usd.is_some() || self.quote_id.is_some() || self.keep_outputs.is_some() {
            return later("max_cost_usd/quote_id/keep_outputs (cloud)", "M5");
        }
        let title_len = self.title.chars().count();
        if title_len == 0 || title_len > 80 {
            return Err(ApiError::bad_request(
                "`title` must be 1-80 characters",
                "say what the job is, e.g. \"Eval checkpoint 40k on held-out set\"",
            ));
        }
        let desc_len = self.description.chars().count();
        if desc_len == 0 || desc_len > 500 {
            return Err(ApiError::bad_request(
                "`description` must be 1-500 characters",
                "one or two sentences: why the job is being run and what it belongs to",
            ));
        }
        if self.command.trim().is_empty() {
            return Err(ApiError::bad_request("`command` is empty", "give the shell command to run"));
        }
        if !is_snapshot_id(&self.snapshot) {
            return Err(ApiError::bad_request(
                "`snapshot` must look like sha256:<64 lowercase hex>",
                "upload the repo with POST /v1/snapshots and use the returned id",
            ));
        }
        let r = &self.resources;
        if r.nodes != 1 {
            return Err(ApiError::bad_request(
                "multi-node jobs (`resources.nodes` > 1) are not available yet",
                "they arrive in milestone M6; use nodes = 1",
            ));
        }
        if r.cpus == 0 {
            return Err(ApiError::bad_request("`resources.cpus` must be at least 1", "set cpus >= 1"));
        }
        if !(r.mem_gb > 0.0 && r.mem_gb.is_finite()) {
            return Err(ApiError::bad_request("`resources.mem_gb` must be positive", "e.g. 4"));
        }
        let time = parse_duration(&r.time).ok_or_else(|| {
            ApiError::bad_request(
                format!("`resources.time` {:?} is not a duration", r.time),
                "use forms like \"30m\", \"2h\", \"1h30m\", \"1d\"",
            )
        })?;
        if let Some(os) = &self.os
            && os != "mac" && os != "linux" {
                return Err(ApiError::bad_request("`os` must be \"mac\" or \"linux\"", "or leave it null"));
            }
        if let Some(a) = &self.array
            && (a.count == 0 || a.count > 10_000) {
                return Err(ApiError::bad_request("`array.count` must be 1-10000", "split larger sweeps"));
            }
        for k in self.env.keys() {
            if !valid_env_key(k) {
                return Err(ApiError::bad_request(
                    format!("env key {k:?} is not a valid variable name"),
                    "use letters, digits and _ only, not starting with a digit",
                ));
            }
            if k.starts_with("JOB_") || k == "ARRAY_INDEX" {
                return Err(ApiError::bad_request(
                    format!("env key {k:?} is reserved"),
                    "JOB_* and ARRAY_INDEX are set by the runner",
                ));
            }
        }
        if let Some(key) = &self.idempotency_key
            && (key.is_empty() || key.len() > 200) {
                return Err(ApiError::bad_request("`idempotency_key` must be 1-200 characters", ""));
            }
        Ok(Resources {
            nodes: 1,
            cpus: r.cpus,
            mem_mb: (r.mem_gb * 1024.0).ceil() as u64,
            gpus: r.gpus,
            gpu_type: r.gpu_type.clone(),
            time_s: time.as_secs(),
        })
    }
}

/// A job as stored. One row per array task.
#[derive(Clone, Debug)]
pub struct Job {
    pub id: i64,
    pub array_id: Option<i64>,
    pub array_index: Option<u32>,
    pub title: String,
    pub description: String,
    pub command: String,
    pub snapshot: String,
    pub resources: Resources,
    pub backends: Option<Vec<String>>,
    pub os: Option<String>,
    pub env: BTreeMap<String, String>,
    pub secrets: Vec<String>,
    pub retries_left: u32,
    pub priority: i64,
    pub idempotency_key: Option<String>,
    pub key_fingerprint: String,
    pub state: JobState,
    pub backend: Option<String>,
    pub handle: Option<serde_json::Value>,
    pub reason: Option<String>,
    pub est_start: Option<DateTime<Utc>>,
    pub est_finish: Option<DateTime<Utc>>,
    pub est_source: Option<String>,
    pub exit_code: Option<i32>,
    pub created_at: DateTime<Utc>,
    pub started_at: Option<DateTime<Utc>>,
    pub ended_at: Option<DateTime<Utc>>,
    pub last_seen: Option<DateTime<Utc>>,
}

/// Fields for inserting a new job.
#[derive(Clone, Debug)]
pub struct NewJob {
    pub array_id: Option<i64>,
    pub array_index: Option<u32>,
    pub title: String,
    pub description: String,
    pub command: String,
    pub snapshot: String,
    pub resources: Resources,
    pub backends: Option<Vec<String>>,
    pub os: Option<String>,
    pub env: BTreeMap<String, String>,
    pub secrets: Vec<String>,
    pub retries: u32,
    pub priority: i64,
    pub idempotency_key: Option<String>,
    pub key_fingerprint: String,
}

impl Job {
    /// A transient job built from a request, used for dry runs and submit-time checks.
    pub fn preview(req: &JobRequest, res: Resources) -> Job {
        Job {
            id: 0,
            array_id: None,
            array_index: None,
            title: req.title.clone(),
            description: req.description.clone(),
            command: req.command.clone(),
            snapshot: req.snapshot.clone(),
            resources: res,
            backends: req.backends.clone(),
            os: req.os.clone(),
            env: req.env.clone(),
            secrets: req.secrets.clone(),
            retries_left: req.retries,
            priority: req.priority,
            idempotency_key: req.idempotency_key.clone(),
            key_fingerprint: String::new(),
            state: JobState::Queued,
            backend: None,
            handle: None,
            reason: None,
            est_start: None,
            est_finish: None,
            est_source: None,
            exit_code: None,
            created_at: Utc::now(),
            started_at: None,
            ended_at: None,
            last_seen: None,
        }
    }

    /// The key used to group array tasks that can share one quote and one Slurm array.
    pub fn group_key(&self) -> String {
        match self.array_id {
            Some(a) => format!("a{a}"),
            None => format!("j{}", self.id),
        }
    }
}
