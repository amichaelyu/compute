//! Job requests, validated resources and job records.

use crate::error::ApiError;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::time::Duration;

#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum JobState {
    Waiting,
    Queued,
    Staging,
    Pending,
    Running,
    Succeeded,
    Failed,
    Cancelled,
    Timeout,
    Lost,
    DependencyFailed,
}

impl JobState {
    pub const ALL: [JobState; 11] = [
        JobState::Waiting,
        JobState::Queued,
        JobState::Staging,
        JobState::Pending,
        JobState::Running,
        JobState::Succeeded,
        JobState::Failed,
        JobState::Cancelled,
        JobState::Timeout,
        JobState::Lost,
        JobState::DependencyFailed,
    ];
    pub fn as_str(self) -> &'static str {
        match self {
            JobState::Waiting => "waiting",
            JobState::Queued => "queued",
            JobState::Staging => "staging",
            JobState::Pending => "pending",
            JobState::Running => "running",
            JobState::Succeeded => "succeeded",
            JobState::Failed => "failed",
            JobState::Cancelled => "cancelled",
            JobState::Timeout => "timeout",
            JobState::Lost => "lost",
            JobState::DependencyFailed => "dependency_failed",
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
                | JobState::DependencyFailed
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
    /// Disk the job needs free for its outputs and working files, in GB.
    #[serde(default)]
    pub disk_gb: Option<f64>,
    /// The time limit. Absent or null: none, for a job on paid backends only, which then runs
    /// until it has spent `max_cost_usd` (its limit is worked out from the price it is placed at).
    #[serde(default)]
    pub time: Option<String>,
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

/// A dependency: a job id, or `{"array": <array id>}` for every task of an array.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[serde(untagged)]
pub enum DepRef {
    Job(i64),
    Array { array: i64 },
}

impl std::fmt::Display for DepRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DepRef::Job(id) => write!(f, "job {id}"),
            DepRef::Array { array } => write!(f, "array {array}"),
        }
    }
}

/// Another job's outputs copied in as this job's inputs, under `$JOB_INPUT_DIR/<as>`.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct InputSpec {
    pub from_job: i64,
    #[serde(default)]
    pub glob: Option<String>,
    #[serde(rename = "as")]
    pub as_: String,
    /// Also take the outputs if that job failed (it becomes an `after_any` dependency).
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub allow_failed: bool,
}

/// `container` in a request: a registry image, or a Dockerfile in the snapshot to build.
#[derive(Deserialize, Clone, Debug, Default)]
#[serde(deny_unknown_fields)]
pub struct ContainerReq {
    #[serde(default)]
    pub image: Option<String>,
    #[serde(default)]
    pub build: Option<String>,
    /// Build context, relative to the repo root. Default: the Dockerfile's directory.
    #[serde(default)]
    pub context: Option<String>,
}

/// The body of `POST /v1/jobs`.
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
    pub container: Option<ContainerReq>,
    #[serde(default)]
    pub datasets: Vec<String>,
    #[serde(default)]
    pub inputs: Vec<InputSpec>,
    #[serde(default)]
    pub after_ok: Vec<DepRef>,
    #[serde(default)]
    pub after_any: Vec<DepRef>,
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
    /// Requeues after the job is lost (host gone, no heartbeat). Default 1.
    #[serde(default = "one")]
    pub retries: u32,
    #[serde(default)]
    pub priority: i64,
    #[serde(default)]
    pub idempotency_key: Option<String>,
    #[serde(default)]
    pub dry_run: bool,
}

/// A dataset version a job uses, resolved at submit time.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct DatasetUse {
    pub name: String,
    pub version: String,
}

/// An image build: a Dockerfile and its context inside the snapshot.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct BuildSpec {
    pub dockerfile: String,
    pub context: String,
    /// No registry: built with `docker build` on the Docker host that runs the job.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub local: bool,
}

/// The image a job runs in. For builds, `image` is the tag the build pushes.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct ContainerSpec {
    pub image: String,
    #[serde(default)]
    pub build: Option<BuildSpec>,
}

/// Everything about a job beyond M1's columns, stored as one JSON column.
#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq)]
pub struct JobSpec {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub after_ok: Vec<DepRef>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub after_any: Vec<DepRef>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub inputs: Vec<InputSpec>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub datasets: Vec<DatasetUse>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub container: Option<ContainerSpec>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_cost_usd: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub keep_outputs: Option<String>,
    /// A guest's job: it runs in a container cut off from the host (a private copy of the
    /// snapshot, no host paths, no tailnet or LAN).
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub sandbox: bool,
}

impl JobSpec {
    pub fn has_deps(&self) -> bool {
        !self.after_ok.is_empty() || !self.after_any.is_empty()
    }
}

/// Paths in globs and names: letters, digits and `_ . * ? / - [ ]`, relative, no `..`.
pub fn valid_glob(g: &str) -> bool {
    !g.is_empty()
        && !g.contains("..")
        && !g.starts_with('/')
        && g.chars().all(|c| c.is_ascii_alphanumeric() || "_.*?/-[]".contains(c))
}

/// Names used as directory names (input `as`, dataset names).
pub fn valid_dir_name(n: &str) -> bool {
    !n.is_empty()
        && n.len() <= 64
        && n != "."
        && n != ".."
        && n.chars().all(|c| c.is_ascii_alphanumeric() || "_.-".contains(c))
}

/// A path inside the snapshot (Dockerfile, build context).
pub fn valid_rel_path(p: &str) -> bool {
    !p.is_empty()
        && !p.starts_with('/')
        && !p.split('/').any(|seg| seg == "..")
        && p.chars().all(|c| c.is_ascii_alphanumeric() || "_.-/".contains(c))
}

/// Container image references: registry/name:tag or @digest, conservative character set.
pub fn valid_image_ref(r: &str) -> bool {
    !r.is_empty()
        && r.len() <= 300
        && !r.starts_with('-')
        && r.chars().all(|c| c.is_ascii_alphanumeric() || "_.-/:@".contains(c))
}

/// Validated, normalised resources stored with each job.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct Resources {
    pub nodes: u32,
    pub cpus: u32,
    pub mem_mb: u64,
    pub gpus: u32,
    pub gpu_type: Option<String>,
    /// The time limit. For a job with `no_time_limit`, 0 until it is placed, then the limit
    /// its `max_cost_usd` buys on the backend it was placed on.
    pub time_s: u64,
    /// Free disk the job needs (0: not stated).
    #[serde(default)]
    pub disk_mb: u64,
    /// No time limit was given: the job runs on paid backends only, until `max_cost_usd` is spent.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub no_time_limit: bool,
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
        if r.nodes == 0 || r.nodes > 64 {
            return Err(ApiError::bad_request("`resources.nodes` must be 1-64", "most jobs use 1"));
        }
        if r.nodes > 1 {
            if self.container.is_none() {
                return Err(ApiError::bad_request(
                    "multi-node jobs must be container jobs",
                    "set `container` so every node runs the same environment",
                ));
            }
            if self.array.is_some() {
                return Err(ApiError::bad_request("multi-node jobs can't be arrays", "submit one job per run"));
            }
        }
        if let Some(c) = &self.container {
            match (&c.image, &c.build) {
                (Some(_), Some(_)) | (None, None) => {
                    return Err(ApiError::bad_request(
                        "`container` needs exactly one of `image` or `build`",
                        "e.g. {\"image\": \"python:3.12-slim\"} or {\"build\": \"docker/train.Dockerfile\"}",
                    ));
                }
                (Some(i), None) if !valid_image_ref(i) => {
                    return Err(ApiError::bad_request(format!("image {i:?} is not a valid reference"), "e.g. ghcr.io/org/name:tag"));
                }
                (None, Some(b)) if !valid_rel_path(b) => {
                    return Err(ApiError::bad_request(format!("build path {b:?} is not a relative path in the repo"), "e.g. docker/train.Dockerfile"));
                }
                _ => {}
            }
            if c.context.as_deref().is_some_and(|x| !valid_rel_path(x) && x != ".") {
                return Err(ApiError::bad_request("`container.context` must be a relative path in the repo", "e.g. docker or ."));
            }
            if self.os.as_deref() == Some("mac") {
                return Err(ApiError::bad_request("container jobs run Linux images; `os: mac` can't be a container job", "drop `container` or `os`"));
            }
        }
        for inp in &self.inputs {
            if !valid_dir_name(&inp.as_) {
                return Err(ApiError::bad_request(format!("input name {:?} is invalid", inp.as_), "use letters, digits, _ . and -"));
            }
            if inp.glob.as_deref().is_some_and(|g| !valid_glob(g)) {
                return Err(ApiError::bad_request("input glob has characters that aren't allowed", "use letters, digits and _ . * ? / - [ ] only"));
            }
        }
        if let Some(g) = &self.keep_outputs
            && !valid_glob(g) {
                return Err(ApiError::bad_request("`keep_outputs` has characters that aren't allowed", "e.g. \"checkpoints/final/**\""));
            }
        if let Some(m) = self.max_cost_usd
            && !(m > 0.0 && m.is_finite()) {
                return Err(ApiError::bad_request("`max_cost_usd` must be positive", "e.g. 5"));
            }
        if self.quote_id.is_some() && self.max_cost_usd.is_none() {
            return Err(ApiError::bad_request("`quote_id` without `max_cost_usd`", "quotes are only for jobs that may run on paid backends"));
        }
        let res = r.check()?;
        if res.no_time_limit && self.max_cost_usd.is_none() {
            return Err(ApiError::bad_request(
                "`resources.time` is required",
                "give a limit (e.g. \"2h\"), or set max_cost_usd to run on paid backends with no limit: the job then runs until it has spent max_cost_usd",
            ));
        }
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
        Ok(res)
    }
}

impl ResourcesReq {
    /// Checks cpus, memory, disk and time and normalises them (nodes are checked by the caller).
    pub fn check(&self) -> Result<Resources, ApiError> {
        if self.cpus == 0 {
            return Err(ApiError::bad_request("`resources.cpus` must be at least 1", "set cpus >= 1"));
        }
        if !(self.mem_gb > 0.0 && self.mem_gb.is_finite()) {
            return Err(ApiError::bad_request("`resources.mem_gb` must be positive", "e.g. 4"));
        }
        if let Some(d) = self.disk_gb
            && !(d > 0.0 && d.is_finite())
        {
            return Err(ApiError::bad_request("`resources.disk_gb` must be positive", "e.g. 50"));
        }
        if self.gpu_type.as_deref().is_some_and(|t| t.trim().is_empty()) {
            return Err(ApiError::bad_request("`resources.gpu_type` is empty", "e.g. \"H100\", or null for any GPU"));
        }
        let time = match &self.time {
            None => None,
            Some(t) => Some(parse_duration(t).ok_or_else(|| {
                ApiError::bad_request(
                    format!("`resources.time` {t:?} is not a duration"),
                    "use forms like \"30m\", \"2h\", \"1h30m\", \"1d\", or null for no limit (paid backends only)",
                )
            })?),
        };
        Ok(Resources {
            nodes: self.nodes,
            cpus: self.cpus,
            mem_mb: (self.mem_gb * 1024.0).ceil() as u64,
            gpus: self.gpus,
            gpu_type: self.gpu_type.clone(),
            time_s: time.map_or(0, |t| t.as_secs()),
            disk_mb: self.disk_gb.map(|d| (d * 1024.0).ceil() as u64).unwrap_or(0),
            no_time_limit: time.is_none(),
        })
    }
}

impl From<&Resources> for ResourcesReq {
    /// A stored job's resources as a request, to apply an edit to.
    fn from(r: &Resources) -> Self {
        ResourcesReq {
            nodes: r.nodes,
            cpus: r.cpus,
            mem_gb: r.mem_mb as f64 / 1024.0,
            gpus: r.gpus,
            gpu_type: r.gpu_type.clone(),
            disk_gb: (r.disk_mb > 0).then(|| r.disk_mb as f64 / 1024.0),
            time: (!r.no_time_limit).then(|| format!("{}s", r.time_s)),
        }
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
    pub spec: JobSpec,
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
    pub spec: JobSpec,
}

impl Job {
    /// A transient job built from a request, used for dry runs and submit-time checks.
    pub fn preview(req: &JobRequest, res: Resources, spec: JobSpec) -> Job {
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
            spec,
        }
    }

    pub fn is_container(&self) -> bool {
        self.spec.container.is_some()
    }

    /// An image built on the Docker host that runs the job (no registry to share it).
    pub fn local_build(&self) -> bool {
        self.spec.container.as_ref().and_then(|c| c.build.as_ref()).is_some_and(|b| b.local)
    }

    /// The key used to group array tasks that can share one quote and one Slurm array.
    pub fn group_key(&self) -> String {
        match self.array_id {
            Some(a) => format!("a{a}"),
            None => format!("j{}", self.id),
        }
    }
}
