//! Sandbox lifecycle: create, poll, terminate, list and tag. Mirrors `go/sandbox.go`,
//! `go/sandbox_common.go`, `go/sandbox_impl_v1.go` and `go/sandbox_impl_v2.go` in
//! modal-labs/modal-client.
//!
//! Modal has two Sandbox backends. Like the Go SDK, this creates V2 Sandboxes unless a GPU is
//! requested (V2 has no GPUs) or `MODAL_SANDBOX_V2=0` is set, and routes every later call by
//! the shape of the Sandbox ID.

use std::collections::{BTreeMap, HashSet};
use std::fmt;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow, bail};
use tonic::{Code, Status};

use crate::client::{CallOpts, Client};
use crate::proto::client as pb;
use crate::proto::client::generic_result::GenericStatus;
use crate::router::{Access, RouterSeed, is_v1_sandbox_id};

/// How long creating a V1 (GPU) Sandbox waits for capacity (Go: `sandboxSchedulingTimeout`).
const SANDBOX_SCHEDULING_TIMEOUT: Duration = Duration::from_secs(21 * 60);
const TASK_ID_POLL_INTERVAL: Duration = Duration::from_millis(500);
/// Default Sandbox lifetime when `timeout_secs` is 0, as in the Go SDK.
const DEFAULT_TIMEOUT_SECS: u32 = 300;

/// What to run and with which resources; see [`Client::sandbox_create`].
#[derive(Clone, Debug, Default)]
pub struct SandboxSpec {
    /// App the Sandbox belongs to (from [`Client::app_get_or_create`]).
    pub app_id: String,
    /// Image to run (from [`Client::image_from_registry`]).
    pub image_id: String,
    /// Optional name, unique among the app's running Sandboxes (e.g. `compute-42`).
    pub name: Option<String>,
    /// Entrypoint command, e.g. `["sleep", "infinity"]`. Empty uses the image's default.
    pub entrypoint: Vec<String>,
    /// CPU request in physical cores; 0 leaves it to Modal's default.
    pub cpu: f64,
    /// Memory request in MiB; 0 leaves it to Modal's default.
    pub memory_mb: u32,
    /// Modal GPU string such as `"L40S"`, `"A100-80GB:2"` or `"H100:8"`.
    pub gpu: Option<String>,
    /// Hard lifetime limit enforced by Modal, in seconds; 0 means Modal's 5-minute default.
    pub timeout_secs: u32,
    /// Secrets whose keys become environment variables of the Sandbox.
    pub secret_ids: Vec<String>,
    /// Absolute working directory of the entrypoint and of exec'd commands.
    pub workdir: Option<String>,
    /// Cloud provider to run on (e.g. `"aws"`, `"gcp"`, `"oci"`).
    pub cloud: Option<String>,
    /// Regions to run in (e.g. `["us-east"]`); empty means anywhere.
    pub regions: Vec<String>,
}

/// How a Sandbox finished; see [`Client::sandbox_poll`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SandboxExit {
    /// Entrypoint exit code. As in the official SDKs, a Modal timeout is reported as 124 and
    /// termination (including [`Client::sandbox_terminate`]) as 137.
    pub exit_code: Option<i32>,
    /// Lower-case result status: `success`, `failure`, `terminated`, `timeout`, `init_failure`,
    /// `internal_failure`, `idle_timeout`, `memory_manager_eviction`, ...
    pub status: String,
    /// Failure message from Modal, when there is one.
    pub message: Option<String>,
}

/// A Sandbox returned by [`Client::sandbox_list`].
#[derive(Clone, Debug, PartialEq)]
pub struct SandboxInfo {
    /// Sandbox ID.
    pub sandbox_id: String,
    /// Tags set with [`Client::sandbox_set_tags`].
    pub tags: BTreeMap<String, String>,
    /// Creation time in seconds since the Unix epoch.
    pub created_at: Option<f64>,
    /// False once the Sandbox has reached a terminal state.
    pub running: bool,
}

/// Why waiting for a Sandbox's task failed (`pollTaskID` in Go).
#[derive(Debug)]
pub(crate) enum TaskIdError {
    /// The Sandbox finished before it was assigned a task.
    Finished(String),
    TimedOut(String),
    Rpc(Status),
}

impl fmt::Display for TaskIdError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TaskIdError::Finished(msg) | TaskIdError::TimedOut(msg) => f.write_str(msg),
            TaskIdError::Rpc(status) => write!(f, "SandboxGetTaskId: {status}"),
        }
    }
}

impl std::error::Error for TaskIdError {}

impl Client {
    /// Creates a Sandbox and returns its ID.
    ///
    /// GPU Sandboxes use Modal's V1 backend, and as in the Go SDK this call then waits (up to 21
    /// minutes) until the Sandbox is scheduled, terminating it if no capacity turns up. Other
    /// Sandboxes are V2 and return as soon as they are created.
    pub async fn sandbox_create(&self, spec: &SandboxSpec) -> Result<String> {
        let definition = sandbox_definition(spec)?;
        let use_v2 = spec.gpu.as_deref().is_none_or(str::is_empty) && sandbox_v2_enabled();
        if use_v2 {
            let req = pb::SandboxCreateV2Request {
                app_id: spec.app_id.clone(),
                secret_sources: spec
                    .secret_ids
                    .iter()
                    .map(|id| pb::SecretSource {
                        source: Some(pb::secret_source::Source::SecretId(id.clone())),
                    })
                    .collect(),
                definition: Some(definition),
                ..Default::default()
            };
            let resp = self
                .call(
                    "SandboxCreateV2",
                    req,
                    CallOpts::default(),
                    |mut c, r| async move { c.sandbox_create_v2(r).await },
                )
                .await
                .map_err(create_error)?;
            if !resp.task_id.is_empty() {
                let access = resp.command_router_access.map(|a| Access {
                    jwt: a.jwt,
                    url: a.url,
                });
                self.seed_router(
                    &resp.sandbox_id,
                    RouterSeed {
                        task_id: resp.task_id,
                        access,
                    },
                );
            }
            tracing::debug!(sandbox_id = resp.sandbox_id, "modal: created V2 sandbox");
            return Ok(resp.sandbox_id);
        }

        let req = pb::SandboxCreateRequest {
            app_id: spec.app_id.clone(),
            definition: Some(definition),
            ..Default::default()
        };
        let resp = self
            .call(
                "SandboxCreate",
                req,
                CallOpts::default(),
                |mut c, r| async move { c.sandbox_create(r).await },
            )
            .await
            .map_err(create_error)?;
        let sandbox_id = resp.sandbox_id;
        tracing::debug!(
            sandbox_id,
            "modal: created V1 sandbox; waiting for scheduling"
        );
        match self
            .wait_for_task_id(&sandbox_id, SANDBOX_SCHEDULING_TIMEOUT, true)
            .await
        {
            Ok(task_id) => {
                self.seed_router(
                    &sandbox_id,
                    RouterSeed {
                        task_id,
                        access: None,
                    },
                );
                Ok(sandbox_id)
            }
            Err(TaskIdError::Finished(msg)) => Err(anyhow!(
                "sandbox {sandbox_id} finished before it started: {msg}"
            )),
            Err(err) => {
                // Don't leave a queued Sandbox behind to start later with nobody watching it.
                if let Err(e) = self.sandbox_terminate(&sandbox_id).await {
                    tracing::debug!(sandbox_id, error = %e, "modal: failed to terminate unscheduled sandbox");
                }
                match err {
                    TaskIdError::TimedOut(_) => Err(anyhow!(
                        "insufficient capacity to create sandbox {sandbox_id}"
                    )),
                    other => Err(anyhow::Error::new(other)
                        .context(format!("waiting for sandbox {sandbox_id} to be scheduled"))),
                }
            }
        }
    }

    /// Polls a Sandbox: `None` while it runs, its exit information once it has finished.
    pub async fn sandbox_poll(&self, sandbox_id: &str) -> Result<Option<SandboxExit>> {
        let req = pb::SandboxWaitRequest {
            sandbox_id: sandbox_id.to_string(),
            timeout: 0.0,
        };
        let resp = if is_v1_sandbox_id(sandbox_id) {
            self.call(
                "SandboxWait",
                req,
                CallOpts::default(),
                |mut c, r| async move { c.sandbox_wait(r).await },
            )
            .await
        } else {
            self.call(
                "SandboxWaitV2",
                req,
                CallOpts::default(),
                |mut c, r| async move { c.sandbox_wait_v2(r).await },
            )
            .await
        }
        .with_context(|| format!("SandboxWait {sandbox_id}"))?;
        let Some(result) = resp
            .result
            .filter(|r| r.status() != GenericStatus::Unspecified)
        else {
            return Ok(None);
        };
        self.drop_router(sandbox_id);
        Ok(Some(sandbox_exit(&result)))
    }

    /// Terminates a Sandbox. Terminating one that no longer exists is not an error.
    pub async fn sandbox_terminate(&self, sandbox_id: &str) -> Result<()> {
        let req = pb::SandboxTerminateRequest {
            sandbox_id: sandbox_id.to_string(),
        };
        let result = if is_v1_sandbox_id(sandbox_id) {
            self.call(
                "SandboxTerminate",
                req,
                CallOpts::default(),
                |mut c, r| async move { c.sandbox_terminate(r).await },
            )
            .await
        } else {
            self.call(
                "SandboxTerminateV2",
                req,
                CallOpts::default(),
                |mut c, r| async move { c.sandbox_terminate_v2(r).await },
            )
            .await
        };
        self.drop_router(sandbox_id);
        match result {
            Ok(_) => Ok(()),
            Err(err) if err.code() == Code::NotFound => Ok(()),
            Err(err) => {
                Err(anyhow::Error::new(err).context(format!("SandboxTerminate {sandbox_id}")))
            }
        }
    }

    /// Lists the app's unfinished Sandboxes (both backends) with their tags and creation times,
    /// newest first.
    pub async fn sandbox_list(
        &self,
        app_id: &str,
        environment: Option<&str>,
    ) -> Result<Vec<SandboxInfo>> {
        let environment = environment.unwrap_or_default();
        let mut out = Vec::new();
        let mut seen = HashSet::new();
        for v2 in [false, true] {
            let mut before = 0.0;
            loop {
                let req = pb::SandboxListRequest {
                    app_id: app_id.to_string(),
                    before_timestamp: before,
                    // The Go SDK scopes V2 listings by app only when an app ID is given.
                    environment_name: if v2 && !app_id.is_empty() {
                        String::new()
                    } else {
                        environment.to_string()
                    },
                    include_finished: false,
                    tags: Vec::new(),
                };
                let resp = if v2 {
                    self.call(
                        "SandboxListV2",
                        req,
                        CallOpts::default(),
                        |mut c, r| async move { c.sandbox_list_v2(r).await },
                    )
                    .await
                } else {
                    self.call(
                        "SandboxList",
                        req,
                        CallOpts::default(),
                        |mut c, r| async move { c.sandbox_list(r).await },
                    )
                    .await
                }
                .context("SandboxList")?;
                let Some(last) = resp.sandboxes.last() else {
                    break;
                };
                let next_before = last.created_at;
                for info in resp.sandboxes {
                    if seen.insert(info.id.clone()) {
                        out.push(self.sandbox_info(info).await);
                    }
                }
                if next_before <= 0.0 || (before > 0.0 && next_before >= before) {
                    break;
                }
                before = next_before;
            }
        }
        out.sort_by(|a, b| {
            b.created_at
                .unwrap_or(0.0)
                .total_cmp(&a.created_at.unwrap_or(0.0))
        });
        Ok(out)
    }

    async fn sandbox_info(&self, info: pb::SandboxInfo) -> SandboxInfo {
        let mut tags: BTreeMap<String, String> = info
            .tags
            .into_iter()
            .map(|t| (t.tag_name, t.tag_value))
            .collect();
        if tags.is_empty() {
            // SandboxInfo.tags is marked "not yet exposed" upstream; fall back to SandboxTagsGet.
            match self.sandbox_get_tags(&info.id).await {
                Ok(t) => tags = t,
                Err(e) => {
                    tracing::debug!(sandbox_id = info.id, error = %e, "modal: failed to get sandbox tags")
                }
            }
        }
        let running = match &info.status {
            Some(status) => !status.is_terminal,
            None => info.task_info.as_ref().is_none_or(|t| t.result.is_none()),
        };
        SandboxInfo {
            sandbox_id: info.id,
            tags,
            created_at: (info.created_at > 0.0).then_some(info.created_at),
            running,
        }
    }

    async fn sandbox_get_tags(&self, sandbox_id: &str) -> Result<BTreeMap<String, String>> {
        let req = pb::SandboxTagsGetRequest {
            sandbox_id: sandbox_id.to_string(),
        };
        let resp = if is_v1_sandbox_id(sandbox_id) {
            self.call(
                "SandboxTagsGet",
                req,
                CallOpts::default(),
                |mut c, r| async move { c.sandbox_tags_get(r).await },
            )
            .await
        } else {
            self.call(
                "SandboxTagsGetV2",
                req,
                CallOpts::default(),
                |mut c, r| async move { c.sandbox_tags_get_v2(r).await },
            )
            .await
        }?;
        Ok(resp
            .tags
            .into_iter()
            .map(|t| (t.tag_name, t.tag_value))
            .collect())
    }

    /// Replaces the Sandbox's tags (key/value labels, e.g. to find leftovers after a restart).
    pub async fn sandbox_set_tags(
        &self,
        sandbox_id: &str,
        tags: &BTreeMap<String, String>,
    ) -> Result<()> {
        let req = pb::SandboxTagsSetRequest {
            environment_name: String::new(),
            sandbox_id: sandbox_id.to_string(),
            tags: tags
                .iter()
                .map(|(k, v)| pb::SandboxTag {
                    tag_name: k.clone(),
                    tag_value: v.clone(),
                })
                .collect(),
        };
        if is_v1_sandbox_id(sandbox_id) {
            self.call(
                "SandboxTagsSet",
                req,
                CallOpts::default(),
                |mut c, r| async move { c.sandbox_tags_set(r).await },
            )
            .await
        } else {
            self.call(
                "SandboxTagsSetV2",
                req,
                CallOpts::default(),
                |mut c, r| async move { c.sandbox_tags_set_v2(r).await },
            )
            .await
        }
        .with_context(|| format!("SandboxTagsSet {sandbox_id}"))
    }

    /// Polls until the Sandbox has a task (`pollTaskID` in Go). With `retry_transient`, transient
    /// lookup errors keep polling until the deadline instead of failing.
    pub(crate) async fn wait_for_task_id(
        &self,
        sandbox_id: &str,
        timeout: Duration,
        retry_transient: bool,
    ) -> Result<String, TaskIdError> {
        let deadline = Instant::now() + timeout;
        let no_time_left =
            || deadline.saturating_duration_since(Instant::now()) <= TASK_ID_POLL_INTERVAL;
        let timed_out = || {
            TaskIdError::TimedOut(format!(
                "timed out waiting for task ID for sandbox {sandbox_id}"
            ))
        };
        let v1 = is_v1_sandbox_id(sandbox_id);
        loop {
            let req = pb::SandboxGetTaskIdRequest {
                sandbox_id: sandbox_id.to_string(),
                ..Default::default()
            };
            let resp = if v1 {
                self.call(
                    "SandboxGetTaskId",
                    req,
                    CallOpts::default(),
                    |mut c, r| async move { c.sandbox_get_task_id(r).await },
                )
                .await
            } else {
                self.call(
                    "SandboxGetTaskIdV2",
                    req,
                    CallOpts::default(),
                    |mut c, r| async move { c.sandbox_get_task_id_v2(r).await },
                )
                .await
            };
            match resp {
                Err(err) => {
                    let transient = matches!(
                        err.code(),
                        Code::Unavailable | Code::DeadlineExceeded | Code::Internal
                    );
                    if !transient || (!retry_transient && Instant::now() < deadline) {
                        return Err(TaskIdError::Rpc(err));
                    }
                    if no_time_left() {
                        return Err(if err.code() == Code::Internal {
                            TaskIdError::Rpc(err)
                        } else {
                            timed_out()
                        });
                    }
                }
                Ok(resp) => {
                    if let Some(task_id) = resp.task_id.filter(|t| !t.is_empty()) {
                        return Ok(task_id);
                    }
                    if let Some(result) = resp.task_result {
                        let msg = if result.exception.is_empty() {
                            format!("sandbox {sandbox_id} has already finished")
                        } else {
                            result.exception
                        };
                        return Err(TaskIdError::Finished(msg));
                    }
                    if no_time_left() {
                        return Err(timed_out());
                    }
                }
            }
            tokio::time::sleep(TASK_ID_POLL_INTERVAL).await;
        }
    }
}

/// Builds the `Sandbox` definition the way `buildSandboxCreateRequestProto` does in Go.
fn sandbox_definition(spec: &SandboxSpec) -> Result<pb::Sandbox> {
    if let Some(workdir) = &spec.workdir
        && !workdir.starts_with('/')
    {
        bail!("workdir must be an absolute path, got: {workdir:?}");
    }
    if !(spec.cpu >= 0.0 && spec.cpu.is_finite()) {
        bail!("the CPU request ({}) must be a positive number", spec.cpu);
    }
    let resources = pb::Resources {
        milli_cpu: (spec.cpu * 1000.0) as u32,
        memory_mb: spec.memory_mb,
        gpu_config: Some(parse_gpu_config(spec.gpu.as_deref().unwrap_or_default())?),
        ..Default::default()
    };
    Ok(pb::Sandbox {
        entrypoint_args: spec.entrypoint.clone(),
        image_id: spec.image_id.clone(),
        secret_ids: spec.secret_ids.clone(),
        resources: Some(resources),
        timeout_secs: if spec.timeout_secs == 0 {
            DEFAULT_TIMEOUT_SECS
        } else {
            spec.timeout_secs
        },
        workdir: spec.workdir.clone(),
        network_access: Some(pb::NetworkAccess {
            network_access_type: pb::network_access::NetworkAccessType::Open as i32,
            ..Default::default()
        }),
        open_ports_oneof: Some(pb::sandbox::OpenPortsOneof::OpenPorts(pb::PortSpecs {
            ports: Vec::new(),
        })),
        cloud_provider_str: spec.cloud.clone().unwrap_or_default(),
        scheduler_placement: (!spec.regions.is_empty()).then(|| pb::SchedulerPlacement {
            regions: spec.regions.clone(),
            ..Default::default()
        }),
        // The Go SDK always sets the name, to "" when there is none.
        name: Some(spec.name.clone().unwrap_or_default()),
        ..Default::default()
    })
}

/// Parses `"TYPE"` or `"TYPE:COUNT"` (`parseGPUConfig` in Go). Empty means no GPU.
fn parse_gpu_config(gpu: &str) -> Result<pb::GpuConfig> {
    if gpu.is_empty() {
        return Ok(pb::GpuConfig::default());
    }
    let (gpu_type, count) = match gpu.split_once(':') {
        Some((gpu_type, count)) => {
            let count: u32 = count.parse().ok().filter(|c| *c >= 1).ok_or_else(|| {
                anyhow!("invalid GPU count: {count}, value must be a positive integer")
            })?;
            (gpu_type, count)
        }
        None => (gpu, 1),
    };
    Ok(pb::GpuConfig {
        r#type: 0,
        count,
        gpu_type: gpu_type.to_uppercase(),
    })
}

/// Whether non-GPU Sandboxes use the V2 backend: on unless `MODAL_SANDBOX_V2` is `0`/`false`/empty,
/// the same switch the Go SDK reads.
fn sandbox_v2_enabled() -> bool {
    match std::env::var("MODAL_SANDBOX_V2") {
        Ok(v) => !matches!(v.to_ascii_lowercase().as_str(), "" | "0" | "false"),
        Err(_) => true,
    }
}

fn create_error(err: Status) -> anyhow::Error {
    if err.code() == Code::AlreadyExists {
        anyhow!("sandbox already exists: {}", err.message())
    } else {
        anyhow::Error::new(err).context("SandboxCreate")
    }
}

/// Converts a Sandbox result the way `getReturnCode` does in Go.
fn sandbox_exit(result: &pb::GenericResult) -> SandboxExit {
    let status = result.status();
    let exit_code = match status {
        GenericStatus::Timeout => 124,
        GenericStatus::Terminated => 137,
        _ => result.exitcode,
    };
    let name = status
        .as_str_name()
        .trim_start_matches("GENERIC_STATUS_")
        .to_ascii_lowercase();
    SandboxExit {
        exit_code: Some(exit_code),
        status: name,
        message: (!result.exception.is_empty()).then(|| result.exception.clone()),
    }
}
