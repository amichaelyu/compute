//! Task command router client: the per-Sandbox gRPC endpoint that runs commands and carries
//! their stdio. Mirrors `go/task_command_router_client.go` in modal-labs/modal-client.

use std::sync::{Arc, Mutex, RwLock, Weak};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow};
use bytes::Bytes;
use tokio::sync::{OnceCell, mpsc};
use tonic::metadata::AsciiMetadataValue;
use tonic::transport::Channel;
use tonic::{Code, Request, Response, Status};

use crate::client::{
    Client, Inner, MAX_MESSAGE_SIZE, MAX_THROTTLE_WAIT, ascii, is_retryable, jwt_expiry,
    make_channel, server_retry_after, unix_now,
};
use crate::proto::client as pb;
use crate::proto::task_command_router as tcr;
use crate::proto::task_command_router::task_command_router_client::TaskCommandRouterClient;

/// How long to wait for a Sandbox to be assigned a task before giving up (Go: `getTaskIDTimeout`).
const GET_TASK_ID_TIMEOUT: Duration = Duration::from_secs(5 * 60);
/// A JWT expiring later than this is not refreshed on an auth failure (Go: `jwtRefreshBufferSeconds`).
const JWT_REFRESH_BUFFER_SECS: i64 = 30;

/// Lazily connected router for one Sandbox, plus access details known from creating it.
#[derive(Default)]
pub(crate) struct RouterSlot {
    cell: OnceCell<Arc<Router>>,
    seed: Mutex<Option<RouterSeed>>,
}

/// Task ID and router access returned by `SandboxCreateV2`, good for the first connection only.
pub(crate) struct RouterSeed {
    pub task_id: String,
    pub access: Option<Access>,
}

#[derive(Clone)]
pub(crate) struct Access {
    pub jwt: String,
    pub url: String,
}

pub(crate) struct Router {
    control: Weak<Inner>,
    pub task_id: String,
    sandbox_id: String,
    is_v2: bool,
    url: String,
    stub: TaskCommandRouterClient<Channel>,
    jwt: RwLock<Jwt>,
    refresh: tokio::sync::Mutex<()>,
}

struct Jwt {
    bearer: AsciiMetadataValue,
    expiry: Option<i64>,
}

/// Retry policy for router calls (`retryOptions` in Go).
#[derive(Clone, Copy)]
pub(crate) struct RetryOpts {
    pub base_delay: Duration,
    pub factor: u32,
    /// `None` retries forever.
    pub max_retries: Option<u32>,
    pub deadline: Option<Instant>,
    pub per_call_timeout: Option<Duration>,
}

impl Default for RetryOpts {
    fn default() -> Self {
        RetryOpts {
            base_delay: Duration::from_millis(10),
            factor: 2,
            max_retries: Some(10),
            deadline: None,
            per_call_timeout: None,
        }
    }
}

/// Whether a Sandbox ID belongs to the original (V1) Sandbox backend. Other IDs are V2
/// (`isV1SandboxID` in Go).
pub(crate) fn is_v1_sandbox_id(sandbox_id: &str) -> bool {
    match sandbox_id.split_once('-') {
        Some(("sb", suffix)) => {
            suffix.len() == 22 && suffix.bytes().all(|b| b.is_ascii_alphanumeric())
        }
        _ => false,
    }
}

impl Client {
    /// Remembers the task and router access returned when a V2 Sandbox was created.
    pub(crate) fn seed_router(&self, sandbox_id: &str, seed: RouterSeed) {
        let slot = self.router_slot(sandbox_id);
        *slot.seed.lock().expect("router seed lock") = Some(seed);
    }

    /// Forgets the router connection of a Sandbox that has finished.
    pub(crate) fn drop_router(&self, sandbox_id: &str) {
        self.inner
            .routers
            .lock()
            .expect("routers lock")
            .remove(sandbox_id);
    }

    fn router_slot(&self, sandbox_id: &str) -> Arc<RouterSlot> {
        self.inner
            .routers
            .lock()
            .expect("routers lock")
            .entry(sandbox_id.to_string())
            .or_default()
            .clone()
    }

    /// Returns the command router for a Sandbox, waiting (up to 5 minutes) for the Sandbox to be
    /// scheduled and connecting on first use.
    pub(crate) async fn router(&self, sandbox_id: &str) -> Result<Arc<Router>> {
        let slot = self.router_slot(sandbox_id);
        let router = slot
            .cell
            .get_or_try_init(|| async {
                let seed = slot.seed.lock().expect("router seed lock").take();
                let is_v2 = !is_v1_sandbox_id(sandbox_id);
                let (task_id, access) = match seed {
                    Some(seed) => (seed.task_id, seed.access),
                    None => (
                        self.wait_for_task_id(sandbox_id, GET_TASK_ID_TIMEOUT, false)
                            .await?,
                        None,
                    ),
                };
                let access = match access {
                    Some(access) => access,
                    None => {
                        self.command_router_access(sandbox_id, &task_id, is_v2)
                            .await?
                    }
                };
                Router::new(self, sandbox_id, task_id, is_v2, access).map(Arc::new)
            })
            .await?;
        Ok(router.clone())
    }

    /// Fetches router URL and JWT (`getCommandRouterAccess` in Go).
    async fn command_router_access(
        &self,
        sandbox_id: &str,
        task_id: &str,
        is_v2: bool,
    ) -> Result<Access> {
        if is_v2 {
            let req = pb::SandboxGetCommandRouterAccessRequest {
                target: Some(
                    pb::sandbox_get_command_router_access_request::Target::SandboxId(
                        sandbox_id.to_string(),
                    ),
                ),
            };
            let resp = self
                .call(
                    "SandboxGetCommandRouterAccess",
                    req,
                    Default::default(),
                    |mut c, r| async move { c.sandbox_get_command_router_access(r).await },
                )
                .await
                .context("SandboxGetCommandRouterAccess")?;
            Ok(Access {
                jwt: resp.jwt,
                url: resp.url,
            })
        } else {
            let req = pb::TaskGetCommandRouterAccessRequest {
                task_id: task_id.to_string(),
            };
            let resp = self
                .call(
                    "TaskGetCommandRouterAccess",
                    req,
                    Default::default(),
                    |mut c, r| async move { c.task_get_command_router_access(r).await },
                )
                .await
                .context("TaskGetCommandRouterAccess")?;
            Ok(Access {
                jwt: resp.jwt,
                url: resp.url,
            })
        }
    }
}

impl Router {
    fn new(
        client: &Client,
        sandbox_id: &str,
        task_id: String,
        is_v2: bool,
        access: Access,
    ) -> Result<Router> {
        if !access.url.starts_with("https://") {
            return Err(anyhow!(
                "task router URL must be https, got: {}",
                access.url
            ));
        }
        let (channel, _) = make_channel(&access.url)?;
        let stub = TaskCommandRouterClient::new(channel)
            .max_decoding_message_size(MAX_MESSAGE_SIZE)
            .max_encoding_message_size(MAX_MESSAGE_SIZE);
        tracing::debug!(
            sandbox_id,
            task_id,
            url = access.url,
            "modal: connecting to task command router"
        );
        Ok(Router {
            control: Arc::downgrade(&client.inner),
            task_id,
            sandbox_id: sandbox_id.to_string(),
            is_v2,
            url: access.url,
            stub,
            jwt: RwLock::new(Jwt::parse(&access.jwt)?),
            refresh: tokio::sync::Mutex::new(()),
        })
    }

    pub(crate) fn stub(&self) -> TaskCommandRouterClient<Channel> {
        self.stub.clone()
    }

    /// Wraps a message with the router's auth headers.
    pub(crate) fn request<T>(&self, msg: T, throttle_attempt: u32) -> Request<T> {
        let mut request = Request::new(msg);
        let md = request.metadata_mut();
        md.insert(
            "authorization",
            self.jwt.read().expect("jwt lock").bearer.clone(),
        );
        md.insert(
            "x-throttle-retry-attempt",
            AsciiMetadataValue::from(throttle_attempt),
        );
        request
    }

    /// Refreshes the JWT unless it is still valid for a while (`refreshJwt` in Go).
    pub(crate) async fn refresh_jwt(&self) -> Result<()> {
        let fresh_enough = || {
            let exp = self.jwt.read().expect("jwt lock").expiry;
            matches!(exp, Some(exp) if exp - unix_now() > JWT_REFRESH_BUFFER_SECS)
        };
        if fresh_enough() {
            return Ok(());
        }
        let _guard = self.refresh.lock().await;
        if fresh_enough() {
            return Ok(());
        }
        let control = self
            .control
            .upgrade()
            .ok_or_else(|| anyhow!("Modal client was dropped"))?;
        let client = Client { inner: control };
        let access = client
            .command_router_access(&self.sandbox_id, &self.task_id, self.is_v2)
            .await
            .context("failed to refresh command router JWT")?;
        if access.url != self.url {
            tracing::warn!(
                sandbox_id = self.sandbox_id,
                "modal: task router URL changed during session"
            );
        }
        let jwt = match Jwt::parse(&access.jwt) {
            Ok(jwt) => jwt,
            Err(_) => Jwt {
                bearer: ascii(&format!("Bearer {}", access.jwt))?,
                expiry: None,
            },
        };
        *self.jwt.write().expect("jwt lock") = jwt;
        Ok(())
    }

    /// Unary router call with one JWT refresh on `Unauthenticated` and retries on transient
    /// errors (`callWithRetriesOnTransientErrors` + `callWithAuthRetry` in Go).
    pub(crate) async fn unary<Req, Resp, F, Fut>(
        &self,
        req: Req,
        opts: RetryOpts,
        f: F,
    ) -> Result<Resp, Status>
    where
        Req: Clone,
        F: Fn(TaskCommandRouterClient<Channel>, Request<Req>) -> Fut,
        Fut: std::future::Future<Output = Result<Response<Resp>, Status>>,
    {
        let start = Instant::now();
        let mut delay = opts.base_delay;
        let mut retries: u32 = 0;
        let mut throttle_retries: u32 = 0;
        let mut throttle_wait = Duration::ZERO;
        loop {
            if opts.deadline.is_some_and(|d| Instant::now() >= d) {
                return Err(Status::deadline_exceeded("deadline exceeded"));
            }
            let attempt = |throttle: u32| {
                let mut request = self.request(req.clone(), throttle);
                if let Some(timeout) = opts.per_call_timeout {
                    request.set_timeout(timeout);
                }
                f(self.stub(), request)
            };
            let mut result = attempt(throttle_retries).await;
            if let Err(err) = &result
                && err.code() == Code::Unauthenticated
            {
                self.refresh_jwt()
                    .await
                    .map_err(|e| Status::unauthenticated(format!("{e:#}")))?;
                result = attempt(throttle_retries).await;
            }
            let err = match result {
                Ok(resp) => return Ok(resp.into_inner()),
                Err(err) => err,
            };

            if err.code() == Code::ResourceExhausted
                && let Some(retry_after) = server_retry_after(&err)
            {
                if opts
                    .deadline
                    .is_some_and(|d| Instant::now() + retry_after >= d)
                {
                    return Err(Status::deadline_exceeded("deadline exceeded"));
                }
                if throttle_wait + retry_after > MAX_THROTTLE_WAIT {
                    return Err(err);
                }
                tokio::time::sleep(retry_after).await;
                throttle_retries += 1;
                throttle_wait += retry_after;
                continue;
            }
            if !is_retryable(err.code()) || opts.max_retries.is_some_and(|max| retries >= max) {
                return Err(err);
            }
            let mut sleep_for = delay;
            if let Some(deadline) = opts.deadline {
                sleep_for = sleep_for.min(deadline.saturating_duration_since(Instant::now()));
            }
            tracing::debug!(code = ?err.code(), message = err.message(), elapsed = ?start.elapsed(), "modal: retrying command router call");
            tokio::time::sleep(sleep_for).await;
            delay *= opts.factor;
            retries += 1;
        }
    }

    /// Streams one exec's stdout or stderr into `tx`, reopening the stream at the current offset
    /// after transient errors (`execStdioReader` in Go). Ends when the output is finished, an
    /// error is fatal (sent to `tx`), or the receiver is dropped.
    pub(crate) async fn pump_exec_stdio(
        self: Arc<Self>,
        exec_id: String,
        fd: tcr::TaskExecStdioFileDescriptor,
        tx: mpsc::Sender<std::io::Result<Bytes>>,
    ) {
        let mut offset: u64 = 0;
        let mut retry = StdioRetry::new();
        loop {
            let req = tcr::TaskExecStdioReadRequest {
                task_id: self.task_id.clone(),
                exec_id: exec_id.clone(),
                offset,
                file_descriptor: fd as i32,
            };
            let mut stub = self.stub();
            let opened = tokio::select! {
                _ = tx.closed() => return,
                r = stub.task_exec_stdio_read(self.request(req, 0)) => r,
            };
            let mut stream = match opened {
                Ok(resp) => resp.into_inner(),
                Err(err) => match retry.recover(&self, err).await {
                    Ok(()) => continue,
                    Err(err) => {
                        let _ = tx.send(Err(status_to_io(err))).await;
                        return;
                    }
                },
            };
            loop {
                let next = tokio::select! {
                    _ = tx.closed() => return,
                    m = stream.message() => m,
                };
                match next {
                    Ok(Some(item)) => {
                        retry.reset();
                        offset += item.data.len() as u64;
                        if item.data.is_empty() {
                            continue;
                        }
                        if tx.send(Ok(Bytes::from(item.data))).await.is_err() {
                            return;
                        }
                    }
                    Ok(None) => return,
                    Err(err) => match retry.recover(&self, err).await {
                        Ok(()) => break,
                        Err(err) => {
                            let _ = tx.send(Err(status_to_io(err))).await;
                            return;
                        }
                    },
                }
            }
        }
    }
}

impl Jwt {
    fn parse(jwt: &str) -> Result<Jwt> {
        if jwt.split('.').count() != 3 {
            return Err(anyhow!("malformed command router JWT"));
        }
        Ok(Jwt {
            bearer: ascii(&format!("Bearer {jwt}"))?,
            expiry: jwt_expiry(jwt),
        })
    }
}

/// Backoff for reopening stdio streams (`stdioRetry` in Go).
struct StdioRetry {
    delay: Duration,
    remaining: u32,
    did_auth_retry: bool,
}

impl StdioRetry {
    fn new() -> Self {
        StdioRetry {
            delay: Duration::from_millis(10),
            remaining: 10,
            did_auth_retry: false,
        }
    }

    fn reset(&mut self) {
        *self = StdioRetry::new();
    }

    /// Returns `Ok` after waiting out the backoff if the error is worth retrying.
    async fn recover(&mut self, router: &Router, err: Status) -> Result<(), Status> {
        if err.code() == Code::Unauthenticated && !self.did_auth_retry {
            router
                .refresh_jwt()
                .await
                .map_err(|e| Status::unauthenticated(format!("{e:#}")))?;
            self.did_auth_retry = true;
            return Ok(());
        }
        if !is_retryable(err.code()) || self.remaining == 0 {
            return Err(err);
        }
        tracing::debug!(code = ?err.code(), message = err.message(), delay = ?self.delay, "modal: retrying stdio read");
        tokio::time::sleep(self.delay).await;
        self.delay *= 2;
        self.remaining -= 1;
        Ok(())
    }
}

fn status_to_io(status: Status) -> std::io::Error {
    std::io::Error::other(format!("error reading exec output: {status}"))
}
