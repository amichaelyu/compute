//! Control plane connection: channel setup, required metadata headers, auth tokens and retries.
//! Mirrors `go/client.go` and `go/auth_token_manager.go` in modal-labs/modal-client.

use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow, bail};
use prost::Message;
use tonic::metadata::{AsciiMetadataValue, MetadataMap};
use tonic::transport::{Channel, ClientTlsConfig, Endpoint};
use tonic::{Code, Request, Response, Status};

use crate::proto::client as pb;
use crate::proto::client::modal_client_client::ModalClientClient;
use crate::router::RouterSlot;

/// Default control plane URL.
pub const DEFAULT_SERVER_URL: &str = "https://api.modal.com";

/// gRPC message size limit used by the official SDKs, both directions.
pub(crate) const MAX_MESSAGE_SIZE: usize = 100 * 1024 * 1024;
/// HTTP/2 stream and connection flow-control windows used by the official SDKs.
const WINDOW_SIZE: u32 = 64 * 1024 * 1024;

/// Modal identifies SDKs by client type; this crate speaks the Go SDK's protocol revision.
const CLIENT_TYPE_LIBMODAL_GO: i32 = pb::ClientType::LibmodalGo as i32;
/// "Behaves like this Python SDK version", as sent by the Go SDK.
const CLIENT_VERSION: &str = "1.0.0";
/// Version of the Go SDK whose behaviour this crate follows.
const LIBMODAL_VERSION: &str = "modal-go/0.11.2";

const DEFAULT_RETRIES: u32 = 3;
const RETRY_BASE_DELAY: Duration = Duration::from_millis(100);
const RETRY_MAX_DELAY: Duration = Duration::from_secs(1);
/// Upper bound on time spent honouring server-requested retry delays (throttling) for one call.
/// The Go SDK waits indefinitely unless `MODAL_MAX_THROTTLE_WAIT` is set; a daemon wants a bound.
pub(crate) const MAX_THROTTLE_WAIT: Duration = Duration::from_secs(600);

/// Fallback token lifetime when the auth token has no `exp` claim (Go: `DefaultExpiryOffset`).
const DEFAULT_TOKEN_LIFETIME_SECS: i64 = 20 * 60;
const AUTH_TOKEN_GET_TIMEOUT: Duration = Duration::from_secs(30);

const AUTH_TOKEN_GET_METHOD: &str = "AuthTokenGet";

/// Connection to Modal's control plane, authenticated with a workspace token.
///
/// Cheap to clone; clones share the connection, the auth token cache and the per-Sandbox
/// command router connections.
#[derive(Clone)]
pub struct Client {
    pub(crate) inner: Arc<Inner>,
}

pub(crate) struct Inner {
    stub: ModalClientClient<Channel>,
    token_id: AsciiMetadataValue,
    token_secret: AsciiMetadataValue,
    host: AsciiMetadataValue,
    auth: RwLock<Option<AuthToken>>,
    auth_refresh: tokio::sync::Mutex<()>,
    /// Image builder version per environment name, from `EnvironmentGetOrCreate`.
    pub(crate) builder_versions: Mutex<HashMap<String, String>>,
    /// Task command router connections, keyed by Sandbox ID.
    pub(crate) routers: Mutex<HashMap<String, Arc<RouterSlot>>>,
}

#[derive(Clone)]
struct AuthToken {
    token: AsciiMetadataValue,
    expiry: i64,
    refresh_at: i64,
}

/// Per-call options for [`Client::call`].
#[derive(Clone, Copy)]
pub(crate) struct CallOpts {
    /// Retries after the first attempt on transient gRPC codes.
    pub retries: u32,
    /// Deadline for each attempt.
    pub timeout: Option<Duration>,
}

impl Default for CallOpts {
    fn default() -> Self {
        CallOpts {
            retries: DEFAULT_RETRIES,
            timeout: None,
        }
    }
}

impl Client {
    /// Connects to Modal with a workspace token (`token_id` / `token_secret`, as in `~/.modal.toml`).
    ///
    /// `server_url` defaults to [`DEFAULT_SERVER_URL`]. The connection itself is established
    /// lazily, but this fetches an auth token up front, so bad credentials or an unreachable
    /// server fail here.
    pub async fn connect(
        token_id: &str,
        token_secret: &str,
        server_url: Option<&str>,
    ) -> Result<Client> {
        if token_id.is_empty() || token_secret.is_empty() {
            bail!("missing Modal token_id or token_secret");
        }
        let url = server_url.unwrap_or(DEFAULT_SERVER_URL);
        let (channel, host) = make_channel(url)?;
        let stub = ModalClientClient::new(channel)
            .max_decoding_message_size(MAX_MESSAGE_SIZE)
            .max_encoding_message_size(MAX_MESSAGE_SIZE);
        let client = Client {
            inner: Arc::new(Inner {
                stub,
                token_id: ascii(token_id).context("invalid token_id")?,
                token_secret: ascii(token_secret).context("invalid token_secret")?,
                host: ascii(&host)?,
                auth: RwLock::new(None),
                auth_refresh: tokio::sync::Mutex::new(()),
                builder_versions: Mutex::new(HashMap::new()),
                routers: Mutex::new(HashMap::new()),
            }),
        };
        client
            .auth_token()
            .await
            .context("failed to authenticate with Modal")?;
        Ok(client)
    }

    /// Adds the headers every control plane request carries (`injectRequiredHeaders` in Go).
    fn inject_required_headers(&self, md: &mut MetadataMap) {
        let inner = &self.inner;
        md.insert(
            "x-modal-client-type",
            AsciiMetadataValue::from(CLIENT_TYPE_LIBMODAL_GO),
        );
        md.insert(
            "x-modal-client-version",
            AsciiMetadataValue::from_static(CLIENT_VERSION),
        );
        md.insert(
            "x-modal-libmodal-version",
            AsciiMetadataValue::from_static(LIBMODAL_VERSION),
        );
        md.insert("x-modal-host", inner.host.clone());
        md.insert("x-modal-token-id", inner.token_id.clone());
        md.insert("x-modal-token-secret", inner.token_secret.clone());
    }

    /// Makes a unary control plane call with the Go SDK's interceptor chain: required headers,
    /// `x-modal-auth-token`, idempotency/retry headers, exponential backoff on transient codes,
    /// and server-directed retry delays (`RPCRetryPolicy` in the status details).
    pub(crate) async fn call<Req, Resp, F, Fut>(
        &self,
        method: &'static str,
        req: Req,
        opts: CallOpts,
        f: F,
    ) -> Result<Resp, Status>
    where
        Req: Clone,
        F: Fn(ModalClientClient<Channel>, Request<Req>) -> Fut,
        Fut: Future<Output = Result<Response<Resp>, Status>>,
    {
        let auth_token =
            if method == AUTH_TOKEN_GET_METHOD {
                None
            } else {
                // Boxed because fetching a token is itself a call.
                Some(Box::pin(self.auth_token()).await.map_err(|e| {
                    Status::unauthenticated(format!("failed to get auth token: {e:#}"))
                })?)
            };

        let idempotency_key = ascii(&uuid::Uuid::new_v4().to_string()).expect("uuid is ascii");
        let start = Instant::now();
        let mut delay = RETRY_BASE_DELAY;
        let mut attempt: u32 = 0;
        let mut throttle_retries: u32 = 0;
        loop {
            let mut request = Request::new(req.clone());
            let md = request.metadata_mut();
            self.inject_required_headers(md);
            if let Some(token) = &auth_token {
                md.insert("x-modal-auth-token", token.clone());
            }
            md.insert("x-idempotency-key", idempotency_key.clone());
            md.insert("x-retry-attempt", AsciiMetadataValue::from(attempt));
            md.insert(
                "x-throttle-retry-attempt",
                AsciiMetadataValue::from(throttle_retries),
            );
            let elapsed = ascii(&format!("{:.3}", start.elapsed().as_secs_f64())).expect("ascii");
            if attempt > 0 {
                md.insert("x-retry-delay", elapsed.clone());
            }
            if throttle_retries > 0 {
                md.insert("x-throttle-retry-delay", elapsed);
            }
            if let Some(timeout) = opts.timeout {
                request.set_timeout(timeout);
            }

            let err = match f(self.inner.stub.clone(), request).await {
                Ok(resp) => return Ok(resp.into_inner()),
                Err(err) => err,
            };

            if let Some(server_delay) = server_retry_after(&err) {
                if start.elapsed() + server_delay >= MAX_THROTTLE_WAIT {
                    return Err(err);
                }
                tracing::debug!(method, code = ?err.code(), message = err.message(), ?server_delay, "modal: server requested retry delay");
                throttle_retries += 1;
                tokio::time::sleep(server_delay).await;
                continue;
            }
            if !is_retryable(err.code()) || attempt >= opts.retries {
                return Err(err);
            }
            tracing::debug!(method, code = ?err.code(), message = err.message(), attempt, "modal: retrying transient failure");
            tokio::time::sleep(delay).await;
            delay = (delay * 2).min(RETRY_MAX_DELAY);
            attempt += 1;
        }
    }

    /// Builds a request for a server-streaming call. As in the Go SDK, streams only get the
    /// required headers (no auth token or retry headers); callers handle retries.
    pub(crate) fn stream_request<Req>(&self, req: Req) -> Request<Req> {
        let mut request = Request::new(req);
        self.inject_required_headers(request.metadata_mut());
        request
    }

    pub(crate) fn stub(&self) -> ModalClientClient<Channel> {
        self.inner.stub.clone()
    }

    /// Returns a valid auth token, fetching or refreshing it as needed. A failed refresh falls
    /// back to the cached token while it has not expired.
    async fn auth_token(&self) -> Result<AsciiMetadataValue> {
        let now = unix_now();
        if let Some(t) = self.cached_token()
            && now < t.refresh_at
        {
            return Ok(t.token);
        }

        let _guard = self.inner.auth_refresh.lock().await;
        let cached = self.cached_token();
        let now = unix_now();
        if let Some(t) = &cached
            && now < t.refresh_at
        {
            return Ok(t.token.clone());
        }
        let usable = cached.filter(|t| now < t.expiry);
        // With a usable cached token, a failed refresh is not user-visible, so don't retry.
        let retries = if usable.is_some() { 0 } else { DEFAULT_RETRIES };
        let opts = CallOpts {
            retries,
            timeout: Some(AUTH_TOKEN_GET_TIMEOUT),
        };
        let fetched = self
            .call(
                AUTH_TOKEN_GET_METHOD,
                pb::AuthTokenGetRequest {},
                opts,
                |mut c, r| async move { c.auth_token_get(r).await },
            )
            .await;
        match fetched {
            Ok(resp) if !resp.token.is_empty() => {
                let now = unix_now();
                let expiry = jwt_expiry(&resp.token).unwrap_or_else(|| {
                    tracing::warn!("modal: auth token has no exp claim");
                    now + DEFAULT_TOKEN_LIFETIME_SECS
                });
                // Refresh three quarters of the way through the token's remaining lifetime.
                let refresh_at = now + (expiry - now).max(0) * 3 / 4;
                let token = ascii(&resp.token).context("auth token is not ASCII")?;
                *self.inner.auth.write().expect("auth lock") = Some(AuthToken {
                    token: token.clone(),
                    expiry,
                    refresh_at,
                });
                Ok(token)
            }
            Ok(_) => Err(anyhow!(
                "internal error: did not receive auth token from server"
            )),
            Err(status) => match usable {
                Some(t)
                    if !matches!(
                        status.code(),
                        Code::Unauthenticated | Code::PermissionDenied
                    ) =>
                {
                    tracing::warn!(error = %status, "modal: auth token refresh failed; using cached token");
                    Ok(t.token)
                }
                _ => Err(anyhow::Error::new(status).context("AuthTokenGet")),
            },
        }
    }

    fn cached_token(&self) -> Option<AuthToken> {
        self.inner.auth.read().expect("auth lock").clone()
    }
}

/// Creates a lazily connecting channel with the Go SDK's transport settings. Returns the channel
/// and the URL's host name.
pub(crate) fn make_channel(url: &str) -> Result<(Channel, String)> {
    let uri: tonic::codegen::http::Uri = url
        .parse()
        .with_context(|| format!("invalid server URL: {url}"))?;
    let host = uri
        .host()
        .ok_or_else(|| anyhow!("server URL has no host: {url}"))?
        .to_string();
    let mut endpoint = Endpoint::from_shared(url.to_string())
        .with_context(|| format!("invalid server URL: {url}"))?
        .initial_stream_window_size(WINDOW_SIZE)
        .initial_connection_window_size(WINDOW_SIZE)
        .http2_keep_alive_interval(Duration::from_secs(30))
        .keep_alive_timeout(Duration::from_secs(10))
        .keep_alive_while_idle(true)
        .connect_timeout(Duration::from_secs(30))
        .tcp_nodelay(true);
    match uri.scheme_str() {
        Some("https") => {
            install_crypto_provider();
            let tls = ClientTlsConfig::new()
                .with_enabled_roots()
                .domain_name(host.clone());
            endpoint = endpoint.tls_config(tls).context("TLS configuration")?;
        }
        Some("http") => {}
        _ => bail!("server URL must start with https:// or http://: {url}"),
    }
    Ok((endpoint.connect_lazy(), host))
}

/// Makes ring the process-wide rustls provider unless one is installed already, so TLS works
/// even if another dependency also compiles in a different rustls provider.
fn install_crypto_provider() {
    let _ = rustls::crypto::ring::default_provider().install_default();
}

/// gRPC codes the Go SDK retries (`retryableGrpcStatusCodes`).
pub(crate) fn is_retryable(code: Code) -> bool {
    matches!(
        code,
        Code::DeadlineExceeded
            | Code::Unavailable
            | Code::Cancelled
            | Code::Internal
            | Code::Unknown
    )
}

/// `google.rpc.Status`, the payload of the `grpc-status-details-bin` trailer.
#[derive(Clone, PartialEq, Message)]
struct RpcStatus {
    #[prost(int32, tag = "1")]
    code: i32,
    #[prost(string, tag = "2")]
    message: String,
    #[prost(message, repeated, tag = "3")]
    details: Vec<prost_types::Any>,
}

/// Delay requested by the server through an `RPCRetryPolicy` status detail, if any
/// (`getServerRetryPolicy` in Go). At least 100 ms.
pub(crate) fn server_retry_after(status: &Status) -> Option<Duration> {
    let details = status.details();
    if details.is_empty() {
        return None;
    }
    let rpc_status = RpcStatus::decode(details).ok()?;
    rpc_status.details.iter().find_map(|any| {
        if !any.type_url.ends_with("/modal.client.RPCRetryPolicy") {
            return None;
        }
        let policy = pb::RpcRetryPolicy::decode(any.value.as_slice()).ok()?;
        let secs = if policy.retry_after_secs.is_finite() {
            policy.retry_after_secs.max(0.0)
        } else {
            0.0
        };
        Some(Duration::from_secs_f64(f64::from(secs)).max(Duration::from_millis(100)))
    })
}

/// Reads the `exp` claim of a JWT without verifying it.
pub(crate) fn jwt_expiry(jwt: &str) -> Option<i64> {
    use base64::Engine;
    let payload = jwt.split('.').nth(1)?;
    let decoded = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(payload.trim_end_matches('='))
        .ok()?;
    let claims: serde_json::Value = serde_json::from_slice(&decoded).ok()?;
    let exp = claims.get("exp")?;
    exp.as_i64().or_else(|| exp.as_f64().map(|f| f as i64))
}

pub(crate) fn unix_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

pub(crate) fn ascii(s: &str) -> Result<AsciiMetadataValue> {
    AsciiMetadataValue::try_from(s).map_err(|_| anyhow!("value is not valid ASCII header text"))
}
