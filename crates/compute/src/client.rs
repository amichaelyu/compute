//! Talking to the daemon: configuration, SSH-key login and JSON requests.
//!
//! Configuration comes from `~/.config/compute/client.env` (or `$COMPUTE_CLIENT_CONFIG`), with
//! environment variables of the same names taking precedence:
//!   COMPUTE_URL   where computed listens (required)
//!   COMPUTE_KEY   an SSH public key listed in the daemon's allowed_signers
//!                 (default ~/.ssh/id_ed25519.pub; its private half may be in ssh-agent)
//!   COMPUTE_SKILL_DIR  where `compute update` keeps the skill (default: Claude Code's, plus
//!                 Codex's when ~/.codex exists)
//! The session token is cached in `~/.config/compute/session`.

use anyhow::{Context, Result, bail};
use serde_json::{Value, json};
use std::io::{Read, Write};
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::time::Duration;

/// An error body from the API: `{"error": {"code", "message", "hint"}}`.
#[derive(Debug)]
pub struct ApiError {
    pub status: u16,
    pub code: String,
    pub message: String,
    pub hint: String,
}

impl std::fmt::Display for ApiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} ({}): {}", self.code, self.status, self.message)?;
        if !self.hint.is_empty() {
            write!(f, "\nhint: {}", self.hint)?;
        }
        Ok(())
    }
}

impl std::error::Error for ApiError {}

#[derive(Clone)]
pub struct Client {
    pub url: String,
    key: PathBuf,
    session_file: PathBuf,
    agent: ureq::Agent,
}

/// The home directory: `$HOME`, else `%USERPROFILE%` (Windows).
pub fn home() -> PathBuf {
    ["HOME", "USERPROFILE"]
        .iter()
        .find_map(|k| std::env::var_os(k).filter(|v| !v.is_empty()))
        .map(PathBuf::from)
        .unwrap_or_default()
}

/// Expands `~`, `$HOME` and `${HOME}` (the forms client.env uses).
fn expand(v: &str) -> String {
    let h = home().to_string_lossy().into_owned();
    let v = v.replace("${HOME}", &h).replace("$HOME", &h);
    match v.strip_prefix("~/") {
        Some(rest) => format!("{h}/{rest}"),
        None => v,
    }
}

pub fn config_dir() -> PathBuf {
    std::env::var_os("XDG_CONFIG_HOME").map(PathBuf::from).unwrap_or_else(|| home().join(".config")).join("compute")
}

/// The client config file: `$COMPUTE_CLIENT_CONFIG`, else `~/.config/compute/client.env`.
pub fn config_file() -> PathBuf {
    std::env::var_os("COMPUTE_CLIENT_CONFIG").map(PathBuf::from).unwrap_or_else(|| config_dir().join("client.env"))
}

/// A setting: the environment variable, else its `KEY=value` line in the config file.
pub fn setting(key: &str) -> Option<String> {
    if let Some(v) = std::env::var(key).ok().filter(|v| !v.is_empty()) {
        return Some(expand(&v));
    }
    let text = std::fs::read_to_string(config_file()).ok()?;
    text.lines().find_map(|line| {
        let line = line.trim();
        let line = line.strip_prefix("export ").unwrap_or(line);
        let (k, v) = line.split_once('=')?;
        let v = v.split(" #").next().unwrap_or(v).trim().trim_matches('"').trim_matches('\'');
        (k.trim() == key && !v.is_empty()).then(|| expand(v))
    })
}

/// The SSH public key to sign in with: `COMPUTE_KEY`, else `~/.ssh/id_ed25519.pub`.
pub fn default_key() -> PathBuf {
    let k = setting("COMPUTE_KEY").unwrap_or_else(|| expand("~/.ssh/id_ed25519.pub"));
    PathBuf::from(if k.ends_with(".pub") { k } else { format!("{k}.pub") })
}

impl Client {
    pub fn from_env() -> Result<Client> {
        let url = setting("COMPUTE_URL").with_context(|| {
            format!(
                "COMPUTE_URL is not set: run `compute setup <address>`, or put COMPUTE_URL=http://<coordinator>:8765 in {}",
                config_file().display()
            )
        })?;
        Ok(Client::new(&url, default_key()))
    }

    pub fn new(url: &str, key: PathBuf) -> Client {
        let agent: ureq::Agent = ureq::Agent::config_builder()
            .http_status_as_error(false)
            .timeout_connect(Some(Duration::from_secs(10)))
            .build()
            .into();
        Client { url: url.trim_end_matches('/').to_string(), key, session_file: config_dir().join("session"), agent }
    }

    fn token(&self) -> Option<String> {
        std::fs::read_to_string(&self.session_file).ok().map(|t| t.trim().to_string()).filter(|t| !t.is_empty())
    }

    /// Signs a fresh challenge with the SSH key and caches the 12 h session token.
    pub fn login(&self) -> Result<()> {
        let mut r = self.agent.post(format!("{}/v1/auth/challenge", self.url)).send_empty().context("reaching the daemon")?;
        let ch: Value = r.body_mut().read_json()?;
        let nonce = ch["nonce"].as_str().context("no nonce in the challenge")?;
        // Sign with the private key next to the .pub when it's there, otherwise through ssh-agent.
        let private = self.key.with_extension("");
        let signer = if self.key.extension().is_some_and(|e| e == "pub") && private.is_file() { &private } else { &self.key };
        let mut child = Command::new("ssh-keygen")
            .args(["-Y", "sign", "-n", "compute", "-f"])
            .arg(signer)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .context("running ssh-keygen")?;
        child.stdin.take().context("ssh-keygen stdin")?.write_all(nonce.as_bytes())?;
        let out = child.wait_with_output()?;
        if !out.status.success() {
            bail!("signing with {} failed: {}", self.key.display(), String::from_utf8_lossy(&out.stderr).trim());
        }
        let public_key = std::fs::read_to_string(&self.key).with_context(|| format!("reading {}", self.key.display()))?;
        let body = json!({"nonce": nonce, "signature": String::from_utf8_lossy(&out.stdout), "public_key": public_key});
        let mut r = self.agent.post(format!("{}/v1/auth/token", self.url)).send_json(&body)?;
        let status = r.status().as_u16();
        let v: Value = r.body_mut().read_json().unwrap_or(Value::Null);
        if status != 200 {
            return Err(api_error(status, &v).into());
        }
        let token = v["token"].as_str().context("no token in the response")?;
        if let Some(dir) = self.session_file.parent() {
            std::fs::create_dir_all(dir)?;
        }
        write_private(&self.session_file, token)?;
        Ok(())
    }

    /// A JSON request; logs in again once on 401.
    pub fn call(&self, method: &str, path: &str, body: Option<&Value>) -> Result<Value> {
        for attempt in 0..2 {
            let (status, v) = self.call_once(method, path, body)?;
            if status == 401 && attempt == 0 {
                self.login()?;
                continue;
            }
            if status >= 400 {
                return Err(api_error(status, &v).into());
            }
            return Ok(v);
        }
        unreachable!()
    }

    fn call_once(&self, method: &str, path: &str, body: Option<&Value>) -> Result<(u16, Value)> {
        let url = format!("{}{path}", self.url);
        let auth = format!("Bearer {}", self.token().unwrap_or_default());
        let resp = match (method, body) {
            ("GET", _) => self.agent.get(&url).header("Authorization", &auth).call(),
            ("DELETE", _) => self.agent.delete(&url).header("Authorization", &auth).call(),
            (m, Some(b)) => self.with_body(m, &url)?.header("Authorization", &auth).send_json(b),
            (m, None) => self.with_body(m, &url)?.header("Authorization", &auth).send_empty(),
        };
        let mut r = resp.with_context(|| format!("reaching the daemon at {}", self.url))?;
        let status = r.status().as_u16();
        let text = r.body_mut().read_to_string().unwrap_or_default();
        let v = if text.trim().is_empty() { Value::Null } else { serde_json::from_str(&text).unwrap_or(Value::String(text)) };
        Ok((status, v))
    }

    fn with_body(&self, method: &str, url: &str) -> Result<ureq::RequestBuilder<ureq::typestate::WithBody>> {
        Ok(match method {
            "POST" => self.agent.post(url),
            "PUT" => self.agent.put(url),
            "PATCH" => self.agent.patch(url),
            m => bail!("unsupported method {m}"),
        })
    }

    pub fn get(&self, path: &str) -> Result<Value> {
        self.call("GET", path, None)
    }

    pub fn post(&self, path: &str, body: &Value) -> Result<Value> {
        self.call("POST", path, Some(body))
    }

    /// Uploads a zstd stream (snapshots, datasets). Logs in first if needed, since a stream
    /// can't be replayed.
    pub fn upload(&self, method: &str, path: &str, data: impl Read) -> Result<Value> {
        if self.get("/v1/whoami").is_err() {
            self.login()?;
        }
        let auth = format!("Bearer {}", self.token().unwrap_or_default());
        let mut data = data;
        let mut r = self
            .with_body(method, &format!("{}{path}", self.url))?
            .header("Authorization", &auth)
            .header("Content-Type", "application/zstd")
            .send(ureq::SendBody::from_reader(&mut data))?;
        let status = r.status().as_u16();
        let v: Value = r.body_mut().read_json().unwrap_or(Value::Null);
        if status >= 400 {
            return Err(api_error(status, &v).into());
        }
        Ok(v)
    }

    /// A public download (the CLI binaries), without signing in.
    pub fn fetch_public(&self, path: &str) -> Result<Vec<u8>> {
        let mut r = self.agent.get(format!("{}{path}", self.url)).call().with_context(|| format!("reaching {}", self.url))?;
        let status = r.status().as_u16();
        if status >= 400 {
            let v: Value = r.body_mut().read_json().unwrap_or(Value::Null);
            return Err(api_error(status, &v).into());
        }
        Ok(r.body_mut().with_config().limit(512 << 20).read_to_vec()?)
    }

    /// Streams a response body (job outputs).
    pub fn download(&self, path: &str) -> Result<impl Read + use<>> {
        if self.get("/v1/whoami").is_err() {
            self.login()?;
        }
        let auth = format!("Bearer {}", self.token().unwrap_or_default());
        let r = self.agent.get(format!("{}{path}", self.url)).header("Authorization", &auth).call()?;
        let status = r.status().as_u16();
        if status >= 400 {
            let mut r = r;
            let v: Value = r.body_mut().read_json().unwrap_or(Value::Null);
            return Err(api_error(status, &v).into());
        }
        Ok(r.into_body().into_reader())
    }
}

fn api_error(status: u16, v: &Value) -> ApiError {
    let e = &v["error"];
    ApiError {
        status,
        code: e["code"].as_str().unwrap_or("http").to_string(),
        message: e["message"].as_str().map(str::to_string).unwrap_or_else(|| format!("HTTP {status}")),
        hint: e["hint"].as_str().unwrap_or_default().to_string(),
    }
}

/// Writes a file only its owner may read (mode 0600 on Unix; Windows keeps the profile's ACL).
fn write_private(path: &std::path::Path, data: &str) -> Result<()> {
    let mut o = std::fs::OpenOptions::new();
    o.write(true).create(true).truncate(true);
    #[cfg(unix)]
    std::os::unix::fs::OpenOptionsExt::mode(&mut o, 0o600);
    o.open(path)?.write_all(data.as_bytes())?;
    Ok(())
}

/// Percent-encodes a query value.
pub fn q(s: &str) -> String {
    s.bytes()
        .map(|b| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => (b as char).to_string(),
            _ => format!("%{b:02X}"),
        })
        .collect()
}

pub fn err_text(e: &anyhow::Error) -> String {
    match e.downcast_ref::<ApiError>() {
        Some(a) => a.to_string(),
        None => format!("{e:#}"),
    }
}
