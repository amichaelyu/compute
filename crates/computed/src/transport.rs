//! Reaching hosts: ssh (direct or through a `rex forward` tunnel) and `rex run`.
//!
//! Remote scripts are built by the daemon from its own values plus `shell_quote`d paths. Job
//! commands are never interpolated into them; they travel as file contents.

use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use std::collections::HashMap;
use std::pin::Pin;
use std::process::Stdio;
use std::task::Poll;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWriteExt, ReadBuf};
use tokio::process::{Child, ChildStdout, Command};
use tokio::sync::Mutex;

/// PATH additions so `uv`, Homebrew tools, Docker (OrbStack) and user binaries are found in
/// non-interactive shells.
pub const PATH_PRELUDE: &str =
    "export PATH=\"$HOME/.local/bin:$HOME/.orbstack/bin:/opt/homebrew/bin:/usr/local/bin:$PATH\"";

/// A byte stream (tar archives going to or coming from a host).
pub type Reader = Box<dyn AsyncRead + Send + Unpin>;

/// Somewhere scripts run: a host over ssh, or a cloud sandbox through its exec API.
#[async_trait]
pub trait Shell: Send + Sync {
    /// Runs a bash script, optionally feeding `input` to stdin. Fails on a non-zero exit.
    async fn run_input(&self, script: &str, input: Option<Vec<u8>>) -> Result<Output>;
    async fn run(&self, script: &str) -> Result<Output> {
        self.run_input(script, None).await
    }
    /// Runs a script with `data` streamed to its stdin (for `tar -x`).
    async fn pipe_in(&self, script: &str, data: Reader) -> Result<()>;
    /// Starts a script and returns its stdout as a stream (for `tar -c`).
    async fn pipe_out(&self, script: &str) -> Result<Reader>;
}

/// A child process's stdout that keeps the child alive while it's being read.
pub struct ChildReader {
    _child: Child,
    stdout: ChildStdout,
}

impl ChildReader {
    pub fn new(mut child: Child) -> Result<ChildReader> {
        let stdout = child.stdout.take().context("child has no stdout")?;
        Ok(ChildReader { _child: child, stdout })
    }
}

impl AsyncRead for ChildReader {
    fn poll_read(mut self: Pin<&mut Self>, cx: &mut std::task::Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.stdout).poll_read(cx, buf)
    }
}

/// Single-quotes a string for POSIX shells.
pub fn shell_quote(s: &str) -> String {
    format!("'{}'", s.replace('\'', r"'\''"))
}

#[derive(Clone, Debug)]
pub struct Ssh {
    pub host: String,
    pub user: Option<String>,
    pub port: Option<u16>,
    /// True when going through a local `rex forward` tunnel on 127.0.0.1.
    pub tunnel: bool,
    /// Backend name, used for `HostKeyAlias` on tunnels and in error messages.
    pub alias: String,
    /// A throwaway cloud instance: don't record or check its host key.
    pub ephemeral: bool,
}

pub struct Output {
    pub stdout: Vec<u8>,
}

impl Output {
    pub fn text(&self) -> String {
        String::from_utf8_lossy(&self.stdout).into_owned()
    }
}

impl Ssh {
    fn command(&self, script: &str) -> Command {
        let mut c = Command::new("ssh");
        c.args([
            "-o", "BatchMode=yes",
            "-o", "ConnectTimeout=15",
            "-o", "ServerAliveInterval=15",
            "-o", "ServerAliveCountMax=4",
        ]);
        if let Some(p) = self.port {
            c.arg("-p").arg(p.to_string());
        }
        if self.tunnel {
            // Every tunnel is 127.0.0.1, so pin host keys per backend instead of per address.
            c.arg("-o").arg(format!("HostKeyAlias=compute-{}", self.alias));
            c.args(["-o", "StrictHostKeyChecking=accept-new"]);
        }
        if self.ephemeral {
            c.args(["-o", "StrictHostKeyChecking=no", "-o", "UserKnownHostsFile=/dev/null", "-o", "LogLevel=ERROR"]);
        }
        let dest = match &self.user {
            Some(u) => format!("{u}@{}", self.host),
            None => self.host.clone(),
        };
        let full = format!("{PATH_PRELUDE}\n{script}");
        c.arg(dest).arg(format!("bash -lc {}", shell_quote(&full)));
        c.kill_on_drop(true);
        c
    }

    async fn run_input_impl(&self, script: &str, input: Option<Vec<u8>>) -> Result<Output> {
        let mut c = self.command(script);
        c.stdin(if input.is_some() { Stdio::piped() } else { Stdio::null() })
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let mut child = c.spawn().context("spawning ssh")?;
        if let Some(data) = input {
            let mut stdin = child.stdin.take().context("ssh stdin")?;
            stdin.write_all(&data).await.context("writing to ssh")?;
            drop(stdin);
        }
        let out = tokio::time::timeout(Duration::from_secs(600), child.wait_with_output())
            .await
            .context("ssh timed out after 600 s")??;
        if !out.status.success() {
            bail!(
                "ssh {} failed ({}): {}",
                self.alias,
                out.status,
                String::from_utf8_lossy(&out.stderr).trim()
            );
        }
        Ok(Output { stdout: out.stdout })
    }

    /// Starts a script whose stdin and/or stdout the caller streams.
    pub fn spawn(&self, script: &str, stdin: Stdio, stdout: Stdio) -> Result<Child> {
        let mut c = self.command(script);
        c.stdin(stdin).stdout(stdout).stderr(Stdio::piped());
        c.spawn().context("spawning ssh")
    }
}

#[async_trait]
impl Shell for Ssh {
    async fn run_input(&self, script: &str, input: Option<Vec<u8>>) -> Result<Output> {
        self.run_input_impl(script, input).await
    }

    async fn pipe_in(&self, script: &str, mut data: Reader) -> Result<()> {
        let mut child = self.spawn(script, Stdio::piped(), Stdio::null())?;
        let mut stdin = child.stdin.take().context("ssh stdin")?;
        let copy = tokio::io::copy(&mut data, &mut stdin).await;
        drop(stdin);
        let out = child.wait_with_output().await?;
        if !out.status.success() {
            bail!("ssh {} failed ({}): {}", self.alias, out.status, crate::backend::tail(&String::from_utf8_lossy(&out.stderr), 2000));
        }
        copy.context("streaming data")?;
        Ok(())
    }

    async fn pipe_out(&self, script: &str) -> Result<Reader> {
        let mut c = self.command(script);
        c.stdin(Stdio::null()).stdout(Stdio::piped()).stderr(Stdio::null());
        let child = c.spawn().context("spawning ssh")?;
        Ok(Box::new(ChildReader::new(child)?))
    }
}

/// A shell on the coordinator itself, for outputs kept there after cloud jobs.
pub struct LocalShell;

#[async_trait]
impl Shell for LocalShell {
    async fn run_input(&self, script: &str, input: Option<Vec<u8>>) -> Result<Output> {
        let mut c = Command::new("bash");
        c.arg("-c").arg(script).kill_on_drop(true);
        c.stdin(if input.is_some() { Stdio::piped() } else { Stdio::null() }).stdout(Stdio::piped()).stderr(Stdio::piped());
        let mut child = c.spawn().context("spawning bash")?;
        if let Some(data) = input {
            let mut stdin = child.stdin.take().context("stdin")?;
            stdin.write_all(&data).await?;
            drop(stdin);
        }
        let out = child.wait_with_output().await?;
        if !out.status.success() {
            bail!("local script failed ({}): {}", out.status, String::from_utf8_lossy(&out.stderr).trim());
        }
        Ok(Output { stdout: out.stdout })
    }

    async fn pipe_in(&self, script: &str, mut data: Reader) -> Result<()> {
        let mut child = Command::new("bash")
            .arg("-c")
            .arg(script)
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()?;
        let mut stdin = child.stdin.take().context("stdin")?;
        let copy = tokio::io::copy(&mut data, &mut stdin).await;
        drop(stdin);
        let out = child.wait_with_output().await?;
        if !out.status.success() {
            bail!("local script failed: {}", String::from_utf8_lossy(&out.stderr).trim());
        }
        copy?;
        Ok(())
    }

    async fn pipe_out(&self, script: &str) -> Result<Reader> {
        let child = Command::new("bash")
            .arg("-c")
            .arg(script)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .spawn()?;
        Ok(Box::new(ChildReader::new(child)?))
    }
}

/// `rex forward` tunnels to local hosts' sshd, kept alive by the daemon.
#[derive(Default)]
pub struct Tunnels {
    children: Mutex<HashMap<String, Child>>,
}

impl Tunnels {
    pub async fn ensure(&self, label: &str, port: u16) -> Result<()> {
        let mut map = self.children.lock().await;
        if let Some(child) = map.get_mut(label) {
            if child.try_wait()?.is_none() && port_open(port).await {
                return Ok(());
            }
            let _ = child.kill().await;
            map.remove(label);
        }
        ensure_rex_session(label).await?;
        let child = Command::new("rex")
            .args(["-S", label, "forward", "new", "-s", "compute", "-L"])
            .arg(format!("{port}:localhost:22"))
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .spawn()
            .with_context(|| format!("starting rex forward to {label}"))?;
        map.insert(label.to_string(), child);
        for _ in 0..40 {
            if port_open(port).await {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
        bail!("rex forward to {label} did not open port {port} within 10 s")
    }
}

async fn port_open(port: u16) -> bool {
    matches!(
        tokio::time::timeout(
            Duration::from_secs(2),
            tokio::net::TcpStream::connect(("127.0.0.1", port))
        )
        .await,
        Ok(Ok(_))
    )
}

async fn rex(args: &[&str]) -> Result<String> {
    let out = tokio::time::timeout(
        Duration::from_secs(60),
        Command::new("rex").args(args).stdin(Stdio::null()).output(),
    )
    .await
    .context("rex timed out")??;
    if !out.status.success() {
        bail!(
            "rex {} failed: {}",
            args.join(" "),
            String::from_utf8_lossy(&out.stderr).trim()
        );
    }
    Ok(String::from_utf8_lossy(&out.stdout).into_owned())
}

/// Makes sure the host has a Rex session named `compute` for job blocks and tunnels.
pub async fn ensure_rex_session(label: &str) -> Result<()> {
    let list = rex(&["-S", label, "ls", "--json"]).await?;
    let exists = serde_json::from_str::<serde_json::Value>(&list)
        .ok()
        .map(|v| json_has_label(&v, "compute"))
        .unwrap_or_else(|| list.contains("\"compute\""));
    if !exists {
        rex(&["-S", label, "new", "compute"]).await?;
    }
    Ok(())
}

fn json_has_label(v: &serde_json::Value, name: &str) -> bool {
    match v {
        serde_json::Value::Object(m) => {
            m.get("label").and_then(|l| l.as_str()) == Some(name)
                || m.get("name").and_then(|l| l.as_str()) == Some(name)
                || m.values().any(|x| json_has_label(x, name))
        }
        serde_json::Value::Array(a) => a.iter().any(|x| json_has_label(x, name)),
        _ => false,
    }
}

/// The login user of a Rex host: runs `id -un` there in a short-lived window, reads it back and
/// closes the window. Used as the ssh user for tunnels, so configs need no usernames.
pub async fn rex_whoami(label: &str) -> Result<String> {
    ensure_rex_session(label).await?;
    let out = rex(&[
        "-S", label, "run", "-s", "compute", "--focus=false", "--keep-open", "--json",
        "--label", "compute: whoami", "--", "id", "-un",
    ])
    .await?;
    let v: serde_json::Value = serde_json::from_str(&out).context("rex run --json output")?;
    let block = v["block_ids"][0].as_str().context("rex run returned no block")?.to_string();
    let window = v["window_id"].as_str().map(str::to_string);
    let mut user = String::new();
    for _ in 0..40 {
        let text = rex(&["-S", label, "capture", "-b", &block, "--trim", "--unwrap"]).await.unwrap_or_default();
        if let Some(line) = text.lines().map(str::trim).find(|l| !l.is_empty()) {
            user = line.to_string();
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    if let Some(w) = window {
        let _ = rex(&["-S", label, "window", "close", &w]).await;
    }
    if user.is_empty() || user.contains(char::is_whitespace) {
        bail!("could not read the login user on {label} (got {user:?})");
    }
    Ok(user)
}

/// Starts `argv` in a new block in the host's `compute` session and returns the block id. The
/// block closes when the command exits (no `--keep-open`).
pub async fn rex_run(label: &str, title: &str, argv: &[String]) -> Result<String> {
    ensure_rex_session(label).await?;
    let mut args: Vec<&str> = vec![
        "-S", label, "run", "-s", "compute", "--focus=false", "--json", "--label", title, "--",
    ];
    args.extend(argv.iter().map(String::as_str));
    let out = rex(&args).await?;
    let block = serde_json::from_str::<serde_json::Value>(&out)
        .ok()
        .and_then(|v| v["block_ids"][0].as_str().map(str::to_string))
        .or_else(|| {
            out.split_whitespace()
                .find(|w| w.trim_matches('"').starts_with("block:"))
                .map(|w| w.trim_matches(|c| c == '"' || c == ',').to_string())
        })
        .unwrap_or_default();
    Ok(block)
}
