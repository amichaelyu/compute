//! Paths, `hosts.toml` loading and backend configuration.

use anyhow::{Context, Result, bail};
use serde::Deserialize;
use std::path::{Path, PathBuf};

/// Where the daemon keeps its configuration and state. Worked out from the environment at run
/// time; nothing here is a fixed machine path.
#[derive(Clone, Debug)]
pub struct Paths {
    pub config_dir: PathBuf,
    pub state_dir: PathBuf,
}

fn env_path(key: &str) -> Option<PathBuf> {
    std::env::var_os(key).filter(|v| !v.is_empty()).map(PathBuf::from)
}

impl Paths {
    pub fn from_env() -> Result<Paths> {
        let home = env_path("HOME").context("HOME is not set")?;
        let config_dir = env_path("COMPUTE_CONFIG_DIR")
            .or_else(|| env_path("XDG_CONFIG_HOME").map(|p| p.join("compute")))
            .unwrap_or_else(|| home.join(".config/compute"));
        let state_dir = env_path("COMPUTE_STATE_DIR")
            .or_else(|| env_path("XDG_STATE_HOME").map(|p| p.join("compute")))
            .unwrap_or_else(|| home.join(".local/state/compute"));
        Ok(Paths { config_dir, state_dir })
    }
    pub fn hosts_toml(&self) -> PathBuf {
        self.config_dir.join("hosts.toml")
    }
    pub fn allowed_signers(&self) -> PathBuf {
        self.config_dir.join("allowed_signers")
    }
    pub fn secrets_dir(&self) -> PathBuf {
        self.config_dir.join("secrets")
    }
    pub fn db(&self) -> PathBuf {
        self.state_dir.join("compute.db")
    }
    pub fn snapshots_dir(&self) -> PathBuf {
        self.state_dir.join("snapshots")
    }
}

#[derive(Deserialize, Clone, Debug)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// Address to listen on. `COMPUTE_LISTEN` overrides it.
    #[serde(default = "default_listen")]
    pub listen: String,
    /// Time zone used to read Slurm's local timestamps and to format times in responses.
    #[serde(default = "default_tz")]
    pub tz: String,
    #[serde(rename = "backend", default)]
    pub backends: Vec<BackendConfig>,
}

fn default_listen() -> String {
    "127.0.0.1:8765".into()
}
fn default_tz() -> String {
    "America/Los_Angeles".into()
}

#[derive(Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum BackendKind {
    Local,
    Slurm,
}

impl BackendKind {
    pub fn as_str(self) -> &'static str {
        match self {
            BackendKind::Local => "local",
            BackendKind::Slurm => "slurm",
        }
    }
}

/// How the daemon reaches a host.
#[derive(Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Transport {
    /// ssh through a `rex forward` tunnel to the host's sshd; local jobs start as Rex blocks.
    Rex,
    /// Direct ssh (over Tailscale or the LAN); local jobs start detached with nohup.
    Ssh,
}

impl Transport {
    pub fn as_str(self) -> &'static str {
        match self {
            Transport::Rex => "rex",
            Transport::Ssh => "ssh",
        }
    }
}

pub const DEFAULT_LOCAL_ROOT: &str = "${XDG_DATA_HOME:-$HOME/.local/share}/compute";
const TUNNEL_PORT_BASE: u16 = 52200;

#[derive(Deserialize, Clone, Debug)]
#[serde(deny_unknown_fields)]
pub struct BackendConfig {
    pub name: String,
    pub kind: BackendKind,
    /// Default on/off. `POST /v1/backends/{name}/enable|disable` overrides it.
    #[serde(default = "yes")]
    pub enabled: bool,
    /// `rex` or `ssh`. Defaults: `rex` for local backends, `ssh` for Slurm.
    #[serde(default)]
    pub transport: Option<Transport>,
    /// Root directory on the backend, as a shell expression expanded there (for example
    /// `/scratch/users/$USER/compute`). Local backends default to `DEFAULT_LOCAL_ROOT`.
    #[serde(default)]
    pub root: Option<String>,
    /// Rex host label (`rex -S`), required for the `rex` transport.
    #[serde(default)]
    pub rex_label: Option<String>,
    /// ssh destination (a host, IP or ~/.ssh/config alias), required for the `ssh` transport.
    #[serde(default)]
    pub ssh_host: Option<String>,
    #[serde(default)]
    pub ssh_user: Option<String>,
    #[serde(default)]
    pub ssh_port: Option<u16>,
    /// Local port for the `rex forward` tunnel. Assigned automatically when omitted.
    #[serde(default)]
    pub tunnel_port: Option<u16>,
    /// The share of a local host the scheduler may use. When omitted, the detected total minus
    /// `reserve_cpus` / `reserve_mem_gb`.
    #[serde(default)]
    pub cpus: Option<u32>,
    #[serde(default)]
    pub mem_gb: Option<u32>,
    #[serde(default = "two")]
    pub reserve_cpus: u32,
    #[serde(default = "four")]
    pub reserve_mem_gb: u32,
    #[serde(default)]
    pub gpus: u32,
    #[serde(default)]
    pub gpu_type: Option<String>,
    /// `macos` or `linux` and `arm64`/`amd64`. Detected by the probe when omitted.
    #[serde(default)]
    pub os: Option<String>,
    #[serde(default)]
    pub arch: Option<String>,
    #[serde(default = "native")]
    pub runtimes: Vec<String>,
    #[serde(default = "one")]
    pub max_nodes: u32,
    /// Readable restriction lines shown by `GET /v1/backends/{name}`.
    #[serde(default)]
    pub restrictions: Vec<String>,
    #[serde(default)]
    pub slurm: Option<SlurmConfig>,
}

fn yes() -> bool {
    true
}
fn two() -> u32 {
    2
}
fn four() -> u32 {
    4
}

impl BackendConfig {
    pub fn transport(&self) -> Transport {
        self.transport.unwrap_or(match self.kind {
            BackendKind::Local => Transport::Rex,
            BackendKind::Slurm => Transport::Ssh,
        })
    }
    pub fn root(&self) -> String {
        self.root.clone().unwrap_or_else(|| DEFAULT_LOCAL_ROOT.to_string())
    }
}
fn one() -> u32 {
    1
}
fn native() -> Vec<String> {
    vec!["native".into()]
}

#[derive(Deserialize, Clone, Debug)]
#[serde(deny_unknown_fields)]
pub struct SlurmConfig {
    #[serde(default = "gpu_partition")]
    pub gpu_partition: String,
    #[serde(default = "normal")]
    pub cpu_partition: String,
    #[serde(default = "normal")]
    pub cpu_qos: String,
    #[serde(default = "mem_per_cpu")]
    pub mem_per_cpu_mb: u32,
    #[serde(default = "max_gpus")]
    pub max_gpus: u32,
    #[serde(default = "node_cpus")]
    pub node_cpus: u32,
    #[serde(default = "node_mem")]
    pub node_mem_mb: u32,
    /// Largest CPU count a single CPU-only task may ask for.
    #[serde(default = "max_cpu_task")]
    pub max_cpu_task_cpus: u32,
}

fn gpu_partition() -> String {
    "gpu".into()
}
fn normal() -> String {
    "normal".into()
}
fn mem_per_cpu() -> u32 {
    4000
}
fn max_gpus() -> u32 {
    4
}
fn node_cpus() -> u32 {
    64
}
fn node_mem() -> u32 {
    256_000
}
fn max_cpu_task() -> u32 {
    128
}

impl Config {
    pub fn load(path: &Path) -> Result<Config> {
        let text = std::fs::read_to_string(path)
            .with_context(|| format!("reading {}", path.display()))?;
        let mut cfg: Config =
            toml::from_str(&text).with_context(|| format!("parsing {}", path.display()))?;
        cfg.tz()?;
        let mut seen = std::collections::HashSet::new();
        let taken: std::collections::HashSet<u16> =
            cfg.backends.iter().filter_map(|b| b.tunnel_port).collect();
        let mut next_port = TUNNEL_PORT_BASE;
        for b in &mut cfg.backends {
            if !seen.insert(b.name.clone()) {
                bail!("backend {:?} is defined twice", b.name);
            }
            match b.transport() {
                Transport::Rex => {
                    if b.rex_label.is_none() {
                        bail!("backend {:?} uses transport rex and needs rex_label", b.name);
                    }
                    if b.tunnel_port.is_none() {
                        while taken.contains(&next_port) {
                            next_port += 1;
                        }
                        b.tunnel_port = Some(next_port);
                        next_port += 1;
                    }
                }
                Transport::Ssh => {
                    if b.ssh_host.is_none() {
                        bail!("backend {:?} uses transport ssh and needs ssh_host", b.name);
                    }
                }
            }
            if b.kind == BackendKind::Slurm {
                if b.slurm.is_none() {
                    b.slurm = Some(toml::from_str("").expect("slurm defaults"));
                }
                if b.root.is_none() {
                    bail!("slurm backend {:?} needs root (e.g. a scratch path)", b.name);
                }
            }
        }
        Ok(cfg)
    }

    pub fn tz(&self) -> Result<chrono_tz::Tz> {
        self.tz
            .parse::<chrono_tz::Tz>()
            .map_err(|e| anyhow::anyhow!("bad tz {:?}: {e}", self.tz))
    }
}
