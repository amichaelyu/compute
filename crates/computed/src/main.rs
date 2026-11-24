//! computed: the compute scheduler daemon.

mod access;
mod api;
mod auth;
mod backend;
mod config;
mod datasets;
mod error;
mod images;
mod model;
mod scheduler;
mod secrets;
mod snapshots;
mod store;
mod transport;
mod ui;
mod users;

use anyhow::{Context, Result};
use backend::Backend;
use backend::cloud::{Cloud, CloudBackend};
use backend::local::LocalBackend;
use backend::slurm::SlurmBackend;
use config::{BackendKind, Config, Paths};
use std::sync::Arc;

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "computed=info".into()),
        )
        .init();

    let arg = std::env::args().nth(1).unwrap_or_else(|| "serve".into());
    match arg.as_str() {
        "serve" => {
            // Before any threads exist: the environment is only safe to change here.
            load_env()?;
            tokio::runtime::Builder::new_multi_thread().enable_all().build()?.block_on(serve())
        }
        "--version" | "version" => {
            println!("computed {}", env!("CARGO_PKG_VERSION"));
            Ok(())
        }
        other => anyhow::bail!("unknown command {other:?}; usage: computed [serve|version]"),
    }
}

/// Puts common user tool directories on PATH (services start with a bare one), then applies
/// `<config dir>/computed.env`: `KEY=value` lines, `#` comments, `$VAR`/`${VAR}` expanded, so
/// `PATH=/some/dir:$PATH` works. Under systemd the same file is also the EnvironmentFile.
fn load_env() -> Result<()> {
    let home = std::env::var("HOME").unwrap_or_default();
    let path = std::env::var("PATH").unwrap_or_default();
    let mut dirs: Vec<String> = path.split(':').filter(|d| !d.is_empty()).map(str::to_string).collect();
    for d in [format!("{home}/.local/bin"), "/opt/homebrew/bin".into(), "/usr/local/bin".into(), "/usr/bin".into(), "/bin".into()] {
        if !dirs.contains(&d) {
            dirs.push(d);
        }
    }
    // SAFETY: called from main before the runtime and any other thread start.
    unsafe { std::env::set_var("PATH", dirs.join(":")) };
    let file = Paths::from_env()?.config_dir.join("computed.env");
    let Ok(text) = std::fs::read_to_string(&file) else { return Ok(()) };
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let line = line.strip_prefix("export ").unwrap_or(line);
        let Some((k, v)) = line.split_once('=') else { continue };
        let v = v.trim().trim_matches('"').trim_matches('\'');
        // SAFETY: as above, still single-threaded.
        unsafe { std::env::set_var(k.trim(), expand(v)) };
    }
    Ok(())
}

/// Expands `$VAR` and `${VAR}` from the current environment.
fn expand(v: &str) -> String {
    let mut out = String::new();
    let mut chars = v.chars().peekable();
    while let Some(c) = chars.next() {
        if c != '$' {
            out.push(c);
            continue;
        }
        let braced = chars.peek() == Some(&'{');
        if braced {
            chars.next();
        }
        let mut name = String::new();
        while let Some(&n) = chars.peek() {
            if n.is_ascii_alphanumeric() || n == '_' {
                name.push(n);
                chars.next();
            } else {
                break;
            }
        }
        if braced && chars.peek() == Some(&'}') {
            chars.next();
        }
        if name.is_empty() {
            out.push('$');
        } else {
            out.push_str(&std::env::var(&name).unwrap_or_default());
        }
    }
    out
}

async fn serve() -> Result<()> {
    let paths = Paths::from_env()?;
    let cfg_path = paths.hosts_toml();
    let cfg = Config::load(&cfg_path).with_context(|| {
        format!("loading config; copy the repo's hosts.example.toml, edited, to {}", cfg_path.display())
    })?;
    let tz = cfg.tz()?;
    if !paths.allowed_signers().exists() {
        tracing::warn!(
            "{} does not exist; nobody can log in until it lists a public key",
            paths.allowed_signers().display()
        );
    }

    let store = Arc::new(store::Store::open(&paths.db())?);
    let tunnels = Arc::new(transport::Tunnels::default());
    let secrets = Arc::new(secrets::Secrets::new(paths.secrets_dir()));
    let mut backends: Vec<Arc<dyn Backend>> = Vec::new();
    for b in &cfg.backends {
        backends.push(match b.kind {
            BackendKind::Local => Arc::new(LocalBackend::new(b.clone(), tunnels.clone())),
            BackendKind::Slurm => Arc::new(SlurmBackend::new(b.clone(), tz, tunnels.clone())),
            _ => {
                Arc::new(Cloud(Arc::new(CloudBackend::new(b.clone(), &paths, store.clone())?)))
            }
        });
    }
    let snapshots = Arc::new(snapshots::Snapshots::new(paths.snapshots_dir(), store.clone()));
    let auth = Arc::new(auth::Auth::new(paths.allowed_signers(), store.clone()));
    let sched = scheduler::Scheduler::new(
        store,
        backends,
        snapshots,
        secrets,
        scheduler::SchedulerConfig { paths: paths.clone(), registry: cfg.registry.clone(), budget: cfg.budget.clone(), tz },
    );
    sched.reconcile()?;
    tokio::spawn(sched.clone().run());

    let listen = std::env::var("COMPUTE_LISTEN").ok().filter(|v| !v.is_empty()).unwrap_or(cfg.listen.clone());
    let listener = tokio::net::TcpListener::bind(&listen)
        .await
        .with_context(|| format!("binding {listen}"))?;
    tracing::info!("computed {} listening on {listen}", env!("CARGO_PKG_VERSION"));
    let access = access::Access::from_env().map(Arc::new);
    match &access {
        Some(a) => tracing::info!("Cloudflare Access logins enabled for team {}", a.team_domain()),
        None => tracing::info!("Cloudflare Access logins disabled (COMPUTE_CF_* not set)"),
    }
    let app = api::router(api::AppState { sched, auth, access, tz });
    // Exit at once on a signal: all state is in SQLite and running jobs outlive the daemon. A
    // graceful shutdown would keep the scheduler running while systemd kills its ssh children,
    // so polls fail and healthy jobs look lost.
    tokio::select! {
        r = axum::serve(listener, app) => r?,
        _ = shutdown() => {}
    }
    Ok(())
}

async fn shutdown() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    let term = async {
        if let Ok(mut s) = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            s.recv().await;
        }
    };
    tokio::select! {
        _ = ctrl_c => {}
        _ = term => {}
    }
    tracing::info!("shutting down");
}
