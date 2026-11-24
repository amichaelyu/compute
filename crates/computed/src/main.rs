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

use anyhow::{Context, Result};
use backend::Backend;
use backend::cloud::{Cloud, CloudBackend};
use backend::local::LocalBackend;
use backend::slurm::SlurmBackend;
use config::{BackendKind, Config, Paths};
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "computed=info".into()),
        )
        .init();

    let arg = std::env::args().nth(1).unwrap_or_else(|| "serve".into());
    match arg.as_str() {
        "serve" => serve().await,
        "--version" | "version" => {
            println!("computed {}", env!("CARGO_PKG_VERSION"));
            Ok(())
        }
        other => anyhow::bail!("unknown command {other:?}; usage: computed [serve|version]"),
    }
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
    let registry_creds = match (&cfg.registry.secret, cfg.registry.host()) {
        (Some(s), Some(host)) => {
            let user = secrets.value(s, "REGISTRY_USERNAME")?;
            let pass = secrets.value(s, "REGISTRY_PASSWORD")?;
            user.zip(pass).map(|(u, p)| (host, u, p))
        }
        _ => None,
    };
    let mut backends: Vec<Arc<dyn Backend>> = Vec::new();
    for b in &cfg.backends {
        backends.push(match b.kind {
            BackendKind::Local => Arc::new(LocalBackend::new(b.clone(), tunnels.clone())),
            BackendKind::Slurm => Arc::new(SlurmBackend::new(b.clone(), tz, tunnels.clone())),
            _ => {
                let cb = Arc::new(CloudBackend::new(b.clone(), &paths)?);
                cb.set_registry(registry_creds.clone());
                Arc::new(Cloud(cb))
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
    axum::serve(listener, app).with_graceful_shutdown(shutdown()).await?;
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
