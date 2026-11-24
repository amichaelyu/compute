//! Paid cloud backends: RunPod, Lambda and Modal. Each job gets its own instance (or
//! cluster, for multi-node jobs), named `compute-<job id>-<nonce>`:
//!
//! 1. Staging creates the instance, waits until it's reachable, installs the runner and pushes
//!    the snapshot, datasets and job files (over ssh for RunPod and Lambda, through Modal's
//!    exec API for Modal).
//! 2. Launch starts the runner on every node. Multi-node jobs get NNODES, NODE_RANK,
//!    GPUS_PER_NODE, MASTER_ADDR and MASTER_PORT.
//! 3. When the job ends, its log and outputs (`keep_outputs`, default everything) are copied
//!    to the coordinator, then the instance is torn down. Logs and outputs are served from
//!    there afterwards.
//!
//! A sweep every 5 minutes tears down `compute-*` instances that no live job owns, and live
//! ones past their time limit plus 10 minutes.

use super::{Alloc, Backend, PollResult, Probe, Quote, Remote, Shape, StageFiles, parse_status, tar_dir_script};
use crate::config::{BackendConfig, BackendKind, CloudConfig, Paths};
use crate::model::{Job, JobState};
use crate::transport::{LocalShell, Reader, Shell, Ssh, shell_quote};
use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::Path;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};
use tokio::sync::Mutex;

const OFFERS_TTL: Duration = Duration::from_secs(120);
const MASTER_PORT: u16 = 29500;

/// One way to rent the GPUs a job asks for.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Offer {
    /// Provider id: RunPod GPU type id, Lambda instance type, Modal GPU name.
    pub id: String,
    pub gpu_type: String,
    pub gpus: u32,
    pub cpus: u32,
    pub mem_gb: u32,
    /// Price per hour for one node.
    pub usd_per_hour: f64,
    pub available: bool,
    #[serde(default)]
    pub region: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SshAddr {
    pub host: String,
    pub port: u16,
    pub user: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Node {
    pub rank: u32,
    #[serde(default)]
    pub ssh: Option<SshAddr>,
    #[serde(default)]
    pub sandbox: Option<String>,
    #[serde(default)]
    pub private_ip: Option<String>,
}

/// A rented instance (or cluster) for one job.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Instance {
    pub provider_id: String,
    pub name: String,
    pub job_id: i64,
    #[serde(default)]
    pub cluster: bool,
    #[serde(default = "one")]
    pub node_count: u32,
    #[serde(default)]
    pub nodes: Vec<Node>,
    pub created_at: DateTime<Utc>,
    /// Tear down after this even if the job still looks alive.
    pub deadline: DateTime<Utc>,
    pub offer: Offer,
}

fn one() -> u32 {
    1
}

/// What to create.
pub struct CreateReq<'a> {
    pub name: String,
    pub job: &'a Job,
    pub image: String,
    pub offer: Offer,
    pub nodes: u32,
    pub pubkey: String,
    pub registry: Option<(String, String, String)>,
    pub timeout_s: u64,
}

#[async_trait]
pub trait Provider: Send + Sync {
    /// Offers for `gpus` GPUs per node on `nodes` nodes.
    async fn offers(&self, gpus: u32, nodes: u32) -> Result<Vec<Offer>>;
    async fn create(&self, req: &CreateReq<'_>) -> Result<Instance>;
    /// Waits until every node is reachable and fills in their addresses.
    async fn wait_ready(&self, inst: &mut Instance, deadline: Instant) -> Result<()>;
    /// False once the provider reports the instance gone or failed.
    async fn alive(&self, inst: &Instance) -> Result<bool>;
    async fn terminate(&self, inst: &Instance) -> Result<()>;
    /// `(provider id, name, cluster)` of instances that look like ours.
    async fn list(&self) -> Result<Vec<(String, String, bool)>>;
    fn shell(&self, node: &Node) -> Result<Arc<dyn Shell>>;
    /// Where jobs live on an instance.
    fn root(&self) -> &'static str;
    /// `docker` when the job's image runs under Docker on a VM; `native` when the instance is
    /// the image.
    fn runtime(&self) -> &'static str;
    /// Steps after the instance is reachable (Docker group, registry login).
    async fn prepare(&self, _shell: &dyn Shell, _registry: Option<&(String, String, String)>) -> Result<()> {
        Ok(())
    }
}

/// `(state, exit code, started, ended, note)` of a finished cloud job.
type Outcome = (Option<JobState>, Option<i32>, Option<DateTime<Utc>>, Option<DateTime<Utc>>, Option<String>);
type OfferCache = HashMap<(u32, u32), (Instant, Vec<Offer>)>;

/// Outcome of copying a finished job off its instance.
#[derive(Clone)]
struct Collected {
    result: Outcome,
}

pub struct CloudBackend {
    cfg: BackendConfig,
    ccfg: CloudConfig,
    provider: Arc<dyn Provider>,
    outputs_dir: std::path::PathBuf,
    pubkey: String,
    probed: RwLock<bool>,
    offers: Mutex<OfferCache>,
    instances: Mutex<HashMap<i64, Instance>>,
    create_locks: Mutex<HashMap<i64, Arc<Mutex<()>>>>,
    collecting: Mutex<HashSet<i64>>,
    collected: Mutex<HashMap<i64, Collected>>,
    boot_secs: RwLock<Option<f64>>,
    registry: RwLock<Option<(String, String, String)>>,
}

/// The coordinator's own ssh public key, which cloud instances authorize.
fn daemon_pubkey() -> Option<String> {
    if let Ok(k) = std::env::var("COMPUTE_SSH_PUBKEY")
        && !k.trim().is_empty()
    {
        return Some(k.trim().to_string());
    }
    let home = std::env::var_os("HOME")?;
    ["id_ed25519.pub", "id_ecdsa.pub", "id_rsa.pub"]
        .iter()
        .find_map(|f| std::fs::read_to_string(std::path::Path::new(&home).join(".ssh").join(f)).ok())
        .map(|s| s.trim().to_string())
}

impl CloudBackend {
    pub fn new(cfg: BackendConfig, paths: &Paths) -> Result<CloudBackend> {
        let ccfg = cfg.cloud.clone().unwrap_or_default();
        let key_file = paths.config_dir.join(ccfg.key_file.clone().unwrap_or_else(|| match cfg.kind {
            BackendKind::Modal => "modal.toml".into(),
            k => format!("{}.key", k.as_str()),
        }));
        let provider: Arc<dyn Provider> = match cfg.kind {
            BackendKind::Runpod => Arc::new(RunPod::new(&key_file, &ccfg)),
            BackendKind::Lambda => Arc::new(Lambda::new(&key_file, &ccfg)),
            BackendKind::Modal => Arc::new(Modal::new(&key_file, &ccfg)),
            k => bail!("{} is not a cloud backend", k.as_str()),
        };
        Ok(CloudBackend {
            pubkey: daemon_pubkey().unwrap_or_default(),
            cfg,
            ccfg,
            provider,
            outputs_dir: paths.outputs_dir(),
            probed: RwLock::new(false),
            offers: Mutex::new(HashMap::new()),
            instances: Mutex::new(HashMap::new()),
            create_locks: Mutex::new(HashMap::new()),
            collecting: Mutex::new(HashSet::new()),
            collected: Mutex::new(HashMap::new()),
            boot_secs: RwLock::new(None),
            registry: RwLock::new(None),
        })
    }

    /// Registry credentials for private images, set by the scheduler at startup.
    pub fn set_registry(&self, creds: Option<(String, String, String)>) {
        *self.registry.write().unwrap_or_else(|e| e.into_inner()) = creds;
    }

    fn boot(&self) -> f64 {
        self.boot_secs.read().unwrap_or_else(|e| e.into_inner()).unwrap_or(f64::from(self.ccfg.boot_minutes) * 60.0)
    }

    fn job_dir_local(&self, id: i64) -> std::path::PathBuf {
        self.outputs_dir.join(id.to_string())
    }

    async fn cached_offers(&self, gpus: u32, nodes: u32) -> Result<Vec<Offer>> {
        if let Some((at, o)) = self.offers.lock().await.get(&(gpus, nodes))
            && at.elapsed() < OFFERS_TTL
        {
            return Ok(o.clone());
        }
        let mut o = self.provider.offers(gpus, nodes).await?;
        if !self.ccfg.gpu_types.is_empty() {
            o.retain(|x| self.ccfg.gpu_types.iter().any(|t| contains_ci(&x.gpu_type, t) || contains_ci(&x.id, t)));
        }
        o.sort_by(|a, b| a.usd_per_hour.total_cmp(&b.usd_per_hour));
        self.offers.lock().await.insert((gpus, nodes), (Instant::now(), o.clone()));
        Ok(o)
    }

    fn image_for(&self, job: &Job) -> Option<String> {
        job.spec.container.as_ref().map(|c| c.image.clone()).or_else(|| self.ccfg.default_image.clone())
    }

    /// The job's instance: from memory, from its handle (after a restart), or created now.
    async fn instance(&self, job: &Job, create: bool) -> Result<Instance> {
        if let Some(i) = self.instances.lock().await.get(&job.id) {
            return Ok(i.clone());
        }
        if let Some(h) = &job.handle
            && let Ok(i) = serde_json::from_value::<Instance>(h["instance"].clone())
        {
            self.instances.lock().await.insert(job.id, i.clone());
            return Ok(i);
        }
        if !create {
            bail!("job {} has no instance", job.id);
        }
        let lock = self.create_locks.lock().await.entry(job.id).or_default().clone();
        let _g = lock.lock().await;
        if let Some(i) = self.instances.lock().await.get(&job.id) {
            return Ok(i.clone());
        }
        self.create_instance(job).await
    }

    async fn create_instance(&self, job: &Job) -> Result<Instance> {
        let image = self.image_for(job).context("no image: set container.image (this backend has no default_image)")?;
        let r = &job.resources;
        let offer = self
            .cached_offers(r.gpus, r.nodes)
            .await?
            .into_iter()
            .find(|o| o.available && offer_fits(o, job))
            .context("no offer with capacity right now")?;
        if self.pubkey.is_empty() {
            bail!("the coordinator has no ssh public key (~/.ssh/id_ed25519.pub) for cloud instances");
        }
        let name = format!("compute-{}-{}", job.id, hex::encode(rand::random::<[u8; 3]>()));
        let registry = self.registry.read().unwrap_or_else(|e| e.into_inner()).clone().filter(|(h, ..)| image.starts_with(&format!("{h}/")));
        let req = CreateReq {
            name,
            job,
            image,
            offer,
            nodes: r.nodes,
            pubkey: self.pubkey.clone(),
            registry: registry.clone(),
            timeout_s: r.time_s + 600 + self.boot() as u64,
        };
        let started = Instant::now();
        let mut inst = self.provider.create(&req).await?;
        tracing::info!("{}: created {} ({}) for job {}", self.cfg.name, inst.name, inst.provider_id, job.id);
        self.instances.lock().await.insert(job.id, inst.clone());
        let deadline = Instant::now() + Duration::from_secs(20 * 60);
        if let Err(e) = self.provider.wait_ready(&mut inst, deadline).await {
            let _ = self.provider.terminate(&inst).await;
            self.instances.lock().await.remove(&job.id);
            return Err(e.context(format!("{} did not become ready", inst.name)));
        }
        let secs = started.elapsed().as_secs_f64();
        {
            let mut b = self.boot_secs.write().unwrap_or_else(|e| e.into_inner());
            *b = Some(match *b {
                Some(old) => 0.5 * old + 0.5 * secs,
                None => secs,
            });
        }
        for node in &inst.nodes {
            let shell = self.provider.shell(node)?;
            self.provider.prepare(shell.as_ref(), registry.as_ref()).await?;
        }
        self.instances.lock().await.insert(job.id, inst.clone());
        Ok(inst)
    }

    fn remote(&self, inst: &Instance, rank: usize) -> Result<Remote> {
        let node = inst.nodes.get(rank).with_context(|| format!("{} has no node {rank}", inst.name))?;
        Ok(Remote::with_shell(self.cfg.clone(), self.provider.shell(node)?, self.provider.root().to_string()))
    }

    /// Copies the log and kept outputs to the coordinator, then tears the instance down. The
    /// final state is reported by the next poll.
    async fn collect_and_teardown(&self, job: &Job, inst: &Instance, forced: Option<JobState>) -> Collected {
        let local = self.job_dir_local(job.id);
        let _ = tokio::fs::create_dir_all(local.join("out")).await;
        let mut result: Collected = Collected { result: (forced, None, None, Some(Utc::now()), None) };
        let mut statuses = Vec::new();
        for rank in 0..inst.nodes.len() {
            let Ok(remote) = self.remote(inst, rank) else { continue };
            let status = remote.read_statuses(&[job.id]).await.ok().and_then(|m| m.get(&job.id).cloned());
            if let Some((s, beat)) = status {
                statuses.push((rank, parse_status(job.id, &s, beat)));
            }
            let root = self.provider.root();
            let jobdir = Remote::jobdir(root, job.id);
            // Log: node 0's is the job's log; the others go under out/nodes/<rank>/log.
            let log_dest = if rank == 0 { local.join("log") } else { local.join("out").join("nodes").join(rank.to_string()).join("log") };
            if let Some(dir) = log_dest.parent() {
                let _ = tokio::fs::create_dir_all(dir).await;
            }
            if let Ok(shell) = self.provider.shell(&inst.nodes[rank]) {
                if let Ok(mut r) = shell.pipe_out(&format!("cat {} 2>/dev/null || true", shell_quote(&format!("{jobdir}/log")))).await
                    && let Ok(mut f) = tokio::fs::File::create(&log_dest).await
                {
                    let _ = tokio::io::copy(&mut r, &mut f).await;
                }
                if rank == 0 {
                    let glob = job.spec.keep_outputs.clone().filter(|g| g != "**" && g != "*");
                    if let Ok(data) = shell.pipe_out(&tar_dir_script(&format!("{jobdir}/out"), glob.as_deref())).await {
                        let dest = shell_quote(&local.join("out").to_string_lossy());
                        if let Err(e) = LocalShell.pipe_in(&format!("tar -x -C {dest}"), data).await {
                            tracing::warn!("copying outputs of job {} off {}: {e:#}", job.id, inst.name);
                        }
                    }
                }
            }
        }
        if let Err(e) = self.provider.terminate(inst).await {
            tracing::warn!("terminating {}: {e:#} (the sweep will retry)", inst.name);
        } else {
            tracing::info!("{}: terminated {} after job {}", self.cfg.name, inst.name, job.id);
        }
        self.instances.lock().await.remove(&job.id);
        if forced.is_none() {
            // Overall state: any node failed > timeout > cancelled > succeeded.
            let order = |s: Option<JobState>| match s {
                Some(JobState::Failed) => 0,
                Some(JobState::Timeout) => 1,
                Some(JobState::Cancelled) => 2,
                Some(JobState::Succeeded) => 3,
                _ => 4,
            };
            if let Some((rank, worst)) = statuses.iter().min_by_key(|(_, p)| order(p.state)) {
                let note = (inst.nodes.len() > 1 && *rank > 0 && worst.state != Some(JobState::Succeeded))
                    .then(|| format!("node {rank} {}", worst.state.map(|s| s.as_str()).unwrap_or("ended")));
                let main = statuses.iter().find(|(r, _)| *r == 0).map(|(_, p)| p);
                result.result = (
                    worst.state.or(Some(JobState::Lost)),
                    main.and_then(|p| p.exit_code).or(worst.exit_code),
                    main.and_then(|p| p.started),
                    main.and_then(|p| p.ended).or(Some(Utc::now())),
                    note,
                );
            } else {
                result.result = (Some(JobState::Lost), None, None, Some(Utc::now()), Some("no status from the instance".into()));
            }
        }
        result
    }

    fn spawn_collect(self: &Arc<Self>, job: Job, inst: Instance, forced: Option<JobState>) {
        let me = self.clone();
        tokio::spawn(async move {
            if !me.collecting.lock().await.insert(job.id) {
                return;
            }
            let c = me.collect_and_teardown(&job, &inst, forced).await;
            me.collected.lock().await.insert(job.id, c);
            me.collecting.lock().await.remove(&job.id);
        });
    }

    fn collected_locally(&self, id: i64) -> bool {
        self.job_dir_local(id).join("log").exists()
    }
}

fn contains_ci(hay: &str, needle: &str) -> bool {
    hay.to_ascii_lowercase().contains(&needle.to_ascii_lowercase())
}

fn offer_fits(o: &Offer, job: &Job) -> bool {
    let r = &job.resources;
    let type_ok = r.gpu_type.as_deref().is_none_or(|t| contains_ci(&o.gpu_type, t) || contains_ci(&o.id, t));
    let cpus_ok = o.cpus == 0 || o.cpus >= r.cpus;
    let mem_ok = o.mem_gb == 0 || u64::from(o.mem_gb) * 1024 >= r.mem_mb;
    type_ok && cpus_ok && mem_ok && o.gpus == r.gpus
}

/// Wraps the backend so `spawn_collect` can hold an `Arc` to it.
pub struct Cloud(pub Arc<CloudBackend>);

#[async_trait]
impl Backend for Cloud {
    fn name(&self) -> &str {
        &self.0.cfg.name
    }
    fn config(&self) -> &BackendConfig {
        &self.0.cfg
    }
    fn root(&self) -> Option<String> {
        self.0.probed.read().unwrap_or_else(|e| e.into_inner()).then(|| self.0.provider.root().to_string())
    }
    fn runtimes(&self) -> Vec<String> {
        vec![self.0.provider.runtime().to_string()]
    }
    fn is_paid(&self) -> bool {
        true
    }

    async fn probe(&self) -> Result<Probe> {
        let b = &self.0;
        let offers = b.provider.offers(1, 1).await?;
        let mut by_type: BTreeMap<String, Value> = BTreeMap::new();
        for o in &offers {
            by_type.insert(
                o.gpu_type.clone(),
                json!({"id": o.id, "usd_per_hour_per_gpu": (o.usd_per_hour * 100.0).round() / 100.0, "available": o.available}),
            );
        }
        *b.probed.write().unwrap_or_else(|e| e.into_inner()) = true;
        b.offers.lock().await.clear();
        Ok(Probe {
            root: b.provider.root().to_string(),
            hardware: json!({
                "gpu_types": by_type,
                "max_nodes": b.cfg.max_nodes,
                "runtime": b.provider.runtime(),
                "default_image": b.ccfg.default_image,
                "price_source": match b.cfg.kind { BackendKind::Modal => format!("rate table in hosts.toml ({})", b.ccfg.rates_date.clone().unwrap_or_else(|| "undated".into())), _ => "provider API".into() },
            }),
        })
    }

    async fn quote(&self, job: &Job, _active: &[Alloc]) -> Quote {
        let b = &self.0;
        let r = &job.resources;
        let Some(max) = job.spec.max_cost_usd else {
            return Quote::no("paid backend; set max_cost_usd to allow it", true);
        };
        if r.gpus == 0 {
            return Quote::no("cloud backends here run GPU jobs only", true);
        }
        if r.nodes > b.cfg.max_nodes {
            return Quote::no(format!("nodes={}; {} allows at most {}", r.nodes, b.cfg.name, b.cfg.max_nodes), true);
        }
        if b.image_for(job).is_none() {
            return Quote::no("cloud jobs run in a container: set container.image or container.build", true);
        }
        if job.os.as_deref() == Some("mac") {
            return Quote::no("os=mac; cloud instances are linux", true);
        }
        let offers = match b.cached_offers(r.gpus, r.nodes).await {
            Ok(o) => o,
            Err(e) => return Quote::no(format!("price lookup failed: {e:#}"), false),
        };
        let fitting: Vec<&Offer> = offers.iter().filter(|o| offer_fits(o, job)).collect();
        if fitting.is_empty() {
            let want = r.gpu_type.clone().map(|t| format!(" {t}")).unwrap_or_default();
            return Quote::no(format!("no{want} offer with {} GPU(s) per node", r.gpus), true);
        }
        let Some(o) = fitting.iter().find(|o| o.available) else {
            return Quote::no("no capacity right now for the GPUs asked", false);
        };
        let boot = b.boot();
        let hours = (r.time_s as f64 + boot) / 3600.0;
        // Providers that bill CPU and memory separately (Modal) list those rates in hosts.toml.
        let extras = b.ccfg.cpu_rate * f64::from(r.cpus) + b.ccfg.mem_gb_rate * (r.mem_mb as f64 / 1024.0);
        let per_hour = (o.usd_per_hour + extras) * f64::from(r.nodes);
        let cost = (per_hour * hours * 100.0).ceil() / 100.0;
        if cost > max {
            let cheapest = fitting.iter().map(|o| o.usd_per_hour).fold(f64::INFINITY, f64::min) * f64::from(r.nodes) * hours;
            return Quote::no(
                format!("cheapest offer is ~${:.2} for the full time limit, over max_cost_usd {max}", cheapest.min(cost)),
                true,
            );
        }
        let shape = Shape {
            cpus: o.cpus.max(r.cpus),
            mem_mb: (u64::from(o.mem_gb) * 1024).max(r.mem_mb),
            gpus: r.gpus,
            nodes: r.nodes,
            offer: serde_json::to_value(o).ok(),
            usd_per_hour: Some(per_hour),
            ..Default::default()
        };
        let nodes = if r.nodes > 1 { format!("{} nodes x ", r.nodes) } else { String::new() };
        Quote::Ok {
            start: Utc::now() + chrono::Duration::seconds(boot as i64),
            source: "provider".into(),
            reason: format!("{nodes}{} x{} at ${:.2}/h on {}", o.gpu_type, o.gpus, per_hour, b.cfg.name),
            shape,
            cost: Some(cost),
        }
    }

    async fn stage(&self, job: &Job, snapshot: &Path, files: &StageFiles) -> Result<()> {
        let b = &self.0;
        let inst = b.instance(job, true).await?;
        let n = inst.nodes.len().max(1) as u32;
        let master = inst.nodes.first().and_then(|x| x.private_ip.clone()).unwrap_or_else(|| "127.0.0.1".into());
        let data_paths: Vec<String> = files.datasets.iter().map(|(_, p)| p.clone()).collect();
        for rank in 0..inst.nodes.len() {
            let remote = b.remote(&inst, rank)?;
            remote.probe_root("").await?;
            let mut f = files.clone();
            if n > 1 {
                f.env.extend([
                    ("NNODES".to_string(), n.to_string()),
                    ("NODE_RANK".to_string(), rank.to_string()),
                    ("GPUS_PER_NODE".to_string(), job.resources.gpus.to_string()),
                    ("MASTER_ADDR".to_string(), master.clone()),
                    ("MASTER_PORT".to_string(), MASTER_PORT.to_string()),
                ]);
            }
            if rank > 0 {
                // Datasets were copied to node 0; replicate them.
                let src = b.provider.shell(&inst.nodes[0])?;
                let dst = b.provider.shell(&inst.nodes[rank])?;
                for p in &data_paths {
                    let data = src.pipe_out(&tar_dir_script(p, None)).await?;
                    let q = shell_quote(p);
                    dst.pipe_in(&format!("set -e\nmkdir -p {q}\ntar -x -C {q}"), data).await?;
                }
            }
            remote.stage_job(job, snapshot, &f, "", job.resources.time_s).await?;
        }
        Ok(())
    }

    async fn launch(&self, jobs: &[Job], _shape: &Shape) -> Result<Vec<Value>> {
        let b = &self.0;
        let mut handles = Vec::new();
        for job in jobs {
            let inst = b.instance(job, false).await?;
            let root = b.provider.root();
            // Inputs were received on node 0; replicate them for multi-node jobs.
            let jobdir = Remote::jobdir(root, job.id);
            for rank in 1..inst.nodes.len() {
                let src = b.provider.shell(&inst.nodes[0])?;
                let dst = b.provider.shell(&inst.nodes[rank])?;
                let data = src.pipe_out(&tar_dir_script(&format!("{jobdir}/in"), None)).await?;
                let q = shell_quote(&format!("{jobdir}/in"));
                dst.pipe_in(&format!("set -e\nmkdir -p {q}\ntar -x -C {q}"), data).await?;
            }
            for rank in (0..inst.nodes.len()).rev() {
                let shell = b.provider.shell(&inst.nodes[rank])?;
                shell
                    .run(&format!(
                        "nohup bash {} {} </dev/null >/dev/null 2>&1 &",
                        shell_quote(&format!("{root}/bin/compute-run")),
                        shell_quote(&jobdir)
                    ))
                    .await
                    .with_context(|| format!("starting the runner on node {rank}"))?;
            }
            handles.push(json!({"instance": inst}));
        }
        Ok(handles)
    }

    async fn poll(&self, jobs: &[Job]) -> Result<Vec<PollResult>> {
        let b = &self.0;
        let mut out = Vec::new();
        for job in jobs {
            if let Some(c) = b.collected.lock().await.remove(&job.id) {
                let (state, exit_code, started, ended, note) = c.result;
                out.push(PollResult { id: job.id, state, exit_code, started, ended, note, ..Default::default() });
                continue;
            }
            if b.collecting.lock().await.contains(&job.id) {
                out.push(PollResult { id: job.id, state: Some(JobState::Running), heartbeat: Some(Utc::now()), ..Default::default() });
                continue;
            }
            let Ok(inst) = b.instance(job, false).await else {
                out.push(PollResult { id: job.id, state: Some(JobState::Lost), note: Some("instance record is gone".into()), ..Default::default() });
                continue;
            };
            let mut node_results = Vec::new();
            let mut reach_err = None;
            for rank in 0..inst.nodes.len() {
                match b.remote(&inst, rank)?.read_statuses(&[job.id]).await {
                    Ok(m) => node_results.push(m.get(&job.id).map(|(s, beat)| parse_status(job.id, s, *beat))),
                    Err(e) => {
                        reach_err = Some(e);
                        break;
                    }
                }
            }
            if let Some(e) = reach_err {
                if !b.provider.alive(&inst).await.unwrap_or(true) {
                    b.spawn_collect(job.clone(), inst, Some(JobState::Lost));
                    out.push(PollResult { id: job.id, state: Some(JobState::Running), heartbeat: Some(Utc::now()), ..Default::default() });
                } else {
                    tracing::warn!("polling job {} on {}: {e:#}", job.id, inst.name);
                    out.push(PollResult { id: job.id, ..Default::default() });
                }
                continue;
            }
            let terminal = |p: &Option<PollResult>| p.as_ref().and_then(|p| p.state).is_some_and(JobState::is_terminal);
            let any_bad = node_results.iter().any(|p| p.as_ref().and_then(|p| p.state).is_some_and(|s| s.is_terminal() && s != JobState::Succeeded));
            if node_results.iter().all(terminal) {
                b.spawn_collect(job.clone(), inst, None);
                out.push(PollResult { id: job.id, state: Some(JobState::Running), heartbeat: Some(Utc::now()), ..Default::default() });
                continue;
            }
            if any_bad && inst.nodes.len() > 1 {
                // One node failed: stop the others; the job ends when they all have.
                for rank in 0..inst.nodes.len() {
                    if let Ok(r) = b.remote(&inst, rank) {
                        let _ = r.touch_cancel(job.id).await;
                    }
                }
            }
            let main = node_results.into_iter().next().flatten().unwrap_or(PollResult { id: job.id, ..Default::default() });
            let mut r = main;
            if r.state.is_some_and(JobState::is_terminal) {
                r.state = Some(JobState::Running);
                r.heartbeat = Some(Utc::now());
            }
            out.push(r);
        }
        Ok(out)
    }

    async fn logs(&self, job: &Job, offset: u64, max: u64) -> Result<(Vec<u8>, u64)> {
        let b = &self.0;
        if b.collected_locally(job.id) {
            let dir = b.job_dir_local(job.id);
            let data = tokio::fs::read(dir.join("log")).await.unwrap_or_default();
            let size = data.len() as u64;
            let start = (offset as usize).min(data.len());
            let end = (start + max as usize).min(data.len());
            return Ok((data[start..end].to_vec(), size));
        }
        let inst = b.instance(job, false).await?;
        b.remote(&inst, 0)?.logs(job.id, offset, max).await
    }

    async fn fetch(&self, job: &Job, glob: Option<&str>) -> Result<Reader> {
        let b = &self.0;
        let local_out = b.job_dir_local(job.id).join("out");
        if local_out.exists() && (job.state.is_terminal() || b.collected_locally(job.id)) {
            return LocalShell.pipe_out(&tar_dir_script(&local_out.to_string_lossy(), glob)).await;
        }
        let inst = b.instance(job, false).await?;
        b.remote(&inst, 0)?.fetch(job.id, glob).await
    }

    async fn cancel(&self, job: &Job) -> Result<()> {
        let b = &self.0;
        let Ok(inst) = b.instance(job, false).await else { return Ok(()) };
        for rank in 0..inst.nodes.len() {
            if let Ok(r) = b.remote(&inst, rank) {
                let _ = tokio::time::timeout(Duration::from_secs(20), r.touch_cancel(job.id)).await;
            }
        }
        // Give the runner a moment to stop, then copy what there is and tear down.
        let me = b.clone();
        let job = job.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(15)).await;
            if !me.collecting.lock().await.insert(job.id) {
                return;
            }
            let _ = me.collect_and_teardown(&job, &inst, Some(JobState::Cancelled)).await;
            me.collecting.lock().await.remove(&job.id);
        });
        Ok(())
    }

    async fn availability(&self) -> Result<Value> {
        let b = &self.0;
        let offers = b.cached_offers(1, 1).await?;
        let available: Vec<Value> = offers
            .iter()
            .filter(|o| o.available)
            .take(12)
            .map(|o| json!({"gpu_type": o.gpu_type, "usd_per_hour": (o.usd_per_hour * 100.0).round() / 100.0}))
            .collect();
        let running = b.instances.lock().await.len();
        Ok(json!({"in_stock": available, "my_instances": running, "boot_estimate_s": b.boot().round()}))
    }

    async fn shell_for(&self, job: Option<&Job>) -> Result<(Arc<dyn Shell>, String)> {
        let b = &self.0;
        let job = job.context("cloud backends have no shared host")?;
        let inst = b.instance(job, true).await?;
        let node = inst.nodes.first().context("instance has no nodes")?;
        Ok((b.provider.shell(node)?, b.provider.root().to_string()))
    }

    async fn sweep(&self, live: &HashMap<i64, Option<Value>>) -> Result<()> {
        let b = &self.0;
        let known: HashMap<i64, Instance> = b.instances.lock().await.clone();
        let mut owned: HashSet<String> = known.values().map(|i| i.provider_id.clone()).collect();
        for h in live.values().flatten() {
            if let Some(id) = h["instance"]["provider_id"].as_str() {
                owned.insert(id.to_string());
            }
        }
        let now = Utc::now();
        for (id, name, cluster) in b.provider.list().await? {
            let Some(job_id) = name.strip_prefix("compute-").and_then(|r| r.split('-').next()).and_then(|x| x.parse::<i64>().ok()) else {
                continue;
            };
            let keep = live.contains_key(&job_id) && owned.contains(&id) && known.get(&job_id).is_none_or(|i| i.deadline > now);
            if keep {
                continue;
            }
            tracing::warn!("{}: tearing down leftover instance {name} ({id})", b.cfg.name);
            let fake = Instance {
                provider_id: id.clone(),
                name: name.clone(),
                job_id,
                cluster,
                node_count: 1,
                nodes: Vec::new(),
                created_at: now,
                deadline: now,
                offer: Offer { id: String::new(), gpu_type: String::new(), gpus: 0, cpus: 0, mem_gb: 0, usd_per_hour: 0.0, available: false, region: None },
            };
            if let Err(e) = b.provider.terminate(&fake).await {
                tracing::warn!("terminating {name}: {e:#}");
            }
        }
        Ok(())
    }

    fn pricing(&self) -> Option<Value> {
        let b = &self.0;
        let o = b.offers.try_lock().ok()?.get(&(1, 1)).map(|(_, o)| o.clone())?;
        Some(json!(o.iter().map(|o| json!({"gpu_type": o.gpu_type, "usd_per_hour_per_gpu": o.usd_per_hour, "available": o.available})).collect::<Vec<_>>()))
    }
}

// ---------------------------------------------------------------------------- HTTP

/// Calls a JSON API with curl. The request (including the API key) goes in a curl config on
/// stdin, so no secret appears in a process list.
async fn http(method: &str, url: &str, auth: &str, body: Option<&Value>) -> Result<Value> {
    use tokio::io::AsyncWriteExt;
    let esc = |s: &str| s.replace('\\', "\\\\").replace('"', "\\\"");
    let mut cfg = format!(
        "url = \"{}\"\nrequest = \"{method}\"\nheader = \"{}\"\nheader = \"Accept: application/json\"\nsilent\nshow-error\nmax-time = 90\nwrite-out = \"\\n%{{http_code}}\"\n",
        esc(url),
        esc(auth)
    );
    if let Some(b) = body {
        cfg.push_str("header = \"Content-Type: application/json\"\n");
        cfg.push_str(&format!("data-binary = \"{}\"\n", esc(&b.to_string())));
    }
    let mut child = tokio::process::Command::new("curl")
        .args(["--config", "-"])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .context("running curl")?;
    {
        let mut stdin = child.stdin.take().context("curl stdin")?;
        stdin.write_all(cfg.as_bytes()).await?;
    }
    let out = child.wait_with_output().await?;
    let text = String::from_utf8_lossy(&out.stdout).into_owned();
    let (body, code) = text.rsplit_once('\n').unwrap_or(("", text.as_str()));
    let code: u16 = code.trim().parse().unwrap_or(0);
    if !out.status.success() || code == 0 {
        bail!("{method} {url}: {}", String::from_utf8_lossy(&out.stderr).trim());
    }
    if code >= 400 {
        bail!("{method} {url}: HTTP {code}: {}", super::tail(body, 600));
    }
    if body.trim().is_empty() {
        return Ok(Value::Null);
    }
    serde_json::from_str(body).with_context(|| format!("{method} {url}: response is not JSON"))
}

fn read_key(path: &Path) -> Result<String> {
    let k = std::fs::read_to_string(path).with_context(|| format!("reading API key {}", path.display()))?;
    let k = k.trim().to_string();
    if k.is_empty() {
        bail!("{} is empty", path.display());
    }
    Ok(k)
}

fn ssh_shell(addr: &SshAddr, name: &str) -> Arc<dyn Shell> {
    Arc::new(Ssh {
        host: addr.host.clone(),
        user: Some(addr.user.clone()),
        port: Some(addr.port),
        tunnel: false,
        alias: name.to_string(),
        ephemeral: true,
    })
}

async fn wait_ssh(addr: &SshAddr, name: &str, deadline: Instant) -> Result<()> {
    let shell = ssh_shell(addr, name);
    let mut last = String::new();
    while Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_secs(30), shell.run("echo ok")).await {
            Ok(Ok(o)) if o.text().trim() == "ok" => return Ok(()),
            Ok(Err(e)) => last = format!("{e:#}"),
            _ => {}
        }
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
    bail!("ssh to {name} never worked: {last}")
}

/// Container entrypoint for images without sshd: install it if needed, authorize the
/// coordinator's key, start sshd, then idle until the deadline.
fn boot_script(timeout_s: u64) -> String {
    format!(
        r#"if ! [ -x /usr/sbin/sshd ]; then
  if command -v apt-get >/dev/null 2>&1; then
    (apt-get update -qq && DEBIAN_FRONTEND=noninteractive apt-get install -y -qq openssh-server bash tar) > /tmp/compute-boot.log 2>&1
  elif command -v apk >/dev/null 2>&1; then
    apk add --no-cache openssh bash tar > /tmp/compute-boot.log 2>&1
  elif command -v dnf >/dev/null 2>&1; then
    dnf install -y openssh-server bash tar > /tmp/compute-boot.log 2>&1
  fi
fi
mkdir -p /root/.ssh /run/sshd /var/run/sshd
chmod 700 /root/.ssh
printf '%s\n' "$PUBLIC_KEY" > /root/.ssh/authorized_keys
chmod 600 /root/.ssh/authorized_keys
ssh-keygen -A > /dev/null 2>&1
/usr/sbin/sshd -o PermitRootLogin=prohibit-password -o PasswordAuthentication=no
sleep {timeout_s}"#
    )
}

// ---------------------------------------------------------------------------- RunPod

/// RunPod GPU Pods (single node) and Instant Clusters (multi-node) through REST v2.
pub struct RunPod {
    key_file: std::path::PathBuf,
    tier: String,
    disk_gb: u32,
    regions: Vec<String>,
    registry_ids: Mutex<HashMap<String, String>>,
}

const RUNPOD: &str = "https://api.runpod.io/v2";

impl RunPod {
    fn new(key_file: &Path, c: &CloudConfig) -> RunPod {
        RunPod {
            key_file: key_file.to_path_buf(),
            tier: c.tier.clone().unwrap_or_else(|| "SECURE".into()).to_ascii_uppercase(),
            disk_gb: c.disk_gb,
            regions: c.regions.clone(),
            registry_ids: Mutex::new(HashMap::new()),
        }
    }
    fn auth(&self) -> Result<String> {
        Ok(format!("Authorization: Bearer {}", read_key(&self.key_file)?))
    }
    async fn call(&self, method: &str, path: &str, body: Option<&Value>) -> Result<Value> {
        http(method, &format!("{RUNPOD}{path}"), &self.auth()?, body).await
    }
    /// A registry credential id for private images (created once per registry host).
    async fn registry_id(&self, creds: &(String, String, String)) -> Result<String> {
        let (host, user, pass) = creds;
        if let Some(id) = self.registry_ids.lock().await.get(host) {
            return Ok(id.clone());
        }
        let name = format!("compute-{}", host.replace('.', "-"));
        let list = self.call("GET", "/registries", None).await?;
        if let Some(id) = list["registries"].as_array().into_iter().flatten().find(|r| r["name"] == name.as_str()).and_then(|r| r["id"].as_str()) {
            // Refresh the stored password in case it was rotated.
            let _ = self.call("PATCH", &format!("/registries/{id}"), Some(&json!({"username": user, "password": pass}))).await;
            self.registry_ids.lock().await.insert(host.clone(), id.to_string());
            return Ok(id.to_string());
        }
        let r = self.call("POST", "/registries", Some(&json!({"name": name, "username": user, "password": pass}))).await?;
        let id = r["id"].as_str().context("RunPod returned no registry id")?.to_string();
        self.registry_ids.lock().await.insert(host.clone(), id.clone());
        Ok(id)
    }
    fn ssh_of(pod: &Value) -> Option<SshAddr> {
        let d = &pod["ssh"]["direct"];
        Some(SshAddr {
            host: d["host"].as_str()?.to_string(),
            port: d["port"].as_u64()? as u16,
            user: d["username"].as_str().unwrap_or("root").to_string(),
        })
    }
}

#[async_trait]
impl Provider for RunPod {
    async fn offers(&self, gpus: u32, nodes: u32) -> Result<Vec<Offer>> {
        let product = if nodes > 1 { "CLUSTER" } else { "POD" };
        let v = self.call("GET", &format!("/catalog/gpus?include=AVAILABILITY&product={product}&count={gpus}"), None).await?;
        let secure = self.tier == "SECURE";
        let mut out = Vec::new();
        for g in v["gpus"].as_array().into_iter().flatten() {
            let price = g["price"][if secure { "secure" } else { "community" }].as_f64().unwrap_or(0.0);
            let max = g["maxCount"][if secure { "secure" } else { "community" }].as_u64().unwrap_or(0) as u32;
            let offered = g[if secure { "secure" } else { "community" }].as_bool().unwrap_or(false);
            if price <= 0.0 || !offered || max < gpus.max(1) {
                continue;
            }
            let stock = g["availability"].as_str().unwrap_or("NONE");
            out.push(Offer {
                id: g["id"].as_str().unwrap_or_default().to_string(),
                gpu_type: g["name"].as_str().or(g["id"].as_str()).unwrap_or_default().to_string(),
                gpus: gpus.max(1),
                cpus: g["minVcpuCount"].as_u64().unwrap_or(0) as u32,
                mem_gb: g["minRam"].as_u64().unwrap_or(0) as u32,
                usd_per_hour: price * f64::from(gpus.max(1)),
                available: stock != "NONE",
                region: None,
            });
        }
        Ok(out)
    }

    async fn create(&self, req: &CreateReq<'_>) -> Result<Instance> {
        let registry = match &req.registry {
            Some(c) => Some(self.registry_id(c).await?),
            None => None,
        };
        let mut body = json!({
            "name": req.name,
            "image": req.image,
            "disk": self.disk_gb,
            "ports": ["22/tcp"],
            "env": {"PUBLIC_KEY": req.pubkey},
            "entrypoint": ["bash", "-c", boot_script(req.timeout_s)],
            "startSsh": true,
        });
        if let Some(id) = registry {
            body["registry"] = id.into();
        }
        if !self.regions.is_empty() {
            body["dataCenterIds"] = json!(self.regions);
        }
        let now = Utc::now();
        let deadline = now + chrono::Duration::seconds(req.timeout_s as i64);
        if req.nodes > 1 {
            body["type"] = "TRAINING".into();
            body["compute"] = json!({"gpuTypeId": req.offer.id, "gpuCountPerPod": req.offer.gpus, "podCount": req.nodes});
            let r = self.call("POST", "/clusters", Some(&body)).await?;
            let id = r["id"].as_str().context("RunPod returned no cluster id")?.to_string();
            return Ok(Instance { provider_id: id, name: req.name.clone(), job_id: req.job.id, cluster: true, node_count: req.nodes, nodes: Vec::new(), created_at: now, deadline, offer: req.offer.clone() });
        }
        body["cloud"] = self.tier.clone().into();
        body["gpu"] = json!({"id": req.offer.id, "count": req.offer.gpus});
        let r = self.call("POST", "/pods", Some(&body)).await?;
        let id = r["id"].as_str().or(r["podId"].as_str()).context("RunPod returned no pod id")?.to_string();
        Ok(Instance { provider_id: id, name: req.name.clone(), job_id: req.job.id, cluster: false, node_count: 1, nodes: Vec::new(), created_at: now, deadline, offer: req.offer.clone() })
    }

    async fn wait_ready(&self, inst: &mut Instance, deadline: Instant) -> Result<()> {
        loop {
            if Instant::now() > deadline {
                bail!("not ready within 20 minutes");
            }
            let pods: Vec<Value> = if inst.cluster {
                self.call("GET", &format!("/clusters/{}/pods", inst.provider_id), None).await?["pods"].as_array().cloned().unwrap_or_default()
            } else {
                vec![self.call("GET", &format!("/pods/{}", inst.provider_id), None).await?]
            };
            if pods.iter().any(|p| matches!(p["status"].as_str(), Some("ERROR" | "TERMINATED" | "EXITED"))) {
                bail!("a pod failed to start ({})", pods.iter().filter_map(|p| p["status"].as_str()).collect::<Vec<_>>().join(", "));
            }
            let ready = pods.len() >= inst.node_count.max(1) as usize && pods.iter().all(|p| p["status"] == "RUNNING" && Self::ssh_of(p).is_some());
            if ready {
                let mut nodes: Vec<Node> = pods
                    .iter()
                    .enumerate()
                    .map(|(i, p)| Node {
                        rank: p["cluster"]["rank"].as_u64().map(|r| r as u32).unwrap_or(i as u32),
                        ssh: Self::ssh_of(p),
                        sandbox: None,
                        private_ip: p["cluster"]["ip"].as_str().map(str::to_string),
                    })
                    .collect();
                nodes.sort_by_key(|n| n.rank);
                for n in &nodes {
                    if let Some(a) = &n.ssh {
                        wait_ssh(a, &inst.name, deadline).await?;
                    }
                }
                inst.nodes = nodes;
                return Ok(());
            }
            tokio::time::sleep(Duration::from_secs(10)).await;
        }
    }

    async fn alive(&self, inst: &Instance) -> Result<bool> {
        let path = if inst.cluster { format!("/clusters/{}", inst.provider_id) } else { format!("/pods/{}", inst.provider_id) };
        match self.call("GET", &path, None).await {
            Ok(v) => Ok(!matches!(v["status"].as_str(), Some("TERMINATED" | "EXITED" | "ERROR"))),
            Err(e) if format!("{e}").contains("HTTP 404") => Ok(false),
            Err(e) => Err(e),
        }
    }

    async fn terminate(&self, inst: &Instance) -> Result<()> {
        let path = if inst.cluster { format!("/clusters/{}", inst.provider_id) } else { format!("/pods/{}", inst.provider_id) };
        match self.call("DELETE", &path, None).await {
            Ok(_) => Ok(()),
            Err(e) if format!("{e}").contains("HTTP 404") => Ok(()),
            Err(e) => Err(e),
        }
    }

    async fn list(&self) -> Result<Vec<(String, String, bool)>> {
        let mut out = Vec::new();
        for p in self.call("GET", "/pods", None).await?["pods"].as_array().into_iter().flatten() {
            if p["cluster"].is_object() {
                continue; // torn down with its cluster
            }
            if let (Some(id), Some(name)) = (p["id"].as_str(), p["name"].as_str())
                && name.starts_with("compute-")
            {
                out.push((id.to_string(), name.to_string(), false));
            }
        }
        for c in self.call("GET", "/clusters", None).await?["clusters"].as_array().into_iter().flatten() {
            if let (Some(id), Some(name)) = (c["id"].as_str(), c["name"].as_str())
                && name.starts_with("compute-")
            {
                out.push((id.to_string(), name.to_string(), true));
            }
        }
        Ok(out)
    }

    fn shell(&self, node: &Node) -> Result<Arc<dyn Shell>> {
        let a = node.ssh.as_ref().context("pod has no ssh address")?;
        Ok(ssh_shell(a, "runpod"))
    }
    fn root(&self) -> &'static str {
        "/root/compute"
    }
    fn runtime(&self) -> &'static str {
        "native"
    }
}

// ---------------------------------------------------------------------------- Lambda

/// Lambda Cloud on-demand instances. Jobs run under Docker with `--gpus all`.
pub struct Lambda {
    key_file: std::path::PathBuf,
    regions: Vec<String>,
    key_name: Mutex<Option<String>>,
}

const LAMBDA: &str = "https://cloud.lambda.ai/api/v1";

impl Lambda {
    fn new(key_file: &Path, c: &CloudConfig) -> Lambda {
        Lambda { key_file: key_file.to_path_buf(), regions: c.regions.clone(), key_name: Mutex::new(None) }
    }
    async fn call(&self, method: &str, path: &str, body: Option<&Value>) -> Result<Value> {
        http(method, &format!("{LAMBDA}{path}"), &format!("Authorization: Bearer {}", read_key(&self.key_file)?), body).await
    }
    /// The name of a registered ssh key matching the coordinator's, registering it if needed.
    async fn ssh_key_name(&self, pubkey: &str) -> Result<String> {
        if let Some(n) = self.key_name.lock().await.clone() {
            return Ok(n);
        }
        let body_of = |k: &str| k.split_whitespace().take(2).collect::<Vec<_>>().join(" ");
        let mine = body_of(pubkey);
        let keys = self.call("GET", "/ssh-keys", None).await?;
        let found = keys["data"].as_array().into_iter().flatten().find(|k| k["public_key"].as_str().map(body_of).as_deref() == Some(mine.as_str()));
        let name = match found.and_then(|k| k["name"].as_str()) {
            Some(n) => n.to_string(),
            None => {
                let name = format!("compute-daemon-{}", &hex::encode(<sha2::Sha256 as sha2::Digest>::digest(mine.as_bytes()))[..8]);
                self.call("POST", "/ssh-keys", Some(&json!({"name": name, "public_key": pubkey}))).await?;
                name
            }
        };
        *self.key_name.lock().await = Some(name.clone());
        Ok(name)
    }
}

#[async_trait]
impl Provider for Lambda {
    async fn offers(&self, gpus: u32, _nodes: u32) -> Result<Vec<Offer>> {
        let v = self.call("GET", "/instance-types", None).await?;
        let mut out = Vec::new();
        for (name, t) in v["data"].as_object().into_iter().flatten() {
            let it = &t["instance_type"];
            let specs = &it["specs"];
            let n = specs["gpus"].as_u64().unwrap_or(0) as u32;
            if n != gpus.max(1) {
                continue;
            }
            let regions: Vec<String> = t["regions_with_capacity_available"].as_array().into_iter().flatten().filter_map(|r| r["name"].as_str().map(str::to_string)).collect();
            let region = if self.regions.is_empty() { regions.first().cloned() } else { self.regions.iter().find(|r| regions.contains(r)).cloned() };
            out.push(Offer {
                id: name.clone(),
                gpu_type: it["gpu_description"].as_str().unwrap_or(name).to_string(),
                gpus: n,
                cpus: specs["vcpus"].as_u64().unwrap_or(0) as u32,
                mem_gb: specs["memory_gib"].as_u64().unwrap_or(0) as u32,
                usd_per_hour: it["price_cents_per_hour"].as_f64().unwrap_or(0.0) / 100.0,
                available: region.is_some(),
                region,
            });
        }
        Ok(out)
    }

    async fn create(&self, req: &CreateReq<'_>) -> Result<Instance> {
        let key = self.ssh_key_name(&req.pubkey).await?;
        let region = req.offer.region.clone().context("no region with capacity")?;
        let r = self
            .call(
                "POST",
                "/instance-operations/launch",
                Some(&json!({"region_name": region, "instance_type_name": req.offer.id, "ssh_key_names": [key], "name": req.name, "quantity": 1})),
            )
            .await?;
        let id = r["data"]["instance_ids"][0].as_str().context("Lambda returned no instance id")?.to_string();
        let now = Utc::now();
        Ok(Instance {
            provider_id: id,
            name: req.name.clone(),
            job_id: req.job.id,
            cluster: false,
            node_count: 1,
            nodes: Vec::new(),
            created_at: now,
            deadline: now + chrono::Duration::seconds(req.timeout_s as i64),
            offer: req.offer.clone(),
        })
    }

    async fn wait_ready(&self, inst: &mut Instance, deadline: Instant) -> Result<()> {
        loop {
            if Instant::now() > deadline {
                bail!("not ready within 20 minutes");
            }
            let v = self.call("GET", &format!("/instances/{}", inst.provider_id), None).await?;
            let d = &v["data"];
            match d["status"].as_str() {
                Some("active") => {
                    if let Some(ip) = d["ip"].as_str() {
                        let addr = SshAddr { host: ip.to_string(), port: 22, user: "ubuntu".into() };
                        wait_ssh(&addr, &inst.name, deadline).await?;
                        inst.nodes = vec![Node { rank: 0, ssh: Some(addr), sandbox: None, private_ip: d["private_ip"].as_str().map(str::to_string) }];
                        return Ok(());
                    }
                }
                Some("terminated" | "terminating" | "unhealthy") => bail!("instance is {}", d["status"]),
                _ => {}
            }
            tokio::time::sleep(Duration::from_secs(10)).await;
        }
    }

    async fn alive(&self, inst: &Instance) -> Result<bool> {
        match self.call("GET", &format!("/instances/{}", inst.provider_id), None).await {
            Ok(v) => Ok(!matches!(v["data"]["status"].as_str(), Some("terminated" | "terminating"))),
            Err(e) if format!("{e}").contains("HTTP 404") => Ok(false),
            Err(e) => Err(e),
        }
    }

    async fn terminate(&self, inst: &Instance) -> Result<()> {
        self.call("POST", "/instance-operations/terminate", Some(&json!({"instance_ids": [inst.provider_id]}))).await.map(|_| ())
    }

    async fn list(&self) -> Result<Vec<(String, String, bool)>> {
        let v = self.call("GET", "/instances", None).await?;
        Ok(v["data"]
            .as_array()
            .into_iter()
            .flatten()
            .filter(|i| !matches!(i["status"].as_str(), Some("terminated" | "terminating")))
            .filter_map(|i| Some((i["id"].as_str()?.to_string(), i["name"].as_str()?.to_string(), false)))
            .filter(|(_, n, _)| n.starts_with("compute-"))
            .collect())
    }

    fn shell(&self, node: &Node) -> Result<Arc<dyn Shell>> {
        Ok(ssh_shell(node.ssh.as_ref().context("instance has no ssh address")?, "lambda"))
    }
    fn root(&self) -> &'static str {
        "/home/ubuntu/compute"
    }
    fn runtime(&self) -> &'static str {
        "docker"
    }

    async fn prepare(&self, shell: &dyn Shell, registry: Option<&(String, String, String)>) -> Result<()> {
        // New ssh sessions pick up the docker group.
        shell.run("sudo usermod -aG docker \"$(id -un)\" >/dev/null 2>&1 || true").await?;
        if let Some((host, user, pass)) = registry {
            shell
                .run_input(
                    &format!("IFS= read -r u\nIFS= read -r p\nprintf '%s' \"$p\" | sudo docker login {} -u \"$u\" --password-stdin >/dev/null\nsudo cp -r /root/.docker \"$HOME/\" 2>/dev/null; sudo chown -R \"$(id -un)\" \"$HOME/.docker\" 2>/dev/null; true", shell_quote(host)),
                    Some(format!("{user}\n{pass}\n").into_bytes()),
                )
                .await?;
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------- Modal

/// Modal Sandboxes through the `modal-client` gRPC crate. Prices come from the rate table in
/// hosts.toml, since Modal has no pricing API. The sandbox's own timeout is the provider-side
/// limit.
pub struct Modal {
    key_file: std::path::PathBuf,
    cfg: CloudConfig,
    client: RwLock<Option<(modal_client::Client, String)>>,
}

impl Modal {
    fn new(key_file: &Path, c: &CloudConfig) -> Modal {
        Modal { key_file: key_file.to_path_buf(), cfg: c.clone(), client: RwLock::new(None) }
    }

    /// The client and the app id, connecting on first use. The key file is a modal.toml-style
    /// file with `token_id` and `token_secret`.
    async fn client(&self) -> Result<(modal_client::Client, String)> {
        if let Some(c) = self.client.read().unwrap_or_else(|e| e.into_inner()).clone() {
            return Ok(c);
        }
        let text = std::fs::read_to_string(&self.key_file).with_context(|| format!("reading {}", self.key_file.display()))?;
        let get = |k: &str| {
            text.lines().find_map(|l| {
                let (key, v) = l.split_once('=')?;
                (key.trim() == k).then(|| v.trim().trim_matches('"').trim_matches('\'').to_string())
            })
        };
        let id = get("token_id").context("modal key file has no token_id")?;
        let secret = get("token_secret").context("modal key file has no token_secret")?;
        let client = modal_client::Client::connect(&id, &secret, None).await?;
        let app = client.app_get_or_create(self.cfg.app.as_deref().unwrap_or("compute"), self.cfg.environment.as_deref()).await?;
        *self.client.write().unwrap_or_else(|e| e.into_inner()) = Some((client.clone(), app.clone()));
        Ok((client, app))
    }
}

/// Scripts run in a Modal Sandbox through its exec API.
struct ModalShell {
    client: modal_client::Client,
    sandbox: String,
}

#[async_trait]
impl Shell for ModalShell {
    async fn run_input(&self, script: &str, input: Option<Vec<u8>>) -> Result<crate::transport::Output> {
        let argv = vec!["bash".to_string(), "-c".to_string(), script.to_string()];
        let out = self.client.exec_output(&self.sandbox, &argv, input, Duration::from_secs(600)).await?;
        Ok(crate::transport::Output { stdout: out })
    }

    async fn pipe_in(&self, script: &str, data: Reader) -> Result<()> {
        let argv = vec!["bash".to_string(), "-c".to_string(), script.to_string()];
        let h = self.client.exec(&self.sandbox, &argv, Some(data)).await?;
        let exit = h.wait().await?;
        if exit.code != 0 {
            bail!("sandbox command failed ({}): {}", exit.code, super::tail(&exit.stderr, 1500));
        }
        Ok(())
    }

    async fn pipe_out(&self, script: &str) -> Result<Reader> {
        let argv = vec!["bash".to_string(), "-c".to_string(), script.to_string()];
        let mut h = self.client.exec(&self.sandbox, &argv, None).await?;
        let stdout = h.take_stdout();
        tokio::spawn(async move {
            let _ = h.wait().await;
        });
        Ok(Box::new(stdout))
    }
}

#[async_trait]
impl Provider for Modal {
    async fn offers(&self, gpus: u32, _nodes: u32) -> Result<Vec<Offer>> {
        // No pricing API: the rate table is the offer list; availability is learned at create.
        self.client().await?;
        Ok(self
            .cfg
            .gpu_rates
            .iter()
            .map(|(gpu, rate)| Offer {
                id: gpu.clone(),
                gpu_type: gpu.clone(),
                gpus: gpus.max(1),
                cpus: 0,
                mem_gb: 0,
                usd_per_hour: rate * f64::from(gpus.max(1)),
                available: true,
                region: None,
            })
            .collect())
    }

    async fn create(&self, req: &CreateReq<'_>) -> Result<Instance> {
        let (client, app) = self.client().await?;
        let registry_secret = match &req.registry {
            Some((_, user, pass)) => {
                let env: BTreeMap<String, String> = [("REGISTRY_USERNAME".to_string(), user.clone()), ("REGISTRY_PASSWORD".to_string(), pass.clone())].into();
                Some(client.secret_from_env(None, &env, self.cfg.environment.as_deref()).await?)
            }
            None => None,
        };
        let image = client.image_from_registry_in(&app, &req.image, registry_secret.as_deref(), self.cfg.environment.as_deref()).await?;
        let r = &req.job.resources;
        let gpu = if req.offer.gpus > 1 { format!("{}:{}", req.offer.id, req.offer.gpus) } else { req.offer.id.clone() };
        // Account for CPU and memory in the price, from the rate table.
        let spec = modal_client::SandboxSpec {
            app_id: app,
            image_id: image,
            name: Some(req.name.clone()),
            entrypoint: vec!["sleep".into(), req.timeout_s.to_string()],
            cpu: f64::from(r.cpus),
            memory_mb: r.mem_mb.min(u64::from(u32::MAX)) as u32,
            gpu: Some(gpu),
            timeout_secs: req.timeout_s.min(u64::from(u32::MAX)) as u32,
            regions: self.cfg.regions.clone(),
            ..Default::default()
        };
        let id = client.sandbox_create(&spec).await?;
        let tags: BTreeMap<String, String> = [("compute_name".to_string(), req.name.clone())].into();
        let _ = client.sandbox_set_tags(&id, &tags).await;
        let now = Utc::now();
        Ok(Instance {
            provider_id: id.clone(),
            name: req.name.clone(),
            job_id: req.job.id,
            cluster: false,
            node_count: 1,
            nodes: vec![Node { rank: 0, ssh: None, sandbox: Some(id), private_ip: None }],
            created_at: now,
            deadline: now + chrono::Duration::seconds(req.timeout_s as i64),
            offer: req.offer.clone(),
        })
    }

    async fn wait_ready(&self, inst: &mut Instance, deadline: Instant) -> Result<()> {
        let shell = self.shell(inst.nodes.first().context("no sandbox")?)?;
        let mut last = String::new();
        while Instant::now() < deadline {
            match shell.run("echo ok").await {
                Ok(o) if o.text().trim() == "ok" => return Ok(()),
                Ok(_) => {}
                Err(e) => last = format!("{e:#}"),
            }
            tokio::time::sleep(Duration::from_secs(3)).await;
        }
        bail!("sandbox never answered: {last}")
    }

    async fn alive(&self, inst: &Instance) -> Result<bool> {
        let (client, _) = self.client().await?;
        Ok(client.sandbox_poll(&inst.provider_id).await?.is_none())
    }

    async fn terminate(&self, inst: &Instance) -> Result<()> {
        let (client, _) = self.client().await?;
        client.sandbox_terminate(&inst.provider_id).await
    }

    async fn list(&self) -> Result<Vec<(String, String, bool)>> {
        let (client, app) = self.client().await?;
        Ok(client
            .sandbox_list(&app, self.cfg.environment.as_deref())
            .await?
            .into_iter()
            .filter(|s| s.running)
            .filter_map(|s| s.tags.get("compute_name").cloned().map(|n| (s.sandbox_id, n, false)))
            .collect())
    }

    fn shell(&self, node: &Node) -> Result<Arc<dyn Shell>> {
        let sandbox = node.sandbox.clone().context("node has no sandbox id")?;
        let client = self.client.read().unwrap_or_else(|e| e.into_inner()).clone().map(|(c, _)| c).context("modal client not connected")?;
        Ok(Arc::new(ModalShell { client, sandbox }))
    }
    fn root(&self) -> &'static str {
        "/root/compute"
    }
    fn runtime(&self) -> &'static str {
        "native"
    }
}
