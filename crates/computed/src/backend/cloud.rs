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

use super::{Alloc, Backend, NoCapacity, PollResult, Probe, Quote, Remote, Shape, StageFiles, parse_status, tar_dir_script};
use crate::config::{BackendConfig, BackendKind, CloudConfig, Paths};
use crate::model::{Job, JobState};
use crate::store::{JobUpdate, Store};
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
/// How long an offer that refused a launch for lack of capacity counts as unavailable, whatever
/// the provider's listing says (Lambda's capacity list lags behind its launches).
const FULL_TTL: Duration = Duration::from_secs(600);
/// Offers tried per staging attempt before the job goes back to the queue.
const CREATE_TRIES: usize = 4;
/// Room on a new instance's provider-side timeout for staging (dataset and input copies).
const STAGING_ALLOWANCE_S: u64 = 1800;
/// Room after the time limit for the runner to stop and the outputs to be copied off.
const COLLECT_ALLOWANCE_S: u64 = 900;
/// The shortest run a job with no time limit is placed for: under it, max_cost_usd is too low.
const MIN_OPEN_LIMIT_S: u64 = 600;
const MASTER_PORT: u16 = 29500;

/// `5h30m`-style hours and minutes.
fn hm(secs: u64) -> String {
    match (secs / 3600, secs % 3600 / 60) {
        (0, m) => format!("{m}m"),
        (h, 0) => format!("{h}h"),
        (h, m) => format!("{h}h{m:02}m"),
    }
}

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
    /// Local disk per node in GB (0: unknown, or sized at creation as on RunPod).
    #[serde(default)]
    pub disk_gb: u32,
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

/// What a job needs from an offer. GPU and node counts select the offers; CPUs, memory and
/// the image's CUDA version let a provider that sizes machines per request (RunPod) price the
/// right one.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Need {
    pub gpus: u32,
    pub nodes: u32,
    pub cpus: u32,
    pub mem_gb: u32,
    pub min_cuda: Option<String>,
}

impl Need {
    fn of(job: &Job, image: Option<&str>) -> Need {
        let r = &job.resources;
        Need { gpus: r.gpus, nodes: r.nodes, cpus: r.cpus, mem_gb: r.mem_mb.div_ceil(1024) as u32, min_cuda: image.and_then(min_cuda_of) }
    }
    /// Any one GPU, for price and stock listings.
    fn any_gpu() -> Need {
        Need { gpus: 1, nodes: 1, cpus: 0, mem_gb: 0, min_cuda: None }
    }
}

/// The CUDA version an image tag names (`nvidia/cuda:12.4.1-…`, `…-cuda12.1-…`, `…cu121…`),
/// so the instance's driver can be required to support it.
pub fn min_cuda_of(image: &str) -> Option<String> {
    let tag = image.rsplit_once(':').map(|(_, t)| t).unwrap_or(image).to_ascii_lowercase();
    let name = image.to_ascii_lowercase();
    let digits = |s: &str| -> Option<(u32, u32)> {
        let mut parts = s.split(|c: char| !c.is_ascii_digit()).filter(|x| !x.is_empty());
        Some((parts.next()?.parse().ok()?, parts.next()?.parse().ok()?))
    };
    // nvidia/cuda:<major>.<minor>…
    if name.contains("nvidia/cuda") && let Some((a, b)) = digits(&tag) {
        return Some(format!("{a}.{b}"));
    }
    if let Some(i) = tag.find("cuda") {
        let rest = tag[i + 4..].trim_start_matches(['-', '_', ':']);
        if let Some((a, b)) = digits(rest) {
            return Some(format!("{a}.{b}"));
        }
    }
    // cu121 / cu118 / cu124
    let mut search = tag.as_str();
    while let Some(i) = search.find("cu") {
        let d: String = search[i + 2..].chars().take_while(|c| c.is_ascii_digit()).collect();
        if d.len() == 3 {
            return Some(format!("{}.{}", &d[..2], &d[2..]));
        }
        search = &search[i + 2..];
    }
    None
}

/// What to create.
pub struct CreateReq<'a> {
    pub name: String,
    pub job: &'a Job,
    pub image: String,
    pub offer: Offer,
    pub nodes: u32,
    pub pubkey: String,
    /// More public keys to authorize (`authorized_keys` in hosts.toml).
    pub extra_keys: Vec<String>,
    pub timeout_s: u64,
}

#[async_trait]
pub trait Provider: Send + Sync {
    /// Offers for `need.gpus` GPUs per node (0: a CPU machine) on `need.nodes` nodes.
    async fn offers(&self, need: &Need) -> Result<Vec<Offer>>;
    /// Whether CPU-only jobs can run here (RunPod CPU pods).
    fn cpu_pods(&self) -> bool {
        false
    }
    /// Environment for multi-node jobs (e.g. which interface NCCL must use).
    fn node_env(&self) -> Vec<(String, String)> {
        Vec::new()
    }
    /// Moves an instance's own idle timer (if it has one) to `secs` from now; called at launch,
    /// when the job's time limit starts.
    async fn extend_deadline(&self, _shell: &dyn Shell, _secs: u64) -> Result<()> {
        Ok(())
    }
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
    /// Steps after the instance is reachable (Docker group). Images are public, so there is no
    /// registry login.
    async fn prepare(&self, _shell: &dyn Shell) -> Result<()> {
        Ok(())
    }
}

/// `(state, exit code, started, ended, note)` of a finished cloud job.
type Outcome = (Option<JobState>, Option<i32>, Option<DateTime<Utc>>, Option<DateTime<Utc>>, Option<String>);
type OfferCache = HashMap<Need, (Instant, Vec<Offer>)>;

/// Outcome of copying a finished job off its instance.
#[derive(Clone)]
struct Collected {
    result: Outcome,
}

pub struct CloudBackend {
    store: Arc<Store>,
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
    /// The offer each job was last quoted, so the instance rented is the one priced.
    quoted: Mutex<HashMap<i64, Offer>>,
    /// `(offer id, region)` of offers that recently refused a launch for lack of capacity.
    full: Mutex<HashMap<(String, Option<String>), Instant>>,
}

/// What the outputs directory records about how a job ended (survives a daemon restart
/// between teardown and the next poll).
#[derive(Serialize, Deserialize)]
struct SavedOutcome {
    provider_id: String,
    state: Option<String>,
    exit_code: Option<i32>,
    started: Option<DateTime<Utc>>,
    ended: Option<DateTime<Utc>>,
    note: Option<String>,
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
    pub fn new(cfg: BackendConfig, paths: &Paths, store: Arc<Store>) -> Result<CloudBackend> {
        let _ = KNOWN_HOSTS_DIR.set(paths.state_dir.join("cloud_known_hosts"));
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
            store,
            quoted: Mutex::new(HashMap::new()),
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
            full: Mutex::new(HashMap::new()),
        })
    }

    fn boot(&self) -> f64 {
        self.boot_secs.read().unwrap_or_else(|e| e.into_inner()).unwrap_or(f64::from(self.ccfg.boot_minutes) * 60.0)
    }

    fn job_dir_local(&self, id: i64) -> std::path::PathBuf {
        self.outputs_dir.join(id.to_string())
    }

    async fn cached_offers(&self, need: &Need) -> Result<Vec<Offer>> {
        let cached = self.offers.lock().await.get(need).filter(|(at, _)| at.elapsed() < OFFERS_TTL).map(|(_, o)| o.clone());
        let mut o = match cached {
            Some(o) => o,
            None => {
                let mut o = self.provider.offers(need).await?;
                if !self.ccfg.gpu_types.is_empty() {
                    o.retain(|x| self.ccfg.gpu_types.iter().any(|t| contains_ci(&x.gpu_type, t) || contains_ci(&x.id, t)));
                }
                o.sort_by(|a, b| a.usd_per_hour.total_cmp(&b.usd_per_hour));
                self.offers.lock().await.insert(need.clone(), (Instant::now(), o.clone()));
                o
            }
        };
        let mut full = self.full.lock().await;
        full.retain(|_, at| at.elapsed() < FULL_TTL);
        for x in o.iter_mut() {
            if full.contains_key(&(x.id.clone(), x.region.clone())) {
                x.available = false;
            }
        }
        Ok(o)
    }

    fn image_for(&self, job: &Job) -> Option<String> {
        job.spec.container.as_ref().map(|c| c.image.clone()).or_else(|| self.ccfg.default_image.clone())
    }

    /// The job's instance: from memory, from its handle (after a restart), or created now.
    /// An instance recorded while staging (before launch) is picked up again when staging
    /// resumes, instead of renting a second one.
    async fn instance(&self, job: &Job, create: bool) -> Result<Instance> {
        if let Some(i) = self.instances.lock().await.get(&job.id) {
            return Ok(i.clone());
        }
        let stored = job.handle.as_ref().and_then(|h| {
            let i = serde_json::from_value::<Instance>(h["instance"].clone()).ok()?;
            Some((i, h["staging"].as_bool() == Some(true)))
        });
        if let Some((i, staging)) = &stored
            && !(create && *staging)
        {
            self.instances.lock().await.insert(job.id, i.clone());
            return Ok(i.clone());
        }
        if !create {
            bail!("job {} has no instance", job.id);
        }
        let lock = self.create_locks.lock().await.entry(job.id).or_default().clone();
        let _g = lock.lock().await;
        if let Some(i) = self.instances.lock().await.get(&job.id) {
            return Ok(i.clone());
        }
        if let Some((i, _)) = stored {
            if self.provider.alive(&i).await.unwrap_or(false) {
                tracing::info!("{}: resuming staging of job {} on {}", self.cfg.name, job.id, i.name);
                self.instances.lock().await.insert(job.id, i.clone());
                return self.ready_instance(job, i).await;
            }
            let _ = self.provider.terminate(&i).await;
            // Its billing ends here; the next attempt carries the cost forward.
            if let Err(e) = self.store.cost_end(job.id, Utc::now()) {
                tracing::warn!("closing the cost of {}: {e:#}", i.name);
            }
            // Renting a new one here would skip placement (budget, the key's access and caps, the
            // quoted price): put the job back in the queue to be placed again.
            return Err(NoCapacity(format!("its instance {} from before the restart is gone; placing it again", i.name)).into());
        }
        self.create_instance(job).await
    }

    /// The offer to rent for `job`: the one it was quoted if still available, else a fitting
    /// one no dearer than that; never one over the job's `max_cost_usd`.
    async fn pick_offer(&self, job: &Job) -> Result<Offer> {
        let r = &job.resources;
        let offers = self.cached_offers(&Need::of(job, self.image_for(job).as_deref())).await?;
        let quoted = self.quoted.lock().await.get(&job.id).cloned();
        let offer = match &quoted {
            Some(q) => offers
                .iter()
                .find(|o| o.available && o.id == q.id && o.region == q.region && o.usd_per_hour <= q.usd_per_hour)
                .or_else(|| offers.iter().find(|o| o.available && offer_fits(o, job) && o.usd_per_hour <= q.usd_per_hour))
                .cloned()
                .ok_or_else(|| {
                    NoCapacity(format!("the quoted {} (${:.2}/h) has no capacity any more, and nothing as cheap does", q.gpu_type, q.usd_per_hour))
                })?,
            None => offers.into_iter().find(|o| o.available && offer_fits(o, job)).ok_or_else(|| NoCapacity("no offer with capacity right now".into()))?,
        };
        if let Some(max) = job.spec.max_cost_usd {
            let cost = self.rate(&offer, job) * (r.time_s as f64 + self.boot()) / 3600.0;
            if cost > max {
                bail!("{} would cost ~${cost:.2} for the full time limit, over max_cost_usd {max}", offer.gpu_type);
            }
        }
        Ok(offer)
    }

    /// USD per hour for `job` on `offer`, all nodes (plus CPU/memory rates where billed apart).
    fn rate(&self, offer: &Offer, job: &Job) -> f64 {
        let r = &job.resources;
        let extras = self.ccfg.cpu_rate * f64::from(r.cpus) + self.ccfg.mem_gb_rate * (r.mem_mb as f64 / 1024.0);
        (offer.usd_per_hour + extras) * f64::from(r.nodes)
    }

    async fn create_instance(&self, job: &Job) -> Result<Instance> {
        let image = self.image_for(job).context("no image: set container.image (this backend has no default_image)")?;
        let r = &job.resources;
        if self.pubkey.is_empty() {
            bail!("the coordinator has no ssh public key (~/.ssh/id_ed25519.pub) for cloud instances");
        }
        // A new attempt: what an earlier attempt of this job left locally is stale now.
        let _ = tokio::fs::remove_dir_all(self.job_dir_local(job.id)).await;
        let started = Instant::now();
        // Providers list capacity they no longer have; when a launch is refused for that, mark
        // the offer full and try the next fitting one (pick_offer keeps to the quoted price).
        let mut refused = Vec::new();
        let (req, mut inst) = loop {
            let offer = match self.pick_offer(job).await {
                Err(e) if !refused.is_empty() && e.is::<NoCapacity>() => {
                    return Err(NoCapacity(format!("{e}; launches refused for lack of capacity: {}", refused.join(", "))).into());
                }
                o => o?,
            };
            let req = CreateReq {
                name: format!("compute-{}-{}", job.id, hex::encode(rand::random::<[u8; 3]>())),
                job,
                image: image.clone(),
                offer,
                nodes: r.nodes,
                pubkey: self.pubkey.clone(),
                extra_keys: self.ccfg.authorized_keys.clone(),
                timeout_s: r.time_s + COLLECT_ALLOWANCE_S + STAGING_ALLOWANCE_S + self.boot() as u64,
            };
            match self.provider.create(&req).await {
                Ok(inst) => break (req, inst),
                Err(e) if is_capacity_error(&e) => {
                    let o = &req.offer;
                    tracing::warn!("{}: no capacity for {} in {:?} (job {}): {e:#}", self.cfg.name, o.id, o.region, job.id);
                    self.full.lock().await.insert((o.id.clone(), o.region.clone()), Instant::now());
                    refused.push(match &o.region {
                        Some(reg) => format!("{} in {reg}", o.gpu_type),
                        None => o.gpu_type.clone(),
                    });
                    if refused.len() >= CREATE_TRIES {
                        return Err(NoCapacity(format!("launches refused for lack of capacity: {}", refused.join(", "))).into());
                    }
                }
                Err(e) => return Err(e),
            }
        };
        tracing::info!("{}: created {} ({}) for job {}", self.cfg.name, inst.name, inst.provider_id, job.id);
        // Billing starts now. Record the instance with the job too, so a daemon restart while
        // staging resumes with it instead of leaving it to the sweep.
        inst.deadline = Utc::now() + chrono::Duration::seconds(req.timeout_s as i64);
        self.instances.lock().await.insert(job.id, inst.clone());
        if let Err(e) = self.store.cost_start(job.id, &self.cfg.name, self.rate(&inst.offer, job), Utc::now()) {
            tracing::warn!("recording the cost of {}: {e:#}", inst.name);
        }
        let _ = self.store.update_job_if(
            job.id,
            &[JobState::Staging],
            JobUpdate { handle: Some(Some(json!({"instance": inst, "staging": true}))), ..Default::default() },
        );
        let boot_deadline = Instant::now() + Duration::from_secs(20 * 60);
        if let Err(e) = self.provider.wait_ready(&mut inst, boot_deadline).await {
            let _ = self.provider.terminate(&inst).await;
            self.forget(&inst).await;
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
        self.prepare_instance(job, inst).await
    }

    /// Waits for an instance found again after a restart, then prepares it.
    async fn ready_instance(&self, job: &Job, mut inst: Instance) -> Result<Instance> {
        if inst.nodes.is_empty() {
            let boot_deadline = Instant::now() + Duration::from_secs(20 * 60);
            if let Err(e) = self.provider.wait_ready(&mut inst, boot_deadline).await {
                let _ = self.provider.terminate(&inst).await;
                self.forget(&inst).await;
                return Err(e.context(format!("{} did not become ready", inst.name)));
            }
        }
        self.prepare_instance(job, inst).await
    }

    /// Drops `inst` from memory, unless the job has moved on to another instance.
    async fn forget(&self, inst: &Instance) {
        let mut m = self.instances.lock().await;
        if m.get(&inst.job_id).is_some_and(|i| i.provider_id == inst.provider_id) {
            m.remove(&inst.job_id);
        }
    }

    async fn prepare_instance(&self, job: &Job, inst: Instance) -> Result<Instance> {
        for node in &inst.nodes {
            let shell = self.provider.shell(node)?;
            self.provider.prepare(shell.as_ref()).await?;
            if node.ssh.is_some() && !self.ccfg.authorized_keys.is_empty() {
                let keys: String = self.ccfg.authorized_keys.iter().map(|k| format!("{}\n", k.trim())).collect();
                shell
                    .run_input("mkdir -p ~/.ssh && chmod 700 ~/.ssh && cat >> ~/.ssh/authorized_keys", Some(keys.into_bytes()))
                    .await
                    .context("authorizing extra ssh keys")?;
            }
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
        // A superseded attempt (the job was requeued onto a new instance) only tears down.
        let current = self.instances.lock().await.get(&job.id).is_none_or(|i| i.provider_id == inst.provider_id);
        let nodes = if current { inst.nodes.len() } else { 0 };
        let _ = tokio::fs::create_dir_all(local.join("out")).await;
        let mut result: Collected = Collected { result: (forced, None, None, Some(Utc::now()), None) };
        let mut statuses = Vec::new();
        let mut outputs_from: Option<(Arc<dyn Shell>, String)> = None;
        for rank in 0..nodes {
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
                    if let Ok(mut r) = shell.pipe_out(&format!("cat {} 2>/dev/null || true", shell_quote(&format!("{jobdir}/metrics.jsonl")))).await
                        && let Ok(mut f) = tokio::fs::File::create(local.join("metrics.jsonl")).await
                    {
                        let _ = tokio::io::copy(&mut r, &mut f).await;
                    }
                    outputs_from = Some((shell, jobdir));
                }
            }
        }
        // Outputs last, once every file the daemon writes itself is in place: the archive comes
        // from the job, so a symlink in it must never be written through. Symlinks are dropped
        // after unpacking (tar itself refuses `..` and absolute member names).
        if let Some((shell, jobdir)) = outputs_from {
            let glob = job.spec.keep_outputs.clone().filter(|g| g != "**" && g != "*");
            if let Ok(data) = shell.pipe_out(&tar_dir_script(&format!("{jobdir}/out"), glob.as_deref())).await {
                let dest = shell_quote(&local.join("out").to_string_lossy());
                if let Err(e) = LocalShell
                    .pipe_in(&format!("tar -x --no-same-owner -C {dest}; s=$?; find {dest} -type l -exec rm -f {{}} +; exit $s"), data)
                    .await
                {
                    tracing::warn!("copying outputs of job {} off {}: {e:#}", job.id, inst.name);
                }
            }
        }
        if let Err(e) = self.provider.terminate(inst).await {
            tracing::warn!("terminating {}: {e:#} (the sweep will retry)", inst.name);
        } else {
            tracing::info!("{}: terminated {} after job {}", self.cfg.name, inst.name, job.id);
        }
        self.forget(inst).await;
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
                let main = statuses.iter().find(|(r, _)| *r == 0).map(|(_, p)| p);
                // The runner's diagnosis (out of memory, out of disk) beats a bare state.
                let note = worst.note.clone().or_else(|| main.and_then(|p| p.note.clone())).or_else(|| {
                    (inst.nodes.len() > 1 && *rank > 0 && worst.state != Some(JobState::Succeeded))
                        .then(|| format!("node {rank} {}", worst.state.map(|s| s.as_str()).unwrap_or("ended")))
                });
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
        if current {
            let (state, exit_code, started, ended, note) = result.result.clone();
            let saved = SavedOutcome { provider_id: inst.provider_id.clone(), state: state.map(|s| s.as_str().to_string()), exit_code, started, ended, note };
            if let Ok(text) = serde_json::to_vec(&saved) {
                let _ = tokio::fs::write(local.join("outcome.json"), text).await;
            }
        }
        result
    }

    /// How the job ended on `inst`, as saved by its teardown (None if it wasn't torn down).
    async fn saved_outcome(&self, job_id: i64, inst: &Instance) -> Option<Outcome> {
        let text = tokio::fs::read(self.job_dir_local(job_id).join("outcome.json")).await.ok()?;
        let o: SavedOutcome = serde_json::from_slice(&text).ok()?;
        (o.provider_id == inst.provider_id).then(|| (o.state.as_deref().and_then(JobState::parse), o.exit_code, o.started, o.ended, o.note))
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

/// Whether a provider refused to create an instance because it has none of that type free.
fn is_capacity_error(e: &anyhow::Error) -> bool {
    let msg = format!("{e:#}").to_ascii_lowercase();
    [
        "insufficient-capacity",
        "insufficient capacity",
        "no longer any instances available",
        "not have the resources to deploy",
        "out of capacity",
    ]
    .iter()
    .any(|m| msg.contains(m))
}

fn offer_fits(o: &Offer, job: &Job) -> bool {
    let r = &job.resources;
    let type_ok = r.gpu_type.as_deref().is_none_or(|t| contains_ci(&o.gpu_type, t) || contains_ci(&o.id, t));
    let cpus_ok = o.cpus == 0 || o.cpus >= r.cpus;
    let mem_ok = o.mem_gb == 0 || u64::from(o.mem_gb) * 1024 >= r.mem_mb;
    let disk_ok = o.disk_gb == 0 || u64::from(o.disk_gb) * 1024 >= r.disk_mb;
    type_ok && cpus_ok && mem_ok && disk_ok && o.gpus == r.gpus
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
        let offers = b.provider.offers(&Need::any_gpu()).await?;
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
        if r.gpus == 0 && !b.provider.cpu_pods() {
            return Quote::no(format!("{} runs GPU jobs only", b.cfg.name), true);
        }
        if r.gpus == 0 && r.nodes > 1 {
            return Quote::no("multi-node CPU jobs aren't supported on cloud backends", true);
        }
        if r.nodes > b.cfg.max_nodes {
            return Quote::no(format!("nodes={}; {} allows at most {}", r.nodes, b.cfg.name, b.cfg.max_nodes), true);
        }
        if job.local_build() && b.provider.runtime() != "docker" {
            return Quote::no(
                format!("the image is built on the job's own instance, and {} runs the image as the instance (no Docker to build with); Lambda can", b.cfg.name),
                true,
            );
        }
        if b.image_for(job).is_none() {
            return Quote::no("cloud jobs run in a container: set container.image or container.build", true);
        }
        if job.os.as_deref() == Some("mac") {
            return Quote::no("os=mac; cloud instances are linux", true);
        }
        let offers = match b.cached_offers(&Need::of(job, b.image_for(job).as_deref())).await {
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
        // Providers that bill CPU and memory separately (Modal) list those rates in hosts.toml.
        let extras = b.ccfg.cpu_rate * f64::from(r.cpus) + b.ccfg.mem_gb_rate * (r.mem_mb as f64 / 1024.0);
        let per_hour = (o.usd_per_hour + extras) * f64::from(r.nodes);
        // No time limit given: the job may run for as long as max_cost_usd pays for, boot included.
        let limit = r.no_time_limit.then(|| (max / per_hour * 3600.0 - boot).floor().max(0.0) as u64);
        if let Some(t) = limit
            && t < MIN_OPEN_LIMIT_S
        {
            return Quote::no(
                format!("max_cost_usd {max} buys only {} min at ${per_hour:.2}/h, boot included; raise it", t / 60),
                true,
            );
        }
        let hours = (limit.unwrap_or(r.time_s) as f64 + boot) / 3600.0;
        let cost = ((per_hour * hours * 100.0).ceil() / 100.0).min(if limit.is_some() { max } else { f64::INFINITY });
        if cost > max {
            let cheapest = fitting.iter().map(|o| o.usd_per_hour).fold(f64::INFINITY, f64::min) * f64::from(r.nodes) * hours;
            return Quote::no(
                format!("cheapest offer is ~${:.2} for the full time limit, over max_cost_usd {max}", cheapest.min(cost)),
                true,
            );
        }
        if job.id > 0 {
            b.quoted.lock().await.insert(job.id, (*o).clone());
        }
        let shape = Shape {
            cpus: o.cpus.max(r.cpus),
            mem_mb: (u64::from(o.mem_gb) * 1024).max(r.mem_mb),
            gpus: r.gpus,
            nodes: r.nodes,
            offer: serde_json::to_value(o).ok(),
            usd_per_hour: Some(per_hour),
            time_s: limit,
            ..Default::default()
        };
        let nodes = if r.nodes > 1 { format!("{} nodes x ", r.nodes) } else { String::new() };
        Quote::Ok {
            start: Utc::now() + chrono::Duration::seconds(boot as i64),
            source: "provider".into(),
            reason: {
                let what = if o.gpus == 0 {
                    format!("{} ({} vCPU, {} GB) at ${:.2}/h on {}", o.gpu_type, o.cpus, o.mem_gb, per_hour, b.cfg.name)
                } else {
                    format!("{nodes}{} x{} at ${:.2}/h on {}", o.gpu_type, o.gpus, per_hour, b.cfg.name)
                };
                match limit {
                    Some(t) => format!("{what}; no time limit given, so up to {} (what max_cost_usd buys)", hm(t)),
                    None => what,
                }
            },
            shape,
            cost: Some(cost),
        }
    }

    async fn stage(&self, job: &Job, snapshot: &Path, files: &StageFiles) -> Result<()> {
        let b = &self.0;
        let inst = b.instance(job, true).await?;
        // A guest's job gets an instance of its own, which reaches neither this host nor the
        // tailnet: that is its sandbox. Runtimes that run the image natively (RunPod, Modal)
        // couldn't nest the runner's container sandbox anyway.
        let mut job = job.clone();
        job.spec.sandbox = false;
        let job = &job;
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
                f.env.extend(b.provider.node_env());
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
            let mut inst = b.instance(job, false).await?;
            // The time limit runs from launch, not from creation: staging time doesn't count.
            inst.deadline = Utc::now() + chrono::Duration::seconds((job.resources.time_s + COLLECT_ALLOWANCE_S) as i64);
            b.instances.lock().await.insert(job.id, inst.clone());
            for node in &inst.nodes {
                let shell = b.provider.shell(node)?;
                b.provider.extend_deadline(shell.as_ref(), job.resources.time_s + COLLECT_ALLOWANCE_S).await?;
            }
            b.quoted.lock().await.remove(&job.id);
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
            // Torn down before a daemon restart, but the outcome wasn't reported yet.
            if let Some((state, exit_code, started, ended, note)) = b.saved_outcome(job.id, &inst).await {
                b.forget(&inst).await;
                out.push(PollResult { id: job.id, state, exit_code, started, ended, note, ..Default::default() });
                continue;
            }
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
        let inst = match b.instance(job, false).await {
            Ok(i) => i,
            // Ended before it had an instance (e.g. failed in staging): there is no log.
            Err(_) if job.state.is_terminal() => return Ok((Vec::new(), 0)),
            Err(e) => return Err(e),
        };
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
        // Forget it now, so a requeued job gets a new instance instead of this dying one.
        b.forget(&inst).await;
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

    /// Moves the runner's limit on every node, the instance's own deadline (RunPod's idle timer)
    /// and the deadline the sweep tears it down by. Modal's sandbox timeout is fixed at creation.
    async fn set_time_limit(&self, job: &Job, time_s: u64) -> Result<()> {
        let b = &self.0;
        if b.cfg.kind == BackendKind::Modal {
            bail!("Modal sandboxes have a timeout fixed when they are created");
        }
        let mut inst = b.instance(job, false).await?;
        for rank in 0..inst.nodes.len() {
            b.remote(&inst, rank)?.set_time_limit(job.id, time_s).await?;
        }
        let started = job.started_at.unwrap_or_else(Utc::now);
        inst.deadline = started + chrono::Duration::seconds((time_s + COLLECT_ALLOWANCE_S) as i64);
        let left = (inst.deadline - Utc::now()).num_seconds().max(0) as u64;
        for node in &inst.nodes {
            let shell = b.provider.shell(node)?;
            b.provider.extend_deadline(shell.as_ref(), left).await?;
        }
        b.instances.lock().await.insert(job.id, inst.clone());
        let mut handle = job.handle.clone().unwrap_or_else(|| json!({}));
        handle["instance"] = serde_json::to_value(&inst)?;
        b.store.update_job_if(job.id, &[JobState::Running], JobUpdate { handle: Some(Some(handle)), ..Default::default() })?;
        Ok(())
    }

    async fn availability(&self) -> Result<Value> {
        let b = &self.0;
        let offers = b.cached_offers(&Need::any_gpu()).await?;
        let available: Vec<Value> = offers
            .iter()
            .filter(|o| o.available)
            .take(12)
            .map(|o| json!({"gpu_type": o.gpu_type, "usd_per_hour": (o.usd_per_hour * 100.0).round() / 100.0}))
            .collect();
        // Instances still billing, from the cost records (the in-memory map is empty after a
        // restart until each job is polled).
        let running = b.store.costs_since(chrono::Utc::now())?.iter().filter(|r| r.4.is_none() && r.1 == b.cfg.name).count();
        Ok(json!({"in_stock": available, "my_instances": running, "boot_estimate_s": b.boot().round()}))
    }

    async fn metrics(&self, job: &Job, history: u32) -> Result<Value> {
        let b = &self.0;
        let local = b.job_dir_local(job.id).join("metrics.jsonl");
        if b.collected_locally(job.id) {
            let text = tokio::fs::read_to_string(&local).await.unwrap_or_default();
            let lines: Vec<&str> = text.lines().collect();
            let tail = lines[lines.len().saturating_sub(history as usize)..].join("\n");
            let latest = lines.last().copied().unwrap_or("null");
            return Ok(super::parse_metrics(&format!("{latest}\n---\n{tail}")));
        }
        let inst = b.instance(job, false).await?;
        b.remote(&inst, 0)?.metrics(job.id, history).await
    }

    async fn list_outputs(&self, job: &Job, glob: Option<&str>) -> Result<Vec<(String, u64)>> {
        let b = &self.0;
        let local_out = b.job_dir_local(job.id).join("out");
        if local_out.exists() && (job.state.is_terminal() || b.collected_locally(job.id)) {
            let out = LocalShell.run(&super::list_dir_script(&local_out.to_string_lossy(), glob)).await?.text();
            return Ok(super::parse_listing(&out));
        }
        let inst = b.instance(job, false).await?;
        let node = inst.nodes.first().context("instance has no nodes")?;
        let dir = format!("{}/out", Remote::jobdir(b.provider.root(), job.id));
        let out = b.provider.shell(node)?.run(&super::list_dir_script(&dir, glob)).await?.text();
        Ok(super::parse_listing(&out))
    }

    /// The copy on the coordinator (the instance is gone by the time a job has ended).
    async fn delete_outputs(&self, job: &Job) -> Result<()> {
        let _ = tokio::fs::remove_dir_all(self.0.job_dir_local(job.id).join("out")).await;
        Ok(())
    }

    /// Nothing to do: the instance, and its inputs, are torn down with the job.
    async fn delete_inputs(&self, _job: &Job) -> Result<()> {
        Ok(())
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
        let listed = b.provider.list().await?;
        // Snapshot after listing, so an instance created meanwhile is known.
        let known: HashMap<i64, Instance> = b.instances.lock().await.clone();
        let mut owned: HashSet<String> = known.values().map(|i| i.provider_id.clone()).collect();
        for h in live.values().flatten() {
            if let Some(id) = h["instance"]["provider_id"].as_str() {
                owned.insert(id.to_string());
            }
        }
        let now = Utc::now();
        let deadline_of = |job_id: i64| {
            known.get(&job_id).map(|i| i.deadline).or_else(|| {
                live.get(&job_id)?.as_ref().and_then(|h| serde_json::from_value::<Instance>(h["instance"].clone()).ok()).map(|i| i.deadline)
            })
        };
        for (id, name, cluster) in listed {
            let Some(job_id) = name.strip_prefix("compute-").and_then(|r| r.split('-').next()).and_then(|x| x.parse::<i64>().ok()) else {
                continue;
            };
            let keep = live.contains_key(&job_id) && owned.contains(&id) && deadline_of(job_id).is_none_or(|d| d > now);
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
                offer: Offer { id: String::new(), gpu_type: String::new(), gpus: 0, cpus: 0, mem_gb: 0, usd_per_hour: 0.0, available: false, region: None, disk_gb: 0 },
            };
            if let Err(e) = b.provider.terminate(&fake).await {
                tracing::warn!("terminating {name}: {e:#}");
            }
        }
        Ok(())
    }

    fn pricing(&self) -> Option<Value> {
        let b = &self.0;
        let o = b.offers.try_lock().ok()?.get(&Need::any_gpu()).map(|(_, o)| o.clone())?;
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

/// Where cloud nodes' host keys are pinned: one known_hosts file per address, set at startup.
static KNOWN_HOSTS_DIR: std::sync::OnceLock<std::path::PathBuf> = std::sync::OnceLock::new();

fn known_hosts_file(addr: &SshAddr) -> std::path::PathBuf {
    let dir = KNOWN_HOSTS_DIR.get_or_init(|| std::env::temp_dir().join("compute-known-hosts"));
    let safe: String = format!("{}_{}", addr.host, addr.port)
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() || c == '.' || c == '-' { c } else { '_' })
        .collect();
    dir.join(safe)
}

fn ssh_shell(addr: &SshAddr, name: &str) -> Arc<dyn Shell> {
    Arc::new(Ssh {
        host: addr.host.clone(),
        user: Some(addr.user.clone()),
        port: Some(addr.port),
        tunnel: false,
        alias: name.to_string(),
        known_hosts: Some(known_hosts_file(addr)),
    })
}

/// First contact with a new instance: forgets whatever an earlier instance at the same
/// address presented, so this one's host key is pinned now, before staging sends secrets.
async fn wait_ssh(addr: &SshAddr, name: &str, deadline: Instant) -> Result<()> {
    let file = known_hosts_file(addr);
    if let Some(dir) = file.parent() {
        tokio::fs::create_dir_all(dir).await.with_context(|| format!("creating {}", dir.display()))?;
    }
    let _ = tokio::fs::remove_file(&file).await;
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

/// Container entrypoint (POSIX sh, run as root): keep the image's environment for ssh
/// sessions, install sshd and the runner's tools if missing, authorize the keys, start sshd,
/// then idle until the deadline in /run/compute-deadline (moved at launch). Exits, failing the
/// pod at once, when the image can't be made reachable, instead of idling unreachable.
fn boot_script(timeout_s: u64) -> String {
    format!(
        r#"exec >/tmp/compute-boot.log 2>&1
echo "$(( $(date +%s) + {timeout_s} ))" > /run/compute-deadline
if [ "$(id -u)" != 0 ]; then echo "compute: the image runs as uid $(id -u); RunPod jobs need an image that runs as root"; cat /tmp/compute-boot.log >&2; exit 1; fi
# ssh sessions don't inherit PID 1's environment, and login shells reset PATH: replay it.
export -p | grep -vE '^(export |declare -x )?(HOME|PWD|OLDPWD|SHLVL|HOSTNAME|_|PUBLIC_KEY)=' > /etc/profile.d/00-compute-env.sh
need=""
[ -x /usr/sbin/sshd ] || need="$need sshd"
command -v bash >/dev/null 2>&1 || need="$need bash"
command -v tar >/dev/null 2>&1 || need="$need tar"
command -v ps >/dev/null 2>&1 || need="$need ps"
if [ -n "$need" ]; then
  if command -v apt-get >/dev/null 2>&1; then
    apt-get update -qq || true
    DEBIAN_FRONTEND=noninteractive apt-get install -y -qq openssh-server bash tar procps
  elif command -v apk >/dev/null 2>&1; then
    apk add --no-cache openssh bash tar procps
  elif command -v dnf >/dev/null 2>&1; then
    dnf install -y openssh-server bash tar procps-ng
  fi
fi
if ! [ -x /usr/sbin/sshd ] || ! command -v bash >/dev/null 2>&1; then
  echo "compute: couldn't install sshd and bash in this image (missing:$need)"; cat /tmp/compute-boot.log >&2; exit 1
fi
mkdir -p /root/.ssh /run/sshd /var/run/sshd
chmod 700 /root/.ssh
printf '%s\n' "$PUBLIC_KEY" > /root/.ssh/authorized_keys
chmod 600 /root/.ssh/authorized_keys
ssh-keygen -A
/usr/sbin/sshd -o PermitRootLogin=prohibit-password -o PasswordAuthentication=no || {{ cat /tmp/compute-boot.log >&2; exit 1; }}
echo "compute: ready"
while [ "$(date +%s)" -lt "$(cat /run/compute-deadline 2>/dev/null || echo 0)" ]; do sleep 30; done"#
    )
}

// ---------------------------------------------------------------------------- RunPod

/// RunPod GPU and CPU Pods (single node) and Instant Clusters (multi-node) through REST v2.
pub struct RunPod {
    key_file: std::path::PathBuf,
    tier: String,
    disk_gb: u32,
    regions: Vec<String>,
}

const RUNPOD: &str = "https://api.runpod.io/v2";
/// A pod RUNNING this long with no public ssh address won't get one (e.g. no public IP).
const RUNPOD_NO_SSH_AFTER: Duration = Duration::from_secs(180);

impl RunPod {
    fn new(key_file: &Path, c: &CloudConfig) -> RunPod {
        RunPod {
            key_file: key_file.to_path_buf(),
            tier: c.tier.clone().unwrap_or_else(|| "SECURE".into()).to_ascii_uppercase(),
            disk_gb: c.disk_gb,
            regions: c.regions.clone(),
        }
    }
    fn auth(&self) -> Result<String> {
        Ok(format!("Authorization: Bearer {}", read_key(&self.key_file)?))
    }
    /// Retries rate limits (180 requests a minute per account), and for reads also server and
    /// network errors. A create isn't retried on those: it may have gone through (the sweep
    /// removes a duplicate).
    async fn call(&self, method: &str, path: &str, body: Option<&Value>) -> Result<Value> {
        let url = format!("{RUNPOD}{path}");
        let auth = self.auth()?;
        let mut attempt = 0;
        loop {
            match http(method, &url, &auth, body).await {
                Err(e) if attempt < 4 => {
                    let s = format!("{e}");
                    let transient = s.contains("HTTP 429") || (method != "POST" && (s.contains("HTTP 50") || !s.contains("HTTP ")));
                    if !transient {
                        return Err(e);
                    }
                    attempt += 1;
                    tokio::time::sleep(Duration::from_secs(5 << attempt)).await;
                }
                r => return r,
            }
        }
    }
    fn ssh_of(pod: &Value) -> Option<SshAddr> {
        let d = &pod["ssh"]["direct"];
        Some(SshAddr {
            host: d["host"].as_str()?.to_string(),
            port: d["port"].as_u64()? as u16,
            user: d["username"].as_str().unwrap_or("root").to_string(),
        })
    }
    /// Stock in the configured regions (or overall when none are set).
    fn in_stock(&self, item: &Value) -> bool {
        if self.regions.is_empty() {
            return item["availability"].as_str().is_some_and(|a| a != "NONE");
        }
        item["dataCenters"]
            .as_array()
            .into_iter()
            .flatten()
            .any(|d| d["id"].as_str().is_some_and(|id| self.regions.iter().any(|r| r == id)) && d["availability"].as_str().is_some_and(|a| a != "NONE"))
    }
    /// CPU pods: one offer per flavor and power-of-two vCPU count (RunPod's allowed sizes).
    async fn cpu_offers(&self) -> Result<Vec<Offer>> {
        let v = self.call("GET", "/catalog/cpus?include=AVAILABILITY&product=POD", None).await?;
        let mut out = Vec::new();
        for c in v["cpus"].as_array().into_iter().flatten() {
            let (Some(id), Some(per_vcpu)) = (c["id"].as_str(), c["price"]["securePerVcpu"].as_f64()) else { continue };
            let ram_per = c["ramGbPerVcpu"].as_u64().unwrap_or(0) as u32;
            let (min, max) = (c["vcpu"]["min"].as_u64().unwrap_or(2).max(2) as u32, c["vcpu"]["max"].as_u64().unwrap_or(0) as u32);
            let available = self.in_stock(c);
            let name = c["name"].as_str().unwrap_or(id);
            let mut n = 2u32;
            while n <= max {
                if n >= min {
                    out.push(Offer {
                        id: id.to_string(),
                        gpu_type: format!("{name} CPU ({id})"),
                        gpus: 0,
                        cpus: n,
                        mem_gb: n * ram_per,
                        usd_per_hour: per_vcpu * f64::from(n),
                        available,
                        region: None,
                        disk_gb: 0,
                    });
                }
                n *= 2;
            }
        }
        Ok(out)
    }
}

#[async_trait]
impl Provider for RunPod {
    async fn offers(&self, need: &Need) -> Result<Vec<Offer>> {
        if need.gpus == 0 {
            return self.cpu_offers().await;
        }
        let gpus = need.gpus;
        let cluster = need.nodes > 1;
        let product = if cluster { "CLUSTER" } else { "POD" };
        // Clusters are secure-cloud only.
        let secure = cluster || self.tier == "SECURE";
        let cloud = if secure { "SECURE" } else { "COMMUNITY" };
        let mut q = format!("/catalog/gpus?include=AVAILABILITY&product={product}&count={gpus}");
        if !cluster {
            q.push_str(&format!("&cloud={cloud}"));
        }
        // Ask for machines with enough CPU and memory per GPU, and a new enough driver.
        if need.cpus > 0 {
            q.push_str(&format!("&minVcpuCountPerGpu={}", need.cpus.div_ceil(gpus)));
        }
        if need.mem_gb > 0 {
            q.push_str(&format!("&minRamPerGpu={}", need.mem_gb.div_ceil(gpus)));
        }
        if let Some(c) = &need.min_cuda {
            q.push_str(&format!("&minCudaVersion={c}"));
        }
        let v = self.call("GET", &q, None).await?;
        let key = if secure { "secure" } else { "community" };
        let mut out = Vec::new();
        for g in v["gpus"].as_array().into_iter().flatten() {
            let price = g["price"][key].as_f64().unwrap_or(0.0);
            let max = g["maxCount"][key].as_u64().unwrap_or(0) as u32;
            let offered = g[key].as_bool().unwrap_or(false);
            // AMD (ROCm) GPUs can't run CUDA images.
            let amd = [g["id"].as_str(), g["name"].as_str()].into_iter().flatten().any(|t| contains_ci(t, "amd") || contains_ci(t, "instinct"));
            if price <= 0.0 || !offered || max < gpus || amd {
                continue;
            }
            // The catalog was filtered to machines with this much per GPU, and the create asks
            // for it: the offer has at least what the job needs.
            let cpus = (g["minVcpuCount"].as_u64().unwrap_or(0) as u32).max(need.cpus);
            let mem_gb = (g["minRam"].as_u64().unwrap_or(0) as u32).max(need.mem_gb);
            out.push(Offer {
                id: g["id"].as_str().unwrap_or_default().to_string(),
                gpu_type: g["name"].as_str().or(g["id"].as_str()).unwrap_or_default().to_string(),
                gpus,
                cpus,
                mem_gb,
                usd_per_hour: price * f64::from(gpus),
                available: self.in_stock(g),
                region: None,
                disk_gb: 0,
            });
        }
        Ok(out)
    }

    fn cpu_pods(&self) -> bool {
        true
    }

    fn node_env(&self) -> Vec<(String, String)> {
        // RunPod's docs: NCCL must use the cluster interface.
        vec![("NCCL_SOCKET_IFNAME".into(), "ens1".into())]
    }

    async fn extend_deadline(&self, shell: &dyn Shell, secs: u64) -> Result<()> {
        shell.run(&format!("echo \"$(( $(date +%s) + {secs} ))\" > /run/compute-deadline")).await.map(|_| ())
    }

    async fn create(&self, req: &CreateReq<'_>) -> Result<Instance> {
        let keys: Vec<String> = std::iter::once(req.pubkey.clone()).chain(req.extra_keys.iter().map(|k| k.trim().to_string())).collect();
        let mut body = json!({
            "name": req.name,
            "image": req.image,
            // Room for the job's disk_gb on top of the image and system.
            "disk": self.disk_gb.max(req.job.resources.disk_mb.div_ceil(1024) as u32 + 20),
            "ports": ["22/tcp"],
            // The coordinator's key and the hosts.toml authorized_keys (kept across restarts).
            "env": {"PUBLIC_KEY": keys.join("\n")},
            "entrypoint": ["/bin/sh", "-c", boot_script(req.timeout_s)],
            "cmd": [],
            "startSsh": true,
        });
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
        if req.offer.gpus == 0 {
            // CPU pods: secure cloud only; memory follows from the flavor.
            body["cloud"] = "SECURE".into();
            body["cpu"] = json!({"id": req.offer.id, "vcpuCount": req.offer.cpus});
        } else {
            body["cloud"] = self.tier.clone().into();
            let r = &req.job.resources;
            let mut gpu = json!({"id": req.offer.id, "count": req.offer.gpus});
            if r.cpus > 0 {
                gpu["minVcpuCountPerGpu"] = r.cpus.div_ceil(req.offer.gpus).into();
            }
            if r.mem_mb > 0 {
                gpu["minRamPerGpu"] = (r.mem_mb.div_ceil(1024) as u32).div_ceil(req.offer.gpus).into();
            }
            if let Some(c) = min_cuda_of(&req.image) {
                gpu["minCudaVersion"] = c.into();
            }
            body["gpu"] = gpu;
        }
        let r = self.call("POST", "/pods", Some(&body)).await?;
        let id = r["id"].as_str().or(r["podId"].as_str()).context("RunPod returned no pod id")?.to_string();
        let mut offer = req.offer.clone();
        // The rate the pod is actually billed at.
        if let Some(cost) = r["cost"].as_f64().filter(|c| *c > 0.0) {
            offer.usd_per_hour = cost;
        }
        Ok(Instance { provider_id: id, name: req.name.clone(), job_id: req.job.id, cluster: false, node_count: 1, nodes: Vec::new(), created_at: now, deadline, offer })
    }

    async fn wait_ready(&self, inst: &mut Instance, deadline: Instant) -> Result<()> {
        let mut running_since: Option<Instant> = None;
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
                bail!(
                    "a pod stopped while booting ({}). The image must run as root and have (or be able to apt/apk/dnf install) sshd and bash, and its CUDA must suit the host; RunPod's pod log has the boot output",
                    pods.iter().filter_map(|p| p["status"].as_str()).collect::<Vec<_>>().join(", ")
                );
            }
            let all_running = pods.len() >= inst.node_count.max(1) as usize && pods.iter().all(|p| p["status"] == "RUNNING");
            if all_running && running_since.is_none() {
                running_since = Some(Instant::now());
            }
            // Clusters also need each pod's rank and overlay address before they're usable.
            let placed = |p: &Value| !inst.cluster || (p["cluster"]["rank"].is_u64() && p["cluster"]["ip"].is_string());
            let ready = all_running && pods.iter().all(|p| Self::ssh_of(p).is_some() && placed(p));
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
            if let Some(t) = running_since
                && t.elapsed() > RUNPOD_NO_SSH_AFTER
                && pods.iter().any(|p| Self::ssh_of(p).is_none())
            {
                bail!("the pod runs but has no public ssh address (community-cloud machines may lack a public IP; use tier = \"SECURE\")");
            }
            tokio::time::sleep(Duration::from_secs(10)).await;
        }
    }

    async fn alive(&self, inst: &Instance) -> Result<bool> {
        let dead = |s: Option<&str>| matches!(s, Some("TERMINATED" | "EXITED" | "ERROR"));
        if inst.cluster {
            // A cluster has no status of its own: go by its primary pod and pod counts.
            return match self.call("GET", &format!("/clusters/{}", inst.provider_id), None).await {
                Ok(v) => {
                    let running = v["pods"]["byStatus"]["RUNNING"].as_u64();
                    Ok(!dead(v["primary"]["status"].as_str()) && running != Some(0))
                }
                Err(e) if format!("{e}").contains("HTTP 404") => Ok(false),
                Err(e) => Err(e),
            };
        }
        match self.call("GET", &format!("/pods/{}", inst.provider_id), None).await {
            Ok(v) => Ok(!dead(v["status"].as_str())),
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
        // Both lists are paged; follow the cursor.
        for (path, key, cluster) in [("/pods", "pods", false), ("/clusters", "clusters", true)] {
            let mut cursor: Option<String> = None;
            for _ in 0..50 {
                let url = match &cursor {
                    Some(c) => format!("{path}?cursor={}", urlencode(c)),
                    None => path.to_string(),
                };
                let v = self.call("GET", &url, None).await?;
                for p in v[key].as_array().into_iter().flatten() {
                    if !cluster && p["cluster"].is_object() {
                        continue; // torn down with its cluster
                    }
                    if let (Some(id), Some(name)) = (p["id"].as_str(), p["name"].as_str())
                        && name.starts_with("compute-")
                    {
                        out.push((id.to_string(), name.to_string(), cluster));
                    }
                }
                match v["pagination"]["nextCursor"].as_str() {
                    Some(c) if v["pagination"]["hasNextPage"].as_bool() == Some(true) => cursor = Some(c.to_string()),
                    _ => break,
                }
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

/// Percent-encodes a query value.
fn urlencode(s: &str) -> String {
    s.bytes()
        .map(|b| if b.is_ascii_alphanumeric() || b"-_.~".contains(&b) { (b as char).to_string() } else { format!("%{b:02X}") })
        .collect()
}

// ---------------------------------------------------------------------------- Lambda

/// Lambda Cloud on-demand instances. Jobs run under Docker with `--gpus all`.
pub struct Lambda {
    key_file: std::path::PathBuf,
    regions: Vec<String>,
    key_name: Mutex<Option<String>>,
}

const LAMBDA: &str = "https://cloud.lambda.com/api/v1";

impl Lambda {
    fn new(key_file: &Path, c: &CloudConfig) -> Lambda {
        Lambda { key_file: key_file.to_path_buf(), regions: c.regions.clone(), key_name: Mutex::new(None) }
    }
    /// Retries rate-limited calls: Lambda allows about one request a second, and one launch
    /// every 12 seconds.
    async fn call(&self, method: &str, path: &str, body: Option<&Value>) -> Result<Value> {
        let auth = format!("Authorization: Bearer {}", read_key(&self.key_file)?);
        let mut attempt = 0;
        loop {
            match http(method, &format!("{LAMBDA}{path}"), &auth, body).await {
                Err(e) if attempt < 5 && format!("{e}").contains("HTTP 429") => {
                    attempt += 1;
                    let wait = if path.ends_with("/launch") { 13 } else { 2 * attempt };
                    tokio::time::sleep(Duration::from_secs(wait)).await;
                }
                r => return r,
            }
        }
    }
    /// The name of a registered ssh key matching the coordinator's, registering it if needed.
    /// The lock is held throughout, so jobs launched together register the key once.
    async fn ssh_key_name(&self, pubkey: &str) -> Result<String> {
        let mut cached = self.key_name.lock().await;
        if let Some(n) = cached.clone() {
            return Ok(n);
        }
        let body_of = |k: &str| k.split_whitespace().take(2).collect::<Vec<_>>().join(" ");
        let mine = body_of(pubkey);
        let keys = self.call("GET", "/ssh-keys", None).await?;
        let found = keys["data"].as_array().into_iter().flatten().find(|k| k["public_key"].as_str().map(body_of).as_deref() == Some(mine.as_str()));
        let name = match found.and_then(|k| k["name"].as_str()) {
            Some(n) => n.to_string(),
            None => {
                // The name comes from the key, so "already taken" means it is registered.
                let name = format!("compute-daemon-{}", &hex::encode(<sha2::Sha256 as sha2::Digest>::digest(mine.as_bytes()))[..8]);
                match self.call("POST", "/ssh-keys", Some(&json!({"name": name, "public_key": pubkey}))).await {
                    Err(e) if !format!("{e}").contains("must be unique") => return Err(e),
                    _ => name,
                }
            }
        };
        *cached = Some(name.clone());
        Ok(name)
    }
}

#[async_trait]
impl Provider for Lambda {
    async fn offers(&self, need: &Need) -> Result<Vec<Offer>> {
        let gpus = need.gpus;
        let v = self.call("GET", "/instance-types", None).await?;
        let mut out = Vec::new();
        for (name, t) in v["data"].as_object().into_iter().flatten() {
            let it = &t["instance_type"];
            let specs = &it["specs"];
            let n = specs["gpus"].as_u64().unwrap_or(0) as u32;
            // GH200 types are arm64, and job images are built for amd64.
            if n != gpus.max(1) || it["architecture"].as_str().is_some_and(|a| a != "x86_64") {
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
                disk_gb: specs["storage_gib"].as_u64().unwrap_or(0) as u32,
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
                Some(&json!({"region_name": region, "instance_type_name": req.offer.id, "ssh_key_names": [key], "name": req.name})),
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
                Some("terminated" | "terminating" | "preempted" | "unhealthy") => bail!("instance is {}", d["status"]),
                _ => {}
            }
            tokio::time::sleep(Duration::from_secs(10)).await;
        }
    }

    async fn alive(&self, inst: &Instance) -> Result<bool> {
        match self.call("GET", &format!("/instances/{}", inst.provider_id), None).await {
            Ok(v) => Ok(!matches!(v["data"]["status"].as_str(), Some("terminated" | "terminating" | "preempted"))),
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
            .filter(|i| !matches!(i["status"].as_str(), Some("terminated" | "terminating" | "preempted")))
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

    async fn prepare(&self, shell: &dyn Shell) -> Result<()> {
        // New ssh sessions pick up the docker group. Hold snap and apt updates: they reload
        // systemd mid-job (the runner also guards GPU access against that) and can change
        // the driver under a running job.
        shell
            .run(
                "sudo usermod -aG docker \"$(id -un)\" >/dev/null 2>&1 || true\n\
                 sudo snap refresh --hold >/dev/null 2>&1 || true\n\
                 sudo systemctl stop unattended-upgrades apt-daily.timer apt-daily-upgrade.timer >/dev/null 2>&1 || true",
            )
            .await?;
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
    async fn offers(&self, need: &Need) -> Result<Vec<Offer>> {
        let gpus = need.gpus;
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
                disk_gb: 0,
            })
            .collect())
    }

    async fn create(&self, req: &CreateReq<'_>) -> Result<Instance> {
        let (client, app) = self.client().await?;
        let image = client.image_from_registry_in(&app, &req.image, None, self.cfg.environment.as_deref()).await?;
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
