//! `compute`: the command-line client for the compute scheduler, and a live monitor for Rex.

mod client;
mod fmt;
mod pack;
mod rex;
mod update;
mod watch;

use anyhow::{Context, Result, bail};
use clap::{Parser, Subcommand};
use client::{Client, err_text, q};
use serde_json::{Value, json};
use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::Command;

#[derive(Parser)]
#[command(name = "compute", version = update::VERSION, about = "Submit and watch jobs on the compute scheduler", propagate_version = true)]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Set this machine up for a compute network: sign in, write the config, install the skill
    Setup {
        /// The network's address, e.g. https://compute.example.org/api
        url: String,
        /// SSH public key to sign in with (default $COMPUTE_KEY, else ~/.ssh/id_ed25519.pub)
        #[arg(long)]
        key: Option<PathBuf>,
    },
    /// Install the network's current build of this CLI and refresh the skill
    Update {
        /// Reinstall even when the version already matches
        #[arg(long)]
        force: bool,
    },
    /// Sign a login challenge with your SSH key (done automatically when needed)
    Login,
    /// Who you are signed in as, your scope, and the backends and limits of your key
    Whoami,
    /// Live monitor: all jobs, or one job with status, resource usage and its log
    Watch {
        /// Job to show; omit for the list of jobs
        id: Option<i64>,
    },
    /// Open the monitor in a Rex split beside this pane
    Open { id: Option<i64> },
    /// List jobs
    Jobs {
        /// Comma-separated states, e.g. running,pending
        #[arg(long)]
        state: Option<String>,
        /// Search titles and descriptions
        #[arg(short, long)]
        query: Option<String>,
        /// Tasks of one array
        #[arg(long)]
        array: Option<i64>,
        #[arg(long, default_value_t = 30)]
        limit: u32,
        #[arg(long)]
        json: bool,
    },
    /// Show one job
    Job {
        id: i64,
        #[arg(long)]
        json: bool,
        /// Include the command, env and handle
        #[arg(short, long)]
        verbose: bool,
    },
    /// Print a job's log
    Logs {
        id: i64,
        /// Keep printing new output until the job ends
        #[arg(short, long)]
        follow: bool,
        /// Only the last N bytes
        #[arg(long)]
        tail: Option<u64>,
    },
    /// Resource usage of a job (CPU, memory, GPUs, power, disk)
    Metrics {
        id: i64,
        #[arg(long)]
        json: bool,
    },
    /// Block until a job is running or done
    Wait {
        id: i64,
        #[arg(long, default_value = "done")]
        until: String,
        /// Seconds; 0 waits forever
        #[arg(long, default_value_t = 0)]
        timeout: u64,
    },
    /// Cancel a job (or a whole array with --array)
    Cancel {
        id: i64,
        #[arg(long)]
        array: bool,
    },
    /// Download a job's outputs into a directory (--list shows the files and sizes instead)
    Outputs {
        id: i64,
        #[arg(long)]
        glob: Option<String>,
        /// Destination (default ./outputs/<id>)
        #[arg(short, long)]
        out: Option<PathBuf>,
        /// List the output files with their sizes; download nothing
        #[arg(long)]
        list: bool,
    },
    /// Delete a finished job's outputs on its backend to free disk (its log stays)
    DeleteOutputs { id: i64 },
    /// Upload the repo's files as a snapshot and print its id
    Snapshot {
        /// Repo directory (default: the current one)
        #[arg(long)]
        dir: Option<PathBuf>,
        /// Snapshot a committed revision (jj or git) instead of the working copy
        #[arg(long)]
        rev: Option<String>,
    },
    /// Submit a job from a JSON file ("-" for stdin). A missing or "auto" snapshot uploads the repo.
    Submit {
        file: String,
        /// Price check / placement preview only
        #[arg(long)]
        dry_run: bool,
        /// Snapshot a committed revision (jj or git) instead of the working copy
        #[arg(long)]
        rev: Option<String>,
        /// Don't open the monitor in a Rex split
        #[arg(long)]
        no_watch: bool,
        #[arg(long)]
        json: bool,
    },
    /// Backends: hardware, availability, restrictions
    Backends {
        name: Option<String>,
        #[arg(long)]
        json: bool,
    },
    /// Datasets and their copies
    Datasets { name: Option<String> },
    /// Upload a local directory as a new version of a dataset
    DatasetUpload {
        name: String,
        dir: PathBuf,
        #[arg(long)]
        description: Option<String>,
    },
    /// Secret names and their keys (values are never shown)
    Secrets,
    /// File a report about compute itself; the body is a file, or "-" for stdin
    Report {
        #[arg(long)]
        title: String,
        /// bug, docs or feature
        #[arg(long, default_value = "bug")]
        kind: String,
        /// One of your jobs it's about
        #[arg(long)]
        job: Option<i64>,
        body: String,
    },
    /// Reports: the open ones (--all for closed too), or one by id (--wait blocks until it's closed)
    Reports {
        id: Option<i64>,
        #[arg(long)]
        all: bool,
        #[arg(long)]
        wait: bool,
    },
    /// Cloud spend this month
    Budget,
    /// Call any API path: compute api GET /v1/jobs, compute api POST /v1/jobs job.json
    Api {
        method: String,
        path: String,
        /// JSON body: a file, "-" for stdin, or inline JSON
        body: Option<String>,
    },
}

fn main() {
    if let Err(e) = real_main() {
        eprintln!("compute: {}", err_text(&e));
        std::process::exit(1);
    }
}

fn real_main() -> Result<()> {
    let cli = Cli::parse();
    update::cleanup_old();
    if let Cmd::Setup { url, key } = &cli.cmd {
        return update::setup(url, key.clone());
    }
    let c = Client::from_env()?;
    if !matches!(cli.cmd, Cmd::Update { .. }) {
        update::auto(&c);
    }
    match cli.cmd {
        Cmd::Setup { .. } => unreachable!(),
        Cmd::Update { force } => {
            let v = update::update(&c, force)?;
            println!("compute {v}");
        }
        Cmd::Whoami => print_json(&c.get("/v1/whoami")?)?,
        Cmd::Login => {
            c.login()?;
            let me = c.get("/v1/whoami")?;
            println!("signed in to {} as {}", c.url, me["identity"].as_str().unwrap_or("?"));
        }
        Cmd::Watch { id } => watch::run(c, id)?,
        Cmd::Open { id } => {
            let label = id.map(|i| format!("compute #{i}")).unwrap_or_else(|| "compute".into());
            let block = rex::open_monitor(id, &label)?;
            println!("opened the monitor in {block}");
        }
        Cmd::Jobs { state, query, array, limit, json } => {
            let mut path = format!("/v1/jobs?limit={limit}");
            if let Some(s) = state {
                path.push_str(&format!("&state={}", q(&s)));
            }
            if let Some(s) = query {
                path.push_str(&format!("&q={}", q(&s)));
            }
            if let Some(a) = array {
                path.push_str(&format!("&array={a}"));
            }
            let v = c.get(&path)?;
            if json {
                return print_json(&v);
            }
            let jobs = v["jobs"].as_array().cloned().unwrap_or_default();
            println!("{:>5}  {:<17} {:<8} {:<11} {:<9} {:<14} TITLE", "ID", "STATE", "BACKEND", "START", "ELAPSED", "LEFT");
            for j in &jobs {
                let t = fmt::times(j);
                println!(
                    "{:>5}  {:<17} {:<8} {:<11} {:<9} {:<14} {}",
                    j["id"].as_i64().unwrap_or(0),
                    j["state"].as_str().unwrap_or("").replace('_', " "),
                    j["backend"].as_str().unwrap_or("—"),
                    t.start,
                    t.elapsed,
                    t.left,
                    j["title"].as_str().unwrap_or("")
                );
            }
        }
        Cmd::Job { id, json, verbose } => {
            let v = c.get(&format!("/v1/jobs/{id}{}", if verbose { "?verbose=1" } else { "" }))?;
            if json {
                return print_json(&v);
            }
            print_job(&v);
            if v["state"] == "running" && let Ok(m) = c.get(&format!("/v1/jobs/{id}/metrics")) {
                print_usage(&v, &m);
            }
        }
        Cmd::Logs { id, follow, tail } => logs(&c, id, follow, tail)?,
        Cmd::Metrics { id, json } => {
            let m = c.get(&format!("/v1/jobs/{id}/metrics?history=60"))?;
            if json {
                return print_json(&m);
            }
            let j = c.get(&format!("/v1/jobs/{id}"))?;
            print_usage(&j, &m);
        }
        Cmd::Wait { id, until, timeout } => {
            let start = std::time::Instant::now();
            loop {
                let left = if timeout == 0 { 900 } else { timeout.saturating_sub(start.elapsed().as_secs()).min(900) };
                let v = c.get(&format!("/v1/jobs/{id}/wait?until={}&timeout={left}", q(&until)))?;
                if v["timed_out"] != true || (timeout > 0 && start.elapsed().as_secs() >= timeout) {
                    print_job(&v);
                    if v["timed_out"] == true {
                        std::process::exit(2);
                    }
                    break;
                }
            }
        }
        Cmd::Cancel { id, array } => {
            let v = c.post(&format!("/v1/jobs/{id}/cancel{}", if array { "?array=1" } else { "" }), &Value::Null)?;
            print_json(&v)?;
        }
        Cmd::Outputs { id, glob, out: _, list: true } => {
            let path = match glob {
                Some(g) => format!("/v1/jobs/{id}/outputs/list?glob={}", q(&g)),
                None => format!("/v1/jobs/{id}/outputs/list"),
            };
            let v = c.get(&path)?;
            for f in v["files"].as_array().into_iter().flatten() {
                println!("{:>10}  {}", fmt::gb(f["bytes"].as_f64().unwrap_or(0.0) / 1_048_576.0), f["path"].as_str().unwrap_or(""));
            }
            println!("{} files, {}", v["count"], fmt::gb(v["total_bytes"].as_f64().unwrap_or(0.0) / 1_048_576.0));
        }
        Cmd::DeleteOutputs { id } => {
            c.call("DELETE", &format!("/v1/jobs/{id}/outputs"), None)?;
            println!("deleted the outputs of job {id}");
        }
        Cmd::Outputs { id, glob, out, list: false } => {
            let dest = out.unwrap_or_else(|| PathBuf::from(format!("outputs/{id}")));
            std::fs::create_dir_all(&dest)?;
            let path = match glob {
                Some(g) => format!("/v1/jobs/{id}/outputs?glob={}", q(&g)),
                None => format!("/v1/jobs/{id}/outputs"),
            };
            pack::unpack(c.download(&path)?, &dest)?;
            println!("outputs of job {id} are in {}", dest.display());
        }
        Cmd::Snapshot { dir, rev } => {
            let id = snapshot(&c, dir.as_deref(), rev.as_deref())?;
            println!("{id}");
        }
        Cmd::Submit { file, dry_run, rev, no_watch, json } => submit(&c, &file, dry_run, rev.as_deref(), no_watch, json)?,
        Cmd::Backends { name, json } => {
            let v = match &name {
                Some(n) => c.get(&format!("/v1/backends/{}", q(n)))?,
                None => c.get("/v1/backends")?,
            };
            if json || name.is_some() {
                return print_json(&v);
            }
            for b in v["backends"].as_array().into_iter().flatten() {
                let status = match (b["enabled"].as_bool(), b["reachable"].as_bool()) {
                    (Some(false), _) => "off",
                    (_, Some(true)) => "up",
                    _ => "unreachable",
                };
                println!(
                    "{:<8} {:<11} {:<6} {} active  {}",
                    b["name"].as_str().unwrap_or(""),
                    b["kind"].as_str().unwrap_or(""),
                    status,
                    b["active_jobs"],
                    b["hardware"].as_str().unwrap_or("")
                );
            }
        }
        Cmd::Datasets { name } => {
            let v = match name {
                Some(n) => c.get(&format!("/v1/datasets/{}", q(&n)))?,
                None => c.get("/v1/datasets")?,
            };
            print_json(&v)?;
        }
        Cmd::DatasetUpload { name, dir, description } => {
            let mut path = format!("/v1/datasets/{}/upload", q(&name));
            if let Some(d) = description {
                path.push_str(&format!("?description={}", q(&d)));
            }
            let p = pack::pack_dir(dir)?;
            let v = c.upload("PUT", &path, &p.reader);
            p.finish()?;
            print_json(&v?)?;
        }
        Cmd::Secrets => print_json(&c.get("/v1/secrets")?)?,
        Cmd::Budget => print_json(&c.get("/v1/budget")?)?,
        Cmd::Report { title, kind, job, body } => {
            let mut req = json!({"kind": kind, "title": title, "body": read_text(&body)?});
            if let Some(j) = job {
                req["job_id"] = j.into();
            }
            let v = c.post("/v1/reports", &req)?;
            println!("report {} filed ({}); watch it with: compute reports {} --wait", v["id"], v["state"].as_str().unwrap_or("open"), v["id"]);
        }
        Cmd::Reports { id: Some(id), wait, .. } => {
            let mut v = c.get(&format!("/v1/reports/{id}"))?;
            while wait && v["state"] != "closed" {
                // A timed-out wait (at most 90 s through Cloudflare) or a network blip: wait again.
                match c.get(&format!("/v1/reports/{id}/wait?timeout=600")) {
                    Ok(r) => v = r,
                    Err(_) => std::thread::sleep(std::time::Duration::from_secs(5)),
                }
            }
            print_json(&v)?;
        }
        Cmd::Reports { id: None, all, .. } => print_json(&c.get(if all { "/v1/reports?state=all" } else { "/v1/reports" })?)?,
        Cmd::Api { method, path, body } => {
            let body = match body {
                None => None,
                Some(b) if b == "-" || Path::new(&b).is_file() => Some(serde_json::from_str(&read_text(&b)?).context("the body is not JSON")?),
                Some(b) => Some(serde_json::from_str(&b).context("body is not JSON (or a file)")?),
            };
            let v = c.call(&method.to_ascii_uppercase(), &path, body.as_ref())?;
            print_json(&v)?;
        }
    }
    Ok(())
}

/// A file's text, or stdin's for "-".
fn read_text(src: &str) -> Result<String> {
    if src == "-" {
        let mut s = String::new();
        std::io::stdin().read_to_string(&mut s)?;
        return Ok(s);
    }
    std::fs::read_to_string(src).with_context(|| format!("reading {src}"))
}

fn print_json(v: &Value) -> Result<()> {
    println!("{}", serde_json::to_string_pretty(v)?);
    Ok(())
}

fn print_job(j: &Value) {
    let t = fmt::times(j);
    println!("#{} {}  [{}]", j["id"], j["title"].as_str().unwrap_or(""), j["state"].as_str().unwrap_or("").replace('_', " "));
    if let Some(r) = j["reason"].as_str() {
        println!("  {r}");
    }
    println!("  backend   {}", j["backend"].as_str().unwrap_or("not placed yet"));
    if let Some(by) = j["submitted_by"].as_str() {
        println!("  by        {by}");
    }
    if let Some(c) = j["cancelled_by"].as_object() {
        println!("  cancelled by {} at {}", c["name"].as_str().unwrap_or("?"), c["at"].as_str().unwrap_or("?"));
    }
    let r = &j["resources"];
    if r.is_object() {
        println!(
            "  asked     {} CPUs, {} RAM, {} disk, {} GPUs",
            r["cpus"],
            fmt::gb(r["mem_gb"].as_f64().unwrap_or(0.0) * 1024.0),
            r["disk_gb"].as_f64().map(|d| fmt::gb(d * 1024.0)).unwrap_or_else(|| "unstated".into()),
            r["gpus"]
        );
    }
    println!("  start     {:<16} finish     {}", t.start, t.finish);
    println!("  elapsed   {:<16} time left  {}", t.elapsed, t.left);
    if let Some(c) = j["exit_code"].as_i64() {
        println!("  exit code {c}");
    }
    if let Some(c) = j["cost_usd"].as_f64() {
        println!("  cost      ${c:.2}");
    }
}

fn print_usage(j: &Value, m: &Value) {
    let Some(u) = fmt::usage(&m["latest"]) else {
        println!("  no resource samples yet");
        return;
    };
    let r = &j["resources"];
    println!(
        "  cpu       {:.0}% of {} cores     memory {} of {}",
        u.cpu_pct.unwrap_or(0.0),
        r["cpus"].as_u64().unwrap_or(1),
        fmt::gb(u.rss_mb.unwrap_or(0.0)),
        fmt::gb(r["mem_gb"].as_f64().unwrap_or(0.0) * 1024.0)
    );
    for g in &u.gpus {
        println!(
            "  gpu {}     {}% util, {} of {}{}",
            g["index"],
            g["util_pct"],
            fmt::gb(g["mem_used_mb"].as_f64().unwrap_or(0.0)),
            fmt::gb(g["mem_total_mb"].as_f64().unwrap_or(0.0)),
            g["power_w"].as_f64().map(|p| format!(", {p:.0} W")).unwrap_or_default()
        );
    }
    println!(
        "  disk free {}   power {}",
        u.disk_free_gb.map(|g| format!("{g:.0} GB")).unwrap_or("—".into()),
        u.power_w.map(|p| format!("{p:.0} W")).unwrap_or("not measured".into())
    );
    // Close to running out of memory or disk: say so before the job dies of it.
    let mem_mb = r["mem_gb"].as_f64().unwrap_or(0.0) * 1024.0;
    if let Some(rss) = u.rss_mb
        && mem_mb > 0.0
        && rss > 0.9 * mem_mb
    {
        println!("  warning: memory at {:.0}% of resources.mem_gb; it will be killed (out of memory) if it goes over", 100.0 * rss / mem_mb);
    }
    if let Some(free) = u.disk_free_gb
        && free < 5.0
    {
        println!("  warning: only {free:.1} GB of disk left; writes will fail with \"no space left on device\"");
    }
}

fn logs(c: &Client, id: i64, follow: bool, tail: Option<u64>) -> Result<()> {
    use std::io::Write;
    let mut offset = match tail {
        Some(n) => c.get(&format!("/v1/jobs/{id}/logs?offset=0&max_bytes=1"))?["size"].as_u64().unwrap_or(0).saturating_sub(n),
        None => 0,
    };
    let mut out = std::io::stdout();
    loop {
        let v = c.get(&format!("/v1/jobs/{id}/logs?offset={offset}&max_bytes=1048576"))?;
        let data = v["data"].as_str().unwrap_or("");
        out.write_all(data.as_bytes())?;
        out.flush()?;
        offset = v["next_offset"].as_u64().unwrap_or(offset);
        if v["eof"] == true || !follow {
            if !data.is_empty() && !follow {
                continue_if_more(c, id, &mut offset, &mut out)?;
            }
            break;
        }
        if data.is_empty() {
            std::thread::sleep(std::time::Duration::from_secs(2));
        }
    }
    Ok(())
}

/// Without --follow, keep reading until the current end of the log.
fn continue_if_more(c: &Client, id: i64, offset: &mut u64, out: &mut impl std::io::Write) -> Result<()> {
    loop {
        let v = c.get(&format!("/v1/jobs/{id}/logs?offset={offset}&max_bytes=1048576"))?;
        let data = v["data"].as_str().unwrap_or("");
        if data.is_empty() {
            return Ok(());
        }
        out.write_all(data.as_bytes())?;
        *offset = v["next_offset"].as_u64().unwrap_or(*offset);
    }
}

/// The repo root of `dir` (git or jj).
fn repo_root(dir: &Path) -> Result<PathBuf> {
    for (cmd, args) in [("jj", vec!["root"]), ("git", vec!["rev-parse", "--show-toplevel"])] {
        if let Ok(o) = Command::new(cmd).args(&args).current_dir(dir).output()
            && o.status.success()
        {
            return Ok(PathBuf::from(String::from_utf8_lossy(&o.stdout).trim()));
        }
    }
    bail!("{} is not inside a git or jj repo", dir.display())
}

/// Packs the repo (tracked plus untracked, non-ignored files), or a committed revision, as
/// tar.zst and uploads it. Returns the snapshot id.
fn snapshot(c: &Client, dir: Option<&Path>, rev: Option<&str>) -> Result<String> {
    let cwd = std::env::current_dir()?;
    let root = repo_root(dir.unwrap_or(&cwd))?;
    let packing = match rev {
        Some(r) => {
            // A jj revision resolves to its git commit (colocated repos); plain git takes the rev as is.
            let commit = Command::new("jj")
                .args(["log", "--no-graph", "-r", r, "-T", "commit_id"])
                .current_dir(&root)
                .output()
                .ok()
                .filter(|o| o.status.success())
                .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
                .filter(|s| !s.is_empty() && !s.contains('\n'))
                .unwrap_or_else(|| r.to_string());
            if commit.starts_with('-') {
                bail!("revision {r:?} looks like an option");
            }
            pack::pack_command(&root, "git", &["archive", "--format=tar", &commit])?
        }
        None => pack::pack_files(root.clone(), pack::repo_files(&root)?)?,
    };
    let v = c.upload("POST", "/v1/snapshots", &packing.reader);
    packing.finish().context("packing the repo failed")?;
    let v = v?;
    let id = v["id"].as_str().context("no snapshot id in the response")?.to_string();
    eprintln!("snapshot {id} ({}){}", if v["existed"] == true { "already stored" } else { "uploaded" }, rev.map(|r| format!(" from {r}")).unwrap_or_default());
    Ok(id)
}

fn submit(c: &Client, file: &str, dry_run: bool, rev: Option<&str>, no_watch: bool, json_out: bool) -> Result<()> {
    let text = if file == "-" {
        let mut s = String::new();
        std::io::stdin().read_to_string(&mut s)?;
        s
    } else {
        std::fs::read_to_string(file).with_context(|| format!("reading {file}"))?
    };
    let mut req: Value = serde_json::from_str(&text).context("the job request is not JSON")?;
    // Undersized memory and disk are the usual reasons a job dies partway: ask for both.
    let res = &req["resources"];
    if res["mem_gb"].is_null() {
        eprintln!("warning: resources.mem_gb is not set (default 4 GB); set it to the job's peak RAM plus headroom");
    }
    if res["disk_gb"].is_null() {
        eprintln!("warning: resources.disk_gb is not set; set it to the disk the job writes (outputs, checkpoints, caches) plus headroom");
    }
    let snap = req["snapshot"].as_str().unwrap_or("auto");
    if snap == "auto" || snap.is_empty() {
        req["snapshot"] = snapshot(c, None, rev)?.into();
    }
    if dry_run {
        req["dry_run"] = true.into();
    }
    let v = c.post("/v1/jobs", &req)?;
    if json_out || dry_run {
        print_json(&v)?;
    } else if let Some(a) = v["array_id"].as_i64() {
        println!("array {a}: {} tasks, {}", v["tasks"], v["state"].as_str().unwrap_or(""));
        if let Some(r) = v["first_task_estimate"]["reason"].as_str() {
            println!("  {r}");
        }
    } else {
        let id = v["id"].as_i64().unwrap_or(0);
        println!("job {id}: {} on {}", v["state"].as_str().unwrap_or(""), v["backend"].as_str().unwrap_or("(not placed yet)"));
        if let Some(r) = v["reason"].as_str() {
            println!("  {r}");
        }
        let t = fmt::times(&json!({
            "state": v["state"], "estimated_start": v["estimated_start"], "estimated_finish": v["estimated_finish"],
            "time_s": req["resources"]["time"].as_str().and_then(parse_duration).unwrap_or(0),
        }));
        println!("  start {}  finish {}  ({})", t.start, t.finish, v["estimated_start_source"].as_str().unwrap_or("estimate"));
        if let Some(cost) = v["estimated_cost_usd"].as_f64() {
            println!("  estimated cost ${cost:.2}");
        }
    }
    let watch_env_off = std::env::var("COMPUTE_WATCH").is_ok_and(|v| v == "0");
    if !dry_run && !no_watch && !watch_env_off && rex::inside() {
        let target = v["id"].as_i64();
        let label = target.map(|i| format!("compute #{i}")).unwrap_or_else(|| "compute".into());
        match rex::open_monitor(target, &label) {
            Ok(b) => eprintln!("monitor opened in a Rex split ({b}); --no-watch or COMPUTE_WATCH=0 skips it"),
            Err(e) => eprintln!("couldn't open the monitor: {e:#}"),
        }
    }
    Ok(())
}

fn parse_duration(s: &str) -> Option<i64> {
    let mut total = 0i64;
    let mut num = String::new();
    for ch in s.chars() {
        if ch.is_ascii_digit() {
            num.push(ch);
            continue;
        }
        let n: i64 = num.parse().ok()?;
        num.clear();
        total += n * match ch {
            'd' => 86_400,
            'h' => 3600,
            'm' => 60,
            's' => 1,
            _ => return None,
        };
    }
    Some(total)
}
