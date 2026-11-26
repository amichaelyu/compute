//! Keeping the CLI and the compute skill current. The daemon serves the CLI binaries
//! (`GET /v1/cli`), the skill (`GET /v1/skill`) and the current CLI version (`cli_version` in
//! `GET /v1/`). `compute setup` signs in and installs the skill, `compute update` replaces the
//! binary and the skill, and any command checks for a new version at most once a day.

use crate::client::{Client, config_dir, config_file, default_key, err_text, home, setting};
use anyhow::{Context, Result, bail};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::io::IsTerminal;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

/// The build: `COMPUTE_BUILD` at compile time (deploy/install.sh sets it to
/// `<crate version>+<commit>`), else the crate version for a local build.
pub const VERSION: &str = match option_env!("COMPUTE_BUILD") {
    Some(v) => v,
    None => env!("CARGO_PKG_VERSION"),
};

/// This machine's platform, as `GET /v1/cli` names it.
pub fn platform() -> Option<&'static str> {
    Some(match (std::env::consts::OS, std::env::consts::ARCH) {
        ("macos", "aarch64") => "darwin-arm64",
        ("linux", "x86_64") => "linux-x86_64",
        ("linux", "aarch64") => "linux-arm64",
        ("windows", "x86_64") => "windows-x86_64",
        _ => return None,
    })
}

/// Where the skill goes: `COMPUTE_SKILL_DIR`, else Claude Code's skills folder, plus Codex's
/// when `~/.codex` exists.
fn skill_dirs() -> Vec<PathBuf> {
    if let Some(d) = setting("COMPUTE_SKILL_DIR") {
        return vec![PathBuf::from(d)];
    }
    let h = home();
    let mut dirs = vec![h.join(".claude/skills/compute")];
    if h.join(".codex").is_dir() {
        dirs.push(h.join(".codex/skills/compute"));
    }
    dirs
}

fn is_link(p: &Path) -> bool {
    std::fs::symlink_metadata(p).is_ok_and(|m| m.file_type().is_symlink())
}

/// Downloads the skill into each skill folder. A folder (or SKILL.md) that is a symlink points
/// at a checkout someone edits by hand, so it is left alone. Returns the files written.
pub fn install_skill(c: &Client) -> Result<Vec<PathBuf>> {
    let text = match c.get("/v1/skill")? {
        Value::String(s) => s,
        v => bail!("GET /v1/skill returned JSON instead of the skill: {v}"),
    };
    let mut written = Vec::new();
    for dir in skill_dirs() {
        let file = dir.join("SKILL.md");
        if is_link(&dir) || is_link(&file) {
            eprintln!("compute: {} is a symlink; left as is", if is_link(&dir) { &dir } else { &file }.display());
            continue;
        }
        std::fs::create_dir_all(&dir).with_context(|| format!("creating {}", dir.display()))?;
        let tmp = dir.join(".SKILL.md.tmp");
        std::fs::write(&tmp, &text)?;
        std::fs::rename(&tmp, &file)?;
        written.push(file);
    }
    Ok(written)
}

fn sha256_hex(data: &[u8]) -> String {
    hex::encode(Sha256::digest(data))
}

/// Replaces the running binary with `bytes`. On Windows a running .exe can't be overwritten
/// but can be renamed, so it moves aside to `compute.old.exe` (removed on a later run).
fn replace_exe(bytes: &[u8]) -> Result<PathBuf> {
    let exe = std::env::current_exe().context("finding the compute binary")?;
    let exe = exe.canonicalize().unwrap_or(exe);
    let dir = exe.parent().context("the binary has no directory")?;
    let tmp = dir.join(format!(".compute-new-{}", std::process::id()));
    std::fs::write(&tmp, bytes).with_context(|| format!("can't write to {} (reinstall with the install command instead)", dir.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&tmp, std::fs::Permissions::from_mode(0o755))?;
    }
    #[cfg(windows)]
    {
        let old = exe.with_extension("old.exe");
        let _ = std::fs::remove_file(&old);
        std::fs::rename(&exe, &old).context("moving the old binary aside")?;
    }
    if let Err(e) = std::fs::rename(&tmp, &exe) {
        let _ = std::fs::remove_file(&tmp);
        return Err(e).with_context(|| format!("replacing {}", exe.display()));
    }
    Ok(exe)
}

/// Removes the binary a Windows update moved aside.
pub fn cleanup_old() {
    if cfg!(windows)
        && let Ok(exe) = std::env::current_exe()
    {
        let _ = std::fs::remove_file(exe.with_extension("old.exe"));
    }
}

/// Installs the daemon's CLI build when it differs from this one (or `force`), then refreshes
/// the skill. Returns the version now installed.
pub fn update(c: &Client, force: bool) -> Result<String> {
    let m = c.get("/v1/cli")?;
    let latest = m["version"].as_str().context("this network serves no CLI builds (GET /v1/cli has no version)")?.to_string();
    if latest != VERSION || force {
        let plat = platform().with_context(|| format!("no compute build for {} {}", std::env::consts::OS, std::env::consts::ARCH))?;
        let entry = m["platforms"]
            .as_array()
            .and_then(|ps| ps.iter().find(|p| p["platform"] == plat))
            .with_context(|| format!("this network has no compute build for {plat}"))?;
        let path = format!("/v1/cli/{plat}");
        let bytes = c.fetch_public(&path)?;
        let want = entry["sha256"].as_str().unwrap_or_default();
        if sha256_hex(&bytes) != want {
            bail!("the download of {path} doesn't match its sha256; try again");
        }
        let exe = replace_exe(&bytes)?;
        eprintln!("compute: {} is now {latest} (was {VERSION})", exe.display());
    }
    for f in install_skill(c)? {
        eprintln!("compute: updated the skill at {}", f.display());
    }
    Ok(latest)
}

/// Before a command: at most once a day, update when the daemon has a different build.
/// Local builds (no `+commit` in the version) never update themselves; `COMPUTE_NO_UPDATE=1`
/// turns it off.
pub fn auto(c: &Client) {
    if !VERSION.contains('+') || setting("COMPUTE_NO_UPDATE").is_some_and(|v| v != "0") {
        return;
    }
    let stamp = config_dir().join("update-check");
    let fresh = std::fs::metadata(&stamp)
        .and_then(|m| m.modified())
        .is_ok_and(|t| SystemTime::now().duration_since(t).unwrap_or_default() < Duration::from_secs(24 * 3600));
    if fresh {
        return;
    }
    let _ = std::fs::create_dir_all(config_dir());
    let _ = std::fs::write(&stamp, VERSION);
    let Ok(root) = c.get("/v1/") else { return };
    let Some(latest) = root["cli_version"].as_str() else { return };
    if latest == VERSION {
        return;
    }
    match update(c, false) {
        Ok(v) => eprintln!("compute: updated to {v}. The compute skill changed with it: agents, read it again before going on."),
        Err(e) => eprintln!("compute: couldn't update to {latest}: {}", err_text(&e)),
    }
}

/// Writes `COMPUTE_URL` (and `COMPUTE_KEY`) into the client config, creating it if needed.
fn write_config(url: &str, key: &Path) -> Result<()> {
    let file = config_file();
    if let Some(d) = file.parent() {
        std::fs::create_dir_all(d)?;
    }
    let text = std::fs::read_to_string(&file).unwrap_or_default();
    if text.is_empty() {
        let mut out = format!("# compute client config, written by `compute setup`\nCOMPUTE_URL={url}\nCOMPUTE_KEY={}\n", key.display());
        if let Some(d) = std::env::var("COMPUTE_SKILL_DIR").ok().filter(|d| !d.is_empty()) {
            out.push_str(&format!("COMPUTE_SKILL_DIR={d}\n"));
        }
        std::fs::write(&file, out)?;
        return Ok(());
    }
    let mut found = false;
    let mut out = String::new();
    for line in text.lines() {
        let bare = line.trim().strip_prefix("export ").unwrap_or(line.trim());
        if let Some(old) = bare.strip_prefix("COMPUTE_URL=") {
            if !found {
                out.push_str(&format!("COMPUTE_URL={url}\n"));
                if old != url {
                    eprintln!("compute: {} pointed at {old}; it now points at {url}", file.display());
                }
            }
            found = true;
        } else {
            out.push_str(line);
            out.push('\n');
        }
    }
    if !found {
        out.push_str(&format!("COMPUTE_URL={url}\n"));
    }
    std::fs::write(&file, out)?;
    Ok(())
}

/// `compute setup <url>`: signs in with the SSH key (waiting, when run in a terminal, while the
/// key isn't on the network yet), writes the client config and installs the skill.
pub fn setup(url: &str, key: Option<PathBuf>) -> Result<()> {
    let url = url.trim_end_matches('/');
    let key = key.map(|k| if k.extension().is_some_and(|e| e == "pub") { k } else { k.with_extension("pub") }).unwrap_or_else(default_key);
    if !key.is_file() {
        bail!(
            "no SSH public key at {}. Make one with: ssh-keygen -t ed25519 (or pass --key / set COMPUTE_KEY to your .pub file)",
            key.display()
        );
    }
    let c = Client::new(url, key.clone());
    loop {
        match c.login() {
            Ok(()) => break,
            Err(e) if err_text(&e).contains("allowed_signers") => {
                eprintln!("This SSH key isn't on the compute network yet. Send this line to the person who runs it:\n");
                eprintln!("{}", std::fs::read_to_string(&key)?.trim());
                eprintln!();
                if !std::io::stdin().is_terminal() {
                    bail!("once they've added it, run the same command again");
                }
                eprint!("Press Enter once they've added it (Ctrl-C to stop): ");
                let mut s = String::new();
                if std::io::stdin().read_line(&mut s)? == 0 {
                    bail!("stopped");
                }
            }
            Err(e) => return Err(e),
        }
    }
    write_config(url, &key)?;
    let files = install_skill(&c)?;
    let me = c.get("/v1/whoami")?;
    let name = me["name"].as_str().or(me["identity"].as_str()).unwrap_or("?");
    println!("compute: signed in to {url} as {name} ({} access)", me["scope"].as_str().unwrap_or("?"));
    for f in &files {
        println!("compute: installed the skill at {}", f.display());
    }
    println!("Ask your agent to run something on compute, e.g. \"train this on a GPU with compute\".");
    Ok(())
}
