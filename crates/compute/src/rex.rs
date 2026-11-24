//! Rex integration: open the monitor in a split, and report a job's state with the Program
//! Status Protocol (OSC 7501) so Rex shows it on the tab and in the session picker.

use anyhow::{Context, Result, bail};
use base64::Engine;
use std::io::Write;
use std::process::Command;

/// True when running in a Rex terminal.
pub fn inside() -> bool {
    std::env::var("REX_BLOCK").is_ok_and(|b| !b.is_empty())
}

/// Opens `compute watch [id]` in a split beside the current Rex pane (to the right, 40%),
/// without taking focus. Returns the new block id.
pub fn open_monitor(job: Option<i64>, label: &str) -> Result<String> {
    let block = std::env::var("REX_BLOCK").ok().filter(|b| !b.is_empty());
    let Some(block) = block else {
        bail!("not in a Rex terminal (REX_BLOCK is unset); run `compute watch` yourself");
    };
    let exe = std::env::current_exe().context("finding the compute binary")?;
    let mut cmd = Command::new("rex");
    cmd.args(["split", "-b", &block, "--split=right", "--ratio", "40", "--focus=false", "--json", "--label", label, "--"]);
    cmd.arg(exe).arg("watch");
    if let Some(id) = job {
        cmd.arg(id.to_string());
    }
    let out = cmd.output().context("running rex")?;
    if !out.status.success() {
        bail!("rex split failed: {}", String::from_utf8_lossy(&out.stderr).trim());
    }
    let v: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap_or_default();
    Ok(v["block_ids"][0].as_str().unwrap_or_default().to_string())
}

/// Program status for the terminal: `working` (with progress), `done`, `error` or `idle`.
pub fn status(state: &str, progress: Option<u8>, msg: &str) {
    let b64 = |s: &str| base64::engine::general_purpose::STANDARD.encode(s.chars().filter(|c| !c.is_control()).collect::<String>());
    let mut body = format!("state={state}:app=compute:msg={}", b64(msg));
    if let Some(p) = progress.filter(|_| state == "working") {
        body.push_str(&format!(":progress={}", p.min(100)));
    }
    let mut out = std::io::stdout();
    let _ = write!(out, "\x1b]7501;{body}\x1b\\");
    let _ = out.flush();
}
