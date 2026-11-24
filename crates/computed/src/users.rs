//! The people who may use the API (the keys in `allowed_signers`), and what a new person needs:
//! a public install script, and the compute skill served behind the SSH-key login.

use crate::error::ApiError;
use anyhow::{Context, Result};
use axum::http::{HeaderMap, StatusCode, header};
use serde::Serialize;
use ssh_key::{HashAlg, PublicKey};
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::sync::Mutex;

/// Serializes edits to `allowed_signers`, so two adds can't drop each other's line.
static WRITE: Mutex<()> = Mutex::new(());

const SKILL: &str = include_str!("../../../skills/compute/SKILL.md");
const GUEST_HEADER: &str = include_str!("../guest/header.md");
const INSTALL: &str = include_str!("../guest/install.sh");

#[derive(Serialize)]
pub struct User {
    pub name: String,
    pub fingerprint: String,
    pub key_type: String,
    pub comment: String,
}

/// Parses one `allowed_signers` line: `principal [options] keytype base64 [comment]`.
fn parse_line(line: &str) -> Option<(User, PublicKey)> {
    let line = line.trim();
    if line.is_empty() || line.starts_with('#') {
        return None;
    }
    let parts: Vec<&str> = line.split_whitespace().collect();
    let i = parts.iter().position(|p| p.starts_with("ssh-") || p.starts_with("ecdsa-") || p.starts_with("sk-"))?;
    let key = PublicKey::from_openssh(&format!("{} {}", parts[i], parts.get(i + 1)?)).ok()?;
    let user = User {
        name: if i > 0 { parts[0].to_string() } else { String::new() },
        fingerprint: key.fingerprint(HashAlg::Sha256).to_string(),
        key_type: parts[i].to_string(),
        comment: parts.get(i + 2..).map(|c| c.join(" ")).unwrap_or_default(),
    };
    Some((user, key))
}

fn read(path: &Path) -> Result<String> {
    match std::fs::read_to_string(path) {
        Ok(t) => Ok(t),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(String::new()),
        Err(e) => Err(e).with_context(|| format!("reading {}", path.display())),
    }
}

/// Replaces the file in one rename, keeping its permissions (0600 for a new file).
fn write_atomic(path: &Path, text: &str) -> Result<()> {
    let mode = std::fs::metadata(path).map(|m| m.permissions().mode()).unwrap_or(0o600);
    let tmp = path.with_extension("tmp");
    std::fs::write(&tmp, text).with_context(|| format!("writing {}", tmp.display()))?;
    std::fs::set_permissions(&tmp, std::fs::Permissions::from_mode(mode))?;
    std::fs::rename(&tmp, path).with_context(|| format!("replacing {}", path.display()))
}

pub fn list(path: &Path) -> Result<Vec<User>> {
    Ok(read(path)?.lines().filter_map(parse_line).map(|(u, _)| u).collect())
}

fn conflict(message: String) -> ApiError {
    ApiError::new(StatusCode::CONFLICT, "conflict", message, "remove the existing entry first")
}

/// Appends `name keytype base64 [comment]` to `allowed_signers`.
pub fn add(path: &Path, name: &str, public_key: &str) -> Result<User, ApiError> {
    let name = name.trim();
    if name.is_empty() || name.len() > 32 || !name.chars().all(|c| c.is_ascii_alphanumeric() || "._-".contains(c)) {
        return Err(ApiError::bad_request(
            "name must be 1-32 letters, digits, dots, dashes or underscores",
            "e.g. alice",
        ));
    }
    if public_key.contains("PRIVATE KEY") {
        return Err(ApiError::bad_request(
            "that is a private key",
            "ask them for the .pub file's line (ssh-ed25519 AAAA… name@host); the private key never leaves their machine",
        ));
    }
    let key = PublicKey::from_openssh(public_key.trim()).map_err(|e| {
        ApiError::bad_request(format!("not an SSH public key: {e}"), "paste the whole .pub line: ssh-ed25519 AAAA… name@host")
    })?;
    let _guard = WRITE.lock().unwrap_or_else(|e| e.into_inner());
    let mut text = read(path)?;
    for (u, k) in text.lines().filter_map(parse_line) {
        if k.key_data() == key.key_data() {
            return Err(conflict(format!("this key is already added, as {}", u.name)));
        }
        if u.name == name {
            return Err(conflict(format!("someone is already called {name}")));
        }
    }
    let openssh = key.to_openssh().map_err(|e| ApiError::bad_request(format!("can't encode the key: {e}"), ""))?;
    let line = format!("{name} {openssh}");
    if !text.is_empty() && !text.ends_with('\n') {
        text.push('\n');
    }
    text.push_str(&line);
    text.push('\n');
    write_atomic(path, &text)?;
    tracing::info!("added user {name} ({})", key.fingerprint(HashAlg::Sha256));
    Ok(parse_line(&line).map(|(u, _)| u).expect("a line we just built parses"))
}

/// Removes every line holding the key with this fingerprint; returns the removed users.
pub fn remove(path: &Path, fingerprint: &str) -> Result<Vec<User>, ApiError> {
    let _guard = WRITE.lock().unwrap_or_else(|e| e.into_inner());
    let text = read(path)?;
    let mut kept = String::new();
    let mut removed = Vec::new();
    for line in text.lines() {
        match parse_line(line) {
            Some((u, _)) if u.fingerprint == fingerprint => removed.push(u),
            _ => {
                kept.push_str(line);
                kept.push('\n');
            }
        }
    }
    if removed.is_empty() {
        return Err(ApiError::not_found(format!("no user with key {fingerprint}")));
    }
    write_atomic(path, &kept)?;
    tracing::info!("removed user {} ({fingerprint})", removed[0].name);
    Ok(removed)
}

/// The URL clients should use, as seen by this request: `https://<host>/api` through the
/// public hostname, `http://<host>:<port>` on the tailnet. Odd Host values fall back to
/// `http://localhost`, since the result is pasted into a shell script.
pub fn base_url(headers: &HeaderMap, path: &str) -> String {
    let get = |h: &str| headers.get(h).and_then(|v| v.to_str().ok()).unwrap_or("");
    let proto = if get("x-forwarded-proto") == "https" { "https" } else { "http" };
    let host = get(header::HOST.as_str());
    let host = if !host.is_empty() && host.len() < 256 && host.chars().all(|c| c.is_ascii_alphanumeric() || ".-:[]".contains(c)) {
        host
    } else {
        "localhost"
    };
    let prefix = if path.starts_with("/api/") { "/api" } else { "" };
    format!("{proto}://{host}{prefix}")
}

/// Who runs this network, for the guest skill (`COMPUTE_OWNER` in computed.env).
fn owner() -> String {
    std::env::var("COMPUTE_OWNER").ok().filter(|v| !v.trim().is_empty()).unwrap_or_else(|| "the owner of this network".into())
}

/// The compute skill for people using someone else's network: the guest header (setup and
/// house rules), then the main skill from "Setup per shell" on.
pub fn skill(url: &str) -> String {
    let body = SKILL.find("\n## Setup per shell").map(|i| &SKILL[i + 1..]).unwrap_or(SKILL);
    let head = GUEST_HEADER.replace("{{URL}}", url).replace("{{OWNER}}", &owner());
    format!("{}\n\n{body}", head.trim_end())
}

pub fn install_script(url: &str) -> String {
    INSTALL.replace("{{URL}}", url)
}
