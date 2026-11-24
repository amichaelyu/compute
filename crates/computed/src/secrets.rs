//! Named secrets: write-only through the API, one 0600 JSON file each under
//! `<config>/secrets/`. Values only leave the daemon as a job's `.secrets.env`.

use crate::error::ApiError;
use crate::model::valid_env_name;
use crate::transport::shell_quote;
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::io::Write;
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::PathBuf;

pub struct Secrets {
    dir: PathBuf,
}

#[derive(Serialize, Deserialize)]
struct SecretFile {
    env: BTreeMap<String, String>,
    updated_at: DateTime<Utc>,
}

#[derive(Serialize)]
pub struct SecretInfo {
    pub name: String,
    pub keys: Vec<String>,
    pub updated_at: DateTime<Utc>,
}

pub fn valid_name(name: &str) -> bool {
    let mut c = name.chars();
    matches!(c.next(), Some(f) if f.is_ascii_lowercase() || f.is_ascii_digit())
        && name.len() <= 64
        && c.all(|ch| ch.is_ascii_lowercase() || ch.is_ascii_digit() || ch == '_' || ch == '-')
}

impl Secrets {
    pub fn new(dir: PathBuf) -> Secrets {
        Secrets { dir }
    }

    fn path(&self, name: &str) -> PathBuf {
        self.dir.join(format!("{name}.json"))
    }

    pub fn put(&self, name: &str, env: BTreeMap<String, String>) -> Result<(), ApiError> {
        if !valid_name(name) {
            return Err(ApiError::bad_request(
                format!("secret name {name:?} is invalid"),
                "use lowercase letters, digits, _ and -, up to 64 characters",
            ));
        }
        if env.is_empty() {
            return Err(ApiError::bad_request("`env` is empty", "give at least one KEY: value"));
        }
        for k in env.keys() {
            if !valid_env_name(k) {
                return Err(ApiError::bad_request(format!("{k:?} is not a valid variable name"), ""));
            }
        }
        std::fs::create_dir_all(&self.dir).map_err(anyhow::Error::from)?;
        std::fs::set_permissions(&self.dir, std::fs::Permissions::from_mode(0o700))
            .map_err(anyhow::Error::from)?;
        let tmp = self.dir.join(format!(".{name}.tmp"));
        let data = serde_json::to_vec(&SecretFile { env, updated_at: Utc::now() }).map_err(anyhow::Error::from)?;
        let mut f = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o600)
            .open(&tmp)
            .map_err(anyhow::Error::from)?;
        f.write_all(&data).map_err(anyhow::Error::from)?;
        f.sync_all().map_err(anyhow::Error::from)?;
        std::fs::rename(&tmp, self.path(name)).map_err(anyhow::Error::from)?;
        Ok(())
    }

    fn read(&self, name: &str) -> Result<Option<SecretFile>> {
        let p = self.path(name);
        if !p.exists() {
            return Ok(None);
        }
        let data = std::fs::read(&p).with_context(|| format!("reading secret {name}"))?;
        Ok(Some(serde_json::from_slice(&data)?))
    }

    pub fn list(&self) -> Result<Vec<SecretInfo>> {
        let mut out = Vec::new();
        let Ok(entries) = std::fs::read_dir(&self.dir) else { return Ok(out) };
        for e in entries.flatten() {
            let name = e.file_name().to_string_lossy().to_string();
            let Some(name) = name.strip_suffix(".json") else { continue };
            if !valid_name(name) {
                continue;
            }
            if let Some(f) = self.read(name)? {
                out.push(SecretInfo { name: name.to_string(), keys: f.env.keys().cloned().collect(), updated_at: f.updated_at });
            }
        }
        out.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(out)
    }

    pub fn delete(&self, name: &str) -> Result<bool> {
        if !valid_name(name) {
            return Ok(false);
        }
        match std::fs::remove_file(self.path(name)) {
            Ok(()) => Ok(true),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(e) => Err(e.into()),
        }
    }

    /// Checks that every named secret exists and that no two set the same key.
    pub fn check(&self, names: &[String]) -> Result<(), ApiError> {
        let mut seen: BTreeMap<String, String> = BTreeMap::new();
        for n in names {
            let f = if valid_name(n) { self.read(n)? } else { None };
            let Some(f) = f else {
                return Err(ApiError::new(
                    axum::http::StatusCode::BAD_REQUEST,
                    "secret_missing",
                    format!("secret {n:?} does not exist"),
                    "ask the user for the value; they can store it with PUT /v1/secrets/{name}",
                ));
            };
            for k in f.env.keys() {
                if let Some(other) = seen.insert(k.clone(), n.clone()) {
                    return Err(ApiError::bad_request(
                        format!("secrets {other:?} and {n:?} both set {k}"),
                        "use only one of them for this job",
                    ));
                }
            }
        }
        Ok(())
    }

    /// The `.secrets.env` contents for a job, or None when it uses no secrets.
    pub fn env_file(&self, names: &[String]) -> Result<Option<String>> {
        if names.is_empty() {
            return Ok(None);
        }
        let mut lines = Vec::new();
        for n in names {
            let f = self.read(n)?.with_context(|| format!("secret {n} disappeared"))?;
            for (k, v) in f.env {
                lines.push(format!("{k}={}", shell_quote(&v)));
            }
        }
        lines.push(String::new());
        Ok(Some(lines.join("\n")))
    }

    /// Secret values long enough to redact from logs.
    pub fn values(&self, names: &[String]) -> Vec<String> {
        names
            .iter()
            .filter_map(|n| self.read(n).ok().flatten())
            .flat_map(|f| f.env.into_values())
            .filter(|v| v.len() >= 8)
            .collect()
    }
}
