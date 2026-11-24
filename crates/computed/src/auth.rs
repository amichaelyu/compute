//! SSH-key auth: a client signs a single-use nonce with `ssh-keygen -Y sign -n compute` and gets
//! a 12 h session token. Keys are checked against `allowed_signers`, re-read on every login so
//! editing the file grants or revokes access without a restart.

use crate::error::ApiError;
use crate::store::Store;
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use sha2::{Digest, Sha256};
use ssh_key::{HashAlg, PublicKey, SshSig};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

pub const NAMESPACE: &str = "compute";
const NONCE_TTL: Duration = Duration::from_secs(60);
const SESSION_TTL_HOURS: i64 = 12;
/// Outstanding nonces are capped so anonymous `POST /v1/auth/challenge` calls can't grow memory
/// without bound once the API is reachable from the internet.
const MAX_NONCES: usize = 1024;

pub struct Auth {
    allowed_signers: PathBuf,
    store: Arc<Store>,
    nonces: Mutex<HashMap<String, Instant>>,
}

fn random_hex() -> String {
    hex::encode(rand::random::<[u8; 32]>())
}

pub fn hash_token(token: &str) -> String {
    hex::encode(Sha256::digest(token.as_bytes()))
}

/// Parses `allowed_signers`: `principal [options] keytype base64 [comment]` per line. The
/// principal is the key's nickname (e.g. `michael`).
fn load_allowed(path: &PathBuf) -> Result<Vec<(String, PublicKey)>> {
    let text = std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
    let mut keys = Vec::new();
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let parts: Vec<&str> = line.split_whitespace().collect();
        let Some(i) = parts.iter().position(|p| {
            p.starts_with("ssh-") || p.starts_with("ecdsa-") || p.starts_with("sk-")
        }) else {
            continue;
        };
        if let Some(b64) = parts.get(i + 1)
            && let Ok(k) = PublicKey::from_openssh(&format!("{} {}", parts[i], b64)) {
                let name = if i > 0 { parts[0].to_string() } else { String::new() };
                keys.push((name, k));
            }
    }
    Ok(keys)
}

impl Auth {
    pub fn new(allowed_signers: PathBuf, store: Arc<Store>) -> Auth {
        Auth { allowed_signers, store, nonces: Mutex::new(HashMap::new()) }
    }

    pub fn allowed_signers(&self) -> &std::path::Path {
        &self.allowed_signers
    }

    pub fn challenge(&self) -> Result<(String, DateTime<Utc>), ApiError> {
        let nonce = random_hex();
        let mut map = self.nonces.lock().unwrap_or_else(|e| e.into_inner());
        map.retain(|_, t| t.elapsed() < NONCE_TTL);
        if map.len() >= MAX_NONCES {
            tracing::warn!("refusing a login challenge: {} unused nonces outstanding", map.len());
            return Err(ApiError::new(
                axum::http::StatusCode::TOO_MANY_REQUESTS,
                "rate_limited",
                "too many logins in progress",
                "retry in a minute",
            ));
        }
        map.insert(nonce.clone(), Instant::now());
        Ok((nonce, Utc::now() + chrono::Duration::from_std(NONCE_TTL).unwrap_or_default()))
    }

    /// Verifies a signed nonce and returns `(token, fingerprint, expires_at)`.
    pub fn token(
        &self,
        nonce: &str,
        signature: &str,
        public_key: &str,
    ) -> Result<(String, String, DateTime<Utc>), ApiError> {
        let issued = self
            .nonces
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(nonce)
            .ok_or_else(|| ApiError::unauthorized("unknown or already used nonce"))?;
        if issued.elapsed() >= NONCE_TTL {
            return Err(ApiError::unauthorized("nonce expired (60 s)"));
        }
        let key = PublicKey::from_openssh(public_key.trim())
            .map_err(|e| ApiError::bad_request(format!("bad public_key: {e}"), "send the .pub line"))?;
        let allowed = load_allowed(&self.allowed_signers).map_err(ApiError::from)?;
        let Some((name, _)) = allowed.iter().find(|(_, k)| k.key_data() == key.key_data()) else {
            return Err(ApiError::unauthorized(format!(
                "key {} is not in allowed_signers",
                key.fingerprint(HashAlg::Sha256)
            )));
        };
        let name = name.clone();
        let sig = SshSig::from_pem(signature.trim())
            .map_err(|e| ApiError::bad_request(format!("bad signature: {e}"), "send the full -----BEGIN SSH SIGNATURE----- block"))?;
        key.verify(NAMESPACE, nonce.as_bytes(), &sig).map_err(|_| {
            ApiError::unauthorized(
                "signature does not verify; sign exactly the nonce bytes (printf %s \"$nonce\"), namespace compute",
            )
        })?;
        let token = random_hex();
        let fingerprint = key.fingerprint(HashAlg::Sha256).to_string();
        let expires = Utc::now() + chrono::Duration::hours(SESSION_TTL_HOURS);
        self.store.put_session(&hash_token(&token), &fingerprint, expires)?;
        if !name.is_empty() {
            self.store.set_key_name(&fingerprint, &name)?;
        }
        Ok((token, fingerprint, expires))
    }

    pub fn session(&self, bearer: &str) -> Result<Option<String>> {
        Ok(match self.store.session(&hash_token(bearer))? {
            Some((fp, expires)) if expires > Utc::now() => Some(fp),
            _ => None,
        })
    }
}
