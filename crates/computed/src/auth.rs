//! SSH-key auth: a client signs a nonce with `ssh-keygen -Y sign -n compute` and gets a 12 h
//! session token. Keys are checked against `allowed_signers` at login and again on every
//! request (re-read when the file changes), so editing the file grants or revokes access at
//! once, without a restart.

use crate::error::ApiError;
use crate::store::Store;
use crate::users::Limits;
use anyhow::Result;
use chrono::{DateTime, Utc};
use sha2::{Digest, Sha256};
use ssh_key::{HashAlg, PublicKey, SshSig};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime};

pub const NAMESPACE: &str = "compute";
const NONCE_TTL: Duration = Duration::from_secs(60);
const SESSION_TTL_HOURS: i64 = 12;

/// One usable `allowed_signers` entry.
struct Signer {
    name: String,
    fingerprint: String,
    key: PublicKey,
    limits: Limits,
}

/// The parsed file and the (mtime, length) it was read at.
struct Allowed {
    stamp: Option<(SystemTime, u64)>,
    signers: Arc<Vec<Signer>>,
}

pub struct Auth {
    allowed_signers: PathBuf,
    store: Arc<Store>,
    /// Signs nonces, so outstanding ones cost no memory: anyone may ask for as many as they
    /// like without crowding out real logins. Random per process.
    nonce_key: [u8; 32],
    /// Nonces already presented (until they'd have expired anyway), so each works once.
    used: Mutex<HashMap<String, Instant>>,
    allowed: Mutex<Allowed>,
}

fn random_hex() -> String {
    hex::encode(rand::random::<[u8; 32]>())
}

pub fn hash_token(token: &str) -> String {
    hex::encode(Sha256::digest(token.as_bytes()))
}

fn hmac(key: &[u8; 32], msg: &[u8]) -> [u8; 32] {
    let mut ipad = [0x36u8; 64];
    let mut opad = [0x5cu8; 64];
    for (i, k) in key.iter().enumerate() {
        ipad[i] ^= k;
        opad[i] ^= k;
    }
    let inner = Sha256::new().chain_update(ipad).chain_update(msg).finalize();
    Sha256::new().chain_update(opad).chain_update(inner).finalize().into()
}

fn now_secs() -> u64 {
    SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0)
}

impl Auth {
    pub fn new(allowed_signers: PathBuf, store: Arc<Store>) -> Auth {
        Auth {
            allowed_signers,
            store,
            nonce_key: rand::random(),
            used: Mutex::new(HashMap::new()),
            allowed: Mutex::new(Allowed { stamp: None, signers: Arc::new(Vec::new()) }),
        }
    }

    pub fn allowed_signers(&self) -> &std::path::Path {
        &self.allowed_signers
    }

    /// The file's entries, re-read only when its mtime or size changed.
    fn signers(&self) -> Result<Arc<Vec<Signer>>> {
        let stamp = std::fs::metadata(&self.allowed_signers).ok().map(|m| (m.modified().unwrap_or(SystemTime::UNIX_EPOCH), m.len()));
        let mut a = self.allowed.lock().unwrap_or_else(|e| e.into_inner());
        if a.stamp.is_none() || a.stamp != stamp {
            let signers = crate::users::load(&self.allowed_signers)?
                .into_iter()
                .map(|(u, key, limits)| Signer { name: u.name, fingerprint: u.fingerprint, key, limits })
                .collect();
            *a = Allowed { stamp, signers: Arc::new(signers) };
        }
        Ok(a.signers.clone())
    }

    /// A nonce: issue time, randomness and a MAC over both, hex-encoded.
    pub fn challenge(&self) -> Result<(String, DateTime<Utc>), ApiError> {
        let mut body = [0u8; 24];
        body[..8].copy_from_slice(&now_secs().to_be_bytes());
        body[8..].copy_from_slice(&rand::random::<[u8; 16]>());
        let mac = hmac(&self.nonce_key, &body);
        let nonce = format!("{}{}", hex::encode(body), hex::encode(&mac[..16]));
        Ok((nonce, Utc::now() + chrono::Duration::from_std(NONCE_TTL).unwrap_or_default()))
    }

    /// Checks a nonce is ours, fresh and unused, and marks it used.
    fn take_nonce(&self, nonce: &str) -> Result<(), ApiError> {
        let raw = hex::decode(nonce).ok().filter(|r| r.len() == 40).ok_or_else(|| ApiError::unauthorized("unknown nonce"))?;
        let mac = hmac(&self.nonce_key, &raw[..24]);
        let diff = mac[..16].iter().zip(&raw[24..]).fold(0u8, |d, (a, b)| d | (a ^ b));
        if diff != 0 {
            return Err(ApiError::unauthorized("unknown nonce (or the daemon restarted since); get a new one"));
        }
        let issued = u64::from_be_bytes(raw[..8].try_into().unwrap_or_default());
        let now = now_secs();
        if issued > now + 5 || now.saturating_sub(issued) >= NONCE_TTL.as_secs() {
            return Err(ApiError::unauthorized("nonce expired (60 s)"));
        }
        let mut used = self.used.lock().unwrap_or_else(|e| e.into_inner());
        used.retain(|_, t| t.elapsed() < NONCE_TTL + Duration::from_secs(10));
        if used.insert(nonce.to_string(), Instant::now()).is_some() {
            return Err(ApiError::unauthorized("nonce already used"));
        }
        Ok(())
    }

    /// Verifies a signed nonce and returns `(token, fingerprint, expires_at)`.
    pub fn token(
        &self,
        nonce: &str,
        signature: &str,
        public_key: &str,
    ) -> Result<(String, String, DateTime<Utc>), ApiError> {
        self.take_nonce(nonce)?;
        let key = PublicKey::from_openssh(public_key.trim())
            .map_err(|e| ApiError::bad_request(format!("bad public_key: {e}"), "send the .pub line"))?;
        let signers = self.signers().map_err(ApiError::from)?;
        let fp = key.fingerprint(HashAlg::Sha256);
        let Some(s) = signers.iter().find(|s| s.key.key_data() == key.key_data()) else {
            return Err(ApiError::unauthorized(format!("key {fp} is not in allowed_signers")));
        };
        if let Some(why) = s.limits.check(Utc::now()) {
            return Err(ApiError::unauthorized(format!("key {fp} can't log in: its allowed_signers line is {why}")));
        }
        let name = s.name.clone();
        let sig = SshSig::from_pem(signature.trim())
            .map_err(|e| ApiError::bad_request(format!("bad signature: {e}"), "send the full -----BEGIN SSH SIGNATURE----- block"))?;
        key.verify(NAMESPACE, nonce.as_bytes(), &sig).map_err(|_| {
            ApiError::unauthorized(
                "signature does not verify; sign exactly the nonce bytes (printf %s \"$nonce\"), namespace compute",
            )
        })?;
        let token = random_hex();
        let fingerprint = fp.to_string();
        let expires = Utc::now() + chrono::Duration::hours(SESSION_TTL_HOURS);
        self.store.put_session(&hash_token(&token), &fingerprint, expires)?;
        if !name.is_empty() {
            self.store.set_key_name(&fingerprint, &name)?;
        }
        Ok((token, fingerprint, expires))
    }

    /// `(fingerprint, principal)` for a live session whose key is still allowed right now.
    pub fn session(&self, bearer: &str) -> Result<Option<(String, String)>> {
        let fp = match self.store.session(&hash_token(bearer))? {
            Some((fp, expires)) if expires > Utc::now() => fp,
            _ => return Ok(None),
        };
        let signers = self.signers()?;
        let now = Utc::now();
        Ok(signers
            .iter()
            .find(|s| s.fingerprint == fp && s.limits.check(now).is_none())
            .map(|s| (fp.clone(), if s.name.is_empty() { fp.clone() } else { s.name.clone() })))
    }
}
