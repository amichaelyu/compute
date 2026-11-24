//! Cloudflare Access: verifies the `Cf-Access-Jwt-Assertion` header Cloudflare adds to requests
//! that passed an Access policy. This is the daemon's own check, so a misconfigured Access app
//! still can't let anyone else in.
//!
//! Configured from the environment (never the repo):
//!   COMPUTE_CF_TEAM_DOMAIN   e.g. "myteam.cloudflareaccess.com"
//!   COMPUTE_CF_AUD           the Access application's audience tag
//!   COMPUTE_CF_EMAILS        comma-separated emails allowed in
//! When any is unset, Access logins are disabled.

use anyhow::{Context, Result, bail};
use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode, decode_header};
use serde::Deserialize;
use std::collections::HashMap;
use std::time::{Duration, Instant};
use tokio::process::Command;
use tokio::sync::RwLock;

const KEYS_TTL: Duration = Duration::from_secs(3600);
const MIN_REFRESH: Duration = Duration::from_secs(60);

pub struct Access {
    team_domain: String,
    aud: String,
    emails: Vec<String>,
    keys: RwLock<Keys>,
}

struct Keys {
    fetched: Option<Instant>,
    by_kid: HashMap<String, DecodingKey>,
}

#[derive(Deserialize)]
struct Jwks {
    keys: Vec<Jwk>,
}

#[derive(Deserialize)]
struct Jwk {
    kid: String,
    kty: String,
    n: Option<String>,
    e: Option<String>,
}

#[derive(Deserialize)]
struct Claims {
    email: Option<String>,
}

fn env(key: &str) -> Option<String> {
    std::env::var(key).ok().map(|v| v.trim().to_string()).filter(|v| !v.is_empty())
}

impl Access {
    pub fn from_env() -> Option<Access> {
        let team_domain = env("COMPUTE_CF_TEAM_DOMAIN")?
            .trim_start_matches("https://")
            .trim_end_matches('/')
            .to_string();
        let aud = env("COMPUTE_CF_AUD")?;
        let emails: Vec<String> = env("COMPUTE_CF_EMAILS")?
            .split(',')
            .map(|e| e.trim().to_lowercase())
            .filter(|e| !e.is_empty())
            .collect();
        if emails.is_empty() {
            return None;
        }
        Some(Access { team_domain, aud, emails, keys: RwLock::new(Keys { fetched: None, by_kid: HashMap::new() }) })
    }

    pub fn team_domain(&self) -> &str {
        &self.team_domain
    }

    async fn fetch_keys(&self) -> Result<HashMap<String, DecodingKey>> {
        let url = format!("https://{}/cdn-cgi/access/certs", self.team_domain);
        let out = tokio::time::timeout(
            Duration::from_secs(20),
            Command::new("curl").args(["-fsS", "--max-time", "15", &url]).output(),
        )
        .await
        .context("fetching Access keys timed out")??;
        if !out.status.success() {
            bail!("fetching {url} failed: {}", String::from_utf8_lossy(&out.stderr).trim());
        }
        let jwks: Jwks = serde_json::from_slice(&out.stdout).context("parsing Access keys")?;
        let mut map = HashMap::new();
        for k in jwks.keys.into_iter().filter(|k| k.kty == "RSA") {
            if let (Some(n), Some(e)) = (k.n, k.e) {
                map.insert(k.kid, DecodingKey::from_rsa_components(&n, &e)?);
            }
        }
        if map.is_empty() {
            bail!("{url} returned no RSA keys");
        }
        Ok(map)
    }

    async fn key(&self, kid: &str) -> Result<DecodingKey> {
        {
            let keys = self.keys.read().await;
            let fresh = keys.fetched.is_some_and(|t| t.elapsed() < KEYS_TTL);
            if fresh
                && let Some(k) = keys.by_kid.get(kid) {
                    return Ok(k.clone());
                }
        }
        let mut keys = self.keys.write().await;
        let may_refresh = keys.fetched.is_none_or(|t| t.elapsed() >= MIN_REFRESH);
        if may_refresh {
            keys.by_kid = self.fetch_keys().await?;
            keys.fetched = Some(Instant::now());
        }
        keys.by_kid.get(kid).cloned().with_context(|| format!("unknown Access key id {kid}"))
    }

    /// Returns the verified, allowed email for a token.
    pub async fn verify(&self, token: &str) -> Result<String> {
        let header = decode_header(token).context("malformed Access token")?;
        let kid = header.kid.context("Access token has no key id")?;
        let key = self.key(&kid).await?;
        let mut v = Validation::new(Algorithm::RS256);
        v.set_audience(&[&self.aud]);
        v.set_issuer(&[format!("https://{}", self.team_domain)]);
        v.set_required_spec_claims(&["exp", "aud", "iss"]);
        let data = decode::<Claims>(token, &key, &v).context("Access token rejected")?;
        let email = data.claims.email.context("Access token has no email")?.to_lowercase();
        if !self.emails.contains(&email) {
            bail!("{email} is not allowed");
        }
        Ok(email)
    }
}
