//! Per-key access, set by the owner on the dashboard's Users page: which backends a key may
//! use, how much it may spend on each paid one, and which of the owner's secrets its jobs may use.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Spend caps on one paid backend. None: no cap of that kind (the network's hourly budget
/// still applies). Caps decide whether a job may start; a running job is never stopped by one.
#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct SpendLimit {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub usd_per_hour: Option<f64>,
    /// Per calendar month in the daemon's time zone. A job starts only if what's spent, plus
    /// what running and placed jobs will cost by their time limit, plus its own estimate fits,
    /// so a burst of jobs can't all start under it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub usd_per_month: Option<f64>,
}

impl SpendLimit {
    pub fn is_empty(&self) -> bool {
        self.usd_per_hour.is_none() && self.usd_per_month.is_none()
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct KeyPolicy {
    /// Backends the key may use. None: the default, every backend for owners and the free
    /// ones for guests.
    #[serde(default)]
    pub backends: Option<Vec<String>>,
    /// Spend caps by paid backend name.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub limits: BTreeMap<String, SpendLimit>,
    /// Secrets shared with a guest key: its jobs (and Hugging Face datasets) may use them by
    /// name, and `GET /v1/secrets` lists them, but it can't replace or delete them. The values
    /// reach the job's environment, so the guest can read them there.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub secrets: Vec<String>,
}

impl KeyPolicy {
    pub fn allows(&self, backend: &str, paid: bool, guest: bool) -> bool {
        match &self.backends {
            Some(list) => list.iter().any(|b| b == backend),
            None => !(guest && paid),
        }
    }

    pub fn is_default(&self) -> bool {
        self.backends.is_none() && self.limits.values().all(SpendLimit::is_empty) && self.secrets.is_empty()
    }
}
