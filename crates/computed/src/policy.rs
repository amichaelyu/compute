//! Per-key access, set by the owner on the dashboard's Users page: which backends a key may
//! use, and how much it may spend on each paid one.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Spend caps on one paid backend. None: no cap of that kind (the network's hourly budget
/// still applies).
#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct SpendLimit {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub usd_per_hour: Option<f64>,
    /// Per calendar month in the daemon's time zone, counting what running and placed jobs
    /// will cost by their time limit, so a cap isn't overrun by jobs already started.
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
}

impl KeyPolicy {
    pub fn allows(&self, backend: &str, paid: bool, guest: bool) -> bool {
        match &self.backends {
            Some(list) => list.iter().any(|b| b == backend),
            None => !(guest && paid),
        }
    }

    pub fn is_default(&self) -> bool {
        self.backends.is_none() && self.limits.values().all(SpendLimit::is_empty)
    }
}
