//! Times, durations and sizes as people read them, and the status facts both the text output
//! and the monitor show for a job.

use chrono::{DateTime, Local, Utc};
use serde_json::Value;

pub fn ts(v: &Value) -> Option<DateTime<Utc>> {
    v.as_str().and_then(|s| DateTime::parse_from_rfc3339(s).ok()).map(|t| t.with_timezone(&Utc))
}

/// `1h 12m`, `4m 03s`, `2d 3h`.
pub fn dur(secs: i64) -> String {
    let s = secs.max(0);
    let (d, h, m, sec) = (s / 86_400, s % 86_400 / 3600, s % 3600 / 60, s % 60);
    if d > 0 {
        format!("{d}d {h}h")
    } else if h > 0 {
        format!("{h}h {m:02}m")
    } else if m > 0 {
        format!("{m}m {sec:02}s")
    } else {
        format!("{sec}s")
    }
}

/// A clock time: `14:05` today, `Tue 14:05` within a week, else `Nov 23 14:05`.
pub fn clock(t: DateTime<Utc>) -> String {
    let l = t.with_timezone(&Local);
    let now = Local::now();
    if l.date_naive() == now.date_naive() {
        l.format("%H:%M").to_string()
    } else if (l - now).num_days().abs() < 6 {
        l.format("%a %H:%M").to_string()
    } else {
        l.format("%b %d %H:%M").to_string()
    }
}

pub fn gb(mb: f64) -> String {
    if mb >= 1024.0 { format!("{:.1} GB", mb / 1024.0) } else { format!("{mb:.0} MB") }
}

pub fn active(state: &str) -> bool {
    matches!(state, "waiting" | "queued" | "staging" | "pending" | "running")
}

/// Running, waiting, done or failed, for colors.
pub fn tone(state: &str) -> &'static str {
    match state {
        "running" => "running",
        "waiting" | "queued" | "staging" | "pending" => "waiting",
        "succeeded" | "cancelled" => "done",
        _ => "failed",
    }
}

/// Start, finish, elapsed and time left for a job (a row from the list or a full job).
pub struct Times {
    pub start: String,
    pub finish: String,
    pub elapsed: String,
    pub left: String,
    /// Fraction of the time limit used, for running jobs.
    pub used: Option<f64>,
}

pub fn times(j: &Value) -> Times {
    let now = Utc::now();
    let state = j["state"].as_str().unwrap_or("");
    let started = ts(&j["started_at"]);
    let ended = ts(&j["ended_at"]);
    let est_start = ts(&j["estimated_start"]);
    let est_finish = ts(&j["estimated_finish"]);
    let limit = j["time_s"].as_i64().or(j["resources"]["time_s"].as_i64()).unwrap_or(0);
    let dash = || "—".to_string();
    let start = match (started, est_start) {
        (Some(s), _) => clock(s),
        (None, Some(e)) if active(state) => format!("~{}", clock(e)),
        _ => dash(),
    };
    let (finish, elapsed, left, used) = if let (Some(s), Some(e)) = (started, ended) {
        (clock(e), dur((e - s).num_seconds()), dash(), None)
    } else if let Some(s) = started.filter(|_| state == "running") {
        let end = est_finish.unwrap_or(s + chrono::Duration::seconds(limit));
        let used = (limit > 0).then(|| ((now - s).num_seconds() as f64 / limit as f64).clamp(0.0, 1.0));
        (format!("by {}", clock(end)), dur((now - s).num_seconds()), dur((end - now).num_seconds()), used)
    } else if active(state) {
        let left = match est_start {
            Some(e) if e > now => format!("starts in {}", dur((e - now).num_seconds())),
            _ => "starting".into(),
        };
        (est_finish.map(|e| format!("~{}", clock(e))).unwrap_or_else(dash), dash(), left, None)
    } else {
        (ended.map(clock).unwrap_or_else(dash), dash(), dash(), None)
    };
    Times { start, finish, elapsed, left, used }
}

/// One-line usage summary from a metrics sample: `CPU 620% · 21.4 GB · GPU 83%`.
pub struct Usage {
    pub cpu_pct: Option<f64>,
    pub rss_mb: Option<f64>,
    pub gpu_util: Option<f64>,
    pub power_w: Option<f64>,
    pub disk_free_gb: Option<f64>,
    pub gpus: Vec<Value>,
    pub age_s: Option<i64>,
}

pub fn usage(latest: &Value) -> Option<Usage> {
    if !latest.is_object() {
        return None;
    }
    let gpus: Vec<Value> = latest["gpus"].as_array().cloned().unwrap_or_default();
    let avg = |k: &str| {
        let v: Vec<f64> = gpus.iter().filter_map(|g| g[k].as_f64()).collect();
        (!v.is_empty()).then(|| v.iter().sum::<f64>() / v.len() as f64)
    };
    let sum = |k: &str| {
        let v: Vec<f64> = gpus.iter().filter_map(|g| g[k].as_f64()).collect();
        (!v.is_empty()).then(|| v.iter().sum::<f64>())
    };
    Some(Usage {
        cpu_pct: latest["cpu_pct"].as_f64(),
        rss_mb: latest["rss_mb"].as_f64(),
        gpu_util: avg("util_pct"),
        power_w: sum("power_w"),
        disk_free_gb: latest["disk_free_gb"].as_f64(),
        gpus,
        age_s: latest["t"].as_i64().map(|t| Utc::now().timestamp() - t),
    })
}

/// `[█████░░░░░]` of `width` cells.
pub fn bar(frac: f64, width: usize) -> String {
    let n = ((frac.clamp(0.0, 1.0)) * width as f64).round() as usize;
    format!("{}{}", "█".repeat(n), "░".repeat(width - n))
}
