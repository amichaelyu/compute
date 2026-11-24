//! SQLite state: jobs, sessions, backend overrides and snapshot bookkeeping.

use crate::model::{Job, JobState, NewJob, Resources};
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use rusqlite::{Connection, OptionalExtension, Row, params, params_from_iter};
use std::path::Path;
use std::sync::Mutex;

pub struct Store {
    conn: Mutex<Connection>,
}

const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS arrays (
  id INTEGER PRIMARY KEY,
  created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS jobs (
  id INTEGER PRIMARY KEY,
  array_id INTEGER,
  array_index INTEGER,
  title TEXT NOT NULL,
  description TEXT NOT NULL,
  command TEXT NOT NULL,
  snapshot TEXT NOT NULL,
  resources TEXT NOT NULL,
  backends TEXT,
  os TEXT,
  env TEXT NOT NULL,
  secrets TEXT NOT NULL,
  retries_left INTEGER NOT NULL,
  priority INTEGER NOT NULL,
  idempotency_key TEXT,
  key_fingerprint TEXT NOT NULL,
  state TEXT NOT NULL,
  backend TEXT,
  handle TEXT,
  reason TEXT,
  est_start TEXT,
  est_finish TEXT,
  est_source TEXT,
  exit_code INTEGER,
  created_at TEXT NOT NULL,
  started_at TEXT,
  ended_at TEXT,
  last_seen TEXT
);
CREATE INDEX IF NOT EXISTS jobs_state ON jobs(state);
CREATE INDEX IF NOT EXISTS jobs_array ON jobs(array_id);
CREATE INDEX IF NOT EXISTS jobs_idem ON jobs(idempotency_key);
CREATE TABLE IF NOT EXISTS sessions (
  token_hash TEXT PRIMARY KEY,
  fingerprint TEXT NOT NULL,
  expires_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS backend_overrides (
  name TEXT PRIMARY KEY,
  enabled INTEGER NOT NULL,
  until TEXT
);
CREATE TABLE IF NOT EXISTS snapshots (
  id TEXT PRIMARY KEY,
  size INTEGER NOT NULL,
  created_at TEXT NOT NULL,
  last_used TEXT NOT NULL
);
"#;

fn ts(t: DateTime<Utc>) -> String {
    t.to_rfc3339()
}

fn parse_ts(s: Option<String>) -> Option<DateTime<Utc>> {
    s.and_then(|s| DateTime::parse_from_rfc3339(&s).ok()).map(|t| t.with_timezone(&Utc))
}

/// Changes to apply to a job row. `None` leaves a column unchanged; `Some(None)` clears it.
#[derive(Default, Debug)]
pub struct JobUpdate {
    pub state: Option<JobState>,
    pub backend: Option<Option<String>>,
    pub handle: Option<Option<serde_json::Value>>,
    pub reason: Option<Option<String>>,
    pub est_start: Option<Option<DateTime<Utc>>>,
    pub est_finish: Option<Option<DateTime<Utc>>>,
    pub est_source: Option<Option<String>>,
    pub exit_code: Option<Option<i32>>,
    pub started_at: Option<Option<DateTime<Utc>>>,
    pub ended_at: Option<Option<DateTime<Utc>>>,
    pub last_seen: Option<Option<DateTime<Utc>>>,
    pub retries_left: Option<u32>,
}

#[derive(Default, Debug)]
pub struct JobFilter {
    pub states: Vec<JobState>,
    pub query: Option<String>,
    pub array_id: Option<i64>,
    pub limit: Option<u32>,
}

const JOB_COLS: &str = "id, array_id, array_index, title, description, command, snapshot, \
    resources, backends, os, env, secrets, retries_left, priority, idempotency_key, \
    key_fingerprint, state, backend, handle, reason, est_start, est_finish, est_source, \
    exit_code, created_at, started_at, ended_at, last_seen";

fn row_to_job(r: &Row) -> rusqlite::Result<Job> {
    let json_err = |i: usize, e: serde_json::Error| {
        rusqlite::Error::FromSqlConversionFailure(i, rusqlite::types::Type::Text, Box::new(e))
    };
    let resources: String = r.get(7)?;
    let resources: Resources = serde_json::from_str(&resources).map_err(|e| json_err(7, e))?;
    let backends: Option<String> = r.get(8)?;
    let backends = match backends {
        Some(b) => Some(serde_json::from_str(&b).map_err(|e| json_err(8, e))?),
        None => None,
    };
    let env: String = r.get(10)?;
    let secrets: String = r.get(11)?;
    let state: String = r.get(16)?;
    let handle: Option<String> = r.get(18)?;
    Ok(Job {
        id: r.get(0)?,
        array_id: r.get(1)?,
        array_index: r.get(2)?,
        title: r.get(3)?,
        description: r.get(4)?,
        command: r.get(5)?,
        snapshot: r.get(6)?,
        resources,
        backends,
        os: r.get(9)?,
        env: serde_json::from_str(&env).map_err(|e| json_err(10, e))?,
        secrets: serde_json::from_str(&secrets).map_err(|e| json_err(11, e))?,
        retries_left: r.get(12)?,
        priority: r.get(13)?,
        idempotency_key: r.get(14)?,
        key_fingerprint: r.get(15)?,
        state: JobState::parse(&state).unwrap_or(JobState::Lost),
        backend: r.get(17)?,
        handle: handle.and_then(|h| serde_json::from_str(&h).ok()),
        reason: r.get(19)?,
        est_start: parse_ts(r.get(20)?),
        est_finish: parse_ts(r.get(21)?),
        est_source: r.get(22)?,
        exit_code: r.get(23)?,
        created_at: parse_ts(r.get(24)?).unwrap_or_else(Utc::now),
        started_at: parse_ts(r.get(25)?),
        ended_at: parse_ts(r.get(26)?),
        last_seen: parse_ts(r.get(27)?),
    })
}

impl Store {
    pub fn open(path: &Path) -> Result<Store> {
        if let Some(dir) = path.parent() {
            std::fs::create_dir_all(dir).with_context(|| format!("creating {}", dir.display()))?;
        }
        let conn = Connection::open(path).with_context(|| format!("opening {}", path.display()))?;
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "synchronous", "NORMAL")?;
        conn.pragma_update(None, "foreign_keys", "ON")?;
        conn.execute_batch(SCHEMA)?;
        Ok(Store { conn: Mutex::new(conn) })
    }

    fn conn(&self) -> std::sync::MutexGuard<'_, Connection> {
        self.conn.lock().unwrap_or_else(|e| e.into_inner())
    }

    pub fn new_array(&self) -> Result<i64> {
        let c = self.conn();
        c.execute("INSERT INTO arrays(created_at) VALUES (?1)", params![ts(Utc::now())])?;
        Ok(c.last_insert_rowid())
    }

    pub fn insert_jobs(&self, jobs: &[NewJob]) -> Result<Vec<i64>> {
        let mut c = self.conn();
        let tx = c.transaction()?;
        let mut ids = Vec::with_capacity(jobs.len());
        {
            let mut stmt = tx.prepare(
                "INSERT INTO jobs (array_id, array_index, title, description, command, snapshot, \
                 resources, backends, os, env, secrets, retries_left, priority, idempotency_key, \
                 key_fingerprint, state, created_at) \
                 VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,'queued',?16)",
            )?;
            let now = ts(Utc::now());
            for j in jobs {
                stmt.execute(params![
                    j.array_id,
                    j.array_index,
                    j.title,
                    j.description,
                    j.command,
                    j.snapshot,
                    serde_json::to_string(&j.resources)?,
                    j.backends.as_ref().map(serde_json::to_string).transpose()?,
                    j.os,
                    serde_json::to_string(&j.env)?,
                    serde_json::to_string(&j.secrets)?,
                    j.retries,
                    j.priority,
                    j.idempotency_key,
                    j.key_fingerprint,
                    now,
                ])?;
                ids.push(tx.last_insert_rowid());
            }
        }
        tx.commit()?;
        Ok(ids)
    }

    pub fn get_job(&self, id: i64) -> Result<Option<Job>> {
        let c = self.conn();
        let sql = format!("SELECT {JOB_COLS} FROM jobs WHERE id = ?1");
        Ok(c.query_row(&sql, params![id], row_to_job).optional()?)
    }

    pub fn list_jobs(&self, f: &JobFilter) -> Result<Vec<Job>> {
        let c = self.conn();
        let mut sql = format!("SELECT {JOB_COLS} FROM jobs WHERE 1=1");
        let mut args: Vec<rusqlite::types::Value> = Vec::new();
        if !f.states.is_empty() {
            let marks: Vec<String> = f
                .states
                .iter()
                .map(|s| {
                    args.push(s.as_str().to_string().into());
                    format!("?{}", args.len())
                })
                .collect();
            sql.push_str(&format!(" AND state IN ({})", marks.join(",")));
        }
        if let Some(q) = &f.query {
            args.push(format!("%{q}%").into());
            let n = args.len();
            sql.push_str(&format!(" AND (title LIKE ?{n} OR description LIKE ?{n})"));
        }
        if let Some(a) = f.array_id {
            args.push(a.into());
            sql.push_str(&format!(" AND array_id = ?{}", args.len()));
        }
        sql.push_str(" ORDER BY id DESC");
        if let Some(l) = f.limit {
            sql.push_str(&format!(" LIMIT {l}"));
        }
        let mut stmt = c.prepare(&sql)?;
        let rows = stmt.query_map(params_from_iter(args), row_to_job)?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    }

    /// Jobs in the given states, oldest first (scheduler order is decided by the caller).
    pub fn jobs_in_states(&self, states: &[JobState]) -> Result<Vec<Job>> {
        let mut jobs = self.list_jobs(&JobFilter { states: states.to_vec(), ..Default::default() })?;
        jobs.reverse();
        Ok(jobs)
    }

    pub fn jobs_by_idempotency_key(&self, key: &str) -> Result<Vec<Job>> {
        let c = self.conn();
        let sql = format!("SELECT {JOB_COLS} FROM jobs WHERE idempotency_key = ?1 ORDER BY id");
        let mut stmt = c.prepare(&sql)?;
        let rows = stmt.query_map(params![key], row_to_job)?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    }

    pub fn update_job(&self, id: i64, u: JobUpdate) -> Result<()> {
        let mut sets: Vec<String> = Vec::new();
        let mut args: Vec<rusqlite::types::Value> = Vec::new();
        let mut push = |col: &str, v: rusqlite::types::Value| {
            args.push(v);
            sets.push(format!("{col} = ?{}", args.len()));
        };
        use rusqlite::types::Value as V;
        let opt_s = |o: Option<String>| o.map(V::Text).unwrap_or(V::Null);
        let opt_t = |o: Option<DateTime<Utc>>| o.map(|t| V::Text(ts(t))).unwrap_or(V::Null);
        if let Some(s) = u.state {
            push("state", V::Text(s.as_str().into()));
        }
        if let Some(v) = u.backend {
            push("backend", opt_s(v));
        }
        if let Some(v) = u.handle {
            push("handle", opt_s(v.map(|h| h.to_string())));
        }
        if let Some(v) = u.reason {
            push("reason", opt_s(v));
        }
        if let Some(v) = u.est_start {
            push("est_start", opt_t(v));
        }
        if let Some(v) = u.est_finish {
            push("est_finish", opt_t(v));
        }
        if let Some(v) = u.est_source {
            push("est_source", opt_s(v));
        }
        if let Some(v) = u.exit_code {
            push("exit_code", v.map(|c| V::Integer(c.into())).unwrap_or(V::Null));
        }
        if let Some(v) = u.started_at {
            push("started_at", opt_t(v));
        }
        if let Some(v) = u.ended_at {
            push("ended_at", opt_t(v));
        }
        if let Some(v) = u.last_seen {
            push("last_seen", opt_t(v));
        }
        if let Some(v) = u.retries_left {
            push("retries_left", V::Integer(v.into()));
        }
        if sets.is_empty() {
            return Ok(());
        }
        args.push(id.into());
        let sql = format!("UPDATE jobs SET {} WHERE id = ?{}", sets.join(", "), args.len());
        self.conn().execute(&sql, params_from_iter(args))?;
        Ok(())
    }

    // ---- sessions ----

    pub fn put_session(&self, token_hash: &str, fingerprint: &str, expires: DateTime<Utc>) -> Result<()> {
        let c = self.conn();
        c.execute("DELETE FROM sessions WHERE expires_at < ?1", params![ts(Utc::now())])?;
        c.execute(
            "INSERT INTO sessions(token_hash, fingerprint, expires_at) VALUES (?1, ?2, ?3)",
            params![token_hash, fingerprint, ts(expires)],
        )?;
        Ok(())
    }

    pub fn session(&self, token_hash: &str) -> Result<Option<(String, DateTime<Utc>)>> {
        let c = self.conn();
        let row: Option<(String, String)> = c
            .query_row(
                "SELECT fingerprint, expires_at FROM sessions WHERE token_hash = ?1",
                params![token_hash],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()?;
        Ok(row.and_then(|(f, e)| parse_ts(Some(e)).map(|e| (f, e))))
    }

    // ---- backend overrides ----

    pub fn backend_override(&self, name: &str) -> Result<Option<(bool, Option<DateTime<Utc>>)>> {
        let c = self.conn();
        let row: Option<(bool, Option<String>)> = c
            .query_row(
                "SELECT enabled, until FROM backend_overrides WHERE name = ?1",
                params![name],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()?;
        Ok(row.map(|(e, u)| (e, parse_ts(u))))
    }

    pub fn set_backend_override(&self, name: &str, enabled: bool, until: Option<DateTime<Utc>>) -> Result<()> {
        self.conn().execute(
            "INSERT INTO backend_overrides(name, enabled, until) VALUES (?1, ?2, ?3) \
             ON CONFLICT(name) DO UPDATE SET enabled = excluded.enabled, until = excluded.until",
            params![name, enabled, until.map(ts)],
        )?;
        Ok(())
    }

    pub fn clear_backend_override(&self, name: &str) -> Result<()> {
        self.conn().execute("DELETE FROM backend_overrides WHERE name = ?1", params![name])?;
        Ok(())
    }

    // ---- snapshots ----

    pub fn snapshot_seen(&self, id: &str, size: u64) -> Result<()> {
        let now = ts(Utc::now());
        self.conn().execute(
            "INSERT INTO snapshots(id, size, created_at, last_used) VALUES (?1, ?2, ?3, ?3) \
             ON CONFLICT(id) DO UPDATE SET last_used = excluded.last_used",
            params![id, size as i64, now],
        )?;
        Ok(())
    }

    /// Snapshots not used for `days` and not referenced by any unfinished job.
    pub fn stale_snapshots(&self, days: i64) -> Result<Vec<String>> {
        let cutoff = ts(Utc::now() - chrono::Duration::days(days));
        let c = self.conn();
        let mut stmt = c.prepare(
            "SELECT id FROM snapshots WHERE last_used < ?1 AND id NOT IN \
             (SELECT snapshot FROM jobs WHERE state IN ('queued','staging','pending','running'))",
        )?;
        let rows = stmt.query_map(params![cutoff], |r| r.get::<_, String>(0))?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    }

    pub fn forget_snapshot(&self, id: &str) -> Result<()> {
        self.conn().execute("DELETE FROM snapshots WHERE id = ?1", params![id])?;
        Ok(())
    }
}
