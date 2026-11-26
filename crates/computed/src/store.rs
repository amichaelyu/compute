//! SQLite state: jobs, sessions, backend overrides and snapshot bookkeeping.

use crate::model::{Job, JobSpec, JobState, NewJob, Resources};
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use rusqlite::{Connection, OptionalExtension, Row, params, params_from_iter};
use std::path::Path;
use std::sync::Mutex;

pub struct Store {
    conn: Mutex<Connection>,
}

/// `(request_hash, backend, estimated_cost, expires_at)`
pub type QuoteRow = (String, Option<String>, Option<f64>, DateTime<Utc>);
/// `(job_id, backend, usd_per_hour, started, ended, cost)`
pub type CostRow = (i64, String, f64, DateTime<Utc>, Option<DateTime<Utc>>, Option<f64>);

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
CREATE TABLE IF NOT EXISTS datasets (
  name TEXT NOT NULL,
  version TEXT NOT NULL,
  source TEXT NOT NULL,
  size INTEGER NOT NULL,
  files INTEGER NOT NULL,
  description TEXT NOT NULL,
  created_at TEXT NOT NULL,
  PRIMARY KEY (name, version)
);
CREATE TABLE IF NOT EXISTS dataset_copies (
  name TEXT NOT NULL,
  version TEXT NOT NULL,
  backend TEXT NOT NULL,
  path TEXT NOT NULL,
  size INTEGER NOT NULL,
  owned INTEGER NOT NULL,
  created_at TEXT NOT NULL,
  last_used TEXT NOT NULL,
  PRIMARY KEY (name, version, backend)
);
CREATE TABLE IF NOT EXISTS images (
  tag TEXT PRIMARY KEY,
  dockerfile TEXT NOT NULL,
  context TEXT NOT NULL,
  snapshot TEXT NOT NULL,
  state TEXT NOT NULL,
  builder TEXT,
  error TEXT,
  created_at TEXT NOT NULL,
  finished_at TEXT
);
CREATE TABLE IF NOT EXISTS quotes (
  id TEXT PRIMARY KEY,
  request_hash TEXT NOT NULL,
  backend TEXT,
  estimated_cost REAL,
  expires_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS key_names (
  fingerprint TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS cancels (
  job_id INTEGER PRIMARY KEY,
  by_name TEXT NOT NULL,
  by_key TEXT NOT NULL,
  at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS outputs_deleted (
  job_id INTEGER PRIMARY KEY,
  deleted_at TEXT NOT NULL,
  why TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS ownership (
  kind TEXT NOT NULL,
  id TEXT NOT NULL,
  fingerprint TEXT NOT NULL,
  created_at TEXT NOT NULL,
  PRIMARY KEY (kind, id, fingerprint)
);
CREATE TABLE IF NOT EXISTS reports (
  id INTEGER PRIMARY KEY,
  kind TEXT NOT NULL,
  title TEXT NOT NULL,
  body TEXT NOT NULL,
  job_id INTEGER,
  reporter TEXT NOT NULL,
  fingerprint TEXT NOT NULL,
  state TEXT NOT NULL,
  created_at TEXT NOT NULL,
  closed_at TEXT,
  closed_by TEXT,
  note TEXT
);
CREATE INDEX IF NOT EXISTS reports_state ON reports(state);
CREATE TABLE IF NOT EXISTS cloud_costs (
  job_id INTEGER PRIMARY KEY,
  backend TEXT NOT NULL,
  rate_usd_h REAL NOT NULL,
  started_at TEXT NOT NULL,
  ended_at TEXT,
  cost_usd REAL
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
    /// Only jobs with a smaller id (paging back through the list).
    pub before: Option<i64>,
    pub limit: Option<u32>,
}

const JOB_COLS: &str = "id, array_id, array_index, title, description, command, snapshot, \
    resources, backends, os, env, secrets, retries_left, priority, idempotency_key, \
    key_fingerprint, state, backend, handle, reason, est_start, est_finish, est_source, \
    exit_code, created_at, started_at, ended_at, last_seen, spec";

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
        spec: {
            let spec: String = r.get(28)?;
            serde_json::from_str::<JobSpec>(&spec).map_err(|e| json_err(28, e))?
        },
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
        // Columns added after M1.
        let cols: Vec<String> = conn
            .prepare("SELECT name FROM pragma_table_info('jobs')")?
            .query_map([], |r| r.get::<_, String>(0))?
            .collect::<rusqlite::Result<_>>()?;
        if !cols.iter().any(|c| c == "spec") {
            conn.execute_batch("ALTER TABLE jobs ADD COLUMN spec TEXT NOT NULL DEFAULT '{}'")?;
        }
        // Cost of earlier attempts of the same job (retries reuse the row).
        let cost_cols: Vec<String> = conn
            .prepare("SELECT name FROM pragma_table_info('cloud_costs')")?
            .query_map([], |r| r.get::<_, String>(0))?
            .collect::<rusqlite::Result<_>>()?;
        if !cost_cols.iter().any(|c| c == "prior_usd") {
            conn.execute_batch("ALTER TABLE cloud_costs ADD COLUMN prior_usd REAL NOT NULL DEFAULT 0")?;
        }
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
                 key_fingerprint, state, created_at, spec) \
                 VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18)",
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
                    if j.spec.has_deps() { JobState::Waiting.as_str() } else { JobState::Queued.as_str() },
                    now,
                    serde_json::to_string(&j.spec)?,
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
        if let Some(b) = f.before {
            args.push(b.into());
            sql.push_str(&format!(" AND id < ?{}", args.len()));
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
        self.update_job_if(id, &[], u).map(|_| ())
    }

    /// Applies `u` only while the job is in one of `states` (any state when empty), so a write
    /// based on an older read can't undo a cancel or another transition. Returns whether it
    /// applied.
    pub fn update_job_if(&self, id: i64, states: &[JobState], u: JobUpdate) -> Result<bool> {
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
            return Ok(true);
        }
        args.push(id.into());
        let mut sql = format!("UPDATE jobs SET {} WHERE id = ?{}", sets.join(", "), args.len());
        if !states.is_empty() {
            let list: Vec<String> = states.iter().map(|s| format!("'{}'", s.as_str())).collect();
            sql.push_str(&format!(" AND state IN ({})", list.join(",")));
        }
        Ok(self.conn().execute(&sql, params_from_iter(args))? > 0)
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

    /// Ends every session of a key (when it's removed); returns how many there were.
    pub fn delete_sessions(&self, fingerprint: &str) -> Result<usize> {
        Ok(self.conn().execute("DELETE FROM sessions WHERE fingerprint = ?1", params![fingerprint])?)
    }

    // ---- who did what ----

    /// The nickname (allowed_signers principal) of an SSH key, recorded at login. Kept after
    /// the key is removed, so old jobs still show who submitted them.
    pub fn set_key_name(&self, fingerprint: &str, name: &str) -> Result<()> {
        self.conn().execute(
            "INSERT INTO key_names(fingerprint, name, updated_at) VALUES (?1, ?2, ?3) \
             ON CONFLICT(fingerprint) DO UPDATE SET name = excluded.name, updated_at = excluded.updated_at",
            params![fingerprint, name, ts(Utc::now())],
        )?;
        Ok(())
    }

    pub fn key_name(&self, fingerprint: &str) -> Result<Option<String>> {
        let c = self.conn();
        Ok(c.query_row("SELECT name FROM key_names WHERE fingerprint = ?1", params![fingerprint], |r| r.get(0)).optional()?)
    }

    /// Records that `fingerprint` created (or, for content-addressed snapshots, uploaded) the
    /// `kind` object `id`: `snapshot`, `secret` or `dataset`.
    pub fn claim(&self, kind: &str, id: &str, fingerprint: &str) -> Result<()> {
        self.conn().execute(
            "INSERT OR IGNORE INTO ownership(kind, id, fingerprint, created_at) VALUES (?1, ?2, ?3, ?4)",
            params![kind, id, fingerprint, ts(Utc::now())],
        )?;
        Ok(())
    }

    pub fn owns(&self, kind: &str, id: &str, fingerprint: &str) -> Result<bool> {
        let c = self.conn();
        Ok(c.query_row(
            "SELECT 1 FROM ownership WHERE kind = ?1 AND id = ?2 AND fingerprint = ?3",
            params![kind, id, fingerprint],
            |_| Ok(()),
        )
        .optional()?
        .is_some())
    }

    pub fn disown_all(&self, kind: &str, id: &str) -> Result<()> {
        self.conn().execute("DELETE FROM ownership WHERE kind = ?1 AND id = ?2", params![kind, id])?;
        Ok(())
    }

    /// Replaces dashboard logins' emails in cancel records and job reasons (which every key can
    /// read) with `display`. Returns how many cancels changed.
    pub fn scrub_dashboard_emails(&self, display: &str) -> Result<usize> {
        let mut c = self.conn();
        let tx = c.transaction()?;
        let rows: Vec<(i64, String)> = {
            let mut st = tx.prepare("SELECT job_id, by_name FROM cancels WHERE by_key LIKE 'cf:%'")?;
            st.query_map([], |r| Ok((r.get(0)?, r.get(1)?)))?.collect::<rusqlite::Result<_>>()?
        };
        for (job, email) in &rows {
            tx.execute("UPDATE jobs SET reason = replace(reason, ?1, ?2) WHERE id = ?3", params![email, display, job])?;
        }
        tx.execute("UPDATE cancels SET by_name = ?1, by_key = 'dashboard' WHERE by_key LIKE 'cf:%'", params![display])?;
        tx.commit()?;
        Ok(rows.len())
    }

    pub fn insert_report(&self, r: &NewReport) -> Result<(i64, DateTime<Utc>)> {
        let c = self.conn();
        let now = Utc::now();
        c.execute(
            "INSERT INTO reports(kind, title, body, job_id, reporter, fingerprint, state, created_at) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'open', ?7)",
            params![r.kind, r.title, r.body, r.job_id, r.reporter, r.fingerprint, ts(now)],
        )?;
        Ok((c.last_insert_rowid(), now))
    }

    /// Reports newest first: `state` "open", "closed" or None for both; `fingerprint` limits
    /// them to one key's.
    pub fn reports(&self, state: Option<&str>, fingerprint: Option<&str>, limit: u32) -> Result<Vec<ReportRow>> {
        let c = self.conn();
        let mut st = c.prepare(
            "SELECT id, kind, title, body, job_id, reporter, fingerprint, state, created_at, closed_at, closed_by, note \
             FROM reports WHERE (?1 IS NULL OR state = ?1) AND (?2 IS NULL OR fingerprint = ?2) ORDER BY id DESC LIMIT ?3",
        )?;
        let rows = st.query_map(params![state, fingerprint, limit], report_row)?.collect::<rusqlite::Result<_>>()?;
        Ok(rows)
    }

    pub fn report(&self, id: i64) -> Result<Option<ReportRow>> {
        let c = self.conn();
        Ok(c.query_row(
            "SELECT id, kind, title, body, job_id, reporter, fingerprint, state, created_at, closed_at, closed_by, note \
             FROM reports WHERE id = ?1",
            params![id],
            report_row,
        )
        .optional()?)
    }

    /// Reports one key filed since `since` (the daily cap).
    pub fn reports_since(&self, fingerprint: &str, since: DateTime<Utc>) -> Result<u32> {
        let c = self.conn();
        Ok(c.query_row(
            "SELECT COUNT(*) FROM reports WHERE fingerprint = ?1 AND created_at >= ?2",
            params![fingerprint, ts(since)],
            |r| r.get(0),
        )?)
    }

    pub fn close_report(&self, id: i64, by: &str, note: Option<&str>) -> Result<bool> {
        let n = self.conn().execute(
            "UPDATE reports SET state = 'closed', closed_at = ?2, closed_by = ?3, note = ?4 WHERE id = ?1 AND state = 'open'",
            params![id, ts(Utc::now()), by, note],
        )?;
        Ok(n > 0)
    }

    /// Records who cancelled a job (the first cancel counts).
    pub fn record_cancel(&self, job_id: i64, by_name: &str, by_key: &str) -> Result<()> {
        self.conn().execute(
            "INSERT OR IGNORE INTO cancels(job_id, by_name, by_key, at) VALUES (?1, ?2, ?3, ?4)",
            params![job_id, by_name, by_key, ts(Utc::now())],
        )?;
        Ok(())
    }

    /// `(name, key, at)` of whoever cancelled the job.
    pub fn cancel_of(&self, job_id: i64) -> Result<Option<(String, String, DateTime<Utc>)>> {
        let c = self.conn();
        let row: Option<(String, String, String)> = c
            .query_row("SELECT by_name, by_key, at FROM cancels WHERE job_id = ?1", params![job_id], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
            .optional()?;
        Ok(row.and_then(|(n, k, t)| Some((n, k, parse_ts(Some(t))?))))
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
             (SELECT snapshot FROM jobs WHERE state IN ('waiting','queued','staging','pending','running'))",
        )?;
        let rows = stmt.query_map(params![cutoff], |r| r.get::<_, String>(0))?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    }

    pub fn forget_snapshot(&self, id: &str) -> Result<()> {
        self.conn().execute("DELETE FROM snapshots WHERE id = ?1", params![id])?;
        Ok(())
    }

    pub fn jobs_in_array(&self, array_id: i64) -> Result<Vec<Job>> {
        let mut v = self.list_jobs(&JobFilter { array_id: Some(array_id), ..Default::default() })?;
        v.reverse();
        Ok(v)
    }

    // ---- datasets ----

    pub fn insert_dataset(&self, d: &DatasetRow) -> Result<bool> {
        let n = self.conn().execute(
            "INSERT OR IGNORE INTO datasets(name, version, source, size, files, description, created_at) \
             VALUES (?1,?2,?3,?4,?5,?6,?7)",
            params![d.name, d.version, d.source.to_string(), d.size as i64, d.files as i64, d.description, ts(d.created_at)],
        )?;
        Ok(n > 0)
    }

    pub fn dataset_versions(&self, name: Option<&str>) -> Result<Vec<DatasetRow>> {
        let c = self.conn();
        let mut stmt = c.prepare(
            "SELECT name, version, source, size, files, description, created_at FROM datasets \
             WHERE (?1 IS NULL OR name = ?1) ORDER BY name, created_at DESC",
        )?;
        let rows = stmt.query_map(params![name], |r| {
            let src: String = r.get(2)?;
            Ok(DatasetRow {
                name: r.get(0)?,
                version: r.get(1)?,
                source: serde_json::from_str(&src).unwrap_or(serde_json::Value::Null),
                size: r.get::<_, i64>(3)? as u64,
                files: r.get::<_, i64>(4)? as u64,
                description: r.get(5)?,
                created_at: parse_ts(r.get(6)?).unwrap_or_else(Utc::now),
            })
        })?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    }

    pub fn dataset_copies(&self, backend: Option<&str>, name: Option<&str>) -> Result<Vec<CopyRow>> {
        let c = self.conn();
        let mut stmt = c.prepare(
            "SELECT name, version, backend, path, size, owned, created_at, last_used FROM dataset_copies \
             WHERE (?1 IS NULL OR backend = ?1) AND (?2 IS NULL OR name = ?2) ORDER BY last_used",
        )?;
        let rows = stmt.query_map(params![backend, name], |r| {
            Ok(CopyRow {
                name: r.get(0)?,
                version: r.get(1)?,
                backend: r.get(2)?,
                path: r.get(3)?,
                size: r.get::<_, i64>(4)? as u64,
                owned: r.get(5)?,
                created_at: parse_ts(r.get(6)?).unwrap_or_else(Utc::now),
                last_used: parse_ts(r.get(7)?).unwrap_or_else(Utc::now),
            })
        })?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    }

    pub fn put_copy(&self, c: &CopyRow) -> Result<()> {
        self.conn().execute(
            "INSERT INTO dataset_copies(name, version, backend, path, size, owned, created_at, last_used) \
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8) ON CONFLICT(name, version, backend) DO UPDATE SET \
             path = excluded.path, size = excluded.size, owned = excluded.owned, last_used = excluded.last_used",
            params![c.name, c.version, c.backend, c.path, c.size as i64, c.owned, ts(c.created_at), ts(c.last_used)],
        )?;
        Ok(())
    }

    pub fn touch_copy(&self, name: &str, version: &str, backend: &str) -> Result<()> {
        self.conn().execute(
            "UPDATE dataset_copies SET last_used = ?4 WHERE name = ?1 AND version = ?2 AND backend = ?3",
            params![name, version, backend, ts(Utc::now())],
        )?;
        Ok(())
    }

    pub fn delete_copy(&self, name: &str, version: &str, backend: &str) -> Result<()> {
        self.conn().execute(
            "DELETE FROM dataset_copies WHERE name = ?1 AND version = ?2 AND backend = ?3",
            params![name, version, backend],
        )?;
        Ok(())
    }

    // ---- images ----

    pub fn image(&self, tag: &str) -> Result<Option<ImageRow>> {
        let c = self.conn();
        Ok(c.query_row(
            "SELECT tag, dockerfile, context, snapshot, state, builder, error, created_at, finished_at FROM images WHERE tag = ?1",
            params![tag],
            row_to_image,
        )
        .optional()?)
    }

    pub fn images(&self, limit: u32) -> Result<Vec<ImageRow>> {
        let c = self.conn();
        let mut stmt = c.prepare(&format!(
            "SELECT tag, dockerfile, context, snapshot, state, builder, error, created_at, finished_at FROM images \
             ORDER BY created_at DESC LIMIT {limit}"
        ))?;
        let rows = stmt.query_map([], row_to_image)?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    }

    pub fn put_image(&self, i: &ImageRow) -> Result<()> {
        self.conn().execute(
            "INSERT INTO images(tag, dockerfile, context, snapshot, state, builder, error, created_at, finished_at) \
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9) ON CONFLICT(tag) DO UPDATE SET snapshot = excluded.snapshot, \
             state = excluded.state, builder = excluded.builder, error = excluded.error, \
             created_at = excluded.created_at, finished_at = excluded.finished_at",
            params![i.tag, i.dockerfile, i.context, i.snapshot, i.state, i.builder, i.error, ts(i.created_at), i.finished_at.map(ts)],
        )?;
        Ok(())
    }

    // ---- quotes ----

    pub fn put_quote(&self, id: &str, hash: &str, backend: Option<&str>, cost: Option<f64>, expires: DateTime<Utc>) -> Result<()> {
        let c = self.conn();
        c.execute("DELETE FROM quotes WHERE expires_at < ?1", params![ts(Utc::now() - chrono::Duration::days(1))])?;
        c.execute(
            "INSERT INTO quotes(id, request_hash, backend, estimated_cost, expires_at) VALUES (?1,?2,?3,?4,?5)",
            params![id, hash, backend, cost, ts(expires)],
        )?;
        Ok(())
    }

    /// `(request_hash, backend, estimated_cost, expires_at)`
    pub fn quote(&self, id: &str) -> Result<Option<QuoteRow>> {
        let c = self.conn();
        let row: Option<(String, Option<String>, Option<f64>, String)> = c
            .query_row(
                "SELECT request_hash, backend, estimated_cost, expires_at FROM quotes WHERE id = ?1",
                params![id],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
            )
            .optional()?;
        Ok(row.and_then(|(h, b, c, e)| parse_ts(Some(e)).map(|e| (h, b, c, e))))
    }

    /// When a snapshot was first uploaded.
    pub fn snapshot_created(&self, id: &str) -> Result<Option<DateTime<Utc>>> {
        let c = self.conn();
        let t: Option<String> = c.query_row("SELECT created_at FROM snapshots WHERE id = ?1", params![id], |r| r.get(0)).optional()?;
        Ok(parse_ts(t))
    }

    /// Records that a job's outputs were deleted from its backend.
    pub fn mark_outputs_deleted(&self, job_id: i64, why: &str) -> Result<()> {
        self.conn().execute(
            "INSERT OR REPLACE INTO outputs_deleted(job_id, deleted_at, why) VALUES (?1, ?2, ?3)",
            params![job_id, ts(Utc::now()), why],
        )?;
        Ok(())
    }

    /// When and why a job's outputs were deleted, if they were.
    pub fn outputs_deleted(&self, job_id: i64) -> Result<Option<(DateTime<Utc>, String)>> {
        let c = self.conn();
        let row: Option<(String, String)> = c
            .query_row("SELECT deleted_at, why FROM outputs_deleted WHERE job_id = ?1", params![job_id], |r| Ok((r.get(0)?, r.get(1)?)))
            .optional()?;
        Ok(row.and_then(|(t, w)| Some((parse_ts(Some(t))?, w))))
    }

    /// Finished jobs on `backend` that ended before `before` and still have outputs there.
    pub fn outputs_to_expire(&self, backend: &str, before: DateTime<Utc>, limit: u32) -> Result<Vec<i64>> {
        let c = self.conn();
        let mut stmt = c.prepare(
            "SELECT id FROM jobs WHERE backend = ?1 AND ended_at IS NOT NULL AND ended_at < ?2 \
             AND state IN ('succeeded','failed','cancelled','timeout','lost') \
             AND id NOT IN (SELECT job_id FROM outputs_deleted) ORDER BY id LIMIT ?3",
        )?;
        let ids = stmt.query_map(params![backend, ts(before), limit], |r| r.get(0))?.collect::<rusqlite::Result<Vec<i64>>>()?;
        Ok(ids)
    }

    /// Median run time in seconds of the last few succeeded jobs with this title (array tasks
    /// share their title), for estimating when a running job will really finish.
    pub fn typical_secs(&self, title: &str) -> Result<Option<i64>> {
        let c = self.conn();
        let mut stmt = c.prepare(
            "SELECT started_at, ended_at FROM jobs WHERE title = ?1 AND state = 'succeeded' \
             AND started_at IS NOT NULL AND ended_at IS NOT NULL ORDER BY id DESC LIMIT 7",
        )?;
        let mut secs: Vec<i64> = stmt
            .query_map(params![title], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))?
            .filter_map(|r| r.ok())
            .filter_map(|(a, b)| Some((parse_ts(Some(b))? - parse_ts(Some(a))?).num_seconds()))
            .filter(|s| *s > 0)
            .collect();
        if secs.is_empty() {
            return Ok(None);
        }
        secs.sort_unstable();
        Ok(Some(secs[secs.len() / 2]))
    }

    // ---- cloud cost ledger ----

    /// Opens a cost record when an instance is created. A retry of the same job carries the
    /// earlier attempts' cost forward in `prior_usd` (closing an attempt left open at `started`).
    pub fn cost_start(&self, job_id: i64, backend: &str, rate: f64, started: DateTime<Utc>) -> Result<()> {
        let c = self.conn();
        let prev: Option<(f64, String, Option<f64>, f64)> = c
            .query_row(
                "SELECT rate_usd_h, started_at, cost_usd, prior_usd FROM cloud_costs WHERE job_id = ?1",
                params![job_id],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
            )
            .optional()?;
        let prior = match prev {
            // cost_usd already includes the attempts before it.
            Some((_, _, Some(cost), _)) => cost,
            Some((rate, st, None, prior)) => {
                let st = parse_ts(Some(st)).unwrap_or(started);
                prior + (rate * (started - st).num_seconds().max(0) as f64 / 3600.0 * 100.0).ceil() / 100.0
            }
            None => 0.0,
        };
        c.execute(
            "INSERT INTO cloud_costs(job_id, backend, rate_usd_h, started_at, prior_usd) VALUES (?1,?2,?3,?4,?5) \
             ON CONFLICT(job_id) DO UPDATE SET backend = excluded.backend, rate_usd_h = excluded.rate_usd_h, \
             started_at = excluded.started_at, ended_at = NULL, cost_usd = NULL, prior_usd = excluded.prior_usd",
            params![job_id, backend, rate, ts(started), prior],
        )?;
        Ok(())
    }

    pub fn cost_end(&self, job_id: i64, ended: DateTime<Utc>) -> Result<Option<f64>> {
        let c = self.conn();
        let row: Option<(f64, String, Option<String>, f64)> = c
            .query_row(
                "SELECT rate_usd_h, started_at, ended_at, prior_usd FROM cloud_costs WHERE job_id = ?1",
                params![job_id],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
            )
            .optional()?;
        let Some((rate, started, already, prior)) = row else { return Ok(None) };
        if already.is_some() {
            return Ok(None);
        }
        let started = parse_ts(Some(started)).unwrap_or(ended);
        let hours = (ended - started).num_seconds().max(0) as f64 / 3600.0;
        let cost = prior + (rate * hours * 100.0).ceil() / 100.0;
        c.execute(
            "UPDATE cloud_costs SET ended_at = ?2, cost_usd = ?3 WHERE job_id = ?1",
            params![job_id, ts(ended), cost],
        )?;
        Ok(Some(cost))
    }

    /// `(rate, cost)` of one job's cloud record.
    pub fn cost_of(&self, job_id: i64) -> Result<Option<(f64, Option<f64>)>> {
        let c = self.conn();
        Ok(c.query_row("SELECT rate_usd_h, cost_usd FROM cloud_costs WHERE job_id = ?1", params![job_id], |r| Ok((r.get(0)?, r.get(1)?)))
            .optional()?)
    }

    /// Rows since `since`: `(job_id, backend, rate, started, ended, cost)`. For an open row,
    /// `cost` is what earlier attempts of the job cost (None when there were none).
    pub fn costs_since(&self, since: DateTime<Utc>) -> Result<Vec<CostRow>> {
        let c = self.conn();
        let mut stmt = c.prepare(
            "SELECT job_id, backend, rate_usd_h, started_at, ended_at, \
             CASE WHEN ended_at IS NULL AND prior_usd > 0 THEN prior_usd ELSE cost_usd END FROM cloud_costs \
             WHERE ended_at IS NULL OR ended_at >= ?1 ORDER BY started_at",
        )?;
        let rows = stmt.query_map(params![ts(since)], |r| {
            Ok((
                r.get(0)?,
                r.get(1)?,
                r.get(2)?,
                parse_ts(r.get(3)?).unwrap_or_else(Utc::now),
                parse_ts(r.get(4)?),
                r.get(5)?,
            ))
        })?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    }
}

#[derive(Clone, Debug)]
pub struct DatasetRow {
    pub name: String,
    pub version: String,
    pub source: serde_json::Value,
    pub size: u64,
    pub files: u64,
    pub description: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Clone, Debug)]
pub struct CopyRow {
    pub name: String,
    pub version: String,
    pub backend: String,
    pub path: String,
    pub size: u64,
    /// True when the daemon made this copy (and may delete it); false for a dataset's own
    /// source directory on its host.
    pub owned: bool,
    pub created_at: DateTime<Utc>,
    pub last_used: DateTime<Utc>,
}

#[derive(Clone, Debug)]
pub struct ImageRow {
    pub tag: String,
    pub dockerfile: String,
    pub context: String,
    pub snapshot: String,
    /// building | ready | failed
    pub state: String,
    pub builder: Option<String>,
    pub error: Option<String>,
    pub created_at: DateTime<Utc>,
    pub finished_at: Option<DateTime<Utc>>,
}

fn row_to_image(r: &Row) -> rusqlite::Result<ImageRow> {
    Ok(ImageRow {
        tag: r.get(0)?,
        dockerfile: r.get(1)?,
        context: r.get(2)?,
        snapshot: r.get(3)?,
        state: r.get(4)?,
        builder: r.get(5)?,
        error: r.get(6)?,
        created_at: parse_ts(r.get(7)?).unwrap_or_else(Utc::now),
        finished_at: parse_ts(r.get(8)?),
    })
}

pub struct NewReport {
    pub kind: String,
    pub title: String,
    pub body: String,
    pub job_id: Option<i64>,
    pub reporter: String,
    pub fingerprint: String,
}

pub struct ReportRow {
    pub id: i64,
    pub kind: String,
    pub title: String,
    pub body: String,
    pub job_id: Option<i64>,
    pub reporter: String,
    pub fingerprint: String,
    pub state: String,
    pub created_at: DateTime<Utc>,
    pub closed_at: Option<DateTime<Utc>>,
    pub closed_by: Option<String>,
    pub note: Option<String>,
}

fn report_row(r: &Row) -> rusqlite::Result<ReportRow> {
    Ok(ReportRow {
        id: r.get(0)?,
        kind: r.get(1)?,
        title: r.get(2)?,
        body: r.get(3)?,
        job_id: r.get(4)?,
        reporter: r.get(5)?,
        fingerprint: r.get(6)?,
        state: r.get(7)?,
        created_at: parse_ts(r.get(8)?).unwrap_or_else(Utc::now),
        closed_at: parse_ts(r.get(9)?),
        closed_by: r.get(10)?,
        note: r.get(11)?,
    })
}
