//! `compute watch`: a live monitor for a Rex pane (or any terminal).
//!
//! - All jobs: a table of active and recent jobs with times and resource usage. No logs.
//! - One job: a status header (start, finish, elapsed, time left, CPU, memory, GPUs, power,
//!   disk) with the log below, following new output.
//!
//! A worker thread polls the API so a slow backend never freezes the screen. In Rex, the
//! monitor reports the job's state with the Program Status Protocol, so the tab shows a
//! spinner while it runs and done or failed when it ends.

use crate::client::{Client, err_text};
use crate::fmt::{self, Times, Usage};
use crate::rex;
use anyhow::Result;
use ratatui::Frame;
use ratatui::crossterm::event::{self, Event, KeyCode, KeyEventKind, KeyModifiers};
use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::style::{Color, Modifier, Style, Stylize};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Cell, Paragraph, Row, Table, TableState, Wrap};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

const LOG_KEEP: usize = 5000;

#[derive(Default)]
struct Data {
    jobs: Vec<Value>,
    /// `(backend, job)` for the user's Slurm jobs that compute didn't submit.
    queue: Vec<(String, Value)>,
    job: Option<Value>,
    metrics: HashMap<i64, Value>,
    log: Vec<String>,
    log_partial: String,
    log_offset: Option<u64>,
    log_job: Option<i64>,
    error: Option<String>,
    updated: Option<Instant>,
}

struct Shared {
    data: Mutex<Data>,
    focus: Mutex<Option<i64>>,
    show_all: Mutex<bool>,
    poke: Mutex<bool>,
}

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

/// Polls jobs, the focused job, its log and resource usage.
fn worker(client: Client, shared: Arc<Shared>) {
    let mut last_metrics: HashMap<i64, Instant> = HashMap::new();
    let mut last_queue: Option<Instant> = None;
    loop {
        let focus = *lock(&shared.focus);
        let result: Result<()> = (|| {
            match focus {
                None => {
                    let jobs = client.get("/v1/jobs?limit=200")?["jobs"].as_array().cloned().unwrap_or_default();
                    let running: Vec<i64> = jobs.iter().filter(|j| j["state"] == "running").filter_map(|j| j["id"].as_i64()).take(20).collect();
                    lock(&shared.data).jobs = jobs;
                    if last_queue.is_none_or(|t| t.elapsed() > Duration::from_secs(30)) {
                        last_queue = Some(Instant::now());
                        let mut queue = Vec::new();
                        let backends = client.get("/v1/backends")?["backends"].as_array().cloned().unwrap_or_default();
                        for b in backends.iter().filter(|b| b["kind"] == "slurm" && b["reachable"] == true) {
                            let name = b["name"].as_str().unwrap_or_default().to_string();
                            if let Ok(q) = client.get(&format!("/v1/backends/{name}/queue")) {
                                for j in q["jobs"].as_array().into_iter().flatten().filter(|j| j["compute_job"].is_null()) {
                                    queue.push((name.clone(), j.clone()));
                                }
                            }
                        }
                        lock(&shared.data).queue = queue;
                    }
                    for id in running {
                        if last_metrics.get(&id).is_none_or(|t| t.elapsed() > Duration::from_secs(10)) {
                            last_metrics.insert(id, Instant::now());
                            if let Ok(m) = client.get(&format!("/v1/jobs/{id}/metrics")) {
                                lock(&shared.data).metrics.insert(id, m);
                            }
                        }
                    }
                }
                Some(id) => {
                    let job = client.get(&format!("/v1/jobs/{id}"))?;
                    let state = job["state"].as_str().unwrap_or("").to_string();
                    lock(&shared.data).job = Some(job);
                    fetch_log(&client, &shared, id)?;
                    let due = last_metrics.get(&id).is_none_or(|t| t.elapsed() > Duration::from_secs(5));
                    if due && !matches!(state.as_str(), "waiting" | "queued" | "staging") {
                        last_metrics.insert(id, Instant::now());
                        if let Ok(m) = client.get(&format!("/v1/jobs/{id}/metrics?history=120")) {
                            lock(&shared.data).metrics.insert(id, m);
                        }
                    }
                }
            }
            Ok(())
        })();
        {
            let mut d = lock(&shared.data);
            d.error = result.err().map(|e| err_text(&e));
            d.updated = Some(Instant::now());
        }
        for _ in 0..20 {
            std::thread::sleep(Duration::from_millis(100));
            let mut p = lock(&shared.poke);
            if *p {
                *p = false;
                break;
            }
        }
    }
}

fn fetch_log(client: &Client, shared: &Shared, id: i64) -> Result<()> {
    let offset = {
        let mut d = lock(&shared.data);
        if d.log_job != Some(id) {
            d.log_job = Some(id);
            d.log.clear();
            d.log_partial.clear();
            d.log_offset = None;
        }
        d.log_offset
    };
    let offset = match offset {
        Some(o) => o,
        None => {
            // Start from the last 64 KiB.
            let probe = client.get(&format!("/v1/jobs/{id}/logs?offset=0&max_bytes=1"))?;
            probe["size"].as_u64().unwrap_or(0).saturating_sub(65_536)
        }
    };
    let chunk = client.get(&format!("/v1/jobs/{id}/logs?offset={offset}&max_bytes=262144"))?;
    let next = chunk["next_offset"].as_u64().unwrap_or(offset);
    let text = chunk["data"].as_str().unwrap_or("");
    let mut d = lock(&shared.data);
    if d.log_job != Some(id) {
        return Ok(());
    }
    let mut buf = std::mem::take(&mut d.log_partial);
    buf.push_str(text);
    let mut parts: Vec<&str> = buf.split('\n').collect();
    let last = parts.pop().unwrap_or("").to_string();
    for p in parts {
        // Progress bars redraw with \r; keep what the line ended as.
        let line = p.rsplit('\r').find(|s| !s.is_empty()).unwrap_or("");
        d.log.push(clean(line));
    }
    d.log_partial = last;
    let excess = d.log.len().saturating_sub(LOG_KEEP);
    d.log.drain(..excess);
    d.log_offset = Some(next);
    Ok(())
}

/// Drops escape sequences and control characters so log lines can't move the cursor.
fn clean(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut chars = s.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '\x1b' {
            if chars.peek() == Some(&'[') {
                chars.next();
                for n in chars.by_ref() {
                    if n.is_ascii_alphabetic() {
                        break;
                    }
                }
            } else {
                chars.next();
            }
        } else if c == '\t' {
            out.push_str("    ");
        } else if !c.is_control() {
            out.push(c);
        }
    }
    out
}

struct Ui {
    shared: Arc<Shared>,
    /// Job ids in the order shown in the table.
    order: Vec<i64>,
    table: TableState,
    /// Opened straight into one job (no list to go back to).
    single: bool,
    /// Lines scrolled up from the bottom of the log; 0 follows new output.
    scroll: usize,
    confirm_cancel: bool,
    message: Option<(String, Instant)>,
    reported: String,
    client: Client,
}

pub fn run(client: Client, job: Option<i64>) -> Result<()> {
    // Fail fast (and log in) before taking over the screen.
    client.get("/v1/whoami").or_else(|_| client.login().and_then(|_| client.get("/v1/whoami")))?;
    let shared = Arc::new(Shared {
        data: Mutex::new(Data::default()),
        focus: Mutex::new(job),
        show_all: Mutex::new(false),
        poke: Mutex::new(false),
    });
    {
        let (c, s) = (client.clone(), shared.clone());
        std::thread::spawn(move || worker(c, s));
    }
    let mut ui = Ui {
        shared,
        order: Vec::new(),
        table: TableState::default().with_selected(Some(0)),
        single: job.is_some(),
        scroll: 0,
        confirm_cancel: false,
        message: None,
        reported: String::new(),
        client,
    };
    let mut terminal = ratatui::init();
    let result = (|| -> Result<()> {
        loop {
            terminal.draw(|f| ui.draw(f))?;
            ui.report_status();
            if event::poll(Duration::from_millis(250))?
                && let Event::Key(k) = event::read()?
                && k.kind == KeyEventKind::Press
                && !ui.key(k.code, k.modifiers)
            {
                return Ok(());
            }
        }
    })();
    ratatui::restore();
    if rex::inside() {
        rex::status("idle", None, "");
    }
    result
}

impl Ui {
    fn poke(&self) {
        *lock(&self.shared.poke) = true;
    }

    fn focus(&self) -> Option<i64> {
        *lock(&self.shared.focus)
    }

    fn set_focus(&mut self, id: Option<i64>) {
        *lock(&self.shared.focus) = id;
        let mut d = lock(&self.shared.data);
        d.job = None;
        self.scroll = 0;
        self.confirm_cancel = false;
        drop(d);
        self.poke();
    }

    fn flash(&mut self, msg: impl Into<String>) {
        self.message = Some((msg.into(), Instant::now()));
    }

    /// Returns false to quit.
    fn key(&mut self, code: KeyCode, mods: KeyModifiers) -> bool {
        if self.confirm_cancel {
            self.confirm_cancel = false;
            if matches!(code, KeyCode::Char('y')) {
                if let Some(id) = self.focus().or_else(|| self.selected()) {
                    match self.client.post(&format!("/v1/jobs/{id}/cancel"), &Value::Null) {
                        Ok(_) => self.flash(format!("cancelled job {id}")),
                        Err(e) => self.flash(err_text(&e)),
                    }
                    self.poke();
                }
            } else {
                self.flash("not cancelled");
            }
            return true;
        }
        match code {
            KeyCode::Char('q') => return false,
            KeyCode::Char('c') if mods.contains(KeyModifiers::CONTROL) => return false,
            KeyCode::Esc | KeyCode::Backspace if self.focus().is_some() && !self.single => self.set_focus(None),
            KeyCode::Esc if self.focus().is_none() || self.single => return false,
            KeyCode::Char('x') => {
                if self.focus().or_else(|| self.selected()).is_some() {
                    self.confirm_cancel = true;
                }
            }
            KeyCode::Char('o') => {
                if let Some(id) = self.focus().or_else(|| self.selected()) {
                    match rex::open_monitor(Some(id), &format!("compute #{id}")) {
                        Ok(_) => self.flash(format!("opened job {id} in a split")),
                        Err(e) => self.flash(format!("{e:#}")),
                    }
                }
            }
            _ if self.focus().is_none() => self.list_key(code),
            _ => self.job_key(code),
        }
        true
    }

    fn selected(&self) -> Option<i64> {
        self.table.selected().and_then(|i| self.order.get(i).copied())
    }

    fn list_key(&mut self, code: KeyCode) {
        let n = self.order.len().max(1);
        let i = self.table.selected().unwrap_or(0);
        match code {
            KeyCode::Down | KeyCode::Char('j') => self.table.select(Some((i + 1).min(n - 1))),
            KeyCode::Up | KeyCode::Char('k') => self.table.select(Some(i.saturating_sub(1))),
            KeyCode::Home | KeyCode::Char('g') => self.table.select(Some(0)),
            KeyCode::End | KeyCode::Char('G') => self.table.select(Some(n - 1)),
            KeyCode::Enter => {
                if let Some(id) = self.selected() {
                    self.set_focus(Some(id));
                }
            }
            KeyCode::Char('a') => {
                let mut all = lock(&self.shared.show_all);
                *all = !*all;
            }
            _ => {}
        }
    }

    fn job_key(&mut self, code: KeyCode) {
        match code {
            KeyCode::Up | KeyCode::Char('k') => self.scroll += 1,
            KeyCode::Down | KeyCode::Char('j') => self.scroll = self.scroll.saturating_sub(1),
            KeyCode::PageUp => self.scroll += 20,
            KeyCode::PageDown => self.scroll = self.scroll.saturating_sub(20),
            KeyCode::Char('g') | KeyCode::Home => self.scroll = usize::MAX / 2,
            KeyCode::Char('G') | KeyCode::End | KeyCode::Char('f') => self.scroll = 0,
            _ => {}
        }
    }

    /// Tells Rex what this monitor is showing, when it changes.
    fn report_status(&mut self) {
        if !rex::inside() {
            return;
        }
        let d = lock(&self.shared.data);
        let (state, progress, msg) = match self.focus() {
            Some(id) => {
                let Some(j) = &d.job else { return };
                let s = j["state"].as_str().unwrap_or("");
                let t = fmt::times(j);
                let title = j["title"].as_str().unwrap_or("");
                match s {
                    "succeeded" => ("done", None, format!("#{id} succeeded in {}: {title}", t.elapsed)),
                    "cancelled" => ("idle", None, format!("#{id} cancelled: {title}")),
                    s if fmt::active(s) => {
                        let where_ = j["backend"].as_str().map(|b| format!(" on {b}")).unwrap_or_default();
                        let left = if s == "running" { format!(", {} left", t.left) } else { String::new() };
                        ("working", t.used.map(|u| (u * 100.0) as u8), format!("#{id} {s}{where_}{left}: {title}"))
                    }
                    s => ("error", None, format!("#{id} {s}: {title}")),
                }
            }
            None => {
                let running = d.jobs.iter().filter(|j| j["state"] == "running").count();
                let waiting = d.jobs.iter().filter(|j| fmt::active(j["state"].as_str().unwrap_or("")) && j["state"] != "running").count();
                if running + waiting == 0 {
                    ("idle", None, "no active jobs".to_string())
                } else {
                    ("working", None, format!("{running} running, {waiting} waiting"))
                }
            }
        };
        let key = format!("{state}|{progress:?}|{msg}");
        if key != self.reported {
            drop(d);
            rex::status(state, progress, &msg);
            self.reported = key;
        }
    }

    fn draw(&mut self, f: &mut Frame) {
        let area = f.area();
        let [body, footer] = Layout::vertical([Constraint::Min(3), Constraint::Length(1)]).areas(area);
        match self.focus() {
            None => self.draw_list(f, body),
            Some(id) => self.draw_job(f, body, id),
        }
        self.draw_footer(f, footer);
    }

    fn draw_footer(&mut self, f: &mut Frame, area: Rect) {
        let d = lock(&self.shared.data);
        let keys = if self.confirm_cancel {
            "cancel this job? y to confirm, any other key to keep it".to_string()
        } else if self.focus().is_some() {
            let back = if self.single { "q quit" } else { "esc back  q quit" };
            format!("{back}  ↑↓ scroll  G follow  o open in split  x cancel")
        } else {
            "↑↓ select  enter open  o open in split  x cancel  a all/active  q quit".to_string()
        };
        let mut spans = vec![Span::styled(keys, Style::new().fg(Color::DarkGray))];
        if let Some((m, t)) = &self.message
            && t.elapsed() < Duration::from_secs(5)
        {
            spans.push(Span::raw("   "));
            spans.push(Span::styled(m.clone(), Style::new().fg(Color::Yellow)));
        } else if let Some(e) = &d.error {
            spans.push(Span::raw("   "));
            spans.push(Span::styled(e.lines().next().unwrap_or("").to_string(), Style::new().fg(Color::Red)));
        }
        f.render_widget(Paragraph::new(Line::from(spans)), area);
    }

    fn draw_list(&mut self, f: &mut Frame, area: Rect) {
        let d = lock(&self.shared.data);
        let show_all = *lock(&self.shared.show_all);
        let mut jobs: Vec<&Value> = d.jobs.iter().filter(|j| j["array_index"].as_u64().is_none_or(|i| i == 0) || j["state"] == "running").collect();
        let active: Vec<&Value> = jobs.iter().copied().filter(|j| fmt::active(j["state"].as_str().unwrap_or(""))).collect();
        if !show_all {
            let recent: Vec<&Value> = jobs.iter().copied().filter(|j| !fmt::active(j["state"].as_str().unwrap_or(""))).take(15).collect();
            jobs = active.iter().copied().chain(recent).collect();
        }
        self.order = jobs.iter().filter_map(|j| j["id"].as_i64()).collect();
        let running = active.iter().filter(|j| j["state"] == "running").count();
        let title = format!(
            " compute · {running} running · {} waiting · {} ",
            active.len() - running,
            if show_all { "all jobs" } else { "active and recent" }
        );
        let header = Row::new(["ID", "STATE", "TITLE", "BACKEND", "START", "ELAPSED", "LEFT", "CPU", "MEM", "GPU"])
            .style(Style::new().add_modifier(Modifier::BOLD).fg(Color::Gray));
        let rows: Vec<Row> = jobs
            .iter()
            .map(|j| {
                let id = j["id"].as_i64().unwrap_or(0);
                let state = j["state"].as_str().unwrap_or("");
                let t = fmt::times(j);
                let u = d.metrics.get(&id).and_then(|m| fmt::usage(&m["latest"])).filter(|_| state == "running");
                let (cpu, mem, gpu) = match &u {
                    Some(u) => (
                        u.cpu_pct.map(|c| format!("{c:.0}%")).unwrap_or_default(),
                        u.rss_mb.map(fmt::gb).unwrap_or_default(),
                        u.gpu_util.map(|g| format!("{g:.0}%")).unwrap_or_default(),
                    ),
                    None => Default::default(),
                };
                let mut title = j["title"].as_str().unwrap_or("").to_string();
                if j["array_id"].is_number() {
                    title = format!("{title} [array {}]", j["array_id"]);
                }
                Row::new(vec![
                    Cell::from(id.to_string()).fg(Color::DarkGray),
                    Cell::from(state.replace('_', " ")).style(state_style(state)),
                    Cell::from(title),
                    Cell::from(j["backend"].as_str().unwrap_or("—").to_string()),
                    Cell::from(t.start),
                    Cell::from(t.elapsed),
                    Cell::from(t.left),
                    Cell::from(cpu),
                    Cell::from(mem),
                    Cell::from(gpu),
                ])
            })
            .collect();
        let widths = [
            Constraint::Length(5),
            Constraint::Length(17),
            Constraint::Min(20),
            Constraint::Length(8),
            Constraint::Length(10),
            Constraint::Length(8),
            Constraint::Length(14),
            Constraint::Length(6),
            Constraint::Length(8),
            Constraint::Length(5),
        ];
        let mut rows = rows;
        for (backend, q) in &d.queue {
            let state = q["state"].as_str().unwrap_or("");
            let start = fmt::ts(&q["start"]);
            let elapsed = q["elapsed_s"].as_i64().filter(|_| state == "running");
            let limit = q["time_limit_s"].as_i64().unwrap_or(0);
            let left = match (state, start, elapsed) {
                ("running", _, Some(e)) => fmt::dur(limit - e),
                (_, Some(s), _) if s > chrono::Utc::now() => format!("starts in {}", fmt::dur((s - chrono::Utc::now()).num_seconds())),
                _ => q["reason"].as_str().unwrap_or("").trim_matches(|c| c == '(' || c == ')').to_string(),
            };
            rows.push(
                Row::new(vec![
                    Cell::from("·").fg(Color::DarkGray),
                    Cell::from(state.to_string()).style(state_style(state)),
                    Cell::from(format!("{} (slurm {}, not from compute)", q["name"].as_str().unwrap_or(""), q["slurm_id"].as_str().unwrap_or(""))),
                    Cell::from(backend.clone()),
                    Cell::from(start.map(|s| if state == "running" { fmt::clock(s) } else { format!("~{}", fmt::clock(s)) }).unwrap_or_else(|| "—".into())),
                    Cell::from(elapsed.map(fmt::dur).unwrap_or_else(|| "—".into())),
                    Cell::from(left),
                    Cell::from(""),
                    Cell::from(""),
                    Cell::from(""),
                ])
                .fg(Color::Gray),
            );
        }
        let n = rows.len();
        let table = Table::new(rows, widths)
            .header(header)
            .block(Block::default().borders(Borders::ALL).title(title))
            .row_highlight_style(Style::new().add_modifier(Modifier::REVERSED))
            .column_spacing(1);
        if self.table.selected().is_some_and(|i| i >= n) {
            self.table.select(Some(n.saturating_sub(1)));
        }
        drop(d);
        f.render_stateful_widget(table, area, &mut self.table);
    }

    fn draw_job(&mut self, f: &mut Frame, area: Rect, id: i64) {
        let d = lock(&self.shared.data);
        let Some(j) = d.job.as_ref().filter(|j| j["id"].as_i64() == Some(id)) else {
            let msg = d.error.clone().unwrap_or_else(|| format!("loading job {id}…"));
            f.render_widget(Paragraph::new(msg).block(Block::default().borders(Borders::ALL)), area);
            return;
        };
        let state = j["state"].as_str().unwrap_or("");
        let t = fmt::times(j);
        let metrics = d.metrics.get(&id);
        let u = metrics.and_then(|m| fmt::usage(&m["latest"]));
        // Meters take what's left of the pane after labels and numbers.
        let w = (area.width as usize).saturating_sub(48).clamp(6, 30);
        let header_lines = self.header_lines(j, &t, u.as_ref(), w);
        let [head, logs] = Layout::vertical([Constraint::Length(header_lines.len() as u16 + 2), Constraint::Min(3)]).areas(area);
        let title = Line::from(vec![
            Span::raw(format!(" #{id} ")),
            Span::styled(j["title"].as_str().unwrap_or("").to_string(), Style::new().add_modifier(Modifier::BOLD)),
            Span::raw("  "),
            Span::styled(format!("{} ", state.replace('_', " ")), state_style(state)),
        ]);
        f.render_widget(
            Paragraph::new(header_lines).block(Block::default().borders(Borders::ALL).title(title)).wrap(Wrap { trim: false }),
            head,
        );
        // Log: the last lines that fit, or scrolled back.
        let height = logs.height.saturating_sub(2) as usize;
        let total = d.log.len();
        self.scroll = self.scroll.min(total.saturating_sub(height));
        let end = total - self.scroll;
        let start = end.saturating_sub(height);
        let mut lines: Vec<Line> = d.log[start..end].iter().map(|l| Line::raw(l.clone())).collect();
        if !d.log_partial.is_empty() && self.scroll == 0 {
            lines.push(Line::raw(clean(d.log_partial.rsplit('\r').next().unwrap_or(""))));
            if lines.len() > height {
                lines.remove(0);
            }
        }
        if lines.is_empty() {
            let msg = if fmt::active(state) && state != "running" { "no output yet: the job hasn't started" } else { "no output" };
            lines.push(Line::styled(msg, Style::new().fg(Color::DarkGray)));
        }
        let follow = if self.scroll == 0 { "following".to_string() } else { format!("{} lines up", self.scroll) };
        f.render_widget(
            Paragraph::new(lines).block(Block::default().borders(Borders::ALL).title(format!(" log · {follow} "))),
            logs,
        );
    }

    /// `w` is the meter width, sized to the pane.
    fn header_lines(&self, j: &Value, t: &Times, u: Option<&Usage>, w: usize) -> Vec<Line<'static>> {
        let label = |s: &str| Span::styled(format!("{s:<10}"), Style::new().fg(Color::DarkGray));
        let state = j["state"].as_str().unwrap_or("");
        let r = &j["resources"];
        let mut lines = Vec::new();
        let backend = j["backend"].as_str().unwrap_or("not placed yet").to_string();
        let gpus_req = r["gpus"].as_u64().unwrap_or(0);
        let req = format!(
            "{} CPU, {}{}, {} limit",
            r["cpus"].as_u64().unwrap_or(1),
            fmt::gb(r["mem_gb"].as_f64().unwrap_or(0.0) * 1024.0),
            if gpus_req > 0 { format!(", {gpus_req} GPU") } else { String::new() },
            fmt::dur(r["time_s"].as_i64().unwrap_or(0))
        );
        lines.push(Line::from(vec![label("Backend"), Span::raw(format!("{backend}  ({req})"))]));
        if let Some(reason) = j["reason"].as_str() {
            lines.push(Line::from(vec![label("Status"), Span::styled(reason.to_string(), Style::new().fg(Color::Gray))]));
        }
        lines.push(Line::from(vec![label("Start"), Span::raw(format!("{:<16}", t.start)), label("Finish"), Span::raw(t.finish.clone())]));
        lines.push(Line::from(vec![label("Elapsed"), Span::raw(format!("{:<16}", t.elapsed)), label("Time left"), Span::raw(t.left.clone())]));
        if let Some(used) = t.used {
            lines.push(Line::from(vec![
                label("Limit"),
                Span::styled(fmt::bar(used, w), Style::new().fg(Color::Cyan)),
                Span::raw(format!(" {:.0}% of {}", used * 100.0, fmt::dur(r["time_s"].as_i64().unwrap_or(0)))),
            ]));
        }
        if state == "running" || (!fmt::active(state) && u.is_some()) {
            let stale = u.and_then(|u| u.age_s).is_some_and(|a| a > 60);
            match u {
                None => lines.push(Line::from(vec![label("Usage"), Span::styled("waiting for the first sample…", Style::new().fg(Color::DarkGray))])),
                Some(u) => {
                    let cpus = r["cpus"].as_f64().unwrap_or(1.0).max(1.0);
                    let cpu = u.cpu_pct.unwrap_or(0.0);
                    lines.push(Line::from(vec![
                        label("CPU"),
                        Span::styled(fmt::bar(cpu / (cpus * 100.0), w), Style::new().fg(Color::Green)),
                        Span::raw(format!(" {cpu:.0}% ({:.1} of {cpus:.0} cores)", cpu / 100.0)),
                    ]));
                    let mem_req = r["mem_gb"].as_f64().unwrap_or(0.0) * 1024.0;
                    let rss = u.rss_mb.unwrap_or(0.0);
                    lines.push(Line::from(vec![
                        label("Memory"),
                        Span::styled(fmt::bar(if mem_req > 0.0 { rss / mem_req } else { 0.0 }, w), Style::new().fg(Color::Magenta)),
                        Span::raw(format!(" {} of {}", fmt::gb(rss), fmt::gb(mem_req))),
                    ]));
                    for g in &u.gpus {
                        let util = g["util_pct"].as_f64().unwrap_or(0.0);
                        let used = g["mem_used_mb"].as_f64().unwrap_or(0.0);
                        let total = g["mem_total_mb"].as_f64().unwrap_or(0.0);
                        let power = g["power_w"].as_f64().map(|p| format!(", {p:.0} W")).unwrap_or_default();
                        lines.push(Line::from(vec![
                            label(&format!("GPU {}", g["index"])),
                            Span::styled(fmt::bar(util / 100.0, w), Style::new().fg(Color::Yellow)),
                            Span::raw(format!(" {util:.0}%, {} of {}{power}", fmt::gb(used), fmt::gb(total))),
                        ]));
                    }
                    let mut extra = vec![label("Disk free"), Span::raw(format!("{:<16}", u.disk_free_gb.map(|g| format!("{g:.0} GB")).unwrap_or("—".into())))];
                    extra.push(label("Power"));
                    extra.push(Span::raw(match u.power_w {
                        Some(p) => format!("{p:.0} W (GPUs)"),
                        None => "not measured on this host".into(),
                    }));
                    lines.push(Line::from(extra));
                    if stale {
                        lines.push(Line::styled("usage is over a minute old", Style::new().fg(Color::DarkGray)));
                    }
                }
            }
        }
        if let Some(code) = j["exit_code"].as_i64() {
            lines.push(Line::from(vec![label("Exit code"), Span::raw(code.to_string())]));
        }
        if let Some(c) = j["cost_usd"].as_f64() {
            lines.push(Line::from(vec![label("Cost"), Span::raw(format!("${c:.2}"))]));
        }
        lines
    }
}

fn state_style(state: &str) -> Style {
    match fmt::tone(state) {
        "running" => Style::new().fg(Color::Cyan).add_modifier(Modifier::BOLD),
        "waiting" => Style::new().fg(Color::Yellow),
        "done" => Style::new().fg(Color::Green),
        _ => Style::new().fg(Color::Red).add_modifier(Modifier::BOLD),
    }
}
