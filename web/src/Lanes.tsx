import type { BackendSummary, JobRow } from "./api";
import { ms, when } from "./util";

/** "22:00", or "Tue 00:00" where a new day starts. */
function tickLabel(t: number) {
  const d = new Date(t);
  const hh = `${String(d.getHours()).padStart(2, "0")}:00`;
  return d.getHours() === 0 ? `${d.toLocaleDateString(undefined, { weekday: "short" })} ${hh}` : hh;
}

// The schedule: one lane per backend, from 2 h ago to 6 h ahead, with a now line.
const BEFORE = 2 * 3600_000;
const AFTER = 6 * 3600_000;
const MAX_ROWS = 5;

type Bar = {
  job: JobRow;
  from: number;
  to: number;
  split?: number; // running jobs: elapsed until here, expected after
  kind: "running" | "waiting" | "done" | "failed";
};

function barFor(j: JobRow, now: number): Bar | null {
  const start = ms(j.started_at);
  const end = ms(j.ended_at);
  const est = ms(j.estimated_start);
  const estEnd = ms(j.estimated_finish);
  const len = j.time_s * 1000;
  switch (j.state) {
    case "running": {
      const from = Number.isFinite(start) ? start : now;
      return { job: j, from, to: Math.max(now + 60_000, from + len), split: now, kind: "running" };
    }
    case "queued":
    case "staging":
    case "pending": {
      const from = Number.isFinite(est) ? Math.max(est, now) : now;
      const to = Number.isFinite(estEnd) ? Math.max(estEnd, from + 60_000) : from + len;
      return { job: j, from, to, kind: "waiting" };
    }
    default: {
      if (!Number.isFinite(start) || !Number.isFinite(end)) return null;
      const failed = j.state === "failed" || j.state === "timeout" || j.state === "lost";
      return { job: j, from: start, to: Math.max(end, start + 30_000), kind: failed ? "failed" : "done" };
    }
  }
}

/** Packs bars into rows so overlapping jobs stack instead of hiding each other. */
function pack(bars: Bar[]): Bar[][] {
  const rows: Bar[][] = [];
  const ends: number[] = [];
  for (const b of [...bars].sort((a, c) => a.from - c.from)) {
    const i = ends.findIndex((e) => e <= b.from);
    if (i >= 0) {
      rows[i]!.push(b);
      ends[i] = b.to;
    } else {
      rows.push([b]);
      ends.push(b.to);
    }
  }
  return rows;
}

function laneNote(b: BackendSummary | undefined, bars: Bar[]) {
  if (b && !b.enabled) return "off";
  if (b && !b.reachable) return "can't reach it";
  const running = bars.filter((x) => x.kind === "running").length;
  const waiting = bars.filter((x) => x.kind === "waiting").length;
  if (!running && !waiting) return "idle";
  return [running && `${running} running`, waiting && `${waiting} waiting`].filter(Boolean).join(", ");
}

export function Lanes({ jobs, backends, now }: { jobs: JobRow[]; backends: BackendSummary[]; now: number }) {
  const start = now - BEFORE;
  const end = now + AFTER;
  const pct = (t: number) => ((Math.min(Math.max(t, start), end) - start) / (end - start)) * 100;

  const byLane = new Map<string, Bar[]>();
  for (const b of backends) byLane.set(b.name, []);
  for (const j of jobs) {
    const bar = barFor(j, now);
    if (!bar || bar.to < start || bar.from > end) continue;
    const lane = j.backend ?? "not placed yet";
    if (!byLane.has(lane)) byLane.set(lane, []);
    byLane.get(lane)!.push(bar);
  }

  const ticks: number[] = [];
  for (let t = Math.ceil(start / 3600_000) * 3600_000; t <= end; t += 3600_000) ticks.push(t);

  return (
    <section className="lanes" aria-label="Schedule, two hours back to six hours ahead">
      <div className="lanes-axis" aria-hidden="true">
        {ticks
          .filter((t) => Math.abs(pct(t) - pct(now)) > 4.5)
          .map((t) => (
            <span
              key={t}
              className={`tick${new Date(t).getHours() % 2 ? " tick-odd" : ""}${pct(t) > 93 ? " tick-end" : pct(t) < 5 ? " tick-start" : ""}`}
              style={{ left: `${pct(t)}%` }}
            >
              <span className="t-long">{tickLabel(t)}</span>
              <span className="t-short">{String(new Date(t).getHours()).padStart(2, "0")}:00</span>
            </span>
          ))}
      </div>
      {[...byLane.entries()].map(([name, bars]) => {
        const backend = backends.find((b) => b.name === name);
        const rows = pack(bars);
        const hidden = rows.slice(MAX_ROWS).reduce((n, r) => n + r.length, 0);
        return (
          <div key={name} className={`lane${backend && !backend.enabled ? " lane-off" : ""}`}>
            <div className="lane-name">
              <a href="#/backends">{name}</a>
              <span>{laneNote(backend, bars)}</span>
            </div>
            <div className="lane-track" style={{ minHeight: `${Math.max(1, Math.min(rows.length, MAX_ROWS)) * 1.75 + 0.5}rem` }}>
              {ticks.map((t) => (
                <span key={t} className="gridline" style={{ left: `${pct(t)}%` }} />
              ))}
              <span className="nowline" style={{ left: `${pct(now)}%` }} />
              {rows.slice(0, MAX_ROWS).map((row, r) =>
                row.map((b) => {
                  const left = pct(b.from);
                  const width = Math.max(0.6, pct(b.to) - left);
                  const elapsed = b.split !== undefined ? ((pct(b.split) - left) / width) * 100 : 0;
                  return (
                    <a
                      key={b.job.id}
                      href={`#/jobs/${b.job.id}`}
                      className={`bar bar-${b.kind}`}
                      style={{
                        left: `${left}%`,
                        width: `${width}%`,
                        top: `${r * 1.75 + 0.25}rem`,
                        ...(b.split !== undefined ? { ["--elapsed" as string]: `${Math.max(0, Math.min(100, elapsed))}%` } : {}),
                      }}
                      title={`#${b.job.id} ${b.job.title}\n${b.job.state}, ${when(b.job, now)}`}
                    >
                      {width > 2.5 && (
                        <span>
                          <span className="id">{b.job.id}</span> {b.job.title}
                        </span>
                      )}
                    </a>
                  );
                }),
              )}
              {hidden > 0 && <span className="lane-more">{hidden} more</span>}
            </div>
          </div>
        );
      })}
      <div className="lanes-now" style={{ left: `calc(var(--lane-name) + (100% - var(--lane-name)) * ${pct(now) / 100})` }}>
        now
      </div>
    </section>
  );
}
