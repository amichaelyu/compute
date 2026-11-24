import { useMemo, useState } from "react";
import { ACTIVE, type JobRow, type JobState, PROBLEM, listJobs } from "./api";
import { State } from "./State";
import { usePoll, when } from "./util";

type Filter = "active" | "recent" | "problems";

const FILTERS: { key: Filter; label: string; states: JobState[] | null }[] = [
  { key: "active", label: "Active", states: ACTIVE },
  { key: "recent", label: "Recent", states: null },
  { key: "problems", label: "Problems", states: PROBLEM },
];

type Item = { kind: "job"; job: JobRow } | { kind: "array"; id: number; jobs: JobRow[] };

function group(jobs: JobRow[]): Item[] {
  const items: Item[] = [];
  const arrays = new Map<number, JobRow[]>();
  for (const j of jobs) {
    if (j.array_id === null) {
      items.push({ kind: "job", job: j });
      continue;
    }
    let list = arrays.get(j.array_id);
    if (!list) {
      list = [];
      arrays.set(j.array_id, list);
      items.push({ kind: "array", id: j.array_id, jobs: list });
    }
    list.push(j);
  }
  for (const list of arrays.values()) list.sort((a, b) => (a.array_index ?? 0) - (b.array_index ?? 0));
  return items;
}

function Row({ j, now, task }: { j: JobRow; now: number; task?: boolean }) {
  return (
    <a className={`row${task ? " row-task" : ""}`} href={`#/jobs/${j.id}`}>
      <span className="row-id">{j.id}</span>
      <span className="row-title">{task ? `Task ${j.array_index}` : j.title}</span>
      <State state={j.state} />
      <span className="row-backend">{j.backend ?? "not placed"}</span>
      <span className="row-when">
        {when(j, now)}
        {j.exit_code !== null && j.exit_code !== 0 ? `, exit ${j.exit_code}` : ""}
      </span>
    </a>
  );
}

function ArrayRows({ id, jobs, now }: { id: number; jobs: JobRow[]; now: number }) {
  const [open, setOpen] = useState(false);
  const counts = new Map<JobState, number>();
  for (const j of jobs) counts.set(j.state, (counts.get(j.state) ?? 0) + 1);
  const summary = [...counts.entries()].map(([s, n]) => `${n} ${s}`).join(", ");
  const backends = [...new Set(jobs.map((j) => j.backend ?? "not placed"))].join(", ");
  const first = jobs[0]!;
  return (
    <>
      <button className="row row-array" onClick={() => setOpen(!open)} aria-expanded={open}>
        <span className="row-id">a{id}</span>
        <span className="row-title">
          {first.title}
          <span className="row-sub">
            {jobs.length} tasks: {summary}
          </span>
        </span>
        <State state={jobs.some((j) => j.state === "running") ? "running" : first.state} />
        <span className="row-backend">{backends}</span>
        <span className="row-when">{open ? "Hide tasks" : "Show tasks"}</span>
      </button>
      {open && jobs.map((j) => <Row key={j.id} j={j} now={now} task />)}
    </>
  );
}

export function JobList({ jobs, now }: { jobs: JobRow[]; now: number }) {
  const [chosen, setChosen] = useState<Filter | null>(null);
  const anyActive = jobs.some((j) => ACTIVE.includes(j.state));
  const filter: Filter = chosen ?? (anyActive || jobs.length === 0 ? "active" : "recent");
  const setFilter = setChosen;
  const [query, setQuery] = useState("");
  const q = query.trim();
  // Searching asks the daemon, so older jobs beyond the loaded window are found too.
  const search = usePoll(() => (q ? listJobs({ q, limit: 200 }) : Promise.resolve(null)), 10_000, [q]);

  const shown = useMemo(() => {
    const base = q && search.data ? search.data : jobs;
    const f = FILTERS.find((x) => x.key === filter)!;
    return group(f.states ? base.filter((j) => f.states!.includes(j.state)) : base.slice(0, 100));
  }, [jobs, filter, q, search.data]);

  const count = (states: JobState[] | null) => (states ? jobs.filter((j) => states.includes(j.state)).length : null);

  return (
    <section className="jobs">
      <div className="jobs-bar">
        <div className="tabs" role="tablist" aria-label="Which jobs">
          {FILTERS.map((f) => {
            const n = count(f.states);
            return (
              <button key={f.key} role="tab" aria-selected={filter === f.key} onClick={() => setFilter(f.key)}>
                {f.label}
                {n !== null && <span className="count">{n}</span>}
              </button>
            );
          })}
        </div>
        <input
          type="search"
          placeholder="Search titles and descriptions"
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          aria-label="Search jobs"
        />
      </div>
      <div className="rows">
        {shown.length === 0 ? (
          <p className="empty">
            {filter === "active"
              ? "Nothing is running or waiting. Agents submit jobs through the compute skill."
              : filter === "problems"
                ? "No failed, timed-out or lost jobs."
                : "No jobs match."}
          </p>
        ) : (
          shown.map((it) =>
            it.kind === "job" ? (
              <Row key={it.job.id} j={it.job} now={now} />
            ) : (
              <ArrayRows key={`a${it.id}`} id={it.id} jobs={it.jobs} now={now} />
            ),
          )
        )}
      </div>
    </section>
  );
}
