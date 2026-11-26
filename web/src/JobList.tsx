import { useMemo, useState } from "react";
import { ACTIVE, type JobRow, type JobState, PROBLEM, listAllJobs, listJobs } from "./api";
import { State } from "./State";
import { usePoll, when, who } from "./util";

type Filter = "active" | "recent" | "problems" | "all";

const FILTERS: { key: Filter; label: string; states: JobState[] | null }[] = [
  { key: "active", label: "Active", states: ACTIVE },
  { key: "recent", label: "Recent", states: null },
  { key: "problems", label: "Problems", states: PROBLEM },
  { key: "all", label: "All", states: null },
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

/** "$3.40", "$3.40 so far" while it runs, or nothing for a free backend. */
export function costText(jobs: JobRow[]) {
  const paid = jobs.filter((j) => j.cost_usd !== undefined);
  if (!paid.length) return "";
  const usd = paid.reduce((n, j) => n + (j.cost_usd ?? 0), 0);
  const open = paid.some((j) => ACTIVE.includes(j.state));
  return `$${usd.toFixed(2)}${open ? " so far" : ""}`;
}

function Row({ j, now, task }: { j: JobRow; now: number; task?: boolean }) {
  const cost = costText([j]);
  return (
    <a className={`row${task ? " row-task" : ""}`} href={`#/jobs/${j.id}`}>
      <span className="row-id">{j.id}</span>
      <span className="row-title">{task ? `Task ${j.array_index}` : j.title}</span>
      <State state={j.state} />
      <span className="row-backend">
        {j.backend ?? "not placed"}
        {cost && (
          <>
            {" "}
            <span className="row-cost" title={j.usd_per_hour !== undefined ? `$${j.usd_per_hour.toFixed(2)}/h` : undefined}>
              · {cost}
            </span>
          </>
        )}
      </span>
      <span className="row-who" title="Submitted by">
        {who(j.submitted_by)}
      </span>
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
  const people = [...new Set(jobs.map((j) => who(j.submitted_by)))].join(", ");
  const cost = costText(jobs);
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
        <span className="row-backend">
          {backends}
          {cost && (
            <>
              {" "}
              <span className="row-cost">· {cost}</span>
            </>
          )}
        </span>
        <span className="row-who" title="Submitted by">
          {people}
        </span>
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
  // "" is everyone; otherwise only jobs this person submitted.
  const [person, setPerson] = useState("");
  const q = query.trim();
  // Searching asks the daemon, so older jobs beyond the loaded window are found too.
  const search = usePoll(() => (q ? listJobs({ q, limit: 1000 }) : Promise.resolve(null)), 10_000, [q]);
  // Every job ever, loaded only while the All tab is open.
  const all = usePoll(() => (filter === "all" ? listAllJobs() : Promise.resolve(null)), 30_000, [filter]);

  const people = useMemo(() => [...new Set([...jobs, ...(all.data ?? [])].map((j) => j.submitted_by))].sort(), [jobs, all.data]);
  const shown = useMemo(() => {
    const mine = (list: JobRow[]) => (person ? list.filter((j) => j.submitted_by === person) : list);
    if (filter === "all" && !q) return group(mine(all.data ?? jobs));
    const base = mine(q && search.data ? search.data : jobs);
    const f = FILTERS.find((x) => x.key === filter)!;
    return group(f.states ? base.filter((j) => f.states!.includes(j.state)) : filter === "all" ? base : base.slice(0, 100));
  }, [jobs, filter, q, search.data, all.data, person]);

  const count = (key: Filter, states: JobState[] | null) =>
    key === "all" ? (all.data?.length ?? null) : states ? jobs.filter((j) => states.includes(j.state)).length : null;

  return (
    <section className="jobs">
      <div className="jobs-bar">
        <div className="tabs" role="tablist" aria-label="Which jobs">
          {FILTERS.map((f) => {
            const n = count(f.key, f.states);
            return (
              <button key={f.key} role="tab" aria-selected={filter === f.key} onClick={() => setFilter(f.key)}>
                {f.label}
                {n !== null && <span className="count">{n}</span>}
              </button>
            );
          })}
        </div>
        {people.length > 1 && (
          <select value={person} onChange={(e) => setPerson(e.target.value)} aria-label="Whose jobs">
            <option value="">Everyone</option>
            {people.map((p) => (
              <option key={p} value={p}>
                {who(p)}
              </option>
            ))}
          </select>
        )}
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
