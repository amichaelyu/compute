import { useEffect, useRef, useState } from "react";
import { ApiError, type Job, cancelJob, getJob, getLogs, getMetrics, isActive, outputsUrl } from "./api";
import { Meter } from "./Meter";
import { State } from "./State";
import { clock, dur, ms, usePoll, when, who } from "./util";

const LOG_CAP = 2_000_000; // characters kept in the browser

function useLog(id: number, active: boolean) {
  const [text, setText] = useState("");
  const [done, setDone] = useState(false);
  const offset = useRef(0);
  useEffect(() => {
    setText("");
    setDone(false);
    offset.current = 0;
  }, [id]);
  useEffect(() => {
    if (done) return;
    let alive = true;
    let timer: ReturnType<typeof setTimeout>;
    const pull = async () => {
      try {
        // Drain whatever is there, then wait for more.
        for (let i = 0; i < 20 && alive; i++) {
          const c = await getLogs(id, offset.current);
          if (!alive) return;
          offset.current = c.next_offset;
          if (c.data) setText((t) => (t + c.data).slice(-LOG_CAP));
          if (c.eof) {
            setDone(true);
            return;
          }
          if (!c.data) break;
        }
      } catch {
        // Keep the text we have; the job view shows connection errors.
      }
      if (alive) timer = setTimeout(pull, active ? 2000 : 5000);
    };
    pull();
    return () => {
      alive = false;
      clearTimeout(timer);
    };
  }, [id, active, done]);
  return { text, done };
}

function Log({ id, active }: { id: number; active: boolean }) {
  const { text, done } = useLog(id, active);
  const box = useRef<HTMLPreElement>(null);
  const follow = useRef(true);
  useEffect(() => {
    const el = box.current;
    if (el && follow.current) el.scrollTop = el.scrollHeight;
  }, [text]);
  return (
    <div className="log">
      <div className="log-head">
        <h3>Log</h3>
        <span>{done ? "complete" : active ? "following new output" : ""}</span>
      </div>
      <pre
        ref={box}
        tabIndex={0}
        onScroll={(e) => {
          const el = e.currentTarget;
          follow.current = el.scrollHeight - el.scrollTop - el.clientHeight < 40;
        }}
      >
        {text || (active ? "No output yet." : "This job wrote no output.")}
      </pre>
    </div>
  );
}

/** "job 41 succeeds, array 7 ends" style summary of a job's dependencies. */
function deps(job: Job): string | null {
  const name = (d: number | { array: number }) => (typeof d === "number" ? `job ${d}` : `array ${d.array}`);
  const parts = [
    ...(job.after_ok ?? []).map((d) => `${name(d)} succeeds`),
    ...(job.after_any ?? []).map((d) => `${name(d)} ends`),
  ];
  return parts.length ? parts.join(", ") : null;
}

function Facts({ job, now }: { job: Job; now: number }) {
  const r = job.resources;
  const started = ms(job.started_at);
  const ended = ms(job.ended_at);
  const estStart = ms(job.estimated_start);
  const estEnd = ms(job.estimated_finish);
  const res = [
    `${r.cpus} CPU`,
    `${r.mem_gb % 1 ? r.mem_gb.toFixed(1) : r.mem_gb} GB`,
    r.gpus ? `${r.gpus} GPU${r.gpu_type ? ` (${r.gpu_type})` : ""}` : null,
    `${dur(r.time_s)} limit`,
  ].filter(Boolean);
  const rows: [string, string | null][] = [
    ["Runs on", job.backend],
    ["Why there", job.reason],
    ["Resources", res.join(", ")],
    ["Submitted", `${clock(ms(job.created_at), now)}`],
    [
      "Started",
      Number.isFinite(started)
        ? clock(started, now)
        : Number.isFinite(estStart)
          ? `expected ${clock(estStart, now)} (${job.estimated_start_source === "slurm" ? "Slurm's estimate" : "daemon's estimate"})`
          : null,
    ],
    [
      Number.isFinite(ended) ? "Ended" : "Expected to end",
      Number.isFinite(ended) ? clock(ended, now) : Number.isFinite(estEnd) ? `by ${clock(estEnd, now)}` : null,
    ],
    ["Exit code", job.exit_code === null ? null : String(job.exit_code)],
    ["Runs after", deps(job)],
    [
      "Inputs",
      job.inputs?.length ? job.inputs.map((i) => `${i.as} from job ${i.from_job}${i.glob ? ` (${i.glob})` : ""}`).join(", ") : null,
    ],
    ["Datasets", job.datasets?.length ? job.datasets.join(", ") : null],
    ["Image", job.image ?? null],
    [
      "Cost",
      job.cost_usd != null
        ? `$${job.cost_usd.toFixed(2)}`
        : job.usd_per_hour != null
          ? `$${job.usd_per_hour.toFixed(2)}/h, capped at $${job.max_cost_usd ?? "?"}`
          : null,
    ],
    ["Secrets", job.secrets.length ? job.secrets.join(", ") : null],
    ["Retries left", job.retries_left ? String(job.retries_left) : null],
    ["Submitted by", who(job.submitted_by)],
  ];
  return (
    <dl className="facts">
      {rows
        .filter(([, v]) => v)
        .map(([k, v]) => (
          <div key={k}>
            <dt>{k}</dt>
            <dd>{v}</dd>
          </div>
        ))}
    </dl>
  );
}

function CancelButton({ job, onDone }: { job: Job; onDone: () => void }) {
  const [confirm, setConfirm] = useState(false);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  if (!isActive(job.state)) return null;
  if (!confirm)
    return (
      <button className="btn btn-danger-quiet" onClick={() => setConfirm(true)}>
        Cancel job
      </button>
    );
  return (
    <span className="confirm" role="group" aria-label="Confirm cancel">
      <span>Stop job {job.id}?</span>
      <button
        className="btn btn-danger"
        disabled={busy}
        onClick={async () => {
          setBusy(true);
          try {
            await cancelJob(job.id);
            onDone();
          } catch (e) {
            setError(e instanceof ApiError ? e.message : String(e));
          } finally {
            setBusy(false);
            setConfirm(false);
          }
        }}
      >
        {busy ? "Cancelling" : "Cancel job"}
      </button>
      <button className="btn" onClick={() => setConfirm(false)}>
        Keep running
      </button>
      {error && <span className="error">{error}</span>}
    </span>
  );
}

/** Live resource use of a running job, as bars of used against what it asked for. */
function Usage({ job }: { job: Job }) {
  const { data } = usePoll(() => getMetrics(job.id), 10_000, [job.id]);
  const s = data?.latest;
  if (!s) return <p className="muted usage-wait">Waiting for the first usage sample…</p>;
  const r = job.resources;
  const memMb = r.mem_gb * 1024;
  const gb = (mb: number) => (mb >= 1024 ? `${(mb / 1024).toFixed(1)} GB` : `${Math.round(mb)} MB`);
  const power = s.gpus.reduce((p, g) => p + (g.power_w ?? 0), 0);
  return (
    <dl className="facts usage">
      <div>
        <dt>CPU</dt>
        <dd>
          <Meter used={s.cpu_pct} total={r.cpus * 100} text={`${(s.cpu_pct / 100).toFixed(1)} of ${r.cpus} cores`} />
        </dd>
      </div>
      <div>
        <dt>Memory</dt>
        <dd>
          <Meter used={s.rss_mb} total={memMb} text={`${gb(s.rss_mb)} of ${gb(memMb)}`} />
        </dd>
      </div>
      {s.gpus.map((g) => (
        <div key={g.index}>
          <dt>GPU {g.index}</dt>
          <dd>
            <Meter tone="gpu" used={g.util_pct ?? 0} total={100} text={`${g.util_pct ?? "?"}%, ${gb(g.mem_used_mb ?? 0)} of ${gb(g.mem_total_mb ?? 0)}${g.power_w != null ? `, ${Math.round(g.power_w)} W` : ""}`} />
          </dd>
        </div>
      ))}
      {s.gpus.length > 0 && (
        <div>
          <dt>Power</dt>
          <dd>{Math.round(power)} W across GPUs</dd>
        </div>
      )}
      {s.disk_free_gb != null && (
        <div>
          <dt>Disk free</dt>
          <dd>{Math.round(s.disk_free_gb)} GB on {s.host}</dd>
        </div>
      )}
    </dl>
  );
}

/** Time used against the job's limit. */
function TimeMeter({ job, now }: { job: Job; now: number }) {
  const started = ms(job.started_at);
  if (job.state !== "running" || !Number.isFinite(started)) return null;
  const used = (now - started) / 1000;
  const limit = job.resources.time_s;
  return (
    <div className="time-meter">
      <Meter tone="time" used={used} total={limit} text={`${dur(used)} of ${dur(limit)}, ${dur(Math.max(0, limit - used))} left`} />
    </div>
  );
}

export function JobPanel({ id, now }: { id: number; now: number }) {
  const { data: job, error, refresh } = usePoll(() => getJob(id), 3000, [id]);
  const close = () => (location.hash = "#/");
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => e.key === "Escape" && close();
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, []);
  const heading = useRef<HTMLHeadingElement>(null);
  useEffect(() => heading.current?.focus(), [id]);

  return (
    <aside className="panel" aria-label={`Job ${id}`}>
      <div className="panel-top">
        <span className="panel-id">Job {id}</span>
        <button className="btn" onClick={close}>
          Close
        </button>
      </div>
      {!job && error && <p className="error">{error instanceof ApiError && error.status === 404 ? `There's no job ${id}.` : error.message}</p>}
      {job && (
        <>
          <h2 ref={heading} tabIndex={-1}>
            {job.title}
          </h2>
          <p className="panel-desc">{job.description}</p>
          <div className="panel-state">
            <State state={job.state} />
            <span>{when(job, now)}</span>
          </div>
          <div className="actions">
            <CancelButton job={job} onDone={refresh} />
            {job.backend && !["waiting", "queued", "staging"].includes(job.state) && (
              <a className="btn" href={outputsUrl(job.id)} download={`job-${job.id}-outputs.tar.zst`}>
                Download outputs
              </a>
            )}
            {job.array_id !== null && <span className="muted">Task {job.array_index} of array {job.array_id}</span>}
          </div>
          <TimeMeter job={job} now={now} />
          {job.state === "running" && <Usage job={job} />}
          <Facts job={job} now={now} />
          <Log id={job.id} active={isActive(job.state)} />
        </>
      )}
    </aside>
  );
}
