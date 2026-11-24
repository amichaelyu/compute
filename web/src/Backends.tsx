import { useState } from "react";
import { ApiError, type BackendDetail, disableBackend, enableBackend, getBackend, listBackends } from "./api";
import { ago, clock, label, ms, usePoll } from "./util";

/** Renders the daemon's hardware/availability objects as a readable list. */
function KV({ value }: { value: unknown }) {
  if (value === null || value === undefined) return <span className="muted">unknown</span>;
  if (typeof value !== "object") {
    const s = String(value);
    const t = ms(s);
    return <span>{/^\d{4}-\d\d-\d\dT/.test(s) && Number.isFinite(t) ? clock(t) : s}</span>;
  }
  if (Array.isArray(value)) {
    return (
      <ul className="kv-list">
        {value.map((v, i) => (
          <li key={i}>
            <KV value={v} />
          </li>
        ))}
      </ul>
    );
  }
  const entries = Object.entries(value as Record<string, unknown>).filter(([, v]) => v !== null);
  return (
    <dl className="kv">
      {entries.map(([k, v]) => (
        <div key={k}>
          <dt>{label(k)}</dt>
          <dd>
            <KV value={v} />
          </dd>
        </div>
      ))}
    </dl>
  );
}

type Rows = [string, string][];
type Obj = Record<string, unknown>;
const num = (v: unknown) => (typeof v === "number" ? v : Number(v));
const obj = (v: unknown): Obj => (v && typeof v === "object" ? (v as Obj) : {});

/** Plain-language hardware rows for the backends we know how to describe. */
function hardwareRows(b: BackendDetail): Rows | null {
  const h = obj(b.hardware);
  if (b.kind === "local") {
    if (!b.hardware) return null;
    const sched = obj(h.schedulable);
    const rows: Rows = [
      ["Processor", `${h.cpu_model ?? "unknown"}, ${h.cpus} cores`],
      ["Memory", `${h.mem_gb} GB`],
      ["System", `${h.os} on ${h.arch}`],
    ];
    if (Number.isFinite(num(h.disk_free_kb))) rows.push(["Disk free", `${Math.round(num(h.disk_free_kb) / 1048576)} GB`]);
    rows.push(["Jobs may use", `${sched.cpus} CPUs and ${sched.mem_gb} GB`]);
    return rows;
  }
  const rows: Rows = [];
  for (const [part, info] of Object.entries(obj(h.partitions))) {
    const p = obj(info);
    const groups = Array.isArray(p.groups) ? (p.groups as Obj[]) : [];
    const lines = groups.map((g) => {
      const gres = String(g.gres_or_nodes ?? "");
      const gpu = /gpu:(\d+)/.exec(gres);
      const extra = gpu ? `, ${gpu[1]} GPUs each` : gres && gres !== "(null)" ? ` (${gres})` : "";
      return `${g.nodes} × ${g.cpus} CPUs, ${g.mem_gb} GB${extra}`;
    });
    if (p.gpu_type) lines.push(`${p.gpu_type} GPUs with ${p.gpu_mem_gb ?? "?"} GB each`);
    rows.push([`${part} partition`, lines.join("; ")]);
  }
  return rows.length ? rows : null;
}

/** Plain-language availability rows. */
function availabilityRows(b: BackendDetail): Rows | null {
  const a = b.availability;
  if (!a) return null;
  if (b.kind === "local") {
    return [
      ["Jobs running", String(a.running_jobs ?? 0)],
      ["CPUs in use", `${a.cpus_in_use ?? 0}, ${a.cpus_free ?? "?"} free`],
      ["Memory in use", `${Math.round(num(a.mem_gb_in_use ?? 0) * 10) / 10} GB`],
      ["Load", String(a.load_average ?? "unknown")],
    ];
  }
  const nodes = Object.entries(obj(a.nodes))
    .map(([n, v]) => [n, num(obj(v).gpus_free)] as const)
    .filter(([, free]) => free > 0)
    .map(([n, free]) => `${n} (${free})`);
  const next = typeof a.next_gpu_free_estimate === "string" ? ms(a.next_gpu_free_estimate) : NaN;
  const rows: Rows = [
    ["GPUs free", `${a.gpus_free_now ?? 0}${nodes.length ? ` on ${nodes.join(", ")}` : ""}`],
    ["Your jobs", `${a.my_running ?? 0} running, ${a.my_pending ?? 0} waiting`],
  ];
  if (Number.isFinite(next) && num(a.gpus_free_now) === 0) rows.push(["Next GPU frees", `around ${clock(next)}`]);
  return rows;
}

function RowsOrKV({ rows, raw }: { rows: Rows | null; raw: unknown }) {
  if (!rows) return <KV value={raw} />;
  return (
    <dl className="kv">
      {rows.map(([k, v]) => (
        <div key={k}>
          <dt>{k}</dt>
          <dd>{v}</dd>
        </div>
      ))}
    </dl>
  );
}

function Toggle({ b, onChange }: { b: BackendDetail; onChange: () => void }) {
  const [until, setUntil] = useState("");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const act = async (fn: () => Promise<unknown>) => {
    setBusy(true);
    setError(null);
    try {
      await fn();
      onChange();
    } catch (e) {
      setError(e instanceof ApiError ? `${e.message}. ${e.hint}` : String(e));
    } finally {
      setBusy(false);
    }
  };
  return (
    <div className="toggle">
      {b.enabled ? (
        <button className="btn" disabled={busy} onClick={() => act(() => disableBackend(b.name))}>
          Turn off
        </button>
      ) : (
        <>
          <label>
            Until <input type="time" value={until} onChange={(e) => setUntil(e.target.value)} />
          </label>
          <button className="btn btn-primary" disabled={busy} onClick={() => act(() => enableBackend(b.name, until || undefined))}>
            Turn on
          </button>
        </>
      )}
      {b.enabled && <span className="muted">Turning off lets running jobs finish and starts nothing new.</span>}
      {error && <span className="error">{error}</span>}
    </div>
  );
}

function Backend({ name }: { name: string }) {
  const { data: b, error, refresh } = usePoll(() => getBackend(name), 30_000, [name]);
  if (!b) return <section className="backend">{error ? <p className="error">{error.message}</p> : <h2>{name}</h2>}</section>;
  const status = !b.enabled
    ? "off"
    : !b.reachable
      ? "on, but can't reach it"
      : b.enabled_until
        ? `on until ${clock(ms(b.enabled_until))}`
        : "on";
  return (
    <section className={`backend${b.enabled ? "" : " backend-off"}`}>
      <header>
        <h2>{b.name}</h2>
        <span className={`backend-status${b.enabled && b.reachable ? " ok" : ""}`}>{status}</span>
        <span className="muted">
          {b.kind === "slurm" ? "Slurm cluster" : "machine"}, reached over {b.transport}
        </span>
      </header>
      <Toggle b={b} onChange={refresh} />
      {b.last_error && !b.reachable && <p className="error">{b.last_error}</p>}
      <div className="backend-cols">
        <div>
          <h3>Right now</h3>
          <RowsOrKV rows={availabilityRows(b)} raw={b.availability} />
          {b.refreshed_at.availability && <p className="muted small">Checked {ago(ms(b.refreshed_at.availability))}</p>}
        </div>
        <div>
          <h3>Hardware</h3>
          <RowsOrKV rows={hardwareRows(b)} raw={b.hardware} />
        </div>
        <div>
          <h3>Limits</h3>
          <ul className="limits">
            {b.restrictions.map((r) => (
              <li key={r}>{r}</li>
            ))}
          </ul>
        </div>
      </div>
    </section>
  );
}

export function Backends() {
  const { data, error } = usePoll(listBackends, 60_000, []);
  return (
    <div className="backends">
      {error && !data && <p className="error">{error.message}</p>}
      {data?.map((b) => <Backend key={b.name} name={b.name} />)}
    </div>
  );
}
