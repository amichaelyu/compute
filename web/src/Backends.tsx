import { Fragment, useState } from "react";
import type { ReactNode } from "react";
import { ApiError, type BackendDetail, disableBackend, enableBackend, getBackend, listBackends } from "./api";
import { Meter } from "./Meter";
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

type Rows = [string, ReactNode][];
type Obj = Record<string, unknown>;
const num = (v: unknown) => (typeof v === "number" ? v : Number(v));
const obj = (v: unknown): Obj => (v && typeof v === "object" ? (v as Obj) : {});

const OS_NAMES: Record<string, string> = { macos: "macOS", linux: "Linux" };
const gb = (bytes: unknown) => Math.round((num(bytes) / 1024 ** 3) * 10) / 10;

/** A local machine's specs as label/value rows. */
function localRows(h: Obj): Rows {
  const sched = obj(h.schedulable);
  const os = String(h.os ?? "");
  const rows: Rows = [
    ["Processor", `${h.cpu_model ?? "unknown"}, ${h.cpus} cores`],
    ["Memory", `${h.mem_gb} GB`],
  ];
  if (Number.isFinite(num(h.docker_mem_bytes))) rows.push(["Docker VM", `${gb(h.docker_mem_bytes)} GB`]);
  rows.push(["System", `${OS_NAMES[os] ?? os}, ${h.arch}`]);
  if (Number.isFinite(num(h.disk_free_kb))) rows.push(["Disk free", `${Math.round(num(h.disk_free_kb) / 1048576)} GB`]);
  rows.push(["Jobs may use", `${sched.cpus} CPUs, ${sched.mem_gb} GB`]);
  return rows;
}

/** Rows per column of the GPU price list before it starts another column. */
const GPU_ROWS = 8;

type GpuStock = {
  gpu_type: string;
  usd_per_hour_per_gpu: number;
  max_gpus?: number;
  sizes?: number[];
  level?: string;
  regions?: string[];
};

/** "up to 8", with the sizes on sale, the stock level and where it is in the tooltip. */
function stockText(s: GpuStock) {
  if (!s.max_gpus) return { text: "", title: undefined };
  const sizes = s.sizes && s.sizes.length > 1 ? `${s.sizes.join(", ")} GPUs per machine` : null;
  const title = [
    sizes ?? `up to ${s.max_gpus} per machine`,
    s.level && `${s.level} stock`,
    s.regions?.length && `in ${s.regions.join(", ")}`,
  ]
    .filter(Boolean)
    .join(", ");
  return { text: s.max_gpus > 1 ? `up to ${s.max_gpus}` : "1", title };
}

/** A cloud's in-stock GPU types as a price list, most expensive first, in balanced columns.
 * Live stock (`availability.gpu_stock`) wins over the daily hardware probe's list. */
function CloudGpus({ h, live }: { h: Obj; live: unknown }) {
  const stock: GpuStock[] = Array.isArray(live)
    ? (live as GpuStock[])
    : Object.entries(obj(h.gpu_types))
        .filter(([, v]) => obj(v).available)
        .map(([t, v]) => ({ gpu_type: t, usd_per_hour_per_gpu: num(obj(v).usd_per_hour_per_gpu) }));
  const types = stock
    .sort((a, b) => b.usd_per_hour_per_gpu - a.usd_per_hour_per_gpu || a.gpu_type.localeCompare(b.gpu_type))
    .map((s) => {
      // Lambda names carry the card's specs in parentheses: "A100 (40 GB SXM4)".
      const m = /^(.*?)\s*\((.+)\)$/.exec(s.gpu_type);
      return { name: m ? m[1]! : s.gpu_type, spec: m ? m[2]! : "", price: s.usd_per_hour_per_gpu, stock: stockText(s) };
    });
  const counts = types.some((t) => t.stock.text);
  if (!types.length) return <p className="muted">No GPUs in stock right now.</p>;
  const cols = Math.min(3, Math.ceil(types.length / GPU_ROWS));
  const per = Math.ceil(types.length / cols);
  const chunks = Array.from({ length: cols }, (_, i) => types.slice(i * per, (i + 1) * per));
  return (
    <div>
      <p className="hw-caption muted">
        {types.length} GPU types in stock, per GPU-hour{counts && ", with the most GPUs one machine can have"}
      </p>
      <div className={`hw-price-cols${counts ? " hw-counts" : ""}`}>
        {chunks.map((chunk, i) => (
          <ul key={i} className="hw-prices">
            {chunk.map((t) => (
              <li key={t.name + t.spec}>
                <span>{t.name}</span>
                <span className="muted">{t.spec}</span>
                {counts && (
                  <span className="hw-count muted" title={t.stock.title}>
                    {t.stock.text}
                  </span>
                )}
                <span className="hw-price">${t.price.toFixed(2)}</span>
              </li>
            ))}
          </ul>
        ))}
      </div>
    </div>
  );
}

/** Node names from Slurm's compressed list, split between hosts but not inside brackets:
 * "barley-[01-04],oat-[01-06]" → ["barley-[01-04]", "oat-[01-06]"]. */
const nodeNames = (s: string) => s.split(/,(?![^[]*\])/).filter(Boolean);

/** A Slurm cluster's partitions: one aligned line per kind of node. */
function SlurmPartitions({ h }: { h: Obj }) {
  const parts = Object.entries(obj(h.partitions)).map(([name, info]) => [name, obj(info)] as const);
  return (
    <div className="hw-parts">
      {parts.map(([name, p]) => {
        const groups = Array.isArray(p.groups) ? (p.groups as Obj[]) : [];
        return (
          <section key={name} className="hw-part">
            <h4>
              {name}
              {p.gpu_type ? (
                <span className="muted">
                  {" "}
                  {String(p.gpu_type)} GPUs, {String(p.gpu_mem_gb ?? "?")} GB each
                </span>
              ) : (
                <span className="muted"> CPU only</span>
              )}
            </h4>
            {groups.map((g, i) => {
              const gres = String(g.gres_or_nodes ?? "");
              const gpus = /gpu:(\d+)/.exec(gres)?.[1];
              const hosts = !gpus && gres && gres !== "(null)" ? nodeNames(gres) : [];
              return (
                <div key={i} className="hw-group">
                  <span>{String(g.nodes)} nodes</span>
                  <span>{String(g.cpus)} CPUs</span>
                  <span>{String(g.mem_gb)} GB</span>
                  <span>{gpus ? `${gpus} GPUs` : ""}</span>
                  {hosts.length > 0 && (
                    <span className="hw-hosts muted">
                      {hosts.map((n, j) => (
                        <Fragment key={n}>
                          <span>{n}</span>
                          {j < hosts.length - 1 ? ", " : ""}
                        </Fragment>
                      ))}
                    </span>
                  )}
                </div>
              );
            })}
          </section>
        );
      })}
    </div>
  );
}

/** The hardware column, laid out for each kind of backend; anything else falls back to KV. */
function Hardware({ b }: { b: BackendDetail }) {
  if (!b.hardware) return <KV value={null} />;
  const h = b.hardware;
  if (b.kind === "local") return <RowsOrKV rows={localRows(h)} raw={h} />;
  if (b.kind === "slurm") return Object.keys(obj(h.partitions)).length ? <SlurmPartitions h={h} /> : <KV value={h} />;
  return (
    <div className="hw">
      <CloudGpus h={h} live={obj(b.availability).gpu_stock} />
      <RowsOrKV
        rows={[
          ["Nodes per job", `up to ${h.max_nodes ?? 1}`],
          ["Prices from", String(h.price_source ?? "provider")],
        ]}
        raw={null}
      />
    </div>
  );
}

/** Plain-language availability rows. */
function availabilityRows(b: BackendDetail): Rows | null {
  const a = b.availability;
  if (!a) return null;
  if (b.kind === "local") {
    const h = obj(b.hardware);
    const sched = obj(h.schedulable);
    const cpus = num(sched.cpus);
    const mem = num(sched.mem_gb);
    const usedMem = num(a.mem_gb_in_use ?? 0);
    const cores = num(a.cores ?? h.cpus);
    const load = num(a.load_1m ?? NaN);
    const rows: Rows = [
      ["Jobs running", String(a.running_jobs ?? 0)],
      ["CPUs", <Meter used={num(a.cpus_in_use ?? 0)} total={cpus} text={`${a.cpus_in_use ?? 0} of ${cpus} reserved`} />],
      ["Memory", <Meter used={usedMem} total={mem} text={`${Math.round(usedMem * 10) / 10} of ${mem} GB reserved`} />],
    ];
    if (Number.isFinite(load) && cores > 0)
      rows.push(["Load", <Meter used={load} total={cores} text={`${load.toFixed(2)} on ${cores} cores`} />]);
    const total = num(a.disk_total_gb ?? NaN);
    const free = num(a.disk_free_gb ?? NaN);
    if (Number.isFinite(total) && Number.isFinite(free))
      rows.push(["Disk", <Meter used={total - free} total={total} text={`${Math.round(free)} GB free of ${Math.round(total)}`} />]);
    return rows;
  }
  if (b.kind !== "slurm") {
    const usd = (v: unknown) => `$${num(v ?? 0).toFixed(2)}`;
    return [
      ["Spending", `${usd(a.usd_per_hour_now)}/h`],
      ["This month", `${usd(a.spent_this_month_usd)} spent`],
      ["Instances", String(a.my_instances ?? 0)],
      ["Boot time", `about ${Math.round(num(a.boot_estimate_s ?? 0) / 60)} min`],
    ];
  }
  const nodes = Object.entries(obj(a.nodes))
    .map(([n, v]) => [n, num(obj(v).gpus_free)] as const)
    .filter(([, free]) => free > 0)
    .map(([n, free]) => `${n} (${free})`);
  const next = typeof a.next_gpu_free_estimate === "string" ? ms(a.next_gpu_free_estimate) : NaN;
  const gpus = num(a.gpus_total ?? NaN);
  const freeGpus = num(a.gpus_free_now ?? 0);
  const rows: Rows = [];
  if (Number.isFinite(gpus) && gpus > 0)
    rows.push(["GPUs", <Meter tone="gpu" used={gpus - freeGpus} total={gpus} text={`${gpus - freeGpus} of ${gpus} in use`} />]);
  if (nodes.length) rows.push(["Free GPUs on", nodes.join(", ")]);
  const cpus = num(a.cpus_total ?? NaN);
  if (Number.isFinite(cpus) && cpus > 0)
    rows.push(["GPU-node CPUs", <Meter used={num(a.cpus_allocated ?? 0)} total={cpus} text={`${a.cpus_allocated ?? 0} of ${cpus} allocated`} />]);
  rows.push(["Your jobs", `${a.my_running ?? 0} running, ${a.my_pending ?? 0} waiting`]);
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

/** A placeholder shaped like a backend section, shown until every backend has loaded. */
function BackendSkeleton() {
  return (
    <section className="backend" aria-hidden="true">
      <header>
        <span className="skel skel-title" />
        <span className="skel skel-word" />
        <span className="skel skel-phrase" />
      </header>
      <div className="backend-cols">
        {[4, 6].map((lines, c) => (
          <div key={c}>
            <span className="skel skel-word skel-heading" />
            {Array.from({ length: lines }, (_, r) => (
              <span key={r} className="skel skel-line" />
            ))}
          </div>
        ))}
      </div>
    </section>
  );
}

/** Every backend's details, fetched together so the page fills in at once. A backend whose
 * details fail keeps its summary and the error. */
async function loadBackends(): Promise<{ name: string; detail: BackendDetail | null; error: string | null }[]> {
  const list = await listBackends();
  return Promise.all(
    list.map((s) =>
      getBackend(s.name).then(
        (detail) => ({ name: s.name, detail, error: null }),
        (e: unknown) => ({ name: s.name, detail: null, error: e instanceof Error ? e.message : String(e) }),
      ),
    ),
  );
}

function Backend({ b, onChange }: { b: BackendDetail; onChange: () => void }) {
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
          {b.kind === "slurm"
            ? `Slurm cluster, reached over ${b.transport}`
            : b.kind === "local"
              ? `machine, reached over ${b.transport}`
              : `${b.kind} cloud, paid per job`}
        </span>
      </header>
      <Toggle b={b} onChange={onChange} />
      {b.last_error && !b.reachable && <p className="error">{b.last_error}</p>}
      <div className="backend-cols">
        <div>
          <h3>Right now</h3>
          <RowsOrKV rows={availabilityRows(b)} raw={b.availability} />
          {b.refreshed_at.availability && <p className="muted small">Checked {ago(ms(b.refreshed_at.availability))}</p>}
        </div>
        <div>
          <h3>Hardware</h3>
          <Hardware b={b} />
        </div>
      </div>
    </section>
  );
}

export function Backends() {
  const { data, error, refresh } = usePoll(loadBackends, 30_000, []);
  return (
    <div className="backends" aria-busy={!data}>
      {error && !data && <p className="error">{error.message}</p>}
      {!data && !error && [0, 1, 2].map((i) => <BackendSkeleton key={i} />)}
      {data?.map((x) =>
        x.detail ? (
          <Backend key={x.name} b={x.detail} onChange={refresh} />
        ) : (
          <section key={x.name} className="backend">
            <header>
              <h2>{x.name}</h2>
            </header>
            <p className="error">{x.error}</p>
          </section>
        ),
      )}
    </div>
  );
}
