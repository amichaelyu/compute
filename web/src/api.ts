// Client for the compute daemon's /v1/ API. In the browser, Cloudflare Access authenticates every
// request; on the tailnet, a pasted API session token (from ~/.config/compute/session) works too.

export type JobState =
  | "waiting"
  | "queued"
  | "staging"
  | "pending"
  | "running"
  | "succeeded"
  | "failed"
  | "cancelled"
  | "timeout"
  | "lost"
  | "dependency_failed";

export const ACTIVE: JobState[] = ["waiting", "queued", "staging", "pending", "running"];
export const PROBLEM: JobState[] = ["failed", "timeout", "lost", "dependency_failed"];

export function isActive(s: JobState) {
  return ACTIVE.includes(s);
}

export interface JobRow {
  id: number;
  array_id: number | null;
  array_index: number | null;
  title: string;
  state: JobState;
  backend: string | null;
  reason: string | null;
  created_at: string;
  started_at: string | null;
  ended_at: string | null;
  estimated_start: string | null;
  estimated_finish: string | null;
  time_s: number;
  exit_code: number | null;
  /** The name of the key that submitted it (its fingerprint if the key is gone). */
  submitted_by: string;
  /** Cloud jobs: the instance's rate and what it cost (so far, while it runs). */
  usd_per_hour?: number;
  cost_usd?: number;
}

export interface Job extends Omit<JobRow, "time_s" | "cost_usd" | "usd_per_hour"> {
  description: string;
  estimated_start_source: string | null;
  resources: { nodes: number; cpus: number; mem_gb: number; gpus: number; gpu_type: string | null; time_s: number };
  secrets: string[];
  retries_left: number;
  priority: number;
  after_ok?: (number | { array: number })[];
  after_any?: (number | { array: number })[];
  inputs?: { from_job: number; glob?: string | null; as: string }[];
  datasets?: string[];
  image?: string | null;
  max_cost_usd?: number | null;
  cost_usd?: number | null;
  usd_per_hour?: number;
}

export interface BackendSummary {
  name: string;
  kind: "local" | "slurm" | "runpod" | "lambda" | "modal";
  transport: "rex" | "ssh";
  enabled: boolean;
  enabled_until: string | null;
  reachable: boolean;
  hardware: string;
  active_jobs: number;
  paid?: boolean;
  runtimes?: string[];
}

export interface BackendDetail extends Omit<BackendSummary, "hardware" | "active_jobs"> {
  last_error: string | null;
  hardware: Record<string, unknown> | null;
  availability: Record<string, unknown> | null;
  restrictions: string[];
  refreshed_at: { hardware: string | null; availability: string | null };
}

export interface LogChunk {
  data: string;
  next_offset: number;
  eof: boolean;
  size?: number;
}

export class ApiError extends Error {
  constructor(
    public status: number,
    public code: string,
    message: string,
    public hint = "",
  ) {
    super(message);
  }
}

const TOKEN_KEY = "compute.token";
export const token = {
  get: () => localStorage.getItem(TOKEN_KEY),
  set: (t: string) => localStorage.setItem(TOKEN_KEY, t.trim()),
  clear: () => localStorage.removeItem(TOKEN_KEY),
};

async function api<T>(path: string, init: RequestInit = {}): Promise<T> {
  const headers = new Headers(init.headers);
  const t = token.get();
  if (t) headers.set("Authorization", `Bearer ${t}`);
  if (init.body) headers.set("Content-Type", "application/json");
  const res = await fetch(path, { ...init, headers, credentials: "same-origin" });
  const body = await res.json().catch(() => null);
  if (!res.ok) {
    const e = body?.error;
    throw new ApiError(res.status, e?.code ?? "http", e?.message ?? `${res.status} ${res.statusText}`, e?.hint ?? "");
  }
  return body as T;
}

export const whoami = () => api<{ identity: string; name?: string; scope: "full" | "guest" | "dashboard" }>("/v1/whoami");

/** A key in allowed_signers: someone who may use the API. */
export interface User {
  name: string;
  fingerprint: string;
  key_type: string;
  comment: string;
  /** Why this key can't sign in (an unsupported option, or outside its validity window). */
  note?: string;
  /** Not a guest: a principal in `owners` (or no owners are set). */
  owner?: boolean;
  access?: UserAccess;
}

/** Caps on one paid backend; null or missing: no cap of that kind. */
export interface SpendLimit {
  usd_per_hour?: number | null;
  usd_per_month?: number | null;
}

/** What the owner set for a key. `backends: null` is the default: owners every backend, guests the free ones. */
export interface KeyPolicy {
  backends: string[] | null;
  limits?: Record<string, SpendLimit>;
}

export interface UserAccess {
  policy: KeyPolicy;
  /** The backends the key may use now. */
  allowed: string[];
  /** For each paid backend it may use: its caps and this month's spend. */
  limits: Record<
    string,
    {
      usd_per_hour: number | null;
      usd_per_month: number | null;
      in_use_usd_per_hour: number;
      spent_this_month_usd: number;
      committed_this_month_usd: number;
    }
  >;
}

export interface BackendRef {
  name: string;
  paid: boolean;
}

export const listUsers = () => api<{ users: User[]; backends: BackendRef[] }>("/v1/users");
export const setUserAccess = (fingerprint: string, policy: KeyPolicy) =>
  api<User>(`/v1/users/access?fingerprint=${encodeURIComponent(fingerprint)}`, {
    method: "PUT",
    body: JSON.stringify(policy),
  });
export const addUser = (name: string, public_key: string) =>
  api<User>("/v1/users", { method: "POST", body: JSON.stringify({ name, public_key }) });
export const removeUser = (fingerprint: string) =>
  api<{ removed: User[]; sessions_ended: number }>(`/v1/users?fingerprint=${encodeURIComponent(fingerprint)}`, {
    method: "DELETE",
  });

export const listJobs = (params: { state?: JobState[]; q?: string; limit?: number; before?: number }) => {
  const sp = new URLSearchParams({ limit: String(params.limit ?? 300) });
  if (params.state?.length) sp.set("state", params.state.join(","));
  if (params.q) sp.set("q", params.q);
  if (params.before !== undefined) sp.set("before", String(params.before));
  return api<{ jobs: JobRow[] }>(`/v1/jobs?${sp}`).then((r) => r.jobs);
};

/** Every job matching `state`, newest first, paging back 1000 at a time. */
export async function listAllJobs(state?: JobState[]): Promise<JobRow[]> {
  const out: JobRow[] = [];
  for (;;) {
    const page = await listJobs({ state, limit: 1000, before: out.at(-1)?.id });
    out.push(...page);
    if (page.length < 1000) return out;
  }
}

/** The newest 300 jobs plus every active one, however old: the dashboard's working set. */
export async function listWorkingJobs(): Promise<JobRow[]> {
  const [recent, active] = await Promise.all([listJobs({ limit: 300 }), listAllJobs(ACTIVE)]);
  const byId = new Map<number, JobRow>();
  for (const j of [...active, ...recent]) byId.set(j.id, j);
  return [...byId.values()].sort((a, b) => b.id - a.id);
}

export const getJob = (id: number) => api<Job>(`/v1/jobs/${id}`);
export const getLogs = (id: number, offset: number) =>
  api<LogChunk>(`/v1/jobs/${id}/logs?offset=${offset}&max_bytes=262144`);
export const cancelJob = (id: number) =>
  api<{ cancelled: number; errors: string[] }>(`/v1/jobs/${id}/cancel`, { method: "POST" });

export const listBackends = () => api<{ backends: BackendSummary[] }>("/v1/backends").then((r) => r.backends);
export const getBackend = (name: string) => api<BackendDetail>(`/v1/backends/${encodeURIComponent(name)}`);
export const enableBackend = (name: string, until?: string) =>
  api<BackendSummary>(`/v1/backends/${encodeURIComponent(name)}/enable`, {
    method: "POST",
    body: JSON.stringify(until ? { until } : {}),
  });
export const disableBackend = (name: string) =>
  api<BackendSummary>(`/v1/backends/${encodeURIComponent(name)}/disable`, { method: "POST" });

export const outputsUrl = (id: number) => `/v1/jobs/${id}/outputs`;

export interface GpuSample {
  index: number;
  util_pct: number | null;
  mem_used_mb: number | null;
  mem_total_mb: number | null;
  power_w: number | null;
}
export interface Sample {
  t: number;
  cpu_pct: number;
  rss_mb: number;
  gpus: GpuSample[];
  disk_free_gb: number | null;
  host: string;
}
export interface Metrics {
  latest: Sample | null;
  history: Sample[];
  requested: { cpus: number; mem_mb: number; gpus: number };
}
export const getMetrics = (id: number) => api<Metrics>(`/v1/jobs/${id}/metrics`);

/** Cloud spend: the hourly limit, what's running against it, and this month's total. */
export interface Budget {
  hourly_usd: number;
  in_use_usd_per_hour: number;
  remaining_usd_per_hour: number;
  spent_this_month_usd: number;
}
export const getBudget = () => api<Budget>("/v1/budget");

/** A job in a Slurm backend's queue; `compute_job` is set when compute submitted it. */
export interface QueuedJob {
  slurm_id: string;
  name: string;
  state: string;
  partition: string;
  start: string | null;
  end: string | null;
  time_limit_s: number | null;
  elapsed_s: number | null;
  gres: string | null;
  cpus: number | null;
  reason: string;
  compute_job: number | null;
}
export const getQueue = (name: string) =>
  api<{ jobs: QueuedJob[] }>(`/v1/backends/${encodeURIComponent(name)}/queue`).then((r) => r.jobs);
