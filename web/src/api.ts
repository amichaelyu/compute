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
}

export interface Job extends Omit<JobRow, "time_s"> {
  description: string;
  estimated_start_source: string | null;
  resources: { nodes: number; cpus: number; mem_gb: number; gpus: number; gpu_type: string | null; time_s: number };
  secrets: string[];
  retries_left: number;
  priority: number;
  submitted_by: string;
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

export const whoami = () => api<{ identity: string; scope: "full" | "dashboard" }>("/v1/whoami");

export const listJobs = (params: { state?: JobState[]; q?: string; limit?: number }) => {
  const sp = new URLSearchParams({ limit: String(params.limit ?? 300) });
  if (params.state?.length) sp.set("state", params.state.join(","));
  if (params.q) sp.set("q", params.q);
  return api<{ jobs: JobRow[] }>(`/v1/jobs?${sp}`).then((r) => r.jobs);
};

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
