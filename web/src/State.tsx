import type { JobState } from "./api";

const TONE: Record<JobState, string> = {
  waiting: "waiting",
  queued: "waiting",
  staging: "waiting",
  pending: "waiting",
  running: "running",
  succeeded: "done",
  cancelled: "done",
  failed: "failed",
  timeout: "failed",
  lost: "failed",
  dependency_failed: "failed",
};

/** A job state, colored by what it means: running, waiting, done or failed. */
export function State({ state }: { state: JobState }) {
  return <span className={`state state-${TONE[state]}`}>{state.replace("_", " ")}</span>;
}
