import { useEffect, useRef, useState } from "react";
import type { JobRow } from "./api";

export const ms = (s: string | null | undefined) => (s ? Date.parse(s) : NaN);

const pad = (n: number) => String(n).padStart(2, "0");

/** "19:40", or "Tue 19:40" when not today. */
export function clock(t: number, now = Date.now()) {
  const d = new Date(t);
  const hm = `${pad(d.getHours())}:${pad(d.getMinutes())}`;
  const today = new Date(now).toDateString() === d.toDateString();
  return today ? hm : `${d.toLocaleDateString(undefined, { weekday: "short" })} ${hm}`;
}

/** "45 s", "12 min", "2 h 5 min", "3 d 4 h". */
export function dur(seconds: number) {
  const s = Math.max(0, Math.round(seconds));
  if (s < 60) return `${s} s`;
  const m = Math.round(s / 60);
  if (m < 60) return `${m} min`;
  const h = Math.floor(m / 60);
  if (h < 48) return m % 60 ? `${h} h ${m % 60} min` : `${h} h`;
  const d = Math.floor(h / 24);
  return h % 24 ? `${d} d ${h % 24} h` : `${d} d`;
}

export function ago(t: number, now = Date.now()) {
  const s = (now - t) / 1000;
  return s < 45 ? "just now" : `${dur(s)} ago`;
}

/** One plain-language line about where a job is in its life. */
export function when(j: Pick<JobRow, "state" | "started_at" | "ended_at" | "estimated_start" | "exit_code">, now = Date.now()) {
  const started = ms(j.started_at);
  const ended = ms(j.ended_at);
  const est = ms(j.estimated_start);
  switch (j.state) {
    case "running":
      return Number.isFinite(started) ? `running for ${dur((now - started) / 1000)}` : "running";
    case "staging":
      return "copying code";
    case "queued":
    case "pending":
      if (!Number.isFinite(est) || est - now < 60_000) return "starting soon";
      return `starts around ${clock(est, now)}`;
    default: {
      const took = Number.isFinite(started) && Number.isFinite(ended) ? `took ${dur((ended - started) / 1000)}, ` : "";
      return Number.isFinite(ended) ? `${took}${ago(ended, now)}` : j.state;
    }
  }
}

/** Polls `fn` every `every` ms while the tab is visible. */
export function usePoll<T>(fn: () => Promise<T>, every: number, deps: unknown[]) {
  const [data, setData] = useState<T | null>(null);
  const [error, setError] = useState<Error | null>(null);
  const [tick, setTick] = useState(0);
  const fnRef = useRef(fn);
  fnRef.current = fn;
  useEffect(() => {
    let alive = true;
    let timer: ReturnType<typeof setTimeout>;
    const run = async () => {
      if (!document.hidden) {
        try {
          const d = await fnRef.current();
          if (alive) {
            setData(d);
            setError(null);
          }
        } catch (e) {
          if (alive) setError(e as Error);
        }
      }
      if (alive) timer = setTimeout(run, every);
    };
    run();
    const onVisible = () => {
      if (!document.hidden) {
        clearTimeout(timer);
        run();
      }
    };
    document.addEventListener("visibilitychange", onVisible);
    return () => {
      alive = false;
      clearTimeout(timer);
      document.removeEventListener("visibilitychange", onVisible);
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [every, tick, ...deps]);
  return { data, error, refresh: () => setTick((t) => t + 1) };
}

/** Re-renders every `every` ms so relative times stay current. */
export function useNow(every = 30_000) {
  const [now, setNow] = useState(Date.now());
  useEffect(() => {
    const t = setInterval(() => setNow(Date.now()), every);
    return () => clearInterval(t);
  }, [every]);
  return now;
}

export type Route = { view: "jobs"; job: number | null } | { view: "backends"; job: number | null };

function parseHash(h: string): Route {
  const parts = h.replace(/^#\/?/, "").split("/").filter(Boolean);
  if (parts[0] === "backends") return { view: "backends", job: null };
  if (parts[0] === "jobs" && parts[1] && /^\d+$/.test(parts[1])) return { view: "jobs", job: Number(parts[1]) };
  return { view: "jobs", job: null };
}

export function useRoute(): Route {
  const [route, setRoute] = useState(() => parseHash(location.hash));
  useEffect(() => {
    const on = () => setRoute(parseHash(location.hash));
    window.addEventListener("hashchange", on);
    return () => window.removeEventListener("hashchange", on);
  }, []);
  return route;
}

/** "SSH key …LYhQ" for API sessions, the email for dashboard (Cloudflare Access) sessions. */
export function who(identity: string) {
  if (identity.startsWith("cf:")) return identity.slice(3);
  if (identity.startsWith("SHA256:")) return `SSH key …${identity.slice(-6)}`;
  return identity;
}

/** Turns "gpus_free_now" into "GPUs free now". */
export function label(key: string) {
  const s = key.replace(/_/g, " ").replace(/\bgpus?\b/gi, (m) => m.toUpperCase().replace("S", "s"));
  return s.charAt(0).toUpperCase() + s.slice(1);
}
