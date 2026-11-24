import { createRoot } from "react-dom/client";
import "./styles.css";
import { ApiError, type QueuedJob, getQueue, listBackends, listJobs, token, whoami } from "./api";
import { Backends } from "./Backends";
import { JobList } from "./JobList";
import { JobPanel } from "./JobPanel";
import { Lanes } from "./Lanes";
import { SignIn } from "./SignIn";
import { Users } from "./Users";
import { useNow, usePoll, useRoute } from "./util";

/** Reloads the page when the daemon serves a newer dashboard build, so an open tab never runs
 * old code against a new API. */
function watchForNewBuild() {
  const current = document.querySelector<HTMLScriptElement>('script[type="module"][src]')?.src;
  if (!current) return;
  setInterval(async () => {
    if (document.hidden) return;
    try {
      const html = await (await fetch("/", { cache: "no-store" })).text();
      const src = /<script[^>]+type="module"[^>]+src="([^"]+)"/.exec(html)?.[1];
      if (src && new URL(src, location.href).href !== current) location.reload();
    } catch {
      // offline: try again next time
    }
  }, 60_000);
}
watchForNewBuild();

function App() {
  const route = useRoute();
  const now = useNow(15_000);
  const me = usePoll(whoami, 300_000, []);
  const jobs = usePoll(() => listJobs({ limit: 300 }), 5000, []);
  const backends = usePoll(listBackends, 30_000, []);
  const slurm = (backends.data ?? []).filter((b) => b.kind === "slurm" && b.enabled && b.reachable).map((b) => b.name);
  const queues = usePoll(
    async () => {
      const out: Record<string, QueuedJob[]> = {};
      await Promise.all(slurm.map(async (n) => (out[n] = await getQueue(n).catch(() => []))));
      return out;
    },
    30_000,
    [slurm.join(",")],
  );

  const unauthorized = [me.error, jobs.error].some((e) => e instanceof ApiError && e.status === 401);
  if (unauthorized) {
    return (
      <SignIn
        onSaved={() => {
          me.refresh();
          jobs.refresh();
          backends.refresh();
        }}
      />
    );
  }

  const offline = jobs.error && !(jobs.error instanceof ApiError);

  return (
    <div className={`app${route.job !== null ? " has-panel" : ""}`}>
      <header className="top">
        <a className="brand" href="#/">
          compute
        </a>
        <nav aria-label="Views">
          <a href="#/" aria-current={route.view === "jobs" ? "page" : undefined}>
            Jobs
          </a>
          <a href="#/backends" aria-current={route.view === "backends" ? "page" : undefined}>
            Backends
          </a>
          {me.data?.scope === "dashboard" && (
            <a href="#/users" aria-current={route.view === "users" ? "page" : undefined}>
              Users
            </a>
          )}
        </nav>
        <span className="who">
          {token.get() && (
            <button
              className="link"
              onClick={() => {
                token.clear();
                me.refresh();
              }}
            >
              Forget token
            </button>
          )}
        </span>
      </header>
      {offline && <p className="banner">Can't reach the daemon. Showing the last data received; retrying.</p>}
      <main className="main">
        {route.view === "backends" ? (
          <Backends />
        ) : route.view === "users" ? (
          <Users />
        ) : (
          <>
            <Lanes jobs={jobs.data ?? []} backends={backends.data ?? []} queues={queues.data ?? {}} now={now} />
            <JobList jobs={jobs.data ?? []} now={now} />
          </>
        )}
      </main>
      {route.job !== null && <JobPanel id={route.job} now={now} />}
    </div>
  );
}

createRoot(document.getElementById("root")!).render(<App />);
