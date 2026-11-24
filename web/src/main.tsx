import { createRoot } from "react-dom/client";
import "./styles.css";
import { ApiError, listBackends, listJobs, token, whoami } from "./api";
import { Backends } from "./Backends";
import { JobList } from "./JobList";
import { JobPanel } from "./JobPanel";
import { Lanes } from "./Lanes";
import { SignIn } from "./SignIn";
import { useNow, usePoll, useRoute, who } from "./util";

function App() {
  const route = useRoute();
  const now = useNow(15_000);
  const me = usePoll(whoami, 300_000, []);
  const jobs = usePoll(() => listJobs({ limit: 300 }), 5000, []);
  const backends = usePoll(listBackends, 30_000, []);

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

  const identity = me.data ? who(me.data.identity) : "";
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
        </nav>
        <span className="who">
          {identity}
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
        ) : (
          <>
            <Lanes jobs={jobs.data ?? []} backends={backends.data ?? []} now={now} />
            <JobList jobs={jobs.data ?? []} now={now} />
          </>
        )}
      </main>
      {route.job !== null && <JobPanel id={route.job} now={now} />}
    </div>
  );
}

createRoot(document.getElementById("root")!).render(<App />);
