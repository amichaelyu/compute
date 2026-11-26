import { useState } from "react";
import { ApiError, type BackendRef, type KeyPolicy, type User, addUser, listUsers, removeUser, setUserAccess } from "./api";
import { usePoll } from "./util";

const errorText = (e: unknown) => (e instanceof ApiError ? `${e.message}${e.hint ? `. ${e.hint}` : ""}` : String(e));

/** The one command a new person runs; the API is under /api on the dashboard's hostname. */
function installCommand() {
  const base = location.protocol === "https:" ? `${location.origin}/api` : location.origin;
  return `curl -fsSL ${base}/v1/install | sh`;
}

function InstallCommand() {
  const cmd = installCommand();
  const [copied, setCopied] = useState(false);
  return (
    <div className="install">
      <code>{cmd}</code>
      <button
        className="btn"
        onClick={async () => {
          await navigator.clipboard.writeText(cmd);
          setCopied(true);
          setTimeout(() => setCopied(false), 1500);
        }}
      >
        {copied ? "Copied" : "Copy"}
      </button>
    </div>
  );
}

function AddUser({ onAdded }: { onAdded: (u: User) => void }) {
  const [name, setName] = useState("");
  const [key, setKey] = useState("");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  return (
    <form
      className="add-user"
      onSubmit={async (e) => {
        e.preventDefault();
        setBusy(true);
        setError(null);
        try {
          const u = await addUser(name.trim(), key.trim());
          setName("");
          setKey("");
          onAdded(u);
        } catch (err) {
          setError(errorText(err));
        } finally {
          setBusy(false);
        }
      }}
    >
      <label>
        Name
        <input type="text" value={name} onChange={(e) => setName(e.target.value)} placeholder="alice" required maxLength={32} />
      </label>
      <label>
        Public key
        <textarea
          value={key}
          onChange={(e) => setKey(e.target.value)}
          placeholder="ssh-ed25519 AAAA… alice@laptop"
          rows={3}
          required
          spellCheck={false}
        />
      </label>
      <div className="toggle">
        <button className="btn btn-primary" type="submit" disabled={busy || !name.trim() || !key.trim()}>
          Add
        </button>
        {error && <span className="error">{error}</span>}
      </div>
    </form>
  );
}

const usd = (n: number) => `$${n.toFixed(2)}`;

/** "lambda $5/h, $50/mo, $3.20 spent" for each paid backend the key may use. */
function capsText(u: User) {
  return Object.entries(u.access?.limits ?? {}).map(([name, l]) => {
    const caps = [
      l.usd_per_hour !== null ? `${usd(l.usd_per_hour)}/h` : null,
      l.usd_per_month !== null ? `${usd(l.usd_per_month)}/mo` : null,
    ].filter(Boolean);
    return `${name} ${caps.length ? caps.join(", ") : "no cap"}, ${usd(l.spent_this_month_usd)} spent`;
  });
}

function AccessSummary({ u }: { u: User }) {
  const a = u.access;
  if (!a) return null;
  const caps = capsText(u);
  return (
    <span className="user-access small muted">
      {u.owner ? "Owner" : "Guest"}
      {" · "}
      {a.allowed.length ? a.allowed.join(", ") : "no backends"}
      {a.policy.backends === null && " (default)"}
      {caps.length > 0 && ` · ${caps.join("; ")}`}
    </span>
  );
}

/** A dollar field: "" is no cap. */
function parseCap(text: string, label: string): number | null {
  const t = text.trim().replace(/^\$/, "");
  if (!t) return null;
  const n = Number(t);
  if (!Number.isFinite(n) || n < 0) throw new Error(`${label} must be a dollar amount, 0 or more (or empty for no cap)`);
  return n;
}

function AccessEditor({
  u,
  backends,
  onSaved,
  onClose,
}: {
  u: User;
  backends: BackendRef[];
  onSaved: () => void;
  onClose: () => void;
}) {
  const policy = u.access?.policy ?? { backends: null };
  const defaults = backends.filter((b) => u.owner || !b.paid).map((b) => b.name);
  const [custom, setCustom] = useState(policy.backends !== null);
  const [chosen, setChosen] = useState<Set<string>>(new Set(policy.backends ?? defaults));
  const [caps, setCaps] = useState<Record<string, { h: string; m: string }>>(() =>
    Object.fromEntries(
      backends
        .filter((b) => b.paid)
        .map((b) => {
          const l = policy.limits?.[b.name];
          return [b.name, { h: l?.usd_per_hour?.toString() ?? "", m: l?.usd_per_month?.toString() ?? "" }];
        }),
    ),
  );
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const allowed = new Set(custom ? chosen : defaults);
  const paidAllowed = backends.filter((b) => b.paid && allowed.has(b.name));

  const save = async () => {
    setError(null);
    let next: KeyPolicy;
    try {
      const limits: KeyPolicy["limits"] = {};
      for (const b of paidAllowed) {
        const c = caps[b.name] ?? { h: "", m: "" };
        const usd_per_hour = parseCap(c.h, `${b.name} $/hour`);
        const usd_per_month = parseCap(c.m, `${b.name} $/month`);
        if (usd_per_hour !== null || usd_per_month !== null) limits[b.name] = { usd_per_hour, usd_per_month };
      }
      next = { backends: custom ? backends.map((b) => b.name).filter((n) => chosen.has(n)) : null, limits };
    } catch (e) {
      setError(errorText(e));
      return;
    }
    setBusy(true);
    try {
      await setUserAccess(u.fingerprint, next);
      onSaved();
      onClose();
    } catch (e) {
      setError(errorText(e));
    } finally {
      setBusy(false);
    }
  };

  return (
    <div className="access" role="group" aria-label={`Access for ${u.name || "this key"}`}>
      <fieldset>
        <legend>Backends</legend>
        <label>
          <input type="radio" checked={!custom} onChange={() => setCustom(false)} />
          Default ({u.owner ? "every backend, as an owner" : "the free ones, as a guest"})
        </label>
        <label>
          <input type="radio" checked={custom} onChange={() => setCustom(true)} />
          Only these:
        </label>
        <div className="access-backends">
          {backends.map((b) => (
            <label key={b.name} className={custom ? undefined : "muted"}>
              <input
                type="checkbox"
                disabled={!custom}
                checked={allowed.has(b.name)}
                onChange={(e) => {
                  const next = new Set(chosen);
                  if (e.target.checked) next.add(b.name);
                  else next.delete(b.name);
                  setChosen(next);
                }}
              />
              {b.name}
              {b.paid && <span className="small muted">paid</span>}
            </label>
          ))}
        </div>
      </fieldset>
      {paidAllowed.length > 0 && (
        <fieldset>
          <legend>Spend caps (empty: no cap beyond the network's hourly budget)</legend>
          <div className="access-caps">
            {paidAllowed.map((b) => {
              const c = caps[b.name] ?? { h: "", m: "" };
              const used = u.access?.limits[b.name];
              const set = (k: "h" | "m", v: string) => setCaps({ ...caps, [b.name]: { ...c, [k]: v } });
              return (
                <div key={b.name} className="access-cap">
                  <span className="access-cap-name">{b.name}</span>
                  <label>
                    $/hour
                    <input type="text" inputMode="decimal" value={c.h} onChange={(e) => set("h", e.target.value)} placeholder="no cap" />
                  </label>
                  <label>
                    $/month
                    <input type="text" inputMode="decimal" value={c.m} onChange={(e) => set("m", e.target.value)} placeholder="no cap" />
                  </label>
                  <span className="small muted">
                    {used
                      ? `${usd(used.spent_this_month_usd)} spent this month, ${usd(used.in_use_usd_per_hour)}/h now`
                      : "nothing spent yet"}
                  </span>
                </div>
              );
            })}
          </div>
        </fieldset>
      )}
      <div className="toggle">
        <button className="btn btn-primary" disabled={busy} onClick={save}>
          Save
        </button>
        <button className="btn" disabled={busy} onClick={onClose}>
          Cancel
        </button>
        {error && <span className="error">{error}</span>}
      </div>
    </div>
  );
}

function UserRow({ u, backends, onChanged }: { u: User; backends: BackendRef[]; onChanged: () => void }) {
  const [editing, setEditing] = useState(false);
  const [confirm, setConfirm] = useState(false);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  return (
    <li className="user">
      <span className="user-name">{u.name || <span className="muted">(no name)</span>}</span>
      <span className="muted small user-key" title={u.fingerprint}>
        {u.key_type.replace(/^ssh-/, "")} {u.fingerprint.replace(/^SHA256:/, "").slice(0, 12)}…{u.comment && ` ${u.comment}`}
      </span>
      <AccessSummary u={u} />
      {u.note && <span className="error small">Can't sign in: {u.note}</span>}
      {!editing && (
        <button className="btn" onClick={() => setEditing(true)} aria-expanded={editing}>
          Access
        </button>
      )}
      {confirm ? (
        <span className="confirm" role="group" aria-label={`Confirm removing ${u.name}`}>
          <span>Remove {u.name || "this key"} and sign them out?</span>
          <button
            className="btn btn-danger"
            disabled={busy}
            onClick={async () => {
              setBusy(true);
              try {
                await removeUser(u.fingerprint);
                onChanged();
              } catch (e) {
                setError(errorText(e));
                setConfirm(false);
              } finally {
                setBusy(false);
              }
            }}
          >
            Remove
          </button>
          <button className="btn" onClick={() => setConfirm(false)}>
            Keep
          </button>
        </span>
      ) : (
        <button className="btn btn-danger-quiet" onClick={() => setConfirm(true)}>
          Remove
        </button>
      )}
      {error && <span className="error">{error}</span>}
      {editing && <AccessEditor u={u} backends={backends} onSaved={onChanged} onClose={() => setEditing(false)} />}
    </li>
  );
}

export function Users() {
  const { data, error, refresh } = usePoll(listUsers, 60_000, []);
  const [added, setAdded] = useState<string | null>(null);
  return (
    <div className="users">
      <section>
        <h2>Add someone</h2>
        <p className="muted">
          They run this command. It prints their public key line for you to paste below, then waits; once you've added
          it they press Enter, and it signs them in and installs the compute skill for Claude Code.
        </p>
        <InstallCommand />
        <AddUser
          onAdded={(u) => {
            setAdded(u.name);
            refresh();
          }}
        />
        {added && <p>Added {added}. They can press Enter in the installer now (or run the command again).</p>}
      </section>
      <section>
        <h2>People with access</h2>
        <p className="muted">
          Owners can do everything. Everyone else is a guest: their jobs run sandboxed in a container, they can't use
          others' secrets, and they can only cancel or read their own jobs. Access sets which backends each person may
          use (by default owners all, guests the free ones) and caps their spend per paid backend. Removing someone
          signs them out at once.
        </p>
        {error && !data && <p className="error">{errorText(error)}</p>}
        <ul className="user-list" aria-busy={!data}>
          {!data && !error
            ? [0, 1, 2].map((i) => (
                <li key={i} className="user" aria-hidden="true">
                  <span className="skel skel-word" />
                  <span className="skel skel-phrase" />
                </li>
              ))
            : data?.users.map((u) => (
                <UserRow key={u.fingerprint} u={u} backends={data.backends} onChanged={refresh} />
              ))}
        </ul>
      </section>
    </div>
  );
}
