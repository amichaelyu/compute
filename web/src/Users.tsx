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

const usd = (n: number) => `$${n % 1 ? n.toFixed(2) : n.toFixed(0)}`;

/** One backend the key may use; paid ones carry this month's spend and their caps. */
function BackendTag({ name, u }: { name: string; u: User }) {
  const l = u.access?.limits[name];
  if (!l) return <span className="tag">{name}</span>;
  const caps = [
    l.usd_per_hour !== null ? `${usd(l.usd_per_hour)}/h` : null,
    l.usd_per_month !== null ? `${usd(l.usd_per_month)}/mo` : null,
  ].filter(Boolean);
  return (
    <span className="tag tag-paid" title={`${name}: ${caps.length ? `capped at ${caps.join(" and ")}` : "no cap"}`}>
      {name}
      <span className="tag-spend">
        {usd(l.spent_this_month_usd)}
        {l.usd_per_month !== null && ` of ${usd(l.usd_per_month)}`} this month
      </span>
      {l.usd_per_hour !== null && <span className="tag-spend">max {usd(l.usd_per_hour)}/h</span>}
    </span>
  );
}

function AccessSummary({ u }: { u: User }) {
  const a = u.access;
  if (!a) return null;
  return (
    <span className="user-access">
      {a.allowed.length ? (
        a.allowed.map((name) => <BackendTag key={name} name={name} u={u} />)
      ) : (
        <span className="muted small">No backends</span>
      )}
      {!u.owner &&
        a.policy.secrets?.map((name) => (
          <span key={`secret:${name}`} className="tag" title={`${u.name || "This key"} may use the secret ${name} in jobs`}>
            secret {name}
          </span>
        ))}
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

function CapInput({ value, onChange, label, disabled }: { value: string; onChange: (v: string) => void; label: string; disabled: boolean }) {
  return (
    <span className={`cap-input${disabled ? " cap-input-off" : ""}`}>
      <span aria-hidden="true">$</span>
      <input
        type="text"
        inputMode="decimal"
        value={value}
        onChange={(e) => onChange(e.target.value)}
        placeholder="no cap"
        aria-label={label}
        disabled={disabled}
      />
    </span>
  );
}

function AccessEditor({
  u,
  backends,
  secrets,
  onSaved,
  onClose,
}: {
  u: User;
  backends: BackendRef[];
  secrets: string[];
  onSaved: () => void;
  onClose: () => void;
}) {
  const policy = u.access?.policy ?? { backends: null };
  const defaults = backends.filter((b) => u.owner || !b.paid).map((b) => b.name);
  const [custom, setCustom] = useState(policy.backends !== null);
  const [chosen, setChosen] = useState<Set<string>>(new Set(policy.backends ?? defaults));
  const [caps, setCaps] = useState<Record<string, { h: string; m: string }>>(() =>
    Object.fromEntries(
      backends.map((b) => {
        const l = policy.limits?.[b.name];
        return [b.name, { h: l?.usd_per_hour?.toString() ?? "", m: l?.usd_per_month?.toString() ?? "" }];
      }),
    ),
  );
  const [shared, setShared] = useState<Set<string>>(new Set(policy.secrets ?? []));
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const who = u.name || "this key";

  const toggle = (name: string, on: boolean) => {
    const next = new Set(chosen);
    if (on) next.add(name);
    else next.delete(name);
    setChosen(next);
    setCustom(true);
  };

  const save = async () => {
    setError(null);
    let next: KeyPolicy;
    try {
      const limits: KeyPolicy["limits"] = {};
      for (const b of backends) {
        if (!b.paid || !chosen.has(b.name)) continue;
        const c = caps[b.name] ?? { h: "", m: "" };
        const usd_per_hour = parseCap(c.h, `${b.name} cap per hour`);
        const usd_per_month = parseCap(c.m, `${b.name} cap per month`);
        if (usd_per_hour !== null || usd_per_month !== null) limits[b.name] = { usd_per_hour, usd_per_month };
      }
      next = {
        backends: custom ? backends.map((b) => b.name).filter((n) => chosen.has(n)) : null,
        limits,
        secrets: u.owner ? [] : secrets.filter((n) => shared.has(n)),
      };
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
    <form
      className="access"
      aria-label={`What ${who} may use`}
      onSubmit={(e) => {
        e.preventDefault();
        save();
      }}
      onKeyDown={(e) => e.key === "Escape" && onClose()}
    >
      <table className="access-table">
        <thead>
          <tr>
            <th scope="col">Backend</th>
            <th scope="col">Max per hour</th>
            <th scope="col">Max per month</th>
            <th scope="col">This month</th>
          </tr>
        </thead>
        <tbody>
          {backends.map((b) => {
            const on = chosen.has(b.name);
            const c = caps[b.name] ?? { h: "", m: "" };
            const set = (k: "h" | "m", v: string) => setCaps({ ...caps, [b.name]: { ...c, [k]: v } });
            const used = u.access?.limits[b.name];
            return (
              <tr key={b.name} className={on ? undefined : "access-off"}>
                <td>
                  <label className="access-backend">
                    <input type="checkbox" checked={on} onChange={(e) => toggle(b.name, e.target.checked)} />
                    {b.name}
                    {b.paid && <span className="small muted">paid</span>}
                  </label>
                </td>
                {b.paid ? (
                  <>
                    <td>
                      <CapInput value={c.h} onChange={(v) => set("h", v)} label={`${b.name} max per hour`} disabled={!on} />
                    </td>
                    <td>
                      <CapInput value={c.m} onChange={(v) => set("m", v)} label={`${b.name} max per month`} disabled={!on} />
                    </td>
                    <td className="small muted">
                      {used ? `${usd(used.spent_this_month_usd)} spent` : "—"}
                      {used && used.in_use_usd_per_hour > 0 && `, ${usd(used.in_use_usd_per_hour)}/h now`}
                    </td>
                  </>
                ) : (
                  <td colSpan={3} className="small muted">
                    Free
                  </td>
                )}
              </tr>
            );
          })}
        </tbody>
      </table>
      <p className="small muted access-default">
        {custom ? (
          <>
            Chosen for {who}.{" "}
            <button
              type="button"
              className="link"
              onClick={() => {
                setCustom(false);
                setChosen(new Set(defaults));
              }}
            >
              Go back to the default
            </button>{" "}
            ({u.owner ? "every backend, as an owner" : "free backends only, as a guest"}).
          </>
        ) : (
          <>Default for {u.owner ? "an owner: every backend" : "a guest: free backends only"}. Tick or untick to choose.</>
        )}{" "}
        Empty caps mean no limit beyond the network's hourly budget. Running jobs are never stopped by a cap.
      </p>
      {!u.owner && (
        <fieldset className="access-secrets">
          <legend>Secrets {who} may use</legend>
          {secrets.length ? (
            secrets.map((n) => (
              <label key={n} className="access-backend">
                <input
                  type="checkbox"
                  checked={shared.has(n)}
                  onChange={(e) => {
                    const next = new Set(shared);
                    if (e.target.checked) next.add(n);
                    else next.delete(n);
                    setShared(next);
                  }}
                />
                {n}
              </label>
            ))
          ) : (
            <span className="small muted">No secrets stored yet.</span>
          )}
          <p className="small muted access-default">
            Their jobs can use these by name, so they can read the values inside a job. They can't replace or delete them.
          </p>
        </fieldset>
      )}
      <div className="toggle">
        <button className="btn btn-primary" type="submit" disabled={busy}>
          {busy ? "Saving…" : "Save access"}
        </button>
        <button className="btn" type="button" disabled={busy} onClick={onClose}>
          Cancel
        </button>
        {error && (
          <span className="error" role="alert">
            {error}
          </span>
        )}
      </div>
    </form>
  );
}

function UserRow({
  u,
  backends,
  secrets,
  onChanged,
}: {
  u: User;
  backends: BackendRef[];
  secrets: string[];
  onChanged: () => void;
}) {
  const [editing, setEditing] = useState(false);
  const [confirm, setConfirm] = useState(false);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  return (
    <li className={`user${editing ? " user-open" : ""}`}>
      <span className="user-who">
        <span className="user-name">{u.name || <span className="muted">(no name)</span>}</span>
        <span className="small muted">{u.owner ? "owner" : "guest"}</span>
      </span>
      <span className="user-actions">
        <button className="btn" onClick={() => setEditing(!editing)} aria-expanded={editing}>
          Access
        </button>
      </span>
      <AccessSummary u={u} />
      <span className="user-remove">
        {confirm ? (
          <span className="confirm" role="group" aria-label={`Confirm removing ${u.name}`}>
            <span>Remove {u.name || "this key"}, sign them out and cancel their queued jobs?</span>
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
      </span>
      <span className="muted small user-key" title={u.fingerprint}>
        {u.key_type.replace(/^ssh-/, "")} {u.fingerprint.replace(/^SHA256:/, "").slice(0, 12)}…{u.comment && ` ${u.comment}`}
      </span>
      {u.note && <span className="error small user-note">Can't sign in: {u.note}</span>}
      {error && <span className="error user-note">{error}</span>}
      {editing && (
        <AccessEditor u={u} backends={backends} secrets={secrets} onSaved={onChanged} onClose={() => setEditing(false)} />
      )}
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
          others' secrets unless you share them, and they can only cancel or read their own jobs. Access sets which
          backends each person may use (by default owners all, guests the free ones), caps their spend per paid backend,
          and shares secrets (such as a Hugging Face token) with a guest. Removing someone
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
                <UserRow key={u.fingerprint} u={u} backends={data.backends} secrets={data.secrets ?? []} onChanged={refresh} />
              ))}
        </ul>
      </section>
    </div>
  );
}
