import { useState } from "react";
import { ApiError, type User, addUser, listUsers, removeUser } from "./api";
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

function UserRow({ u, onRemoved }: { u: User; onRemoved: () => void }) {
  const [confirm, setConfirm] = useState(false);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  return (
    <li className="user">
      <span className="user-name">{u.name || <span className="muted">(no name)</span>}</span>
      <span className="muted small user-key" title={u.fingerprint}>
        {u.key_type.replace(/^ssh-/, "")} {u.fingerprint.replace(/^SHA256:/, "").slice(0, 12)}…{u.comment && ` ${u.comment}`}
      </span>
      {u.note && <span className="error small">Can't sign in: {u.note}</span>}
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
                onRemoved();
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
          paid backends or others' secrets, and they can only cancel or read their own jobs. Removing someone signs them
          out at once.
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
            : data?.map((u) => <UserRow key={u.fingerprint} u={u} onRemoved={refresh} />)}
        </ul>
      </section>
    </div>
  );
}
