import { useState } from "react";
import { token } from "./api";

/** Shown when the daemon says we're not signed in. */
export function SignIn({ onSaved }: { onSaved: () => void }) {
  const [value, setValue] = useState("");
  return (
    <main className="signin">
      <h1>Sign in to see your jobs</h1>
      <p>Open this dashboard at its public address and sign in through Cloudflare Access.</p>
      <p>On the tailnet, you can paste an API session token instead (from ~/.config/compute/session).</p>
      <form
        onSubmit={(e) => {
          e.preventDefault();
          if (!value.trim()) return;
          token.set(value);
          onSaved();
        }}
      >
        <input
          type="password"
          autoComplete="off"
          placeholder="Session token"
          value={value}
          onChange={(e) => setValue(e.target.value)}
          aria-label="API session token"
        />
        <button className="btn btn-primary" type="submit">
          Use token
        </button>
      </form>
    </main>
  );
}
