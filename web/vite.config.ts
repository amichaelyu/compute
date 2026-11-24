// Vite builds the dashboard into dist/, which computed embeds at compile time (crates/computed/build.rs).
//
// `bun run dev` serves the UI with hot reload and proxies /v1 to a running daemon:
//   COMPUTE_URL    daemon to proxy to (default: a local daemon on 127.0.0.1:8765)
//   the dev proxy signs requests with the API session in ~/.config/compute/session (written by
//   the compute skill's login), so no token needs pasting while developing.
// This file is type-checked by Vite itself; tsconfig.json covers only the browser code in src/.
import react from "@vitejs/plugin-react";
import { readFileSync } from "node:fs";
import { homedir } from "node:os";
import { join } from "node:path";
import { defineConfig } from "vite";

function sessionToken(): string | null {
  const file = process.env.COMPUTE_SESSION_FILE ?? join(homedir(), ".config", "compute", "session");
  try {
    return readFileSync(file, "utf8").trim() || null;
  } catch {
    return null;
  }
}

export default defineConfig(({ command }) => {
  const target = process.env.COMPUTE_URL ?? "http://127.0.0.1:8765";
  const token = command === "serve" ? sessionToken() : null;
  return {
    plugins: [react()],
    build: {
      outDir: "dist",
      emptyOutDir: true,
      sourcemap: false,
      assetsDir: "assets",
    },
    server:
      command === "serve"
        ? {
            proxy: {
              "/v1": {
                target,
                changeOrigin: true,
                // Read once at startup; restart the dev server after logging in again.
                headers: token ? { authorization: `Bearer ${token}` } : {},
              },
            },
          }
        : undefined,
  };
});
