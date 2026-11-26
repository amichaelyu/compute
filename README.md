# compute

An agent-native job scheduler. Agents submit jobs to a small HTTP API, and the `computed` daemon
places each one on the backend that can start it soonest:

- **Slurm clusters**: jobs go in with `sbatch`, shaped with `sbatch --test-only` to start earliest.
- **Machines**: macOS or Linux hosts; jobs run directly or in Docker, within a CPU/memory share.
- **GPU clouds** (RunPod, Lambda, Modal): one instance per job, only after a price quote and within
  an hourly budget. Leftover instances are swept.

The daemon runs on a small always-on coordinator, reaches backends over Rex or ssh, and keeps each
job's state, logs and outputs. Jobs can depend on each other, read versioned datasets, and run in
container images (Docker, or Apptainer on Slurm), including ones it builds. A built-in dashboard
shows the schedule, jobs, backends and people.

Agents use it through [`skills/compute/SKILL.md`](skills/compute/SKILL.md); symlink it into
`~/.claude/skills/compute`.

## Layout

```
crates/computed/     the daemon (Rust: axum, SQLite)
crates/compute/      the `compute` CLI
crates/modal-client/ minimal gRPC client for Modal Sandboxes
web/                 the dashboard (React, Vite), embedded in the daemon
runner/compute-run   the per-job runner installed on each backend
skills/compute/      the skill agents read
deploy/              systemd unit and install script
hosts.example.toml   template for hosts.toml
```

## Configuration

The daemon reads `~/.config/compute` (or `$COMPUTE_CONFIG_DIR`):

| File | What it holds |
| --- | --- |
| `hosts.toml` | Backends, `[budget] hourly_usd` (0: paid backends run nothing), `[registry]` for builds, and `owners`. Start from `hosts.example.toml`; keep yours out of git (it's ignored). |
| `allowed_signers` | SSH keys allowed to use the API (`<name> <keytype> <base64>`). The dashboard's Users page edits it. |
| `secrets/` | Named job secrets, managed through the API. |
| `runpod.key`, `lambda.key`, `modal.toml` | Cloud API keys (0600). |
| `computed.env` | `COMPUTE_LISTEN`, Cloudflare Access (`COMPUTE_CF_TEAM_DOMAIN`, `COMPUTE_CF_AUD`, `COMPUTE_CF_EMAILS`), `COMPUTE_OWNER`, `COMPUTE_REGISTRY_PREFIX`. |

State (SQLite, snapshots, datasets, build logs, cloud outputs) lives in `~/.local/state/compute`
(or `$COMPUTE_STATE_DIR`). Clients read `~/.config/compute/client.env`:

```bash
COMPUTE_URL=http://<coordinator>:8765
COMPUTE_KEY=$HOME/.ssh/id_ed25519.pub
```

## Running

```bash
brew install amichaelyu/tap/compute   # or: cargo run -p computed -- serve
brew services start compute
```

The coordinator needs key login to each backend (directly, or through `rex forward`), Docker or
Apptainer where container jobs run, and outbound internet for clouds.

To deploy from your machine to a coordinator over Rex, set `COMPUTE_REX_HOST`, `COMPUTE_SSH_USER`
and `COMPUTE_PORT` in `~/.config/compute/deploy.env`, then:

```bash
deploy/install.sh          # build the dashboard and daemon, install, restart
deploy/install.sh config   # push hosts.toml and restart (running jobs survive)
```

## Sharing it

Someone joins by running `curl -fsSL <url>/v1/install | sh`: it prints their public key, you paste
it on the **Users** page, and it signs them in and installs the skill. Keys named in `owners` have
full access; everyone else is a guest, whose jobs run sandboxed and who sees only their own jobs,
secrets and datasets. Per person, **Access** sets which backends they may use and caps their spend
on each paid one (per hour, per month). Caps decide whether a job starts; running jobs are never
stopped. Removing someone signs them out and cancels their queued jobs.

Agents file bug reports with `POST /v1/reports` and wait on `GET /v1/reports/{id}/wait`; closing a
report with a note tells them it's fixed.

## Public API through Cloudflare

The API is also served at `/api/v1`, so one hostname can carry the dashboard (behind a Cloudflare
Access login) and the API (open to SSH-key clients):

1. Tunnel the hostname to `<coordinator>:8765`, with an Access application on it.
2. Add a second Access application for `<host>/api` with one **Bypass** policy (Everyone).
3. Rate-limit `starts_with(http.request.uri.path, "/api/v1/auth/")` to 5 requests per 10 s per IP.
4. Clients set `COMPUTE_URL=https://<host>/api`.

Through Cloudflare, waits return within 90 s and uploads over 100 MB are refused.

## Images

Job images are public: backends pull without credentials, and jobs get secrets at run time. For
GHCR, store a classic token with `write:packages` as the registry secret
(`PUT /v1/secrets/registry` with `REGISTRY_USERNAME` and `REGISTRY_PASSWORD`), set
`[registry] prefix = "ghcr.io/<owner>/compute"`, and make the package public after the first push.
