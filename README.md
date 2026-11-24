# compute

An agent-native job scheduler. Agents submit jobs to a small HTTP API, and the `computed` daemon
places each job on whichever backend can start it soonest:

- **Slurm clusters**: GPU and CPU jobs are submitted with `sbatch`. Request shapes are compared
  with `sbatch --test-only`, so each job asks for what starts earliest.
- **CPU-only machines**: macOS or Linux desktops and servers. Jobs run directly on the machine,
  and the daemon tracks its CPU and memory.

The daemon runs on a small always-on coordinator. It reaches backends over Rex or plain ssh,
stages the code, runs each job, and serves its state, logs and outputs. A built-in web dashboard
shows the schedule, the jobs and the backends.

Agents use it through `skills/compute/SKILL.md`. Symlink it into `~/.claude/skills/compute`.

## Layout

```
crates/computed/    the daemon (Rust: axum, SQLite)
  src/api.rs          /v1/ HTTP API
  src/scheduler.rs    probe, poll and place loop
  src/backend/        Backend trait; local machines (Rex or ssh) and Slurm clusters
web/                the dashboard (React + TypeScript, Vite), embedded in the daemon
runner/compute-run  the per-job runner the daemon installs on each backend
hosts.toml          backends (copied to ~/.config/compute/hosts.toml on the coordinator)
skills/compute/     the skill file agents use
deploy/             systemd unit and install script for the coordinator
```

## Configuration

The daemon reads `$COMPUTE_CONFIG_DIR` (default `~/.config/compute`):

- `hosts.toml`: the backends. Each one needs a name, a kind (`local` or `slurm`) and a way to
  reach it (`transport = "rex"` with `rex_label`, or `"ssh"` with `ssh_host`). The comments at
  the top of `hosts.toml` list the optional fields.
- `allowed_signers`: SSH public keys allowed to use the API, one per line, in
  `<principal> <keytype> <base64>` form.
- `secrets/`: named job secrets (0600 files), managed through the API.
- `computed.env`: environment for the service.
  - `COMPUTE_LISTEN` sets the address to listen on.
  - `COMPUTE_CF_TEAM_DOMAIN`, `COMPUTE_CF_AUD` and `COMPUTE_CF_EMAILS` turn on Cloudflare Access
    logins for the dashboard.

State (SQLite and code snapshots) lives in `$COMPUTE_STATE_DIR` (default `~/.local/state/compute`).

Clients (agents, and the dashboard's dev server) read `~/.config/compute/client.env`:

```bash
COMPUTE_URL=http://<coordinator>:8765
COMPUTE_KEY=$HOME/.ssh/id_ed25519.pub
```

## Running it

```bash
cargo run -p computed -- serve
```

The coordinator needs:
- **For Rex backends:** `rex` with a host label for each one, and key login to each host's sshd
  (the daemon reaches it through `rex forward`).
- **For ssh backends:** non-interactive key login to each host (an `~/.ssh/config` alias works).

## Deploying to a coordinator

Put the deploy settings in `~/.config/compute/deploy.env` on the machine you deploy from:

```bash
COMPUTE_REX_HOST=<Rex label of the coordinator>
COMPUTE_SSH_USER=<login user on the coordinator>
COMPUTE_PORT=8765
```

On the coordinator:
1. Make it reachable from your clients (for example on a tailnet).
2. Give it access to every backend: Rex host labels or ssh aliases, plus an ssh key authorized
   on each host.
3. Write `~/.config/compute/allowed_signers`.

Then, from your machine (needs `cargo-zigbuild` and `bun`):

```bash
deploy/install.sh          # build the dashboard and daemon, install, restart
deploy/install.sh config   # push hosts.toml and restart (running jobs survive a restart)
```
