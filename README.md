# compute

An agent-native job scheduler. Agents submit jobs to a small HTTP API, and the `computed` daemon
places each job on whichever backend can start it soonest:

- **Slurm clusters**: GPU and CPU jobs are submitted with `sbatch`. Request shapes are compared
  with `sbatch --test-only`, so each job asks for what starts earliest.
- **CPU-only machines**: macOS or Linux desktops and servers. Jobs run directly on the machine,
  and the daemon tracks its CPU and memory.
- **Paid GPU clouds** (RunPod, Lambda, Modal): one instance per job, only for jobs that set a
  cost cap and only after a price quote. Spending is capped per job and per month, and leftover
  instances are swept. RunPod Instant Clusters run multi-node jobs.

The daemon runs on a small always-on coordinator. It reaches backends over Rex or plain ssh,
stages the code, runs each job, and serves its state, logs and outputs. Jobs can depend on other
jobs and take their outputs as inputs, read versioned datasets that the daemon copies where
they're needed, and run in container images (Docker, or Apptainer on Slurm), including images it
builds from a Dockerfile. A built-in web dashboard shows the schedule, the jobs and the
backends.

Agents use it through `skills/compute/SKILL.md`. Symlink it into `~/.claude/skills/compute`.

## Layout

```
crates/computed/    the daemon (Rust: axum, SQLite)
  src/api.rs          /v1/ HTTP API
  src/scheduler.rs    probe, dependency, poll and place loop
  src/datasets.rs     dataset versions and copies
  src/images.rs       container image builds
  src/backend/        Backend trait; local machines (Rex or ssh), Slurm clusters, cloud GPUs
crates/modal-client/ minimal gRPC client for Modal Sandboxes (vendored protos)
web/                the dashboard (React + TypeScript, Vite), embedded in the daemon
runner/compute-run  the per-job runner the daemon installs on each backend
hosts.example.toml  template for hosts.toml (gitignored), copied to ~/.config/compute/ on the coordinator
skills/compute/     the skill file agents use
deploy/             systemd unit and install script for the coordinator
```

## Configuration

The daemon reads `$COMPUTE_CONFIG_DIR` (default `~/.config/compute`):

- `hosts.toml`: the backends. Each one needs a name, a kind (`local`, `slurm`, or a cloud:
  `runpod`, `lambda`, `modal`) and, for machines and clusters, a way to
  reach it (`transport = "rex"` with `rex_label`, or `"ssh"` with `ssh_host`). The comments at
  the top of `hosts.example.toml` list the optional fields. Copy it to `hosts.toml` in the
  repo root (gitignored) and edit it; `deploy/install.sh config` pushes it.
- `allowed_signers`: SSH public keys allowed to use the API, one per line, in
  `<principal> <keytype> <base64>` form.
- `secrets/`: named job secrets (0600 files), managed through the API. The daemon also reads
  two kinds itself: the `[registry] secret` (`REGISTRY_USERNAME`, `REGISTRY_PASSWORD`) for
  pushing builds, and secrets with `HF_TOKEN` named by Hugging Face
  datasets.
- Cloud API keys, one 0600 file each: `runpod.key`, `lambda.key`, and `modal.toml` (with
  `token_id` and `token_secret`). `deploy/install.sh` copies any of these it finds in your
  local config dir to the coordinator. Cloud instances authorize the coordinator's own
  `~/.ssh/id_ed25519.pub`.
- `computed.env`: environment for the service.
  - `COMPUTE_LISTEN` sets the address to listen on.
  - `COMPUTE_CF_TEAM_DOMAIN`, `COMPUTE_CF_AUD` and `COMPUTE_CF_EMAILS` turn on Cloudflare Access
    logins for the dashboard.
  - `COMPUTE_OWNER` is your name as shown to the people you share the network with (in the
    skill served at `/v1/skill`).
  - `COMPUTE_REGISTRY_PREFIX` (e.g. `ghcr.io/<owner>/compute`) turns on image builds. See
    [Images](#images).

Local backends also take `docker_mem_gb` (memory for all containers; detected from `docker info`,
which on macOS is the OrbStack or Docker Desktop VM), `disk_reserve_gb` (disk kept free; 5) and
`keep_outputs_days` (how long finished jobs' outputs stay on the host; 7, also for Slurm).

`hosts.toml` also has `[registry]` (builder backend, credentials secret, platforms) and
`[budget]` (`hourly_usd`: how many USD per hour the running cloud instances may cost together; 0
by default, so paid backends run nothing until it's raised).

State (SQLite, code snapshots, uploaded datasets, build logs, and outputs copied off cloud
instances) lives in `$COMPUTE_STATE_DIR` (default `~/.local/state/compute`).

Clients (agents, and the dashboard's dev server) read `~/.config/compute/client.env`:

```bash
COMPUTE_URL=http://<coordinator>:8765
COMPUTE_KEY=$HOME/.ssh/id_ed25519.pub
```

## Images

Job images are public. The builder (`[registry] builder`) logs in to push builds to
`<prefix>:<hash>`; every backend pulls without credentials, and jobs get credentials at run time
from secrets, never from the image. With GHCR:

1. Create a classic GitHub token with `write:packages` (GHCR doesn't take fine-grained tokens)
   and store it as the registry secret:
   `PUT /v1/secrets/registry` with `{"env":{"REGISTRY_USERNAME":"<user>","REGISTRY_PASSWORD":"<token>"}}`.
2. Set `prefix = "ghcr.io/<owner>/compute"` under `[registry]` (or `COMPUTE_REGISTRY_PREFIX`),
   and `platforms = ["linux/amd64"]` unless you run arm64 jobs.
3. After the first push, make the package public on GitHub (package settings, Change
   visibility). GHCR creates packages private and has no API to change that. Until then builds
   fail with `not_public:` and the settings link. All builds are tags of one package, so this is
   needed once.

Public packages are free on GHCR, with no storage limit.

## Installing with Homebrew

```bash
brew install amichaelyu/tap/compute
```

This builds the daemon (with the dashboard), and installs the skill and `hosts.example.toml` under
`$(brew --prefix)/share/compute`. Write `~/.config/compute/hosts.toml` and `allowed_signers`, then
`brew services start compute`. The daemon reads `~/.config/compute/computed.env` itself, so put
`COMPUTE_LISTEN` there (and `PATH=...:$PATH` if `rex` lives somewhere unusual).

## Running it

```bash
cargo run -p computed -- serve
```

The coordinator needs:
- **For Rex backends:** `rex` with a host label for each one, and key login to each host's sshd
  (the daemon reaches it through `rex forward`).
- **For ssh backends:** non-interactive key login to each host (an `~/.ssh/config` alias works).
- **For container jobs:** Docker on local machines, Apptainer (or an `apptainer` module) on
  Slurm clusters. The probe detects both.
- **For cloud backends:** `curl`, the API key file, and outbound internet.

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

## Public API through Cloudflare

The daemon serves the API at `/v1` and again at `/api/v1`, so one public hostname can carry
both the dashboard and the API: the dashboard behind a Cloudflare Access login, and `/api`
open to SSH-key clients, with the SSH-key login as its only lock. The tunnel hides the
coordinator's address, and Cloudflare adds TLS, DDoS protection and rate limits.

1. Route the dashboard's hostname (e.g. `compute.example.com`) through the coordinator's tunnel
   to `<coordinator tailnet IP>:8765`, with an Access application on it (see the dashboard
   setup).
2. Add a second Access application for the path `compute.example.com/api` with a single
   **Bypass** policy (include: Everyone). Access applies the most specific path, so `/api`
   skips the login page while everything else, including the dashboard's own `/v1` calls,
   still needs it.
3. Add a rate-limiting rule (Security → WAF → Rate limiting rules):
   `http.host eq "compute.example.com" and starts_with(http.request.uri.path, "/api/v1/auth/")`,
   5 requests per 10 s per IP.
4. Clients set `COMPUTE_URL=https://compute.example.com/api` in `client.env`.

Through Cloudflare, `GET /v1/jobs/{id}/wait` returns after at most 90 s (Cloudflare drops
responses that take over 100 s), and request bodies over 100 MB are refused, so big snapshot
and dataset uploads need the private address. The daemon also refuses new login challenges
while 1024 are outstanding.

To let someone in, they run `curl -fsSL https://compute.example.com/api/v1/install | sh`. The
script is public and holds no secrets. Until their key is allowed, it prints their public key
line and waits. Paste it on the dashboard's **Users** page (or add it to `allowed_signers` by
hand); when they press Enter it signs them in (asking again if the key isn't there yet), writes
their `client.env` and installs the compute skill
(`GET /v1/skill`, behind the SSH-key login), which adds house rules for a shared network.
Removing someone on the Users page deletes their line and ends their sessions at once. Only a
Cloudflare Access login can manage users; SSH keys can't. Every key has full access: all jobs,
all secrets (jobs may use any of them), the cloud budget, and every backend, including Slurm
accounts that are yours alone.
