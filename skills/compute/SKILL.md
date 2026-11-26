---
name: compute
description: Run compute jobs (training, eval, sweeps, data processing, GPU work) through the compute scheduler (the `compute` CLI), which places them on the user's machines and clusters. Use when the user asks to run something through compute (or the scheduler); otherwise builds and jobs run locally as usual. When they do ask, use it instead of hand-written sbatch scripts.
---

# compute: the job scheduler (API v1)

A daemon (`computed`) places each job on the backend that can start it soonest: machines it
reaches directly, and Slurm clusters it submits to. Which backends exist, what hardware they
have and what they allow all come from the network (`compute backends`), not from this file.

You describe a job as JSON. The daemon uploads code, picks a backend, runs the job and keeps
its logs and outputs. You drive it with the `compute` CLI, which signs in, packs and uploads
code, and waits for you. This skill targets **API v1**; `compute api GET /v1/` lists
`changes` to the API, newest first.

## Configuration

Each machine you run agents from has the `compute` CLI and a client config file,
`~/.config/compute/client.env` (or the path in `$COMPUTE_CLIENT_CONFIG`). The network's install
command (`curl -fsSL <address>/v1/install | sh`) sets up both. The file is `KEY=value` lines:

```bash
# ~/.config/compute/client.env
COMPUTE_URL=http://<coordinator>:8765       # where computed listens (required)
COMPUTE_KEY=$HOME/.ssh/id_ed25519.pub       # an SSH key in the daemon's allowed_signers (optional)
```

Environment variables of the same names override the file. If neither sets `COMPUTE_URL`,
stop and ask the user for the daemon's address; don't guess one.

`COMPUTE_URL` may be the daemon's private address (`http://<coordinator>:8765`) or its public
`https://<host>/api` URL behind Cloudflare. Through Cloudflare, request bodies over 100 MB are
refused (HTTP 413), so big snapshots and dataset uploads fail. Make the snapshot smaller, or
upload from a machine that can reach the private address.

## The compute CLI

Check it's there with `compute --version`. If the shell can't find it, it may be installed but
not on `PATH`: try `~/.local/bin/compute` (Windows: `%LOCALAPPDATA%\compute\bin\compute.exe`).
If it isn't installed, ask the user to run the network's install command; don't build it.

- It signs in with the SSH key by itself, and again when the 12 h session runs out.
- `compute <command> --help` shows every command's options. Anything without a command of its
  own goes through `compute api METHOD PATH [BODY]` (BODY: inline JSON, a file, or `-` for
  stdin), which prints the JSON response.
- Errors go to stderr as `compute: <code> (<status>): <message>`, then `hint: ...`, with exit
  code 1. Follow the hint.
- **It updates itself and this skill.** When it prints `compute: updated to ...`, read this
  skill again before going on: commands or rules may have changed.
- Where you're in a Rex terminal, `compute submit` opens a live monitor in a split beside you
  (`--no-watch` skips it).

## 1. Look before choosing resources

```bash
compute backends            # one line per backend
compute backends <name>     # hardware, live availability, restrictions (JSON)
compute whoami              # your scope, and the backends and limits of your key
```

Restrictions are enforced by placement, so pick `cpus`, `mem_gb`, `gpus` and `time` within
them. Read them rather than assuming: a Slurm backend, for example, may tie memory to CPU count,
so asking for more memory also means more CPUs.

**Always give RAM and disk.** Running out of memory or disk is the usual reason a job dies
partway, often hours in. Set both in every request, sized from what the job actually uses:

- `mem_gb`: peak RAM plus about 25% headroom (the default 4 GB is rarely right). Over it, the
  job is killed: Docker and Slurm enforce it, and a host's OOM killer does the rest. On a Mac,
  containers share the Docker VM's memory (OrbStack, Docker Desktop), which placement counts
  separately and which can be much less than the host's.
- `disk_gb`: what the job writes (outputs, checkpoints, caches, unpacked data) plus headroom.
  The runner checks that much is free before starting and fails fast if not; a local host
  only takes the job when that much (plus a spare 5 GB) is free; cloud instances are picked or
  sized to have it. Inputs from parent jobs are linked, not copied, so array tasks share them.

Estimate from an earlier run's metrics (`rss_mb`, `disk_free_gb`) when there is one.

## 2. The code (a snapshot)

`compute submit` uploads the code itself when the request has no `snapshot` (or
`"snapshot": "auto"`): tracked files plus new untracked files that aren't gitignored, from the
git or jj repo you're in. Identical content is stored once. `--rev <rev>` uploads a committed
revision instead of the working copy. `compute snapshot` uploads without submitting and prints
the id, for several jobs that should share exactly the same code.

Data and checkpoints never go in snapshots: keep them gitignored and read them from
`$JOB_DATA_DIR`. Snapshots are capped at 2 GiB.

If the repo has a `pyproject.toml`, `uv sync` (frozen when there is a `uv.lock`) runs once per
backend when the snapshot is unpacked for a native job. Commands then use `uv run ...`.

## 3. Submit

Send the JSON through a **quoted heredoc** (`<<'EOF'`), so quotes and `$` in the command reach
the job unchanged. (In PowerShell, write it to a file and pass the file's path instead of `-`.)

```bash
compute submit --dry-run - <<'EOF'     # placement and cost only; then the same without --dry-run
{
  "title": "Eval checkpoint 40k on held-out set",
  "description": "Checks whether the lr-warmup change fixed the eval regression seen in run 17.",
  "command": "uv run python eval.py --ckpt ckpts/40k --out \"$JOB_OUTPUT_DIR\"",
  "resources": {"cpus": 8, "mem_gb": 32, "disk_gb": 50, "gpus": 1, "time": "2h"},
  "secrets": ["wandb"],
  "env": {"WANDB_PROJECT": "evals"},
  "retries": 1,
  "idempotency_key": "eval-40k-2026-11-23"
}
EOF
```

It prints where the job runs, why, and the estimated start, finish and cost. `--json` prints
the whole response instead (with `per_backend`; a dry run always does). Do a dry run first
when the placement matters.

Fields:

- **Required:** `title` (≤ 80 chars), `description` (≤ 500 chars), `command` and
  `resources.time` (`"30m"`, `"2h"`, `"1h30m"`, `"1d"`).
  - `title` says *what* the job is. `description` says *why* it is being run and what it belongs
    to, so the user can tell jobs apart weeks later.
- **Resources:** `cpus` (default 1), `mem_gb` (default 4; always set it), `disk_gb` (always
  set it; see section 1), `gpus` (default 0), `gpu_type` (a type a backend reports, or null for
  any), `nodes` (default 1; see section 3e).
- **Placement:** `backends` (an allowlist of backend names from `compute backends`), `os`
  (`"mac"` or `"linux"`),
  `priority` (higher starts first in the daemon's queue).
- **Arrays:** `array: {"count": 100}` makes 100 tasks. `{index}` in `command` becomes the task
  index, also available as `$ARRAY_INDEX`. Tasks spread across backends.
- `retries`: how many times a *lost* job (host vanished, no heartbeat) is requeued; default 1.
  A failed exit code is not retried. A job whose parent failed or was lost ends as
  `dependency_failed`, with the parent's reason; resubmit the parent, then the child.
- `idempotency_key`: re-sending the same key returns the original job instead of submitting it
  again. Use one whenever a retry of your own call could duplicate a job.
- Pipelines (`after_ok`, `after_any`, `inputs`), `datasets`, `container`, paid cloud
  (`max_cost_usd`, `quote_id`, `keep_outputs`) and `nodes`: sections 3a-3e.

Inside a job, the runner sets:

| Variable | Meaning |
|---|---|
| `$JOB_ID` | the job's id |
| `$JOB_OUTPUT_DIR` | the job's own output directory (`/out` in a container) |
| `$JOB_INPUT_DIR` | other jobs' outputs, one subdirectory per input (`/in`) |
| `$JOB_DATA_DIR` | one read-only subdirectory per dataset (`/data`) |
| `$JOB_SNAPSHOT_DIR` | where the code is, also the working directory (`/work`) |
| `$ARRAY_INDEX` | the task index in an array job |

The working directory is shared by every job using that snapshot, so write results only to
`$JOB_OUTPUT_DIR`.

### 3a. Pipelines

Chain steps instead of waiting and resubmitting by hand. Submit them all at once:

```json
{"title": "Train on preprocessed shards", "...": "...",
 "after_ok": [41],
 "inputs": [{"from_job": 41, "glob": "shards/*", "as": "shards"}]}
```

- `after_ok: [ids]` starts only after every listed job succeeded; `after_any: [ids]` once they
  have all ended, however they ended. `{"array": 7}` instead of an id means every task of array 7.
- `inputs` copies a finished job's outputs (all of them, or the files matching `glob`) to
  `$JOB_INPUT_DIR/<as>` before the job starts, from whichever backend that job ran on. A job
  listed in `inputs` is added to `after_ok` automatically.
- Until its dependencies finish, the job is `waiting`. If an `after_ok` parent fails, is
  cancelled or is lost, the job ends as `dependency_failed` (and so do its own dependents).
- When parent and child both run on a Slurm backend, the child is submitted to Slurm right
  away with `--dependency`, so it ages in the queue while the parent runs.

### 3b. Datasets

Data the job reads goes in a dataset, not the snapshot. A dataset is named and versioned; a
version never changes. The daemon copies it to whichever backend runs the job (counting the copy
time in placement), keeps the copy for later jobs, and evicts least-recently-used copies when a
backend's space runs low.

```bash
# from a directory on a backend, from a job's outputs, from Hugging Face, or from rclone:
compute api POST /v1/datasets '{"name":"demos","description":"Teleop demos, v3","source":{"host_path":{"backend":"<name>","path":"/abs/path"}}}'
compute api POST /v1/datasets '{"name":"shards","source":{"job_outputs":{"job":41,"glob":"shards/*"}}}'
compute api POST /v1/datasets '{"name":"bench","source":{"hf":{"repo":"org/name","repo_type":"dataset","revision":"main"}}}'
compute api POST /v1/datasets '{"name":"raw","source":{"rclone":{"remote":"gdrive:datasets/raw"}}}'
# or upload a directory from here:
compute dataset-upload demos data/demos --description "Teleop demos, v3"
compute datasets            # names, latest versions, where copies are
compute datasets demos      # every version and copy
```

Use them with `"datasets": ["demos", "bench@3f2a"]` (a bare name is the latest version). Each
appears read-only at `$JOB_DATA_DIR/<name>`. Private Hugging Face repos take
`"secret": "<name of a secret with HF_TOKEN>"`. Registering the same content again returns the
existing version (`new_version: false`).

### 3c. Containers

`"container": {"image": "python:3.12-slim"}` runs the command in that image: Docker on machines
that have it, Apptainer on Slurm clusters (with the GPUs passed through). Check `runtimes` in
`compute backends`. Code still comes from the snapshot, mounted at `/work`, so changing code
doesn't need a new image; put system libraries, CUDA and the Python environment in the image.
Images are Linux images, so `os: "mac"` can't be combined with `container`.

`"container": {"build": "docker/train.Dockerfile"}` builds the image from a Dockerfile in the
snapshot (build context: the Dockerfile's directory, or `"context"`). The tag is a hash of the
Dockerfile and its context, so an unchanged image is reused; the job waits in `queued` while a
build runs. With a registry set up (`compute api GET /v1/images` shows `registry`), builds are pushed there
and run anywhere. Without one, the image is built on the Docker host that runs the job, and
cloud backends can't run it.

**Images are public.** Registry builds go to a public GitHub package, and no backend logs in to
pull, so every image a job uses (built or prebuilt `image`, including the Dockerfile's `FROM`)
must be one anyone could pull and read. Write the Dockerfile so it is fit to publish:

- Put in only what anyone may see: OS packages, CUDA, the Python environment. No tokens, keys,
  passwords, `.env` files, private data, unreleased weights or proprietary code.
- No secrets in `ARG`, `ENV` or `RUN` lines (they stay in the image history). Don't `COPY` the
  whole repo; copy just the dependency files (`pyproject.toml`, `uv.lock`,
  `requirements.txt`), and add a `.dockerignore` excluding `.env*`, keys, `data/` and `.git`.
- Credentials come separately, at run time: name them in `"secrets"` and the job gets them as
  environment variables (section 6). Code comes from the snapshot at `/work`, data from
  `datasets`.

A build whose package isn't public yet fails with a reason starting `build_failed: not_public:`
and a settings link. Only the user can make it public (GitHub has no API for it), once per
package: show them the link, wait until they've done it, then resubmit.

```bash
compute api GET /v1/images                        # builds: state, error
compute api GET '/v1/images/<key>/logs?offset=0'  # a build's log
```

A failed build ends the job as `failed` with a reason starting `build_failed:`.

### 3d. Paid cloud backends

Backends with `"paid": true` (`compute backends --json`) cost money. They only take jobs that set
`max_cost_usd`, run GPU jobs in a container (`container`, or the backend's default image), and
count against an hourly limit: the running cloud instances together may cost at most
`hourly_usd` per hour (`compute budget` shows the limit and what's in use). A paid job whose
rate doesn't fit yet waits in the queue until instances finish.

The owner may also limit your key: which backends it may use, and per paid backend a cap in
$/h and per month. `compute whoami` lists `backends` (the ones your key may use) and `limits`
(per paid backend: `usd_per_hour`, `usd_per_month`, `in_use_usd_per_hour`,
`spent_this_month_usd`, `committed_this_month_usd`; null means no cap). A job that would pass a
cap waits, and its reason names the cap. The monthly cap counts running and placed jobs at their
full time limit, so a long `time` uses up more of it. Caps only decide whether a job starts: a
running job is never stopped by one, even if it finishes past it.

1. Send the request with `"max_cost_usd": 5` through `compute submit --dry-run`. The response has `quote_id`,
   `backend`, `estimated_cost_usd` (null when a free backend wins), `cheapest_paid_option` and
   the budget.
2. **Tell the user the backend and estimated cost and wait for their go-ahead.**
3. Submit the same request without `--dry-run`, with `"quote_id": "q_..."` added, within 15 minutes.
   A changed request gets `quote_mismatch`; an old quote gets `quote_expired`.

The instance is deleted when the job ends. Before that, its log and the outputs matching
`keep_outputs` (default: all of them) are copied to the coordinator and served from there, so
set `keep_outputs` (for example `"checkpoints/final/**"`) when outputs are large.
`compute job <id>` shows `usd_per_hour` and, once it ends, `cost_usd`.

The quote picks the cheapest GPU type with capacity, across every paid backend. To pin a
provider or a GPU, set `"backends": ["lambda"]` (names from `compute backends`) or
`resources.gpu_type`. Each instance is named `compute-<job id>-<hex>` in the provider's
console. Keys listed in the backend's `authorized_keys` (hosts.toml) can ssh in while the job
runs (Lambda: `ubuntu@<ip>`, with the IP shown in the Lambda console). The job's files are
under `~/compute`.

### 3e. Multi-node jobs

`"resources": {"nodes": 2, "gpus": 8, ...}` asks for 2 nodes with 8 GPUs each (cpus, mem_gb
and gpus are per node). Only backends whose `max_nodes` allows it take these, all nodes run on
one backend, and the job must use `container`. Every node gets `NNODES`, `NODE_RANK`,
`GPUS_PER_NODE`, `MASTER_ADDR` and `MASTER_PORT`, so this works unchanged:

```bash
torchrun --nnodes=$NNODES --node_rank=$NODE_RANK --nproc_per_node=$GPUS_PER_NODE \
  --master_addr=$MASTER_ADDR --master_port=$MASTER_PORT train.py
```

Node 0's log is the job's log; the others' are under `$JOB_OUTPUT_DIR/nodes/<rank>/log`. If any
node fails, the whole job is stopped.

## 4. Report to the user

The submit response has `backend`, `reason`, `estimated_start` (with `estimated_start_source`:
`now`, `slurm`, `daemon`, `provider` or `dependencies`), `estimated_finish`,
`estimated_cost_usd` (paid backends) and `per_backend` (why each backend would or wouldn't take
it). For a `waiting` job these are a preview; placement is decided when its dependencies finish.
Tell the user:

- where the job runs and why
- the estimated start and finish, and whether the start is Slurm's estimate or the daemon's
- the estimated cost, for a paid backend
- for jobs on a Slurm backend, where it stands in the queue: `compute backends <name>` shows
  `my_pending` and `next_gpu_free_estimate`

Update the estimate once the first log lines show a real rate.

## 5. Wait, read logs, fetch outputs

```bash
compute wait 42 --until running --timeout 600   # blocks; use instead of polling (exit 2: timed out)
compute wait 42 --timeout 900                    # until done; returns early on any end state
compute logs 42                                  # the log so far
compute logs 42 --tail 20000                     # just the last 20 kB
compute logs 42 -f                               # follow until the job ends
compute outputs 42 --list --glob 'metrics/*'     # output files and sizes
compute outputs 42 --glob 'metrics/*.json' -o results/42   # download and unpack
compute jobs --state running,pending,queued,waiting        # what's active (--json for everything)
compute jobs -q eval                             # search titles and descriptions
compute jobs --array 7                           # tasks of array 7
compute api GET '/v1/jobs?limit=1000&before=812' # older jobs: page back by the smallest id seen
compute job 42                                   # state, reason, times, cost (-v: command and env)
compute cancel 42                                # --array cancels a whole array
compute delete-outputs 42                        # free a finished job's disk
```

- `compute wait` without `--timeout` waits as long as it takes. In the background, with a
  `--timeout`, it is the way to watch a long job while you do other work.
- When fetching outputs, use `glob` to skip big checkpoints you don't need. `*` also matches
  `/`. Allowed characters are letters, digits and `_ . * ? / - [ ]`. `outputs/list` shows
  what's there first. A glob that matches nothing gets 404 `no_outputs`; outputs already
  deleted get 410 `outputs_deleted`.
- Outputs on a local or Slurm backend are deleted after its `keep_outputs_days` (7 by default;
  the log stays), unless a waiting job needs them as inputs. Fetch what you want to keep. Once
  you've fetched a big job's outputs, `DELETE` them to free the host's disk.
- `compute job <id> -v` shows what the job ran: `command`, `env`, `snapshot` and
  `snapshot_created_at` (check that it's the code you meant). A cancelled job's reason says who
  cancelled it.
- Put fetched outputs inside the project (a gitignored dir), never loose in `~`.
- End states are `succeeded`, `failed` (see `exit_code` and `reason`), `cancelled`, `timeout`,
  `lost` and `dependency_failed`. A reason starting `staging_failed:` means the code, datasets
  or inputs couldn't be put on the backend, or `uv sync` failed; `build_failed:` means the image
  build failed (see `compute api GET /v1/images`). Read the reason, fix it, and resubmit.

**Check every failed job for out-of-memory and out-of-disk** before anything else:

- Out of memory: a reason starting `out of memory:`, a reason mentioning exit 137 (SIGKILL), or
  `Killed` / `CUDA out of memory` / `MemoryError` in the log. Raise `mem_gb` (for GPU memory:
  a smaller batch, or a GPU with more memory) and resubmit.
- Out of disk: a reason starting `out of disk:` or `not enough disk:`, or `No space left on
  device` / `Disk quota exceeded` in the log. Raise `disk_gb`, or write less (fewer
  checkpoints, `keep_outputs`), and resubmit.
- While a job runs, watch it: `compute metrics <id>` shows memory (against `mem_gb`), GPUs
  and free disk, and warns at 90% of memory or under 5 GB of disk.

## 6. Secrets

Refer to tokens by name only: `"secrets": ["wandb", "hf"]`. Never put a token in `env` or
`command`. Values are injected at run time and redacted from served logs.

```bash
compute secrets     # names and key names only; values are never returned
```

On `secret_missing`, ask the user for the value; don't go looking for one. Create or replace a
secret only with a value the user gave you, and only after they asked. Pass it on stdin, so it
stays out of the command line:

```bash
compute api PUT /v1/secrets/wandb - <<'EOF'
{"env": {"WANDB_API_KEY": "..."}}
EOF
```

## 7. When to use compute

Submit work here when the user asks you to use compute. Otherwise run builds and jobs where
you're working, as usual. When the work can't run there at all (it needs a GPU, say), ask the
user before submitting it. Only the backends listed by `compute backends` run jobs.

## 8. Report problems with compute itself

When compute (the daemon, its API or this skill) is wrong, file a report so the network's owner
sees it. Report:

- an `internal` error or another HTTP 5xx from the daemon that repeats after one retry
  (`backend_unreachable` is a backend being down, not a bug, unless it lasts for hours),
- a response that contradicts this skill, or a `hint` that doesn't work when followed,
- a job stuck in a state it can't be in (running long past its time limit, `lost` while the
  backend shows it running, outputs missing from a job that succeeded),
- a gap in this skill that made you guess.

Don't report the user's own code failing, queue waits, `over_budget`, or a refusal that the
hint or the guest rules explain: those work as designed.

One report per problem. Check the open ones first (`compute reports`) and don't
file one that's already there. Make the body something the owner can act on without asking:
what you were doing, the exact request (method, path, body), what came back (status, error
code and message), what you expected, and when. **Never include a token, a session, a secret's
value or a private key**; describe them by name.

```bash
compute report --title "wait returns 500 for array task 812" --job 812 - <<'TXT'
Doing: waiting for job 812 (array task) with GET /v1/jobs/812/wait?until=done
Got: HTTP 500 {"error":{"code":"internal","message":"..."}} three times, 1 min apart
Expected: the job's state; GET /v1/jobs/812 works and says running
When: 2026-11-24 15:40 PT
TXT
# -> report 7 filed (open); tell the user the report's id
compute reports 7    # its state, and the owner's note once closed
```

`--kind` is `bug` (default), `docs` (this skill is wrong or unclear) or `feature` (something
compute should do). `--job` is optional and must be one of your jobs. Each key may file 30
reports a day.

### Watching your report

The owner closes a report when it's dealt with, with a `resolution` (`fixed`, `wont_fix`,
`duplicate`, `not_a_bug`) and a `note` saying what changed or why. After filing, watch it in
the background (it may take hours) and keep working on whatever doesn't depend on it:

```bash
compute reports 7 --wait    # returns once it's closed, with resolution and note
```

When it closes, tell the user the resolution and note. If it's `fixed`, retry what failed (a
skill fix comes with the CLI's next update: run `compute update`, then read this skill again;
`compute api GET /v1/` lists the `changes`). If it's `wont_fix` or `not_a_bug`, the note says
what to do instead. To pick up a report you filed in an earlier session, list them with `compute reports --all` and
wait on the open ones that are yours.

## Errors

Every error has a code, a message and a hint (`compute: <code> (<status>): <message>` then
`hint: ...`; in the API, `{"error": {"code", "message", "hint"}}`). Follow the hint.

- `unplaceable`: no backend can ever run the job as asked. Change the request; resubmitting
  it unchanged fails again. Never add `max_cost_usd` just to get past this without asking the
  user.
- `over_budget`: only paid backends could run it, and its rate alone is over the hourly limit.
  Tell the user; don't work around it.
- `quote_required`, `quote_expired`, `quote_mismatch`: get a (new) quote with a dry run and
  submit exactly that request.
- `bad_request`, `snapshot_missing`, `dataset_missing`, `secret_missing`: fix the request as
  the hint says.
- `unauthorized`: run `compute login`. If it says the key isn't allowed, the user's key isn't
  on the network (any more); tell the user.
- A Cloudflare 429 page on login: too many logins from here. Wait a minute and retry.
- `backend_not_allowed` (HTTP 403): the request names a backend your key may not use; the
  hint lists the ones it may. Drop it from `backends`, or tell the user to ask the owner.
- `guest_sandbox`, `not_yours`, `forbidden` (HTTP 403): the key is a guest key and the
  request goes past what guests may do (see `compute whoami` and the guest rules). Don't
  retry; tell the user.
- `disk_full` (HTTP 507): the coordinator is short of disk; upload less or ask the owner.
- `internal` (HTTP 500): retry once; if it happens again, file a report (section 8) and tell
  the user.

## Without the CLI

Where there's no `compute` build (an Intel Mac, say), the same API answers over HTTP at
`$COMPUTE_URL/v1/...`. Sign in by signing a challenge with the SSH key, then send the session
as a bearer token; every `compute api` call above maps onto a request like this one:

```bash
nonce=$(curl -fsS -X POST "$COMPUTE_URL/v1/auth/challenge" | jq -r .nonce)
sig=$(printf %s "$nonce" | ssh-keygen -Y sign -n compute -f ~/.ssh/id_ed25519)
jq -n --arg n "$nonce" --arg s "$sig" --rawfile k ~/.ssh/id_ed25519.pub '{nonce:$n, signature:$s, public_key:$k}' \
  | curl -fsS --json @- "$COMPUTE_URL/v1/auth/token" | jq -r .token > ~/.config/compute/session
curl -sS --fail-with-body -H "Authorization: Bearer $(cat ~/.config/compute/session)" "$COMPUTE_URL/v1/backends"
```

Snapshots are a `zstd`-compressed tar of the repo's files, uploaded with `POST /v1/snapshots`
(`Content-Type: application/zstd`); outputs come back the same way.
