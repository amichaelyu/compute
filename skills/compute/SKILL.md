---
name: compute
description: Run compute jobs (training, eval, sweeps, data processing, anything longer than a few minutes, and all GPU work) on the user's machines through the compute scheduler's HTTP API. Use instead of running long jobs on the MacBook, instead of hand-written sbatch scripts, and whenever work needs FarmShare GPUs or the Mac Mini.
---

# compute: the job scheduler API (v1)

The daemon places each job on the backend that can start it soonest:

- **farm**: FarmShare Slurm. The only GPUs (L40S, at most 4 per user) and lots of CPU.
- **mini**: the Mac Mini, macOS/arm64. CPU only.
- **macbook**: only when the user has enabled it.

You describe a job as JSON. The daemon uploads code, picks a backend, runs the job and keeps
its logs and outputs. This skill targets **API v1**: `GET $COMPUTE_URL/v1/` must list `"v1"`
in `api_versions`.

## Setup per shell

```bash
COMPUTE_URL="${COMPUTE_URL:-http://<coordinator>:8765}"   # the daemon on Wesley
COMPUTE_KEY="${COMPUTE_KEY:-$HOME/.ssh/id_ed25519.pub}"   # a key listed in the daemon's allowed_signers
mkdir -p ~/.config/compute

compute_login() {   # signs a challenge with your SSH key; the session lasts 12 h
  local nonce sig
  nonce=$(curl -fsS -X POST "$COMPUTE_URL/v1/auth/challenge" | jq -r .nonce) || return 1
  sig=$(printf %s "$nonce" | ssh-keygen -Y sign -n compute -f "$COMPUTE_KEY") || return 1
  jq -n --arg n "$nonce" --arg s "$sig" --rawfile k "$COMPUTE_KEY" \
     '{nonce:$n, signature:$s, public_key:$k}' \
    | curl -fsS --json @- "$COMPUTE_URL/v1/auth/token" | jq -r .token > ~/.config/compute/session
}
cx() {   # authenticated curl; error bodies are printed
  curl -sS --fail-with-body -H "Authorization: Bearer $(cat ~/.config/compute/session 2>/dev/null)" "$@"
}
[ -s ~/.config/compute/session ] || compute_login
```

If any call returns `401` (`"code": "unauthorized"`), run `compute_login` and repeat the call.
`-f "$COMPUTE_KEY"` may be a `.pub` file whose private key is in ssh-agent.

## 1. Look before choosing resources

```bash
cx "$COMPUTE_URL/v1/backends"                  # one line per backend
cx "$COMPUTE_URL/v1/backends/farm" | jq .      # hardware, live availability, restrictions
```

Restrictions are enforced by placement, so pick `cpus`, `mem_gb`, `gpus` and `time` within
them. On farm, memory is requested as CPUs × 4000 MB, so asking for more memory means more CPUs.

## 2. Upload the code (a snapshot)

Run this from the repo root. It packs tracked files plus new untracked files that aren't
gitignored, and uploads them. Identical content is stored once.

```bash
SNAP=$(git ls-files -co --exclude-standard -z | tar --null -T - -cf - | zstd -q -c \
  | cx --data-binary @- -H 'Content-Type: application/zstd' "$COMPUTE_URL/v1/snapshots" | jq -r .id)
```

In a jj repo without `.git`, list files with `jj file list` instead of `git ls-files` (it
prints one path per line, so drop `-z`/`--null`). Data and checkpoints never go in snapshots:
keep them gitignored and read them from `$JOB_DATA_DIR`. Snapshots are capped at 2 GiB.

If the repo has a `pyproject.toml`, `uv sync` (frozen when there is a `uv.lock`) runs once per
backend when the snapshot is unpacked. Commands then use `uv run ...`.

## 3. Submit

Always send the JSON through a **quoted heredoc** (`<<'EOF'`), so quotes and `$` in the
command reach the job unchanged. Do a `dry_run` first when the placement matters.

```bash
cx --json @- "$COMPUTE_URL/v1/jobs" <<EOF
{
  "title": "Eval checkpoint 40k on held-out set",
  "description": "Checks whether the lr-warmup change fixed the eval regression seen in run 17.",
  "command": "uv run python eval.py --ckpt ckpts/40k --out \"\$JOB_OUTPUT_DIR\"",
  "snapshot": "$SNAP",
  "resources": {"cpus": 8, "mem_gb": 32, "gpus": 1, "time": "2h"},
  "secrets": ["wandb"],
  "env": {"WANDB_PROJECT": "umi"},
  "retries": 1,
  "idempotency_key": "eval-40k-2026-11-23",
  "dry_run": false
}
EOF
```

(This heredoc is unquoted only to insert `$SNAP`, so `$JOB_OUTPUT_DIR` is escaped as `\$`. If
you write the snapshot id in literally, use `<<'EOF'` and no escaping.)

Fields:

- **Required:** `title` (≤ 80 chars), `description` (≤ 500 chars), `command`, `snapshot` and
  `resources.time` (`"30m"`, `"2h"`, `"1h30m"`, `"1d"`).
  - `title` says *what* the job is. `description` says *why* it is being run and what it belongs
    to, so the user can tell jobs apart weeks later.
- **Resources:** `cpus` (default 1), `mem_gb` (default 4), `gpus` (default 0), `gpu_type`
  (`"L40S"` or null), `nodes` (1 only for now).
- **Placement:** `backends` (an allowlist, e.g. `["farm"]`), `os` (`"mac"` or `"linux"`),
  `priority` (higher starts first in the daemon's queue).
- **Arrays:** `array: {"count": 100}` makes 100 tasks. `{index}` in `command` becomes the task
  index, also available as `$ARRAY_INDEX`. Tasks spread across backends.
- `retries`: how many times a *lost* job (host vanished, no heartbeat) is requeued. A failed exit
  code is not retried.
- `idempotency_key`: re-sending the same key returns the original job instead of submitting it
  again. Use one whenever a retry of your own call could duplicate a job.

Inside a job, the runner sets:

| Variable | Meaning |
|---|---|
| `$JOB_ID` | the job's id |
| `$JOB_OUTPUT_DIR` | the job's own output directory |
| `$JOB_DATA_DIR` | the backend's shared data directory |
| `$JOB_SNAPSHOT_DIR` | where the code is (also the working directory) |
| `$ARRAY_INDEX` | the task index in an array job |

The working directory is shared by every job using that snapshot, so write results only to
`$JOB_OUTPUT_DIR`.

Not available yet (the API rejects them, saying which milestone adds them): `after_ok` /
`after_any` / `inputs` (M2), `datasets` (M3), `container` (M4), `max_cost_usd` / `quote_id` /
`keep_outputs` (cloud, M5) and `nodes > 1` (M6).

## 4. Report to the user

The submit response has `backend`, `reason`, `estimated_start` (with `estimated_start_source`:
`now`, `slurm` or `daemon`), `estimated_finish` and `per_backend` (why each backend would or
wouldn't take it). Tell the user:

- where the job runs and why
- the estimated start and finish, and whether the start is Slurm's estimate or the daemon's
- for FarmShare GPU jobs, the queue position: `GET /v1/backends/farm` shows `my_pending` and
  `next_gpu_free_estimate`

Update the estimate once the first log lines show a real rate.

## 5. Wait, read logs, fetch outputs

```bash
cx "$COMPUTE_URL/v1/jobs/42/wait?until=running&timeout=600"   # blocks; use instead of polling
cx "$COMPUTE_URL/v1/jobs/42/wait?until=done&timeout=900"      # returns early on any end state
cx "$COMPUTE_URL/v1/jobs/42/logs?offset=0"                     # {data, next_offset, eof}
cx "$COMPUTE_URL/v1/jobs/42/logs?offset=<next_offset>"         # only the new output
cx "$COMPUTE_URL/v1/jobs/42/outputs?glob=metrics/*.json" | zstd -dc | tar -x -C ./results/42
cx "$COMPUTE_URL/v1/jobs?state=running,pending,queued"         # what's active
cx "$COMPUTE_URL/v1/jobs?q=eval"                               # search titles and descriptions
cx "$COMPUTE_URL/v1/jobs?array=7"                              # tasks of array 7
cx -X POST "$COMPUTE_URL/v1/jobs/42/cancel"                     # ?array=1 cancels a whole array
```

- `wait` returns `timed_out: true` when the timeout passes first. Call it again.
- When fetching outputs, use `glob` to skip big checkpoints you don't need. `*` also matches
  `/`. Allowed characters are letters, digits and `_ . * ? / - [ ]`.
- Put fetched outputs inside the project (a gitignored dir), never loose in `~`.
- End states are `succeeded`, `failed` (see `exit_code` and `reason`), `cancelled`, `timeout` and
  `lost`. A reason starting `staging_failed:` means the code couldn't be unpacked or
  `uv sync` failed; read the reason, fix it, and resubmit.

## 6. Secrets

Refer to tokens by name only: `"secrets": ["wandb", "hf"]`. Never put a token in `env` or
`command`. Values are injected at run time and redacted from served logs.

```bash
cx "$COMPUTE_URL/v1/secrets"     # names and key names only; values are never returned
```

On `secret_missing`, ask the user for the value; don't go looking for one. Create or replace a
secret only with a value the user gave you, and only after they asked:
`cx -X PUT --json '{"env":{"WANDB_API_KEY":"..."}}' "$COMPUTE_URL/v1/secrets/wandb"`.

## 7. The MacBook

The MacBook is off unless the user turns it on. Don't enable it yourself; long jobs don't belong
on it. If the user asks you to:

```bash
cx -X POST --json '{"until":"18:00"}' "$COMPUTE_URL/v1/backends/macbook/enable"
```

`disable` drains it: running jobs finish, and no new ones start.

## Errors

Every error is `{"error": {"code", "message", "hint"}}`. Follow the `hint`.

- `unplaceable`: no backend can ever run the job as asked. Change the request; resubmitting
  it unchanged fails again.
- `bad_request`, `snapshot_missing`, `secret_missing`: fix the request as the hint says.
- `unauthorized`: run `compute_login`.
