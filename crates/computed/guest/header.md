---
name: compute
description: Run compute jobs (training, eval, sweeps, data processing, anything longer than a few minutes, and all GPU work) on {{OWNER}}'s shared compute network through its HTTP API, which places them on their machines and clusters. Use instead of running long jobs on the local machine and instead of hand-written sbatch scripts.
---

# compute: a shared compute network (API v1)

{{OWNER}} runs a job scheduler (`computed`) and shares it. It places each job on the backend
that can start it soonest: machines, Slurm clusters, and possibly paid GPU clouds. Which
backends exist, what hardware they have and what they allow all come from the API
(`GET /v1/backends`), not from this file. You describe a job as JSON; the daemon uploads code,
picks a backend, runs the job and keeps its logs and outputs.

Access is by SSH key, and this skill was installed (and is updated) with:

```bash
curl -fsSL {{URL}}/v1/install | sh
```

That command signs in with `~/.ssh/id_ed25519.pub` (or `$COMPUTE_KEY`), writes
`~/.config/compute/client.env` and the session, and installs this file. If it says the key
isn't on the network yet, the user sends the printed public key line to {{OWNER}} and runs it
again. Rerun it to get the latest version of this skill. This skill targets **API v1**:
`GET $COMPUTE_URL/v1/` must list `"v1"` in `api_versions`.

## Limits of the public address

When `COMPUTE_URL` is an `https://` address (through Cloudflare):
- `wait` returns after at most 90 s (`timed_out: true` if nothing happened); call it again.
- Request bodies over 100 MB are refused (HTTP 413). Keep snapshots small (code only; data
  goes in datasets registered from a URL or Hugging Face). If a dataset must be uploaded from
  here and is over 100 MB, ask {{OWNER}}.
- Logins are rate-limited: a Cloudflare 429 or `rate_limited` means wait a minute and retry.

## House rules (this is a shared network)

Every key has full access to the API, so these rules are what keeps it working for everyone:

- **Only touch your own jobs.** Each job's `submitted_by` is the submitter's name; yours is
  `name` in `GET /v1/whoami`. Never cancel, delete outputs of, or reuse outputs from a job that
  isn't yours. When reporting to the user, show their own jobs.
- **Never use secrets you didn't create.** `GET /v1/secrets` lists everyone's secrets by name.
  Only reference a secret the user created, and name new ones with the user's name as a prefix
  (e.g. `alice-wandb`, not `wandb`). Never read, replace or delete anyone else's.
- **Don't change shared settings.** Never call `/v1/backends/{name}/enable` or `/disable`, and
  never delete dataset copies you didn't upload.
- **Paid clouds spend {{OWNER}}'s money.** Don't submit a job that would run on a paid backend
  (a cost in its quote) unless the user says {{OWNER}} agreed to it.
- **Be modest with shared GPUs.** Ask for the GPUs, memory and time the job needs, not the most
  a backend allows. Cancel jobs you no longer need.
