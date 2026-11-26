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
isn't on the network yet, it prints the public key line and waits: the user sends that line
to {{OWNER}}, and presses Enter once it has been added. Rerun it to get the latest version of this skill. This skill targets **API v1**:
`GET $COMPUTE_URL/v1/` must list `"v1"` in `api_versions`.

## Limits of the public address

When `COMPUTE_URL` is an `https://` address (through Cloudflare):
- `wait` returns after at most 90 s (`timed_out: true` if nothing happened); call it again.
- Request bodies over 100 MB are refused (HTTP 413). Keep snapshots small (code only; data
  goes in datasets registered from a URL or Hugging Face). If a dataset must be uploaded from
  here and is over 100 MB, ask {{OWNER}}.
- Logins are rate-limited: a Cloudflare 429 means wait a minute and retry.

## Guest keys: what the network enforces

Unless {{OWNER}} made your key an owner, it is a guest key (`GET /v1/whoami` says
`"scope": "guest"`), and the daemon enforces this:

- **Jobs run sandboxed in a container.** Every job needs `container.image`, a public image
  (e.g. `{"image": "python:3.12"}` or your own pushed to a public registry); guests can't build
  images here (`container.build`). The job gets a private copy of the code at `/work`, its
  outputs at `/out`, inputs at `/in` and datasets at `/data/<name>`, nothing else from the host.
  The network outside is reachable (pip, Hugging Face), the tailnet and private networks are
  not. Env and secrets are visible only inside the container.
- **One node, no paid clouds.** `max_cost_usd` and `resources.nodes > 1` are refused.
- **Your own things only.** Logs, metrics, outputs, cancel and delete-outputs work on your own
  jobs; other jobs show their state but not their command or env. Secrets, datasets and
  snapshots are visible and usable only by whoever created them (a snapshot counts as yours
  once you've uploaded it). Datasets come from uploads, Hugging Face or your own jobs' outputs,
  not from directories on the hosts or their rclone remotes.
- **Shared settings are the owner's.** Turning backends on or off, image builds and Slurm queue
  views are refused (403).

Errors for these say `guest_sandbox`, `not_yours` or `forbidden`. Still be modest with shared
GPUs: ask for the GPUs, memory and time the job needs, and cancel jobs you no longer need.
