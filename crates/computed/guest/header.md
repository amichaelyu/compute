---
name: compute
description: Run compute jobs (training, eval, sweeps, data processing, anything longer than a few minutes, and all GPU work) on {{OWNER}}'s shared compute network through the `compute` CLI, which places them on their machines and clusters. Use instead of running long jobs on the local machine and instead of hand-written sbatch scripts.
---

# compute: a shared compute network (API v1)

{{OWNER}} runs a job scheduler (`computed`) and shares it. It places each job on the backend
that can start it soonest: machines, Slurm clusters, and possibly paid GPU clouds. Which
backends exist, what hardware they have and what they allow all come from the network
(`compute backends`), not from this file. You describe a job as JSON; the daemon uploads code,
picks a backend, runs the job and keeps its logs and outputs.

Access is by SSH key. The `compute` CLI and this skill were installed (and are reinstalled)
with:

```bash
curl -fsSL {{URL}}/v1/install | sh        # macOS (Apple Silicon) and Linux
irm {{URL}}/v1/install.ps1 | iex          # Windows, in PowerShell
```

That downloads the CLI (to `~/.local/bin`, or `%LOCALAPPDATA%\compute\bin` on Windows) and runs
`compute setup`, which signs in with `~/.ssh/id_ed25519.pub` (or `$COMPUTE_KEY`), writes
`~/.config/compute/client.env` and the session, and installs this file. If it says the key
isn't on the network yet, it prints the public key line and waits: the user sends that line
to {{OWNER}}, and presses Enter once it has been added. After that the CLI keeps itself and
this skill up to date (`compute update` does it at once).

## Limits of the public address

When `COMPUTE_URL` is an `https://` address (through Cloudflare):
- Request bodies over 100 MB are refused (HTTP 413). Keep snapshots small (code only; data
  goes in datasets registered from a URL or Hugging Face). If a dataset must be uploaded from
  here and is over 100 MB, ask {{OWNER}}.
- Logins are rate-limited: a Cloudflare 429 means wait a minute and retry.

## Guest keys: what the network enforces

Unless {{OWNER}} made your key an owner, it is a guest key (`compute whoami` says
`"scope": "guest"`, with these rules in `restrictions` and the backends and secrets you may and
may not use), and the daemon enforces this:

- **Jobs run sandboxed in a container.** Every job needs a `container`: a public `image`
  (e.g. `{"image": "python:3.12"}` or your own pushed to a public registry), or a `build` from a
  Dockerfile in your code. A guest's build runs on the job's own cloud instance while it is set
  up (Lambda, when {{OWNER}} allows it for your key; set `"backends": ["lambda"]` and
  `max_cost_usd`), never on the network's own machines, so every such job builds afresh and pays
  for the build time. The job gets a private copy of the code at `/work`, its
  outputs at `/out`, inputs at `/in` and datasets at `/data/<name>`, nothing else from the host.
  The network outside is reachable (pip, Hugging Face), the tailnet and private networks are
  not. Env and secrets are visible only inside the container.
- **One node.** `resources.nodes > 1` is refused.
- **Only the backends {{OWNER}} allows.** By default that's the free ones; {{OWNER}} may allow
  paid clouds too, with caps per hour and per month. `compute whoami` lists your `backends`
  and, per paid backend, your `limits` and what you've spent. On a paid cloud your job gets an
  instance of its own instead of the container sandbox.
- **Your own things only.** Logs, metrics, outputs, edit, cancel and delete-outputs work on your own
  jobs; other jobs show their state but not their command or env. Secrets, datasets and
  snapshots are visible and usable only by whoever created them (plus any secrets {{OWNER}}
  shared with your key, which `compute secrets` lists and you can use but not change) (a snapshot counts as yours
  once you've uploaded it). Datasets come from uploads, Hugging Face or your own jobs' outputs,
  not from directories on the hosts or their rclone remotes.
- **Shared settings are the owner's.** Turning backends on or off, the image build list and Slurm queue
  views are refused (403).

Errors for these say `guest_sandbox`, `not_yours`, `backend_not_allowed` or `forbidden`. Still be modest with shared
GPUs: ask for the GPUs, memory and time the job needs, and cancel jobs you no longer need.
