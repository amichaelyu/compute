#!/usr/bin/env bash
# Deploys computed to the coordinator over Rex (`rex forward` to its sshd).
#
#   deploy/install.sh          build the dashboard and daemon, install binary + unit, restart
#   deploy/install.sh config   push hosts.toml (and allowed_signers), restart
#
# Settings come from ~/.config/compute/deploy.env (or $COMPUTE_DEPLOY_CONFIG), KEY=value lines;
# environment variables of the same names override the file:
#   COMPUTE_REX_HOST          Rex label of the coordinator (required)
#   COMPUTE_SSH_USER          login user on the coordinator (required)
#   COMPUTE_PORT              API port (default: 8765)
#   COMPUTE_FORWARD           local port for the temporary sshd tunnel (default: 52299)
#   COMPUTE_ALLOWED_SIGNERS   a local allowed_signers file to install as well (optional)
#
# Running jobs survive a restart: the daemon reconciles with the backends on startup.
set -euo pipefail
cd "$(dirname "$0")/.."

deploy_config="${COMPUTE_DEPLOY_CONFIG:-$HOME/.config/compute/deploy.env}"
if [ -f "$deploy_config" ]; then
  # The environment wins over the file: remember COMPUTE_* already set, source, then restore.
  saved=$(env | grep -E '^COMPUTE_[A-Z_]+=' || true)
  set -a
  # shellcheck disable=SC1090
  . "$deploy_config"
  set +a
  while IFS= read -r kv; do [ -n "$kv" ] && export "$kv"; done <<< "$saved"
fi

mode="${1:-install}"
host="${COMPUTE_REX_HOST:?set COMPUTE_REX_HOST (Rex label of the coordinator) in $deploy_config}"
user="${COMPUTE_SSH_USER:?set COMPUTE_SSH_USER (login user on the coordinator) in $deploy_config}"
api_port="${COMPUTE_PORT:-8765}"
fwd="${COMPUTE_FORWARD:-52299}"
target=x86_64-unknown-linux-gnu

if [ "$mode" = install ]; then
  command -v cargo-zigbuild >/dev/null || { echo "install cargo-zigbuild first (brew install cargo-zigbuild)" >&2; exit 1; }
  rustup target list --installed 2>/dev/null | grep -qx "$target" || rustup target add "$target"
  if command -v bun >/dev/null; then
    (cd web && bun install --frozen-lockfile && bun run build)
  else
    echo "bun not found: building without rebuilding the dashboard (web/dist as it is)" >&2
  fi
  cargo zigbuild --release --target "$target" -p computed
fi

if ! rex -S "$host" ls --json | grep -q '"label": "compute"'; then
  rex -S "$host" new compute >/dev/null
fi
rex -S "$host" forward new -s compute -L "$fwd:localhost:22" >/dev/null 2>&1 &
fwd_pid=$!
trap 'kill $fwd_pid 2>/dev/null || true' EXIT
for _ in $(seq 40); do nc -z 127.0.0.1 "$fwd" 2>/dev/null && break; sleep 0.25; done

ssh_opts=(-o HostKeyAlias=compute-coordinator -o StrictHostKeyChecking=accept-new -o BatchMode=yes)
dest="$user@127.0.0.1"
rsh() { ssh -p "$fwd" "${ssh_opts[@]}" "$dest" "$@"; }
put() { scp -q -P "$fwd" "${ssh_opts[@]}" "$1" "$dest:$2"; }

rsh 'mkdir -p ~/.local/bin ~/.config/compute ~/.config/systemd/user'
case "$mode" in
  install)
    put "target/$target/release/computed" .local/bin/computed.new
    put deploy/computed.service .config/systemd/user/computed.service
    if ! rsh 'test -f ~/.config/compute/hosts.toml'; then put hosts.toml .config/compute/hosts.toml; fi
    ;;
  config)
    put hosts.toml .config/compute/hosts.toml
    ;;
  *) echo "usage: $0 [install|config]" >&2; exit 2 ;;
esac
if [ -n "${COMPUTE_ALLOWED_SIGNERS:-}" ]; then
  put "$COMPUTE_ALLOWED_SIGNERS" .config/compute/allowed_signers
fi

rsh bash -s -- "$api_port" <<'REMOTE'
set -euo pipefail
port=$1
if [ -f ~/.local/bin/computed.new ]; then
  mv -f ~/.local/bin/computed.new ~/.local/bin/computed
  chmod 755 ~/.local/bin/computed
fi
if [ ! -f ~/.config/compute/computed.env ]; then
  ip=$(tailscale ip -4 2>/dev/null | head -1 || true)
  [ -n "$ip" ] || { echo "no tailnet IPv4 on the coordinator; install Tailscale first" >&2; exit 1; }
  printf 'COMPUTE_LISTEN=%s:%s\n' "$ip" "$port" > ~/.config/compute/computed.env
fi
[ -f ~/.config/compute/allowed_signers ] || echo "warning: ~/.config/compute/allowed_signers is missing; nobody can log in" >&2
systemctl --user daemon-reload
systemctl --user enable computed >/dev/null 2>&1
systemctl --user restart computed
sleep 2
systemctl --user --no-pager --lines=8 status computed | sed 's/^/  /'
REMOTE
