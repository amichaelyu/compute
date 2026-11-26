#!/usr/bin/env bash
# Deploys computed to the coordinator over Rex (`rex forward` to its sshd).
#
#   deploy/install.sh          build the dashboard, daemon and CLI, install them + unit, restart
#   deploy/install.sh config   push hosts.toml (and allowed_signers, cloud API keys), restart
#
# The install also builds the `compute` CLI for every platform the network serves it to
# (GET /v1/cli): macOS arm64, Linux x86_64 and arm64 (static musl) and Windows x86_64. Its
# version is the crate version plus the jj/git commit, so clients update when it changes.
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
  # shellcheck disable=SC2163  # kv is a KEY=value pair, exported as such
  while IFS= read -r kv; do [ -n "$kv" ] && export "$kv"; done <<< "$saved"
fi

mode="${1:-install}"
[ -f hosts.toml ] || { echo "hosts.toml is missing: copy hosts.example.toml to hosts.toml and edit it" >&2; exit 1; }
host="${COMPUTE_REX_HOST:?set COMPUTE_REX_HOST (Rex label of the coordinator) in $deploy_config}"
user="${COMPUTE_SSH_USER:?set COMPUTE_SSH_USER (login user on the coordinator) in $deploy_config}"
api_port="${COMPUTE_PORT:-8765}"
fwd="${COMPUTE_FORWARD:-52299}"
target=x86_64-unknown-linux-gnu
# CLI builds: "<rust target> <platform name the API uses>".
cli_targets=(
  "aarch64-apple-darwin darwin-arm64"
  "x86_64-unknown-linux-musl linux-x86_64"
  "aarch64-unknown-linux-musl linux-arm64"
  "x86_64-pc-windows-gnu windows-x86_64"
)
cli_stage=target/cli-dist

if [ "$mode" = install ]; then
  command -v cargo-zigbuild >/dev/null || { echo "install cargo-zigbuild first (brew install cargo-zigbuild)" >&2; exit 1; }
  # Use rustup's toolchain (it has the Linux target), even if another cargo comes first on PATH.
  cargo=cargo
  if command -v rustup >/dev/null; then
    cargo=$(rustup which cargo)
    PATH="$(dirname "$cargo"):$PATH"
    export PATH
    for t in "$target" "${cli_targets[@]%% *}"; do
      rustup target list --installed 2>/dev/null | grep -qx "$t" || rustup target add "$t"
    done
  fi
  if command -v bun >/dev/null; then
    (cd web && bun install --frozen-lockfile && bun run build)
  else
    echo "bun not found: building without rebuilding the dashboard (web/dist as it is)" >&2
  fi
  "$cargo" zigbuild --release --target "$target" -p computed

  # The CLI, for every platform, plus the manifest the daemon serves it from.
  commit=$(jj log -r @ --no-graph -T 'commit_id.short(8)' 2>/dev/null || git rev-parse --short=8 HEAD)
  cli_version="$(sed -n 's/^version = "\(.*\)"/\1/p' crates/compute/Cargo.toml | head -1)+$commit"
  rm -rf "$cli_stage" && mkdir -p "$cli_stage"
  entries=()
  for spec in "${cli_targets[@]}"; do
    read -r t platform <<< "$spec"
    if [ "$t" = "aarch64-apple-darwin" ] && [ "$(uname -sm)" = "Darwin arm64" ]; then
      COMPUTE_BUILD="$cli_version" "$cargo" build --release --target "$t" -p compute
    else
      COMPUTE_BUILD="$cli_version" "$cargo" zigbuild --release --target "$t" -p compute
    fi
    exe=compute; [[ "$t" == *windows* ]] && exe=compute.exe
    file="compute-$platform"; [[ "$t" == *windows* ]] && file="$file.exe"
    cp "target/$t/release/$exe" "$cli_stage/$file"
    sum=$(shasum -a 256 "$cli_stage/$file" | cut -d' ' -f1)
    size=$(wc -c < "$cli_stage/$file" | tr -d ' ')
    entries+=("\"$platform\": {\"file\": \"$file\", \"sha256\": \"$sum\", \"size\": $size}")
  done
  { printf '{"version": "%s", "files": {' "$cli_version"
    sep=''; for e in "${entries[@]}"; do printf '%s%s' "$sep" "$e"; sep=', '; done
    printf '}}\n'; } > "$cli_stage/manifest.json"
  echo "compute CLI $cli_version built for ${#entries[@]} platforms"
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
    # The CLI goes to <state dir>/cli, swapped in whole so the manifest always matches the files.
    # shellcheck disable=SC2016  # expands on the coordinator
    state_dir=$(rsh 'set -a; [ -f ~/.config/compute/computed.env ] && . ~/.config/compute/computed.env; echo "${COMPUTE_STATE_DIR:-${XDG_STATE_HOME:-$HOME/.local/state}/compute}"')
    rsh "rm -rf '$state_dir/cli.new' && mkdir -p '$state_dir/cli.new'"
    scp -q -P "$fwd" "${ssh_opts[@]}" "$cli_stage"/* "$dest:$state_dir/cli.new/"
    rsh "chmod 755 '$state_dir'/cli.new/compute-* && rm -rf '$state_dir/cli.old' && { [ ! -d '$state_dir/cli' ] || mv '$state_dir/cli' '$state_dir/cli.old'; } && mv '$state_dir/cli.new' '$state_dir/cli' && rm -rf '$state_dir/cli.old'"
    if ! rsh 'test -f ~/.config/compute/hosts.toml'; then put hosts.toml .config/compute/hosts.toml; fi
    ;;
  config)
    put hosts.toml .config/compute/hosts.toml
    ;;
  *) echo "usage: $0 [install|config]" >&2; exit 2 ;;
esac
# Cloud API keys from the local config dir, when present.
local_config=${COMPUTE_CONFIG_DIR:-${XDG_CONFIG_HOME:-$HOME/.config}/compute}
for f in runpod.key lambda.key modal.toml; do
  if [ -f "$local_config/$f" ]; then
    put "$local_config/$f" ".config/compute/$f"
    rsh "chmod 600 ~/.config/compute/$f"
  fi
done
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
