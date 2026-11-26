#!/bin/sh
# Sets this machine up to use the compute network at {{URL}}: installs the `compute` CLI, signs
# in with your SSH key and installs the compute skill for your agents (Claude Code, and Codex
# when it's there). Run it again any time; the CLI also keeps itself and the skill up to date.
#
#   curl -fsSL {{URL}}/v1/install | sh
#
# COMPUTE_KEY picks the key (default ~/.ssh/id_ed25519.pub), COMPUTE_BIN_DIR where the CLI goes
# (default ~/.local/bin), COMPUTE_SKILL_DIR the skill's folder (default ~/.claude/skills/compute,
# plus ~/.codex/skills/compute when ~/.codex exists).
set -eu

url='{{URL}}'
bin_dir="${COMPUTE_BIN_DIR:-$HOME/.local/bin}"

die() { printf 'compute: %s\n' "$*" >&2; exit 1; }

command -v curl >/dev/null 2>&1 || die "needs curl; install it and run this again"
os=$(uname -s) arch=$(uname -m)
case "$os-$arch" in
  Darwin-arm64) platform=darwin-arm64 ;;
  Linux-x86_64 | Linux-amd64) platform=linux-x86_64 ;;
  Linux-aarch64 | Linux-arm64) platform=linux-arm64 ;;
  MINGW* | MSYS* | CYGWIN*) die "on Windows, run this in PowerShell instead: irm $url/v1/install.ps1 | iex" ;;
  *) die "there's no compute build for $os $arch" ;;
esac

mkdir -p "$bin_dir"
tmp="$bin_dir/.compute-download.$$"
trap 'rm -f "$tmp"' EXIT
curl -fsSL "$url/v1/cli/$platform" -o "$tmp" || die "couldn't download the compute CLI from $url"
chmod 755 "$tmp"
"$tmp" --version >/dev/null 2>&1 || die "the downloaded compute CLI doesn't run on this machine"
mv -f "$tmp" "$bin_dir/compute"
printf 'compute: installed %s at %s/compute\n' "$("$bin_dir/compute" --version)" "$bin_dir"

# Signing in may wait for the network's owner to add the key; the script itself arrives on
# stdin (curl | sh), so that prompt reads from the terminal.
if (exec </dev/tty) 2>/dev/null; then
  "$bin_dir/compute" setup "$url" </dev/tty
else
  "$bin_dir/compute" setup "$url"
fi

# shellcheck disable=SC2016  # $PATH is printed as is, for the user to paste
case ":$PATH:" in
  *":$bin_dir:"*) ;;
  *) printf '\n%s is not on your PATH. Add it (e.g. to ~/.zshrc or ~/.bashrc), so you and your agents can run `compute`:\n  export PATH="%s:$PATH"\n' "$bin_dir" "$bin_dir" ;;
esac
