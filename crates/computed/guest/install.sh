#!/bin/sh
# Sets this machine up to use the compute network at {{URL}}: signs in with your SSH key and
# installs the compute skill for Claude Code. Run it again any time to update the skill.
#
#   curl -fsSL {{URL}}/v1/install | sh
#
# COMPUTE_KEY picks the key (default ~/.ssh/id_ed25519.pub); COMPUTE_SKILL_DIR the skill's
# folder (default ~/.claude/skills/compute).
set -eu

url='{{URL}}'
key="${COMPUTE_KEY:-$HOME/.ssh/id_ed25519.pub}"
skill_dir="${COMPUTE_SKILL_DIR:-$HOME/.claude/skills/compute}"
conf="$HOME/.config/compute"

die() { printf 'compute: %s\n' "$*" >&2; exit 1; }

for t in curl jq ssh-keygen; do
  command -v "$t" >/dev/null 2>&1 || die "needs $t; install it and run this again"
done
case "$key" in *.pub) ;; *) key="$key.pub" ;; esac
[ -f "$key" ] || die "no SSH public key at $key. Make one with: ssh-keygen -t ed25519 (or set COMPUTE_KEY to your .pub file)"

# Sign with the private key next to the .pub when it's there, otherwise through ssh-agent.
signer="$key"
[ -f "${key%.pub}" ] && signer="${key%.pub}"

nonce=$(curl -fsS -X POST "$url/v1/auth/challenge" | jq -r .nonce) || die "can't reach $url"
sig=$(printf %s "$nonce" | ssh-keygen -Y sign -n compute -f "$signer") || die "ssh-keygen couldn't sign with $signer"
resp=$(jq -n --arg n "$nonce" --arg s "$sig" --rawfile k "$key" '{nonce:$n, signature:$s, public_key:$k}' |
  curl -sS -H 'Content-Type: application/json' --data-binary @- "$url/v1/auth/token") || die "can't reach $url"
token=$(printf '%s' "$resp" | jq -r '.token // empty' 2>/dev/null || true)

if [ -z "$token" ]; then
  msg=$(printf '%s' "$resp" | jq -r '.error.message // empty' 2>/dev/null || true)
  case "$msg" in
    *allowed_signers*)
      printf '%s\n\n' "This SSH key isn't on the compute network yet. Send this line to the person who runs it:" >&2
      cat "$key" >&2
      printf '\n%s\n' "Once they've added it, run the same command again." >&2
      exit 1 ;;
    *) die "sign-in failed: ${msg:-$resp}" ;;
  esac
fi

mkdir -p "$conf"
chmod 700 "$conf"
(umask 077 && printf '%s\n' "$token" > "$conf/session")
if [ ! -f "$conf/client.env" ]; then
  printf '# compute client config, written by %s/v1/install\nCOMPUTE_URL=%s\nCOMPUTE_KEY=%s\n' "$url" "$url" "$key" > "$conf/client.env"
elif ! grep -qx "COMPUTE_URL=$url" "$conf/client.env"; then
  printf 'compute: %s/client.env points at another address; left it as is. Set COMPUTE_URL=%s there to use this network.\n' "$conf" "$url" >&2
fi

mkdir -p "$skill_dir"
curl -fsS -H "Authorization: Bearer $token" "$url/v1/skill" -o "$skill_dir/SKILL.md.tmp" || die "couldn't download the skill"
mv "$skill_dir/SKILL.md.tmp" "$skill_dir/SKILL.md"

name=$(curl -fsS -H "Authorization: Bearer $token" "$url/v1/whoami" | jq -r '.name // .identity' 2>/dev/null || echo "?")
printf 'compute: signed in as %s; installed the skill at %s/SKILL.md\n' "$name" "$skill_dir"
printf 'Ask Claude to run something on compute, e.g. "train this on a GPU with compute".\n'
