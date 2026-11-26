# Agent notes

This repo is public. Never commit anything that identifies the owner, the people using a network,
or its machines:

- names, emails, usernames, SSH keys or key fingerprints;
- hostnames, tailnet names (`*.ts.net`), IP addresses, Rex labels or real machine names;
- absolute or machine-specific paths (`/Users/...`, `/home/<user>`, `/scratch/users/<user>`);
- API keys, tokens, sessions, secrets, or anything from `~/.config/compute/`.

Use placeholders in code, docs and examples (`<coordinator>`, `<host>`, `me`). Real settings live
in gitignored files: `hosts.toml`, `docs/`. Before committing, read the diff for any of the above;
if something private was already committed, it must be removed from history, not just deleted.
