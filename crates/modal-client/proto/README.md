# Vendored Modal protobuf definitions

`modal_proto/api.proto` and `modal_proto/task_command_router.proto` are copied unmodified from
[modal-labs/modal-client](https://github.com/modal-labs/modal-client), directory `modal_proto/`,
at commit `9309769216bb22431beebc75213624fabd3f006f` (2026-11-24).

They are licensed under the Apache License 2.0; a copy of the upstream license is in `LICENSE`
next to this file. `api.proto` only imports Google well-known types, which `protox` provides at
build time.

The client logic in `../src` follows the official Go SDK in the same repository (`go/`, version
0.11.2 at that commit). The older `modal-labs/libmodal` repository now points there.

To update, replace both files from a newer upstream commit and update the hash above.
