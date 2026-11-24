//! Minimal async client for Modal's gRPC API (<https://modal.com>), covering what a batch job
//! scheduler needs: apps, images pulled from a registry, secrets, and Sandboxes with exec.
//!
//! The call sequences, request fields and gRPC metadata follow Modal's official Go SDK
//! (`modal-labs/modal-client`, directory `go/`, formerly `modal-labs/libmodal`); see
//! `proto/README.md` for the vendored proto revision. Modal's gRPC API is not a public, stable
//! interface, so this crate pins the proto files it was written against.
//!
//! Commands run in a Sandbox through its task command router, the same path the official SDKs
//! use: `exec` streams stdin in offset-addressed chunks and reads stdout/stderr as resumable
//! streams, so output is binary-safe and survives transient connection errors.

mod client;
mod exec;
mod resources;
mod router;
mod sandbox;

/// Generated protobuf messages and gRPC clients for Modal's API. Not a stable interface.
#[allow(clippy::all, missing_docs, rustdoc::all)]
pub mod proto {
    /// Messages and the `ModalClient` service (package `modal.client`).
    pub mod client {
        include!(concat!(env!("OUT_DIR"), "/modal.client.rs"));
    }
    /// Messages and the `TaskCommandRouter` service (package `modal.task_command_router`).
    pub mod task_command_router {
        include!(concat!(env!("OUT_DIR"), "/modal.task_command_router.rs"));
    }
}

pub use client::{Client, DEFAULT_SERVER_URL};
pub use exec::{ExecExit, ExecHandle, ExecStdout};
pub use sandbox::{SandboxExit, SandboxInfo, SandboxSpec};
