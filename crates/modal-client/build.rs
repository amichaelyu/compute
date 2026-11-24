//! Compiles the vendored Modal protos with the pure-Rust `protox` compiler (no system `protoc`)
//! and generates prost messages plus tonic clients (no servers).

use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let root = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR")?).join("proto");
    let files = [
        root.join("modal_proto/api.proto"),
        root.join("modal_proto/task_command_router.proto"),
    ];
    for file in &files {
        println!("cargo:rerun-if-changed={}", file.display());
    }

    let fds = protox::compile(&files, [&root])?;
    tonic_prost_build::configure()
        .build_server(false)
        .build_transport(false)
        .emit_rerun_if_changed(false)
        .compile_fds(fds)?;
    Ok(())
}
