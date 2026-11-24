//! Embeds the built dashboard (`web/dist`, made by `bun run build` in `web/`) into the binary.
//! When it hasn't been built, the daemon serves a placeholder page instead.

use std::fs;
use std::path::{Path, PathBuf};

fn collect(root: &Path, dir: &Path, out: &mut Vec<(String, PathBuf)>) {
    let Ok(entries) = fs::read_dir(dir) else { return };
    let mut entries: Vec<_> = entries.flatten().collect();
    entries.sort_by_key(|e| e.path());
    for e in entries {
        let path = e.path();
        if path.is_dir() {
            collect(root, &path, out);
        } else if let Ok(rel) = path.strip_prefix(root) {
            println!("cargo:rerun-if-changed={}", path.display());
            let rel = rel.components().map(|c| c.as_os_str().to_string_lossy()).collect::<Vec<_>>().join("/");
            out.push((rel, path.canonicalize().unwrap_or(path)));
        }
    }
}

fn main() {
    let manifest = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR"));
    let dist = manifest.join("../../web/dist");
    println!("cargo:rerun-if-changed={}", dist.display());
    let mut files = Vec::new();
    if dist.join("index.html").is_file() {
        collect(&dist, &dist, &mut files);
    }
    let mut code = String::from("/// Dashboard files: (path relative to web/dist, contents).\npub static ASSETS: &[(&str, &[u8])] = &[\n");
    for (rel, abs) in &files {
        code.push_str(&format!("    ({rel:?}, include_bytes!({:?})),\n", abs.display().to_string()));
    }
    code.push_str("];\n");
    let out = PathBuf::from(std::env::var("OUT_DIR").expect("OUT_DIR")).join("ui_assets.rs");
    fs::write(out, code).expect("writing ui_assets.rs");
}
