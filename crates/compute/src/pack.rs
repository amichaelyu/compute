//! tar.zst in-process, so the CLI needs no `sh`, `tar` or `zstd` (Windows has none of them,
//! and most Macs have no `zstd`). Archives stream through a pipe from a packing thread.

use anyhow::{Context, Result, bail};
use std::collections::HashSet;
use std::io::{PipeReader, Read};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::thread::JoinHandle;

/// A tar.zst being written by a background thread. Read the stream to the end, then call
/// `finish` to learn whether packing worked.
pub struct Packing {
    pub reader: PipeReader,
    handle: JoinHandle<Result<()>>,
}

impl Packing {
    pub fn finish(self) -> Result<()> {
        self.handle.join().map_err(|_| anyhow::anyhow!("the packing thread panicked"))?
    }
}

fn spawn(work: impl FnOnce(zstd::Encoder<'static, std::io::PipeWriter>) -> Result<()> + Send + 'static) -> Result<Packing> {
    let (reader, writer) = std::io::pipe()?;
    let enc = zstd::Encoder::new(writer, 3)?;
    Ok(Packing { reader, handle: std::thread::spawn(move || work(enc)) })
}

fn output(dir: &Path, cmd: &str, args: &[&str]) -> Option<Vec<u8>> {
    let o = Command::new(cmd).args(args).current_dir(dir).stderr(Stdio::null()).output().ok()?;
    o.status.success().then_some(o.stdout)
}

/// The files a snapshot holds: tracked files plus untracked ones that aren't ignored.
pub fn repo_files(root: &Path) -> Result<Vec<String>> {
    let (list, sep) = if root.join(".git").exists() {
        (output(root, "git", &["ls-files", "-co", "--exclude-standard", "-z"]).context("git ls-files failed")?, b'\0')
    } else {
        (output(root, "jj", &["file", "list"]).context("jj file list failed")?, b'\n')
    };
    Ok(list
        .split(|b| *b == sep)
        .filter(|s| !s.is_empty())
        .map(|s| String::from_utf8_lossy(s).replace('\\', "/"))
        .collect())
}

/// Files git records as executable. Only needed where the filesystem has no exec bit (Windows).
fn git_executables(root: &Path) -> HashSet<String> {
    if !cfg!(windows) {
        return HashSet::new();
    }
    let Some(out) = output(root, "git", &["ls-files", "-s", "-z"]) else { return HashSet::new() };
    out.split(|b| *b == 0)
        .filter_map(|e| {
            let e = String::from_utf8_lossy(e);
            let (meta, path) = e.split_once('\t')?;
            meta.starts_with("100755").then(|| path.to_string())
        })
        .collect()
}

/// Packs `files` (paths relative to `root`) as tar.zst. Symlinks stay links; listed files
/// that no longer exist are skipped with a warning.
pub fn pack_files(root: PathBuf, files: Vec<String>) -> Result<Packing> {
    let exec = git_executables(&root);
    spawn(move |enc| {
        let mut tar = tar::Builder::new(enc);
        tar.follow_symlinks(false);
        for f in &files {
            let path = root.join(f);
            let Ok(meta) = std::fs::symlink_metadata(&path) else {
                eprintln!("warning: {f} is listed but missing; left out of the snapshot");
                continue;
            };
            if meta.is_file() && exec.contains(f) {
                let mut h = tar::Header::new_gnu();
                h.set_metadata(&meta);
                h.set_mode(0o755);
                tar.append_data(&mut h, f, std::fs::File::open(&path)?).with_context(|| format!("packing {f}"))?;
            } else {
                tar.append_path_with_name(&path, f).with_context(|| format!("packing {f}"))?;
            }
        }
        tar.into_inner()?.finish()?;
        Ok(())
    })
}

/// Packs everything under `dir` as tar.zst (entries named `./...`).
pub fn pack_dir(dir: PathBuf) -> Result<Packing> {
    if !dir.is_dir() {
        bail!("{} is not a directory", dir.display());
    }
    spawn(move |enc| {
        let mut tar = tar::Builder::new(enc);
        tar.follow_symlinks(false);
        tar.append_dir_all(".", &dir).with_context(|| format!("packing {}", dir.display()))?;
        tar.into_inner()?.finish()?;
        Ok(())
    })
}

/// Compresses what a command writes (e.g. `git archive`) with zstd.
pub fn pack_command(dir: &Path, cmd: &str, args: &[&str]) -> Result<Packing> {
    let mut child = Command::new(cmd)
        .args(args)
        .current_dir(dir)
        .stdout(Stdio::piped())
        .spawn()
        .with_context(|| format!("running {cmd}"))?;
    let mut out = child.stdout.take().context("no stdout")?;
    let cmd = cmd.to_string();
    spawn(move |mut enc| {
        std::io::copy(&mut out, &mut enc)?;
        enc.finish()?;
        if !child.wait()?.success() {
            bail!("{cmd} failed");
        }
        Ok(())
    })
}

/// Unpacks a tar.zst stream into `dest` (entries can't escape it).
pub fn unpack(r: impl Read, dest: &Path) -> Result<()> {
    let mut a = tar::Archive::new(zstd::Decoder::new(r)?);
    a.set_preserve_permissions(true);
    a.unpack(dest).with_context(|| format!("unpacking into {}", dest.display()))
}
