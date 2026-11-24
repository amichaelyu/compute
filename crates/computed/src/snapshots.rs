//! Code snapshots: tar.zst uploads named by the SHA-256 of their bytes.

use crate::error::ApiError;
use crate::model::is_snapshot_id;
use crate::store::Store;
use anyhow::{Context, Result};
use axum::body::Body;
use futures_util::StreamExt;
use sha2::{Digest, Sha256};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::io::AsyncWriteExt;

pub const MAX_BYTES: u64 = 2 * 1024 * 1024 * 1024;
const ZSTD_MAGIC: [u8; 4] = [0x28, 0xB5, 0x2F, 0xFD];
const GC_DAYS: i64 = 14;

pub struct Snapshots {
    dir: PathBuf,
    store: Arc<Store>,
}

impl Snapshots {
    pub fn new(dir: PathBuf, store: Arc<Store>) -> Snapshots {
        Snapshots { dir, store }
    }

    pub fn path(&self, id: &str) -> PathBuf {
        self.dir.join(format!("{}.tar.zst", id.trim_start_matches("sha256:")))
    }

    pub fn exists(&self, id: &str) -> bool {
        is_snapshot_id(id) && self.path(id).is_file()
    }

    pub fn touch(&self, id: &str) -> Result<()> {
        let size = std::fs::metadata(self.path(id)).map(|m| m.len()).unwrap_or(0);
        self.store.snapshot_seen(id, size)
    }

    /// Streams an upload to disk while hashing it. Returns `(id, already_existed)`.
    pub async fn store(&self, body: Body, expected: Option<u64>) -> Result<(String, bool), ApiError> {
        let (hash, size, existed) = store_blob_inner(&self.dir, body, MAX_BYTES, expected, "snapshot is larger than 2 GiB",
            "keep data out of the repo (gitignore it) and use datasets").await?;
        let id = format!("sha256:{hash}");
        self.store.snapshot_seen(&id, size)?;
        Ok((id, existed))
    }

    /// Deletes snapshots unused for 14 days that no unfinished job needs.
    pub fn gc(&self) -> Result<usize> {
        let stale = self.store.stale_snapshots(GC_DAYS)?;
        for id in &stale {
            let p = self.path(id);
            if p.exists() {
                std::fs::remove_file(&p).with_context(|| format!("removing {}", p.display()))?;
            }
            self.store.forget_snapshot(id)?;
        }
        Ok(stale.len())
    }
}

/// The path of a stored blob (`<dir>/<sha256 hex>.tar.zst`).
pub fn blob_path(dir: &std::path::Path, sha: &str) -> PathBuf {
    dir.join(format!("{}.tar.zst", sha.trim_start_matches("sha256:")))
}

/// Stores a zstd upload under `dir` named by its SHA-256. Returns `(hex, size)`.
pub async fn store_blob(dir: &std::path::Path, body: Body, max: u64, expected: Option<u64>) -> Result<(String, u64), ApiError> {
    let (hash, size, _) = store_blob_inner(dir, body, max, expected, "upload is too large", "split it up").await?;
    Ok((hash, size))
}

/// Disk an upload must leave free on the coordinator, which also holds the database.
const MIN_FREE: u64 = 10 * 1024 * 1024 * 1024;
/// How often (bytes written) free space is checked again during an upload.
const RECHECK: u64 = 256 * 1024 * 1024;

/// Free bytes on the filesystem holding `dir` (None if `df` can't tell).
async fn free_bytes(dir: &std::path::Path) -> Option<u64> {
    let out = tokio::process::Command::new("df").arg("-Pk").arg(dir).output().await.ok()?;
    let text = String::from_utf8_lossy(&out.stdout);
    let kb: u64 = text.lines().nth(1)?.split_whitespace().nth(3)?.parse().ok()?;
    Some(kb * 1024)
}

fn disk_full(free: u64, need: u64) -> ApiError {
    ApiError::new(
        axum::http::StatusCode::INSUFFICIENT_STORAGE,
        "disk_full",
        format!("the coordinator has {} GB free; this upload needs {} GB plus {} GB kept free", free >> 30, need.div_ceil(1 << 30), MIN_FREE >> 30),
        "upload less (register data from Hugging Face instead), or ask the network's owner to free space",
    )
}

async fn store_blob_inner(dir: &std::path::Path, body: Body, max: u64, expected: Option<u64>, too_big: &str, hint: &str) -> Result<(String, u64, bool), ApiError> {
    tokio::fs::create_dir_all(dir).await.map_err(anyhow::Error::from)?;
    if let Some(free) = free_bytes(dir).await {
        let need = expected.unwrap_or(0);
        if free < need.saturating_add(MIN_FREE) {
            return Err(disk_full(free, need));
        }
    }
    let tmp = dir.join(format!(".upload-{}", hex::encode(rand::random::<[u8; 8]>())));
    let result = write_hashed(body, &tmp, max, too_big, hint).await;
    let (hash, size) = match result {
        Ok(v) => v,
        Err(e) => {
            let _ = tokio::fs::remove_file(&tmp).await;
            return Err(e);
        }
    };
    let dest = blob_path(dir, &hash);
    let existed = dest.is_file();
    if existed {
        let _ = tokio::fs::remove_file(&tmp).await;
    } else {
        tokio::fs::rename(&tmp, &dest).await.map_err(anyhow::Error::from)?;
    }
    Ok((hash, size, existed))
}

async fn write_hashed(body: Body, tmp: &std::path::Path, max: u64, too_big: &str, hint: &str) -> Result<(String, u64), ApiError> {
    let mut file = tokio::fs::File::create(tmp).await.map_err(anyhow::Error::from)?;
    let mut hasher = Sha256::new();
    let mut size: u64 = 0;
    let mut head: Vec<u8> = Vec::with_capacity(4);
    let mut next_check = RECHECK;
    let dir = tmp.parent().unwrap_or(std::path::Path::new("."));
    let mut stream = body.into_data_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|e| ApiError::bad_request(format!("upload interrupted: {e}"), "retry the upload"))?;
        size += chunk.len() as u64;
        if size > max {
            return Err(ApiError::bad_request(too_big, hint));
        }
        if size >= next_check {
            next_check = size + RECHECK;
            if let Some(free) = free_bytes(dir).await
                && free < MIN_FREE
            {
                return Err(disk_full(free, size));
            }
        }
        if head.len() < 4 {
            head.extend(chunk.iter().take(4 - head.len()));
        }
        hasher.update(&chunk);
        file.write_all(&chunk).await.map_err(anyhow::Error::from)?;
    }
    file.flush().await.map_err(anyhow::Error::from)?;
    if head != ZSTD_MAGIC {
        return Err(ApiError::bad_request(
            "upload is not zstd-compressed",
            "send a tar compressed with zstd: ... | tar -c ... | zstd | curl --data-binary @-",
        ));
    }
    Ok((hex::encode(hasher.finalize()), size))
}
