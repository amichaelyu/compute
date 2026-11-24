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
    pub async fn store(&self, body: Body) -> Result<(String, bool), ApiError> {
        tokio::fs::create_dir_all(&self.dir).await.map_err(anyhow::Error::from)?;
        let tmp = self.dir.join(format!(".upload-{}", hex::encode(rand::random::<[u8; 8]>())));
        let result = self.write(body, &tmp).await;
        let (hash, size) = match result {
            Ok(v) => v,
            Err(e) => {
                let _ = tokio::fs::remove_file(&tmp).await;
                return Err(e);
            }
        };
        let id = format!("sha256:{hash}");
        let dest = self.path(&id);
        let existed = dest.is_file();
        if existed {
            let _ = tokio::fs::remove_file(&tmp).await;
        } else {
            tokio::fs::rename(&tmp, &dest).await.map_err(anyhow::Error::from)?;
        }
        self.store.snapshot_seen(&id, size)?;
        Ok((id, existed))
    }

    async fn write(&self, body: Body, tmp: &PathBuf) -> Result<(String, u64), ApiError> {
        let mut file = tokio::fs::File::create(tmp).await.map_err(anyhow::Error::from)?;
        let mut hasher = Sha256::new();
        let mut size: u64 = 0;
        let mut head: Vec<u8> = Vec::with_capacity(4);
        let mut stream = body.into_data_stream();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|e| ApiError::bad_request(format!("upload interrupted: {e}"), "retry the upload"))?;
            size += chunk.len() as u64;
            if size > MAX_BYTES {
                return Err(ApiError::bad_request(
                    "snapshot is larger than 2 GiB",
                    "keep data out of the repo (gitignore it) and use $JOB_DATA_DIR",
                ));
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
