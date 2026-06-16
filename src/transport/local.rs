use crate::chunk::{read_chunk, write_chunk, Chunk};
use crate::SyncResult;
use crate::transport::Transport;
use crate::util::is_symlink;
use async_trait::async_trait;
use std::path::{Path, PathBuf};
use walkdir::WalkDir;

#[derive(Clone)]
pub struct LocalTransport {
    base_path: PathBuf,
}

impl LocalTransport {
    pub fn new(base_path: PathBuf) -> Self {
        Self { base_path }
    }

    fn full_path(&self, path: &Path) -> PathBuf {
        if path.is_absolute() {
            path.to_path_buf()
        } else {
            self.base_path.join(path)
        }
    }
}

#[async_trait]
impl Transport for LocalTransport {
    async fn connect(&mut self) -> SyncResult<()> {
        if !self.base_path.exists() {
            std::fs::create_dir_all(&self.base_path)?;
        }
        Ok(())
    }

    async fn disconnect(&mut self) -> SyncResult<()> {
        Ok(())
    }

    async fn read_chunk(&self, path: &Path, chunk: &Chunk) -> SyncResult<Vec<u8>> {
        let full_path = self.full_path(path);
        read_chunk(&full_path, chunk)
    }

    async fn write_chunk(&self, path: &Path, chunk: &Chunk, data: &[u8]) -> SyncResult<()> {
        let full_path = self.full_path(path);
        if let Some(parent) = full_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        write_chunk(&full_path, chunk, data)
    }

    async fn create_dir_all(&self, path: &Path) -> SyncResult<()> {
        let full_path = self.full_path(path);
        std::fs::create_dir_all(full_path)?;
        Ok(())
    }

    async fn file_exists(&self, path: &Path) -> SyncResult<bool> {
        let full_path = self.full_path(path);
        Ok(full_path.exists())
    }

    async fn get_file_size(&self, path: &Path) -> SyncResult<u64> {
        let full_path = self.full_path(path);
        let metadata = std::fs::metadata(full_path)?;
        Ok(metadata.len())
    }

    async fn remove_file(&self, path: &Path) -> SyncResult<()> {
        let full_path = self.full_path(path);
        if is_symlink(&full_path)? {
            std::fs::remove_file(full_path)?;
        } else {
            std::fs::remove_file(full_path)?;
        }
        Ok(())
    }

    async fn remove_dir(&self, path: &Path) -> SyncResult<()> {
        let full_path = self.full_path(path);
        std::fs::remove_dir_all(full_path)?;
        Ok(())
    }

    async fn list_files(&self, path: &Path) -> SyncResult<Vec<PathBuf>> {
        let full_path = self.full_path(path);
        let mut files = Vec::new();

        if !full_path.exists() {
            return Ok(files);
        }

        for entry in WalkDir::new(&full_path) {
            let entry = entry?;
            let entry_path = entry.path();
            if entry.file_type().is_file() || entry.file_type().is_symlink() {
                let rel_path = entry_path.strip_prefix(&full_path)?.to_path_buf();
                files.push(rel_path);
            }
        }

        Ok(files)
    }

    async fn read_full_file(&self, path: &Path) -> SyncResult<Vec<u8>> {
        let full_path = self.full_path(path);
        let data = std::fs::read(full_path)?;
        Ok(data)
    }

    async fn write_full_file(&self, path: &Path, data: &[u8]) -> SyncResult<()> {
        let full_path = self.full_path(path);
        if let Some(parent) = full_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        crate::util::atomic_write(&full_path, data)?;
        Ok(())
    }
}
