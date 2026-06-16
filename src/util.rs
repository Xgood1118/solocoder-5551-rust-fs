use crate::SyncError;
use crate::SyncResult;
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};
use std::time::SystemTime;

pub fn sha256_file(path: &Path) -> SyncResult<String> {
    use std::io::Read;
    let mut file = std::fs::File::open(path)?;
    let mut hasher = Sha256::new();
    let mut buffer = [0u8; 1024 * 1024];
    loop {
        let bytes_read = file.read(&mut buffer)?;
        if bytes_read == 0 {
            break;
        }
        hasher.update(&buffer[..bytes_read]);
    }
    Ok(format!("{:x}", hasher.finalize()))
}

pub fn crc32_bytes(data: &[u8]) -> u32 {
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(data);
    hasher.finalize()
}

pub fn get_file_mtime(path: &Path) -> SyncResult<SystemTime> {
    let metadata = std::fs::metadata(path)?;
    metadata.modified().map_err(SyncError::Io)
}

pub fn get_file_size(path: &Path) -> SyncResult<u64> {
    let metadata = std::fs::metadata(path)?;
    Ok(metadata.len())
}

pub fn is_symlink(path: &Path) -> SyncResult<bool> {
    let metadata = std::fs::symlink_metadata(path)?;
    Ok(metadata.file_type().is_symlink())
}

pub fn readlink(path: &Path) -> SyncResult<PathBuf> {
    std::fs::read_link(path).map_err(SyncError::Io)
}

pub fn get_worker_count() -> usize {
    std::env::var("RUST_SYNC_WORKERS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or_else(|| std::cmp::min(num_cpus::get(), 4))
}

pub fn relative_path(base: &Path, path: &Path) -> SyncResult<PathBuf> {
    path.strip_prefix(base)
        .map(|p| p.to_path_buf())
        .map_err(|_| SyncError::InvalidPath(format!("{:?} is not under {:?}", path, base)))
}

pub fn is_case_sensitive() -> bool {
    !cfg!(windows)
}

pub fn check_case_conflict(dir: &Path, filename: &str) -> SyncResult<Option<PathBuf>> {
    if !dir.exists() {
        return Ok(None);
    }

    let lower_name = filename.to_lowercase();
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let existing_name = entry.file_name();
        let existing_str = existing_name.to_string_lossy();
        if existing_str.to_lowercase() == lower_name && existing_str != filename {
            return Ok(Some(entry.path()));
        }
    }
    Ok(None)
}

pub fn atomic_write(path: &Path, data: &[u8]) -> SyncResult<()> {
    let tmp_path = path.with_extension("tmp");
    std::fs::write(&tmp_path, data)?;
    std::fs::rename(&tmp_path, path)?;
    Ok(())
}
