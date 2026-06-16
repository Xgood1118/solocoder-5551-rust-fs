use thiserror::Error;

pub type SyncResult<T> = Result<T, SyncError>;

#[derive(Error, Debug)]
pub enum SyncError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Serialization error: {0}")]
    Serde(#[from] serde_json::Error),

    #[error("TOML deserialize error: {0}")]
    TomlDeserialize(#[from] toml::de::Error),

    #[error("TOML serialize error: {0}")]
    TomlSerialize(#[from] toml::ser::Error),

    #[error("Glob pattern error: {0}")]
    Glob(#[from] globset::Error),

    #[error("SSH error: {0}")]
    Ssh(#[from] ssh2::Error),

    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),

    #[error("Walkdir error: {0}")]
    Walkdir(#[from] walkdir::Error),

    #[error("Path strip prefix error: {0}")]
    StripPrefix(#[from] std::path::StripPrefixError),

    #[error("Invalid path: {0}")]
    InvalidPath(String),

    #[error("Checksum mismatch: expected {expected}, got {actual}")]
    ChecksumMismatch { expected: String, actual: String },

    #[error("Source file modified during sync: {0}")]
    SourceModified(String),

    #[error("Disk full while writing {0}")]
    DiskFull(String),

    #[error("Permission denied: {0}")]
    PermissionDenied(String),

    #[error("Max retries exceeded for chunk {chunk_id} of {file}")]
    ChunkRetryExceeded { file: String, chunk_id: u64 },

    #[error("Max task retries exceeded")]
    TaskRetryExceeded,

    #[error("Sync failed: {0}")]
    SyncFailed(String),

    #[error("Case conflict detected: {0}")]
    CaseConflict(String),

    #[error("Unsupported operation: {0}")]
    Unsupported(String),

    #[error("Other: {0}")]
    Other(String),
}
