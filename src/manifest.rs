use crate::chunk::{Chunk, chunk_file};
use crate::config::{Config, IgnoreRules};
use crate::SyncResult;
use crate::util::{get_file_mtime, get_file_size, sha256_file};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use walkdir::WalkDir;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileManifest {
    pub path: PathBuf,
    pub size: u64,
    pub mtime: DateTime<Utc>,
    pub sha256: String,
    pub chunks: Vec<Chunk>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Manifest {
    pub version: u32,
    pub created_at: DateTime<Utc>,
    pub source: PathBuf,
    pub mode: String,
    pub files: Vec<FileManifest>,
    pub total_size: u64,
    pub total_chunks: u64,
}

impl Manifest {
    pub fn new(source: &Path, mode: String) -> Self {
        Self {
            version: 1,
            created_at: Utc::now(),
            source: source.to_path_buf(),
            mode,
            files: Vec::new(),
            total_size: 0,
            total_chunks: 0,
        }
    }

    pub fn generate(config: &Config, ignore_rules: &IgnoreRules) -> SyncResult<Self> {
        let mut manifest = Manifest::new(&config.source, config.mode.to_string());

        let source = &config.source;
        for entry in WalkDir::new(source).follow_links(config.follow_symlinks) {
            let entry = entry?;
            let path = entry.path();

            if path == source {
                continue;
            }

            let rel_path = path.strip_prefix(source)?;
            if ignore_rules.is_ignored(rel_path) {
                tracing::debug!("Ignoring file: {:?}", rel_path);
                continue;
            }

            let file_type = entry.file_type();

            if file_type.is_dir() {
                continue;
            }

            if file_type.is_symlink() && !config.follow_symlinks {
                continue;
            }

            let mtime = get_file_mtime(path)?;
            let size = get_file_size(path)?;

            let file_manifest = if size > 0 {
                let sha256 = sha256_file(path)?;
                let chunks = chunk_file(path)?;
                manifest.total_chunks += chunks.len() as u64;
                FileManifest {
                    path: rel_path.to_path_buf(),
                    size,
                    mtime: mtime.into(),
                    sha256,
                    chunks,
                }
            } else {
                FileManifest {
                    path: rel_path.to_path_buf(),
                    size: 0,
                    mtime: mtime.into(),
                    sha256: String::new(),
                    chunks: Vec::new(),
                }
            };

            manifest.total_size += file_manifest.size;
            manifest.files.push(file_manifest);
        }

        manifest.files.sort_by(|a, b| a.path.cmp(&b.path));
        Ok(manifest)
    }

    pub fn save(&self, path: &Path) -> SyncResult<()> {
        let json = serde_json::to_string_pretty(self)?;
        std::fs::write(path, json)?;
        Ok(())
    }

    pub fn load(path: &Path) -> SyncResult<Self> {
        let content = std::fs::read_to_string(path)?;
        let manifest: Manifest = serde_json::from_str(&content)?;
        Ok(manifest)
    }

    pub fn get_file(&self, path: &Path) -> Option<&FileManifest> {
        self.files.iter().find(|f| f.path == path)
    }
}
