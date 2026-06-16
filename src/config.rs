use crate::SyncResult;
use globset::{Glob, GlobSet, GlobSetBuilder};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SyncMode {
    Mirror,
    Oneway,
    Append,
}

impl std::str::FromStr for SyncMode {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "mirror" => Ok(SyncMode::Mirror),
            "oneway" => Ok(SyncMode::Oneway),
            "append" => Ok(SyncMode::Append),
            _ => Err(format!("Unknown sync mode: {}", s)),
        }
    }
}

impl std::fmt::Display for SyncMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SyncMode::Mirror => write!(f, "mirror"),
            SyncMode::Oneway => write!(f, "oneway"),
            SyncMode::Append => write!(f, "append"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub source: PathBuf,
    pub destination: Destination,
    pub mode: SyncMode,
    pub workers: usize,
    pub follow_symlinks: bool,
    pub follow_hardlinks: bool,
    pub check_case_conflicts: bool,
    pub pre_check_crc: bool,
    pub prefer_ipv4: bool,
    pub chunk_size: u64,
    pub max_chunk_retries: u32,
    pub max_task_retries: u32,
    pub ignore_file: PathBuf,
    pub state_file: PathBuf,
    pub manifest_file: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "config")]
pub enum Destination {
    Local { path: PathBuf },
    Sftp { host: String, port: u16, user: String, path: PathBuf },
    Http { url: String },
}

impl Default for Config {
    fn default() -> Self {
        Self {
            source: PathBuf::new(),
            destination: Destination::Local { path: PathBuf::new() },
            mode: SyncMode::Oneway,
            workers: 4,
            follow_symlinks: true,
            follow_hardlinks: true,
            check_case_conflicts: true,
            pre_check_crc: true,
            prefer_ipv4: true,
            chunk_size: 4 * 1024 * 1024,
            max_chunk_retries: 5,
            max_task_retries: 3,
            ignore_file: PathBuf::from(".syncignore"),
            state_file: PathBuf::from("state.json"),
            manifest_file: PathBuf::from("manifest.json"),
        }
    }
}

impl Config {
    pub fn load(path: &Path) -> SyncResult<Self> {
        let content = std::fs::read_to_string(path)?;
        let config: Config = toml::from_str(&content)?;
        Ok(config)
    }

    pub fn save(&self, path: &Path) -> SyncResult<()> {
        let content = toml::to_string_pretty(self)?;
        std::fs::write(path, content)?;
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct IgnoreRules {
    patterns: GlobSet,
}

impl IgnoreRules {
    pub fn load(path: &Path) -> SyncResult<Self> {
        let content = if path.exists() {
            std::fs::read_to_string(path)?
        } else {
            String::new()
        };
        Self::from_str(&content)
    }

    pub fn from_str(content: &str) -> SyncResult<Self> {
        let mut builder = GlobSetBuilder::new();
        for line in content.lines() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let glob = Glob::new(line)?;
            builder.add(glob);
        }
        let patterns = builder.build()?;
        Ok(Self { patterns })
    }

    pub fn is_ignored(&self, path: &Path) -> bool {
        self.patterns.is_match(path)
    }
}

impl Default for IgnoreRules {
    fn default() -> Self {
        let default_patterns = r"
.git/
.gitignore
node_modules/
__pycache__/
*.pyc
*.pyo
*.so
*.dylib
*.dll
.DS_Store
Thumbs.db
.sync/
state.json
manifest.json
";
        Self::from_str(default_patterns).unwrap()
    }
}
