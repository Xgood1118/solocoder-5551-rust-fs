use crate::chunk::ChunkState;
use crate::SyncResult;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum FileSyncStatus {
    Pending,
    InProgress,
    Done,
    Failed,
    Skipped,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileState {
    pub path: PathBuf,
    pub status: FileSyncStatus,
    pub chunks: HashMap<u64, ChunkState>,
    pub error: Option<String>,
    pub started_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
    pub bytes_transferred: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TaskStatus {
    Pending,
    InProgress,
    Done,
    Failed,
    Interrupted,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncState {
    pub version: u32,
    pub task_id: String,
    pub status: TaskStatus,
    pub started_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
    pub task_retry_count: u32,
    pub files: HashMap<String, FileState>,
    pub total_files: u64,
    pub completed_files: u64,
    pub total_chunks: u64,
    pub completed_chunks: u64,
    pub total_bytes: u64,
    pub transferred_bytes: u64,
    pub error: Option<String>,
}

impl SyncState {
    pub fn new() -> Self {
        Self {
            version: 1,
            task_id: uuid(),
            status: TaskStatus::Pending,
            started_at: None,
            completed_at: None,
            task_retry_count: 0,
            files: HashMap::new(),
            total_files: 0,
            completed_files: 0,
            total_chunks: 0,
            completed_chunks: 0,
            total_bytes: 0,
            transferred_bytes: 0,
            error: None,
        }
    }

    pub fn init_from_manifest(&mut self, manifest: &crate::manifest::Manifest) {
        self.total_files = manifest.files.len() as u64;
        self.total_chunks = manifest.total_chunks;
        self.total_bytes = manifest.total_size;

        for file in &manifest.files {
            let mut chunks = HashMap::new();
            for chunk in &file.chunks {
                chunks.insert(chunk.id, ChunkState::Pending);
            }

            let file_state = FileState {
                path: file.path.clone(),
                status: FileSyncStatus::Pending,
                chunks,
                error: None,
                started_at: None,
                completed_at: None,
                bytes_transferred: 0,
            };

            self.files.insert(file.path.to_string_lossy().to_string(), file_state);
        }
    }

    pub fn save(&self, path: &Path) -> SyncResult<()> {
        let json = serde_json::to_string_pretty(self)?;
        crate::util::atomic_write(path, json.as_bytes())?;
        Ok(())
    }

    pub fn load(path: &Path) -> SyncResult<Self> {
        let content = std::fs::read_to_string(path)?;
        let state: SyncState = serde_json::from_str(&content)?;
        Ok(state)
    }

    pub fn start(&mut self) {
        self.status = TaskStatus::InProgress;
        self.started_at = Some(Utc::now());
    }

    pub fn complete(&mut self, success: bool) {
        if success {
            self.status = TaskStatus::Done;
        } else {
            self.status = TaskStatus::Failed;
        }
        self.completed_at = Some(Utc::now());
    }

    pub fn start_file(&mut self, file_path: &str) {
        if let Some(file) = self.files.get_mut(file_path) {
            file.status = FileSyncStatus::InProgress;
            file.started_at = Some(Utc::now());
        }
    }

    pub fn complete_file(&mut self, file_path: &str, success: bool, error: Option<String>) {
        if let Some(file) = self.files.get_mut(file_path) {
            if success {
                file.status = FileSyncStatus::Done;
                self.completed_files += 1;
            } else {
                file.status = FileSyncStatus::Failed;
                file.error = error;
            }
            file.completed_at = Some(Utc::now());
        }
    }

    pub fn skip_file(&mut self, file_path: &str) {
        if let Some(file) = self.files.get_mut(file_path) {
            file.status = FileSyncStatus::Skipped;
            file.completed_at = Some(Utc::now());
            self.completed_files += 1;
        }
    }

    pub fn update_chunk_state(&mut self, file_path: &str, chunk_id: u64, state: ChunkState, chunk_size: u64) {
        if let Some(file) = self.files.get_mut(file_path) {
            if let Some(chunk_state) = file.chunks.get_mut(&chunk_id) {
                let was_done = matches!(*chunk_state, ChunkState::Done);
                *chunk_state = state;
                let is_done = matches!(state, ChunkState::Done);

                if !was_done && is_done {
                    self.completed_chunks += 1;
                    self.transferred_bytes += chunk_size;
                    file.bytes_transferred += chunk_size;
                } else if was_done && !is_done {
                    self.completed_chunks = self.completed_chunks.saturating_sub(1);
                    self.transferred_bytes = self.transferred_bytes.saturating_sub(chunk_size);
                    file.bytes_transferred = file.bytes_transferred.saturating_sub(chunk_size);
                }
            }
        }
    }

    pub fn get_pending_chunks(&self, file_path: &str) -> Vec<u64> {
        self.files.get(file_path)
            .map(|f| {
                f.chunks.iter()
                    .filter(|(_, s)| !matches!(s, ChunkState::Done))
                    .map(|(id, _)| *id)
                    .collect()
            })
            .unwrap_or_default()
    }

    pub fn is_file_complete(&self, file_path: &str) -> bool {
        self.files.get(file_path)
            .map(|f| f.chunks.values().all(|s| matches!(s, ChunkState::Done)))
            .unwrap_or(false)
    }

    pub fn progress_percent(&self) -> f64 {
        if self.total_bytes == 0 {
            100.0
        } else {
            (self.transferred_bytes as f64 / self.total_bytes as f64) * 100.0
        }
    }

    pub fn reset_in_progress(&mut self) {
        for file in self.files.values_mut() {
            for chunk_state in file.chunks.values_mut() {
                if matches!(chunk_state, ChunkState::InProgress) {
                    *chunk_state = ChunkState::Pending;
                }
            }
        }
    }
}

impl Default for SyncState {
    fn default() -> Self {
        Self::new()
    }
}

fn uuid() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
    format!("task-{}-{}", now.as_secs(), now.subsec_nanos())
}
