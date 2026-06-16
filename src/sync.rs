use crate::chunk::{Chunk, ChunkState};
use crate::config::{Config, SyncMode, IgnoreRules};
use crate::manifest::{FileManifest, Manifest};
use crate::progress::ProgressReporter;
use crate::retry::RetryStrategy;
use crate::state::SyncState;
use crate::SyncError;
use crate::SyncResult;
use crate::transport::{Transport, TransportEnum};
use crate::util::{check_case_conflict, get_file_mtime, get_file_size, sha256_file};
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::sync::Semaphore;
use tracing::{debug, error, info, warn};

pub struct SyncEngine {
    config: Config,
    _source_transport: TransportEnum,
    dest_transport: TransportEnum,
    state: Arc<Mutex<SyncState>>,
    progress: Arc<ProgressReporter>,
    ignore_rules: IgnoreRules,
    chunk_retry: RetryStrategy,
    task_retry: RetryStrategy,
    semaphore: Arc<Semaphore>,
}

impl SyncEngine {
    pub async fn new(
        config: Config,
        source_transport: TransportEnum,
        mut dest_transport: TransportEnum,
        state: Arc<Mutex<SyncState>>,
        ignore_rules: IgnoreRules,
    ) -> SyncResult<Self> {
        dest_transport.connect().await?;

        let total_bytes = state.lock().await.total_bytes;
        let progress = Arc::new(ProgressReporter::new(total_bytes, state.clone()));

        let chunk_retry = RetryStrategy::exponential_backoff(config.max_chunk_retries);
        let task_retry = RetryStrategy::exponential_backoff(config.max_task_retries);
        let semaphore = Arc::new(Semaphore::new(config.workers));

        Ok(Self {
            config,
            _source_transport: source_transport,
            dest_transport,
            state,
            progress,
            ignore_rules,
            chunk_retry,
            task_retry,
            semaphore,
        })
    }

    pub async fn run(&mut self, manifest: &Manifest) -> SyncResult<bool> {
        let mut success = false;
        let task_retry = self.task_retry.clone();
        let state = self.state.clone();

        state.lock().await.start();
        self.save_state().await?;

        let mut last_error: Option<SyncError>;
        let mut attempt = 0;

        loop {
            match self.sync_task(manifest).await {
                Ok(s) => {
                    success = s;
                    last_error = None;
                    break;
                }
                Err(e) => {
                    last_error = Some(e);
                    attempt += 1;

                    let mut state_guard = self.state.lock().await;
                    state_guard.task_retry_count += 1;

                    if let Err(save_err) = self.save_state().await {
                        return Err(SyncError::Other(format!("Failed to save state: {}", save_err)));
                    }

                    if attempt >= task_retry.max_retries {
                        break;
                    }

                    let delay = task_retry.get_delay(attempt);
                    warn!(
                        "Task attempt {}/{} failed, retrying in {:?}: {}",
                        attempt,
                        task_retry.max_retries,
                        delay,
                        last_error.as_ref().unwrap()
                    );
                    tokio::time::sleep(delay).await;
                }
            }
        }

        if let Some(e) = last_error {
            error!("Task failed after max retries: {}", e);
            let mut state_guard = self.state.lock().await;
            state_guard.error = Some(e.to_string());
            state_guard.complete(false);
            self.save_state().await?;
            return Err(SyncError::TaskRetryExceeded);
        }

        self.state.lock().await.complete(success);
        self.save_state().await?;
        self.progress.finish(success);

        Ok(success)
    }

    async fn sync_task(&mut self, manifest: &Manifest) -> SyncResult<bool> {
        self.cleanup_extra_files(manifest).await?;
        self.sync_files(manifest).await?;
        Ok(true)
    }

    async fn cleanup_extra_files(&mut self, manifest: &Manifest) -> SyncResult<()> {
        if matches!(self.config.mode, SyncMode::Append) {
            return Ok(());
        }

        info!("Scanning destination for extra files...");
        let dest_files = self.dest_transport.list_files(Path::new("")).await?;
        let source_files: HashSet<PathBuf> = manifest
            .files
            .iter()
            .map(|f| f.path.clone())
            .collect();

        let mut to_delete = Vec::new();
        for dest_file in &dest_files {
            if self.ignore_rules.is_ignored(dest_file) {
                continue;
            }
            if !source_files.contains(dest_file) {
                if matches!(self.config.mode, SyncMode::Mirror) {
                    to_delete.push(dest_file.clone());
                }
            }
        }

        if !to_delete.is_empty() {
            info!("Deleting {} extra files in mirror mode", to_delete.len());
            for file in &to_delete {
                debug!("Deleting extra file: {:?}", file);
                self.dest_transport.remove_file(file).await?;
            }
        }

        Ok(())
    }

    async fn sync_files(&mut self, manifest: &Manifest) -> SyncResult<()> {
        let total_files = manifest.files.len();
        info!("Starting sync of {} files", total_files);

        for (idx, file_manifest) in manifest.files.iter().enumerate() {
            let file_path_str = file_manifest.path.to_string_lossy().to_string();
            info!(
                "[{}/{}] Processing: {}",
                idx + 1,
                total_files,
                file_path_str
            );

            let result = self.sync_file(file_manifest).await;

            match result {
                Ok(skipped) => {
                    if skipped {
                        self.state.lock().await.skip_file(&file_path_str);
                        self.progress.skip_file().await;
                    } else {
                        let check_result = self.verify_file(file_manifest).await;
                        let success = check_result.is_ok();
                        if !success {
                            error!(
                                "Final verification failed for {}: {}",
                                file_path_str,
                                check_result.unwrap_err()
                            );
                        }
                        self.state
                            .lock()
                            .await
                            .complete_file(&file_path_str, success, None);
                        self.progress.finish_file(success).await;
                    }
                }
                Err(e) => {
                    error!("Failed to sync {}: {}", file_path_str, e);
                    self.state
                        .lock()
                        .await
                        .complete_file(&file_path_str, false, Some(e.to_string()));
                    self.progress.finish_file(false).await;
                    return Err(e);
                }
            }

            self.save_state().await?;
        }

        Ok(())
    }

    async fn sync_file(&mut self, file_manifest: &FileManifest) -> SyncResult<bool> {
        let file_path_str = file_manifest.path.to_string_lossy().to_string();
        let source_path = self.config.source.join(&file_manifest.path);

        if !matches!(self.config.mode, SyncMode::Append) {
            if !self.check_source_unchanged(&source_path, file_manifest).await? {
                return Err(SyncError::SourceModified(file_path_str.clone()));
            }
        }

        if self.config.check_case_conflicts {
            if let Some(parent) = file_manifest.path.parent() {
                if let Some(filename) = file_manifest.path.file_name() {
                    let conflict = check_case_conflict(
                        &self.config.source.join(parent),
                        &filename.to_string_lossy(),
                    )?;
                    if let Some(conflict_path) = conflict {
                        warn!(
                            "Case conflict detected: {:?} vs {:?}",
                            file_manifest.path, conflict_path
                        );
                    }
                }
            }
        }

        if self.should_skip_file(file_manifest).await? {
            debug!("Skipping unchanged file: {}", file_path_str);
            return Ok(true);
        }

        self.state.lock().await.start_file(&file_path_str);
        self.progress
            .start_file(&file_path_str, file_manifest.size)
            .await;

        if file_manifest.size == 0 {
            debug!("Creating zero-byte file: {}", file_path_str);
            self.dest_transport
                .write_full_file(&file_manifest.path, &[])
                .await?;
            return Ok(false);
        }

        self.transfer_chunks(file_manifest).await?;
        Ok(false)
    }

    async fn should_skip_file(&mut self, file_manifest: &FileManifest) -> SyncResult<bool> {
        if matches!(self.config.mode, SyncMode::Append) {
            if self
                .dest_transport
                .file_exists(&file_manifest.path)
                .await?
            {
                return Ok(true);
            }
        }

        let state = self.state.lock().await;
        if state.is_file_complete(&file_manifest.path.to_string_lossy()) {
            return Ok(true);
        }

        Ok(false)
    }

    async fn check_source_unchanged(
        &mut self,
        source_path: &Path,
        file_manifest: &FileManifest,
    ) -> SyncResult<bool> {
        if !source_path.exists() {
            return Err(SyncError::InvalidPath(format!(
                "Source file not found: {:?}",
                source_path
            )));
        }

        let current_mtime = get_file_mtime(source_path)?;
        let current_size = get_file_size(source_path)?;

        let manifest_mtime: std::time::SystemTime = file_manifest.mtime.into();
        if current_mtime != manifest_mtime || current_size != file_manifest.size {
            warn!(
                "Source file modified: {:?} (mtime: {:?} vs {:?}, size: {} vs {})",
                source_path, current_mtime, manifest_mtime, current_size, file_manifest.size
            );
            return Ok(false);
        }

        Ok(true)
    }

    async fn transfer_chunks(&mut self, file_manifest: &FileManifest) -> SyncResult<()> {
        let file_path_str = file_manifest.path.to_string_lossy().to_string();
        let pending_chunks = self
            .state
            .lock()
            .await
            .get_pending_chunks(&file_path_str);

        if pending_chunks.is_empty() {
            debug!("All chunks already transferred for {}", file_path_str);
            return Ok(());
        }

        debug!(
            "Transferring {} pending chunks for {}",
            pending_chunks.len(),
            file_path_str
        );

        let source_path = self.config.source.clone();
        let dest_path = file_manifest.path.clone();
        let file_manifest = file_manifest.clone();
        let state = self.state.clone();
        let progress = self.progress.clone();
        let semaphore = self.semaphore.clone();
        let chunk_retry = self.chunk_retry.clone();
        let dest_transport = self.dest_transport.clone();
        let max_chunk_retries = self.config.max_chunk_retries;
        let pre_check_crc = self.config.pre_check_crc;

        let mut handles = Vec::new();

        for chunk_id in pending_chunks {
            let chunk = file_manifest
                .chunks
                .iter()
                .find(|c| c.id == chunk_id)
                .ok_or_else(|| {
                    SyncError::Other(format!("Chunk {} not found in manifest", chunk_id))
                })?
                .clone();

            let permit = semaphore.clone().acquire_owned().await.unwrap();
            let source_path = source_path.clone();
            let dest_path = dest_path.clone();
            let file_path_str = file_path_str.clone();
            let state = state.clone();
            let progress = progress.clone();
            let chunk_retry = chunk_retry.clone();
            let dest_transport = dest_transport.clone();
            let pre_check_crc = pre_check_crc;

            let handle = tokio::spawn(async move {
                let _permit = permit;

                let mut last_error: Option<SyncError> = None;
                let mut success = false;

                for attempt in 0..chunk_retry.max_retries {
                    let mut dest_transport = dest_transport.clone();
                    let source_path = source_path.clone();
                    let dest_path = dest_path.clone();
                    let chunk = chunk.clone();
                    let file_path_str = file_path_str.clone();
                    let state = state.clone();
                    let progress = progress.clone();

                    match transfer_single_chunk(
                        &source_path,
                        &dest_path,
                        &chunk,
                        pre_check_crc,
                        &mut dest_transport,
                        &state,
                        &file_path_str,
                        &progress,
                    )
                    .await
                    {
                        Ok(_) => {
                            success = true;
                            last_error = None;
                            break;
                        }
                        Err(e) => {
                            last_error = Some(e);
                            if attempt + 1 < chunk_retry.max_retries {
                                let delay = chunk_retry.get_delay(attempt);
                                warn!(
                                    "Chunk {} attempt {}/{} failed, retrying in {:?}",
                                    chunk.id,
                                    attempt + 1,
                                    chunk_retry.max_retries,
                                    delay
                                );
                                tokio::time::sleep(delay).await;
                            }
                        }
                    }
                }

                let result: Result<(), SyncError> = if success {
                    Ok(())
                } else {
                    Err(last_error.unwrap_or_else(|| SyncError::Other("Unknown error".to_string())))
                };

                match result {
                    Ok(_) => Ok(chunk.id),
                    Err(e) => {
                        error!(
                            "Chunk {} of {} failed after {} retries: {}",
                            chunk.id, file_path_str, max_chunk_retries, e
                        );
                        state.lock().await.update_chunk_state(
                            &file_path_str,
                            chunk.id,
                            ChunkState::Failed,
                            0,
                        );
                        Err(SyncError::ChunkRetryExceeded {
                            file: file_path_str.clone(),
                            chunk_id: chunk.id,
                        })
                    }
                }
            });

            handles.push(handle);
        }

        for handle in handles {
            match handle.await {
                Ok(Ok(_)) => {}
                Ok(Err(e)) => return Err(e),
                Err(e) => return Err(SyncError::Other(format!("Task join error: {}", e))),
            }
        }

        Ok(())
    }

    async fn verify_file(&mut self, file_manifest: &FileManifest) -> SyncResult<()> {
        if file_manifest.size == 0 {
            return Ok(());
        }

        let file_path_str = file_manifest.path.to_string_lossy().to_string();
        debug!("Verifying file: {}", file_path_str);

        let temp_path = PathBuf::from(format!(".verify_{}.tmp", file_path_str.replace('/', "_")));
        let _source_path = self.config.source.join(&file_manifest.path);

        let dest_data = self
            .dest_transport
            .read_full_file(&file_manifest.path)
            .await?;

        std::fs::write(&temp_path, &dest_data)?;
        let actual_sha256 = sha256_file(&temp_path)?;
        std::fs::remove_file(&temp_path).ok();

        if actual_sha256 != file_manifest.sha256 {
            error!(
                "SHA256 mismatch for {}: expected {}, got {}",
                file_path_str, file_manifest.sha256, actual_sha256
            );
            return Err(SyncError::ChecksumMismatch {
                expected: file_manifest.sha256.clone(),
                actual: actual_sha256,
            });
        }

        info!("Verification passed for {}", file_path_str);
        Ok(())
    }

    async fn save_state(&self) -> SyncResult<()> {
        let state = self.state.lock().await;
        state.save(&self.config.state_file)?;
        Ok(())
    }
}

async fn transfer_single_chunk(
    source_root: &Path,
    dest_path: &Path,
    chunk: &Chunk,
    pre_check_crc: bool,
    dest_transport: &mut TransportEnum,
    state: &Arc<Mutex<SyncState>>,
    file_path_str: &str,
    progress: &Arc<ProgressReporter>,
) -> SyncResult<()> {
    let source_file_path = source_root.join(dest_path);

    state.lock().await.update_chunk_state(
        file_path_str,
        chunk.id,
        ChunkState::InProgress,
        chunk.size as u64,
    );

    let data = crate::chunk::read_chunk(&source_file_path, chunk)?;

    if pre_check_crc && !chunk.verify_crc(&data) {
        return Err(SyncError::ChecksumMismatch {
            expected: format!("{:08x}", chunk.crc32),
            actual: format!("{:08x}", crate::util::crc32_bytes(&data)),
        });
    }

    dest_transport.write_chunk(dest_path, chunk, &data).await?;

    let write_result = dest_transport.read_chunk(dest_path, chunk).await;
    match write_result {
        Ok(written_data) => {
            if !chunk.verify_crc(&written_data) {
                return Err(SyncError::ChecksumMismatch {
                    expected: format!("{:08x}", chunk.crc32),
                    actual: format!("{:08x}", crate::util::crc32_bytes(&written_data)),
                });
            }
        }
        Err(e) => {
            warn!("Failed to verify written chunk {}: {}", chunk.id, e);
            return Err(e);
        }
    }

    state.lock().await.update_chunk_state(
        file_path_str,
        chunk.id,
        ChunkState::Done,
        chunk.size as u64,
    );

    progress.update_file_progress(chunk.size as u64).await;

    Ok(())
}
