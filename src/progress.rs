use crate::state::SyncState;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use std::sync::Arc;
use tokio::sync::Mutex;

pub struct ProgressReporter {
    multi: MultiProgress,
    overall: ProgressBar,
    current_file: Mutex<Option<ProgressBar>>,
}

impl ProgressReporter {
    pub fn new(total_bytes: u64, _state: Arc<Mutex<SyncState>>) -> Self {
        let multi = MultiProgress::new();

        let overall = ProgressBar::new(total_bytes);
        overall.set_style(
            ProgressStyle::with_template(
                "{prefix:.bold.dim} [{bar:40.cyan/blue} {percent:>3}% {bytes}/{total_bytes} 速率: {bytes_per_sec} ETA: {eta}",
            )
            .unwrap()
            .progress_chars("=> "),
        );
        overall.set_prefix("总体进度");

        let overall = multi.add(overall);

        Self {
            multi,
            overall,
            current_file: Mutex::new(None),
        }
    }

    pub fn add_file_progress(&self, filename: &str, size: u64) -> ProgressBar {
        let pb = ProgressBar::new(size);
        pb.set_style(
            ProgressStyle::with_template(
                "  {prefix:.green} [{bar:30.green/black} {percent:>3}% {bytes}/{total_bytes}",
            )
            .unwrap()
            .progress_chars("=> "),
        );
        pb.set_prefix(format!("传输中: {}", truncate_path(filename, 40)));
        self.multi.insert_before(&self.overall, pb)
    }

    pub async fn start_file(&self, filename: &str, size: u64) {
        let mut current = self.current_file.lock().await;
        if let Some(pb) = current.as_ref() {
            pb.finish_and_clear();
        }
        let pb = self.add_file_progress(filename, size);
        *current = Some(pb);
    }

    pub async fn update_file_progress(&self, bytes: u64) {
        let current = self.current_file.lock().await;
        if let Some(pb) = current.as_ref() {
            pb.inc(bytes);
        }
        self.overall.inc(bytes);
    }

    pub async fn finish_file(&self, success: bool) {
        let mut current = self.current_file.lock().await;
        if let Some(pb) = current.take() {
            if success {
                pb.finish_with_message("完成");
            } else {
                pb.finish_with_message("失败");
            }
        }
    }

    pub async fn skip_file(&self) {
        let mut current = self.current_file.lock().await;
        if let Some(pb) = current.take() {
            pb.finish_with_message("跳过");
        }
    }

    pub fn finish(&self, success: bool) {
        self.overall.finish_with_message(if success {
            "同步完成"
        } else {
            "同步失败"
        });
        self.multi.clear().ok();
    }

    pub fn set_total(&self, bytes: u64) {
        self.overall.set_position(bytes);
    }
}

fn truncate_path(path: &str, max_len: usize) -> String {
    if path.len() <= max_len {
        path.to_string()
    } else {
        let chars: Vec<char> = path.chars().collect();
        let mid = max_len / 2;
        let prefix: String = chars.iter().take(mid - 1).collect();
        let suffix_start = chars.len().saturating_sub(max_len - mid - 1);
        let suffix: String = chars.iter().skip(suffix_start).collect();
        format!("{}...{}", prefix, suffix)
    }
}

pub struct TransferStats {
    pub start_time: std::time::Instant,
    pub bytes_transferred: u64,
    pub last_bytes: u64,
    pub speed: f64,
}

impl TransferStats {
    pub fn new() -> Self {
        Self {
            start_time: std::time::Instant::now(),
            bytes_transferred: 0,
            last_bytes: 0,
            speed: 0.0,
        }
    }

    pub fn update(&mut self, bytes: u64) {
        let elapsed = self.start_time.elapsed().as_secs_f64();
        if elapsed > 0.0 {
            self.speed = bytes as f64 / elapsed;
        }
        self.bytes_transferred = bytes;
    }

    pub fn speed_mbps(&self) -> f64 {
        self.speed / 1024.0 / 1024.0
    }

    pub fn eta_seconds(&self, total_bytes: u64) -> u64 {
        if self.speed <= 0.0 {
            0
        } else {
            (total_bytes.saturating_sub(self.bytes_transferred) as f64 / self.speed) as u64
        }
    }
}
