use crate::config::{Config, Destination, IgnoreRules, SyncMode};
use crate::manifest::Manifest;
use crate::state::{SyncState, TaskStatus};
use crate::sync::SyncEngine;
use crate::SyncResult;
use crate::transport::{create_transport, http::start_http_server, local::LocalTransport, TransportEnum};
use crate::util::get_worker_count;
use clap::{Parser, Subcommand};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{error, info};

#[derive(Parser, Debug)]
#[command(name = "sync", version, about = "Fast, reliable file sync tool with chunked transfer", long_about = None)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand, Debug)]
pub enum Commands {
    Init {
        #[arg(short, long)]
        source: PathBuf,
        #[arg(short, long)]
        destination: String,
        #[arg(short, long, default_value = "oneway", value_parser = ["mirror", "oneway", "append"])]
        mode: String,
        #[arg(short, long)]
        config: Option<PathBuf>,
    },
    Status {
        #[arg(short, long, default_value = "state.json")]
        state_file: PathBuf,
    },
    Resume {
        #[arg(short, long, default_value = ".sync.toml")]
        config: PathBuf,
        #[arg(short, long, default_value = "manifest.json")]
        manifest: PathBuf,
        #[arg(short, long, default_value = "state.json")]
        state_file: PathBuf,
    },
    Sync {
        #[arg(short, long)]
        source: Option<PathBuf>,
        #[arg(short, long)]
        destination: Option<String>,
        #[arg(short, long, default_value = "oneway", value_parser = ["mirror", "oneway", "append"])]
        mode: Option<String>,
        #[arg(short, long)]
        config: Option<PathBuf>,
        #[arg(short, long, default_value = "manifest.json")]
        manifest: PathBuf,
        #[arg(short, long, default_value = "state.json")]
        state_file: PathBuf,
    },
    Server {
        #[arg(short, long)]
        path: PathBuf,
        #[arg(short, long, default_value_t = 8080)]
        port: u16,
    },
}

impl Cli {
    pub async fn run(self) -> SyncResult<()> {
        match self.command {
            Commands::Init {
                source,
                destination,
                mode,
                config,
            } => {
                let config_path = config.unwrap_or_else(|| PathBuf::from(".sync.toml"));
                cmd_init(&source, &destination, &mode, &config_path).await
            }
            Commands::Status { state_file } => cmd_status(&state_file).await,
            Commands::Resume {
                config,
                manifest,
                state_file,
            } => cmd_resume(&config, &manifest, &state_file).await,
            Commands::Sync {
                source,
                destination,
                mode,
                config,
                manifest,
                state_file,
            } => {
                cmd_sync(
                    source.as_deref(),
                    destination.as_deref(),
                    mode.as_deref(),
                    config.as_deref(),
                    &manifest,
                    &state_file,
                )
                .await
            }
            Commands::Server { path, port } => cmd_server(&path, port).await,
        }
    }
}

async fn cmd_init(
    source: &Path,
    destination: &str,
    mode: &str,
    config_path: &Path,
) -> SyncResult<()> {
    info!("Initializing sync configuration...");

    let dest = parse_destination(destination)?;
    let sync_mode: SyncMode = mode.parse().map_err(|e| crate::SyncError::Other(e))?;

    let workers = get_worker_count();

    let config = Config {
        source: source.to_path_buf(),
        destination: dest,
        mode: sync_mode,
        workers,
        state_file: PathBuf::from("state.json"),
        manifest_file: PathBuf::from("manifest.json"),
        ..Default::default()
    };

    config.save(config_path)?;
    info!("Configuration saved to {:?}", config_path);

    let ignore_rules = IgnoreRules::default();
    let manifest = Manifest::generate(&config, &ignore_rules)?;
    manifest.save(&config.manifest_file)?;
    info!(
        "Manifest generated: {} files, {} chunks, {} bytes",
        manifest.files.len(),
        manifest.total_chunks,
        manifest.total_size
    );

    let mut state = SyncState::new();
    state.init_from_manifest(&manifest);
    state.save(&config.state_file)?;
    info!("State file initialized: {:?}", config.state_file);

    println!("\n✅ 初始化完成!");
    println!("  配置文件: {:?}", config_path);
    println!("  清单文件: manifest.json");
    println!("  状态文件: state.json");
    println!("\n现在可以运行 'sync sync' 开始同步");

    Ok(())
}

async fn cmd_status(state_file: &Path) -> SyncResult<()> {
    if !state_file.exists() {
        error!("State file not found: {:?}", state_file);
        return Err(crate::SyncError::InvalidPath(format!(
            "State file not found: {:?}",
            state_file
        )));
    }

    let state = SyncState::load(state_file)?;

    println!("\n📊 同步状态");
    println!("  任务ID: {}", state.task_id);
    println!("  状态: {:?}", state.status);
    println!(
        "  进度: {}/{} 个文件 ({:.1}%)",
        state.completed_files,
        state.total_files,
        if state.total_files > 0 {
            (state.completed_files as f64 / state.total_files as f64) * 100.0
        } else {
            100.0
        }
    );
    println!(
        "  块: {}/{}",
        state.completed_chunks, state.total_chunks
    );
    println!(
        "  数据: {}/{} bytes ({:.1}%)",
        state.transferred_bytes,
        state.total_bytes,
        state.progress_percent()
    );

    if let Some(started_at) = state.started_at {
        println!("  开始时间: {}", started_at);
    }
    if let Some(completed_at) = state.completed_at {
        println!("  完成时间: {}", completed_at);
    }
    if state.task_retry_count > 0 {
        println!("  重试次数: {}", state.task_retry_count);
    }
    if let Some(err) = &state.error {
        println!("  错误: {}", err);
    }

    let failed_files: Vec<_> = state
        .files
        .iter()
        .filter(|(_, f)| matches!(f.status, crate::state::FileSyncStatus::Failed))
        .collect();

    if !failed_files.is_empty() {
        println!("\n❌ 失败的文件:");
        for (path, file) in &failed_files {
            println!("  - {}: {}", path, file.error.as_deref().unwrap_or("unknown"));
        }
    }

    Ok(())
}

async fn cmd_resume(config_path: &Path, manifest_path: &Path, state_file: &Path) -> SyncResult<()> {
    info!("Resuming sync from state file...");

    let mut config = Config::load(config_path)?;
    config.state_file = state_file.to_path_buf();
    config.manifest_file = manifest_path.to_path_buf();

    let mut state = SyncState::load(state_file)?;

    if matches!(state.status, TaskStatus::Done) {
        info!("Sync already completed successfully");
        println!("✅ 同步已完成");
        return Ok(());
    }

    let manifest = Manifest::load(manifest_path)?;

    state.reset_in_progress();
    state.status = TaskStatus::InProgress;
    state.save(state_file)?;

    let ignore_rules = IgnoreRules::load(&config.ignore_file).unwrap_or_default();

    let source_transport: TransportEnum = TransportEnum::Local(LocalTransport::new(config.source.clone()));
    let dest_transport = create_transport(&config.destination).await?;
    let state_arc = Arc::new(Mutex::new(state));

    let mut engine = SyncEngine::new(
        config,
        source_transport,
        dest_transport,
        state_arc.clone(),
        ignore_rules,
    )
    .await?;

    info!("Starting resume sync...");
    let success = engine.run(&manifest).await?;

    if success {
        println!("\n✅ 同步恢复成功!");
    } else {
        println!("\n❌ 同步恢复失败");
    }

    Ok(())
}

async fn cmd_sync(
    source: Option<&Path>,
    destination: Option<&str>,
    mode: Option<&str>,
    config_path: Option<&Path>,
    manifest_path: &Path,
    state_file: &Path,
) -> SyncResult<()> {
    info!("Starting sync...");

    let mut config = if let Some(config_path) = config_path {
        Config::load(config_path)?
    } else {
        Config::default()
    };

    if let Some(source) = source {
        config.source = source.to_path_buf();
    }
    if let Some(destination) = destination {
        config.destination = parse_destination(destination)?;
    }
    if let Some(mode) = mode {
        config.mode = mode.parse().map_err(|e| crate::SyncError::Other(e))?;
    }
    config.workers = get_worker_count();
    config.state_file = state_file.to_path_buf();
    config.manifest_file = manifest_path.to_path_buf();

    if config.source.as_os_str().is_empty() {
        return Err(crate::SyncError::InvalidPath(
            "Source path is required".to_string(),
        ));
    }

    let ignore_rules = IgnoreRules::load(&config.ignore_file).unwrap_or_default();

    info!("Generating manifest...");
    let manifest = Manifest::generate(&config, &ignore_rules)?;
    manifest.save(manifest_path)?;
    info!(
        "Manifest: {} files, {} chunks, {} bytes",
        manifest.files.len(),
        manifest.total_chunks,
        manifest.total_size
    );

    let mut state = SyncState::new();
    state.init_from_manifest(&manifest);
    state.save(state_file)?;

    let source_transport: TransportEnum = TransportEnum::Local(LocalTransport::new(config.source.clone()));
    let dest_transport = create_transport(&config.destination).await?;
    let state_arc = Arc::new(Mutex::new(state));

    let mut engine = SyncEngine::new(
        config,
        source_transport,
        dest_transport,
        state_arc.clone(),
        ignore_rules,
    )
    .await?;

    info!("Starting sync...");
    let success = engine.run(&manifest).await?;

    let final_state = state_arc.lock().await;
    if success {
        println!("\n✅ 同步完成!");
        println!(
            "  已传输: {} 文件 / {} 字节",
            final_state.completed_files, final_state.transferred_bytes
        );
    } else {
        println!("\n❌ 同步失败");
        if let Some(err) = &final_state.error {
            println!("  错误: {}", err);
        }
    }

    Ok(())
}

async fn cmd_server(path: &Path, port: u16) -> SyncResult<()> {
    info!("Starting HTTP sync server on port {}", port);
    println!("🚀 启动 HTTP 同步服务器");
    println!("  监听端口: {}", port);
    println!("  根目录: {:?}", path);
    println!("  按 Ctrl+C 停止");

    start_http_server(path.to_path_buf(), port).await?;
    Ok(())
}

fn parse_destination(dest: &str) -> SyncResult<Destination> {
    if dest.starts_with("sftp://") {
        let rest = dest.strip_prefix("sftp://").unwrap();
        let parts: Vec<&str> = rest.splitn(2, '@').collect();
        if parts.len() != 2 {
            return Err(crate::SyncError::InvalidPath(format!(
                "Invalid SFTP URL: {}",
                dest
            )));
        }
        let user = parts[0];
        let rest = parts[1];
        let parts: Vec<&str> = rest.splitn(2, ':').collect();
        if parts.len() != 2 {
            return Err(crate::SyncError::InvalidPath(format!(
                "Invalid SFTP URL: {}",
                dest
            )));
        }
        let host = parts[0];
        let parts: Vec<&str> = parts[1].splitn(2, '/').collect();
        let port: u16 = parts[0].parse().unwrap_or(22);
        let path = if parts.len() > 1 {
            PathBuf::from("/".to_string() + parts[1])
        } else {
            PathBuf::from("/")
        };

        Ok(Destination::Sftp {
            host: host.to_string(),
            port,
            user: user.to_string(),
            path,
        })
    } else if dest.starts_with("http://") || dest.starts_with("https://") {
        Ok(Destination::Http {
            url: dest.to_string(),
        })
    } else {
        Ok(Destination::Local {
            path: PathBuf::from(dest),
        })
    }
}
