use crate::chunk::Chunk;
use crate::config::Destination;
use crate::SyncResult;
use async_trait::async_trait;
use std::path::{Path, PathBuf};
pub use local::LocalTransport;
pub use http::HttpTransport;
pub use sftp::SftpTransport;

pub mod local;
pub mod http;
pub mod sftp;

#[derive(Clone)]
pub enum TransportEnum {
    Local(LocalTransport),
    Http(HttpTransport),
    Sftp(SftpTransport),
}

#[async_trait]
pub trait Transport: Send + Sync {
    async fn connect(&mut self) -> SyncResult<()>;
    async fn disconnect(&mut self) -> SyncResult<()>;
    async fn read_chunk(&self, path: &Path, chunk: &Chunk) -> SyncResult<Vec<u8>>;
    async fn write_chunk(&self, path: &Path, chunk: &Chunk, data: &[u8]) -> SyncResult<()>;
    async fn create_dir_all(&self, path: &Path) -> SyncResult<()>;
    async fn file_exists(&self, path: &Path) -> SyncResult<bool>;
    async fn get_file_size(&self, path: &Path) -> SyncResult<u64>;
    async fn remove_file(&self, path: &Path) -> SyncResult<()>;
    async fn remove_dir(&self, path: &Path) -> SyncResult<()>;
    async fn list_files(&self, path: &Path) -> SyncResult<Vec<PathBuf>>;
    async fn read_full_file(&self, path: &Path) -> SyncResult<Vec<u8>>;
    async fn write_full_file(&self, path: &Path, data: &[u8]) -> SyncResult<()>;
}

#[async_trait]
impl Transport for TransportEnum {
    async fn connect(&mut self) -> SyncResult<()> {
        match self {
            TransportEnum::Local(t) => t.connect().await,
            TransportEnum::Http(t) => t.connect().await,
            TransportEnum::Sftp(t) => t.connect().await,
        }
    }

    async fn disconnect(&mut self) -> SyncResult<()> {
        match self {
            TransportEnum::Local(t) => t.disconnect().await,
            TransportEnum::Http(t) => t.disconnect().await,
            TransportEnum::Sftp(t) => t.disconnect().await,
        }
    }

    async fn read_chunk(&self, path: &Path, chunk: &Chunk) -> SyncResult<Vec<u8>> {
        match self {
            TransportEnum::Local(t) => t.read_chunk(path, chunk).await,
            TransportEnum::Http(t) => t.read_chunk(path, chunk).await,
            TransportEnum::Sftp(t) => t.read_chunk(path, chunk).await,
        }
    }

    async fn write_chunk(&self, path: &Path, chunk: &Chunk, data: &[u8]) -> SyncResult<()> {
        match self {
            TransportEnum::Local(t) => t.write_chunk(path, chunk, data).await,
            TransportEnum::Http(t) => t.write_chunk(path, chunk, data).await,
            TransportEnum::Sftp(t) => t.write_chunk(path, chunk, data).await,
        }
    }

    async fn create_dir_all(&self, path: &Path) -> SyncResult<()> {
        match self {
            TransportEnum::Local(t) => t.create_dir_all(path).await,
            TransportEnum::Http(t) => t.create_dir_all(path).await,
            TransportEnum::Sftp(t) => t.create_dir_all(path).await,
        }
    }

    async fn file_exists(&self, path: &Path) -> SyncResult<bool> {
        match self {
            TransportEnum::Local(t) => t.file_exists(path).await,
            TransportEnum::Http(t) => t.file_exists(path).await,
            TransportEnum::Sftp(t) => t.file_exists(path).await,
        }
    }

    async fn get_file_size(&self, path: &Path) -> SyncResult<u64> {
        match self {
            TransportEnum::Local(t) => t.get_file_size(path).await,
            TransportEnum::Http(t) => t.get_file_size(path).await,
            TransportEnum::Sftp(t) => t.get_file_size(path).await,
        }
    }

    async fn remove_file(&self, path: &Path) -> SyncResult<()> {
        match self {
            TransportEnum::Local(t) => t.remove_file(path).await,
            TransportEnum::Http(t) => t.remove_file(path).await,
            TransportEnum::Sftp(t) => t.remove_file(path).await,
        }
    }

    async fn remove_dir(&self, path: &Path) -> SyncResult<()> {
        match self {
            TransportEnum::Local(t) => t.remove_dir(path).await,
            TransportEnum::Http(t) => t.remove_dir(path).await,
            TransportEnum::Sftp(t) => t.remove_dir(path).await,
        }
    }

    async fn list_files(&self, path: &Path) -> SyncResult<Vec<PathBuf>> {
        match self {
            TransportEnum::Local(t) => t.list_files(path).await,
            TransportEnum::Http(t) => t.list_files(path).await,
            TransportEnum::Sftp(t) => t.list_files(path).await,
        }
    }

    async fn read_full_file(&self, path: &Path) -> SyncResult<Vec<u8>> {
        match self {
            TransportEnum::Local(t) => t.read_full_file(path).await,
            TransportEnum::Http(t) => t.read_full_file(path).await,
            TransportEnum::Sftp(t) => t.read_full_file(path).await,
        }
    }

    async fn write_full_file(&self, path: &Path, data: &[u8]) -> SyncResult<()> {
        match self {
            TransportEnum::Local(t) => t.write_full_file(path, data).await,
            TransportEnum::Http(t) => t.write_full_file(path, data).await,
            TransportEnum::Sftp(t) => t.write_full_file(path, data).await,
        }
    }
}

pub async fn create_transport(dest: &Destination) -> SyncResult<TransportEnum> {
    match dest {
        Destination::Local { path } => {
            let t = local::LocalTransport::new(path.clone());
            Ok(TransportEnum::Local(t))
        }
        Destination::Sftp { host, port, user, path } => {
            let t = sftp::SftpTransport::new(host.clone(), *port, user.clone(), path.clone());
            Ok(TransportEnum::Sftp(t))
        }
        Destination::Http { url } => {
            let t = http::HttpTransport::new(url.clone());
            Ok(TransportEnum::Http(t))
        }
    }
}
