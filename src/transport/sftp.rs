use crate::chunk::Chunk;
use crate::SyncError;
use crate::SyncResult;
use crate::transport::Transport;
use async_trait::async_trait;
use std::io::{Read, Seek, SeekFrom, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

pub struct SftpTransport {
    host: String,
    port: u16,
    user: String,
    base_path: PathBuf,
    session: Option<Mutex<ssh2::Session>>,
    sftp: Option<Mutex<ssh2::Sftp>>,
}

impl Clone for SftpTransport {
    fn clone(&self) -> Self {
        Self::new(self.host.clone(), self.port, self.user.clone(), self.base_path.clone())
    }
}

impl SftpTransport {
    pub fn new(host: String, port: u16, user: String, base_path: PathBuf) -> Self {
        Self {
            host,
            port,
            user,
            base_path,
            session: None,
            sftp: None,
        }
    }

    fn full_path(&self, path: &Path) -> PathBuf {
        if path.is_absolute() {
            path.to_path_buf()
        } else {
            self.base_path.join(path)
        }
    }

    fn resolve_ipv4(&self) -> SyncResult<std::net::SocketAddr> {
        use std::net::ToSocketAddrs;
        let addr = format!("{}:{}", self.host, self.port);
        let addrs: Vec<_> = addr.to_socket_addrs()?.collect();

        for addr in &addrs {
            if addr.is_ipv4() {
                return Ok(*addr);
            }
        }

        addrs.into_iter()
            .next()
            .ok_or_else(|| SyncError::Other(format!("Could not resolve host: {}", self.host)))
    }
}

#[async_trait]
impl Transport for SftpTransport {
    async fn connect(&mut self) -> SyncResult<()> {
        let addr = self.resolve_ipv4()?;
        let tcp = TcpStream::connect_timeout(&addr, std::time::Duration::from_secs(30))?;
        tcp.set_read_timeout(Some(std::time::Duration::from_secs(60)))?;
        tcp.set_write_timeout(Some(std::time::Duration::from_secs(60)))?;

        let mut session = ssh2::Session::new()?;
        session.set_tcp_stream(tcp);
        session.handshake()?;
        session.userauth_agent(&self.user)?;

        if !session.authenticated() {
            return Err(SyncError::Other("SSH authentication failed".to_string()));
        }

        let sftp = session.sftp()?;
        self.session = Some(Mutex::new(session));
        self.sftp = Some(Mutex::new(sftp));

        Ok(())
    }

    async fn disconnect(&mut self) -> SyncResult<()> {
        if let Some(session) = self.session.take() {
            let session = session.into_inner().expect("Mutex poisoned");
            session.disconnect(None, "bye", None)?;
        }
        self.sftp = None;
        Ok(())
    }

    async fn read_chunk(&self, path: &Path, chunk: &Chunk) -> SyncResult<Vec<u8>> {
        let sftp = self.sftp.as_ref().ok_or_else(|| SyncError::Other("Not connected".to_string()))?;
        let sftp = sftp.lock().unwrap();

        let full_path = self.full_path(path);
        let mut file = sftp.open(&full_path)?;
        file.seek(SeekFrom::Start(chunk.offset))?;

        let mut buffer = vec![0u8; chunk.size as usize];
        file.read_exact(&mut buffer)?;

        Ok(buffer)
    }

    async fn write_chunk(&self, path: &Path, chunk: &Chunk, data: &[u8]) -> SyncResult<()> {
        let sftp = self.sftp.as_ref().ok_or_else(|| SyncError::Other("Not connected".to_string()))?;
        let sftp = sftp.lock().unwrap();

        let full_path = self.full_path(path);
        if let Some(parent) = full_path.parent() {
            sftp.mkdir(parent, 0o755).ok();
        }

        let mut file = sftp.create(&full_path)?;
        file.seek(SeekFrom::Start(chunk.offset))?;
        file.write_all(data)?;
        file.fsync()?;

        Ok(())
    }

    async fn create_dir_all(&self, path: &Path) -> SyncResult<()> {
        let sftp = self.sftp.as_ref().ok_or_else(|| SyncError::Other("Not connected".to_string()))?;
        let sftp = sftp.lock().unwrap();

        let full_path = self.full_path(path);
        sftp.mkdir(&full_path, 0o755).ok();
        Ok(())
    }

    async fn file_exists(&self, path: &Path) -> SyncResult<bool> {
        let sftp = self.sftp.as_ref().ok_or_else(|| SyncError::Other("Not connected".to_string()))?;
        let sftp = sftp.lock().unwrap();

        let full_path = self.full_path(path);
        Ok(sftp.stat(&full_path).is_ok())
    }

    async fn get_file_size(&self, path: &Path) -> SyncResult<u64> {
        let sftp = self.sftp.as_ref().ok_or_else(|| SyncError::Other("Not connected".to_string()))?;
        let sftp = sftp.lock().unwrap();

        let full_path = self.full_path(path);
        let stat = sftp.stat(&full_path)?;
        Ok(stat.size.unwrap_or(0))
    }

    async fn remove_file(&self, path: &Path) -> SyncResult<()> {
        let sftp = self.sftp.as_ref().ok_or_else(|| SyncError::Other("Not connected".to_string()))?;
        let sftp = sftp.lock().unwrap();

        let full_path = self.full_path(path);
        sftp.unlink(&full_path)?;
        Ok(())
    }

    async fn remove_dir(&self, path: &Path) -> SyncResult<()> {
        let sftp = self.sftp.as_ref().ok_or_else(|| SyncError::Other("Not connected".to_string()))?;
        let sftp = sftp.lock().unwrap();

        let full_path = self.full_path(path);
        sftp.rmdir(&full_path)?;
        Ok(())
    }

    async fn list_files(&self, path: &Path) -> SyncResult<Vec<PathBuf>> {
        let sftp = self.sftp.as_ref().ok_or_else(|| SyncError::Other("Not connected".to_string()))?;
        let sftp = sftp.lock().unwrap();

        let full_path = self.full_path(path);
        let mut files = Vec::new();

        if let Ok(entries) = sftp.readdir(&full_path) {
            for (entry_path, stat) in entries {
                if stat.is_file() {
                    if let Ok(rel_path) = entry_path.strip_prefix(&full_path) {
                        files.push(rel_path.to_path_buf());
                    }
                }
            }
        }

        Ok(files)
    }

    async fn read_full_file(&self, path: &Path) -> SyncResult<Vec<u8>> {
        let sftp = self.sftp.as_ref().ok_or_else(|| SyncError::Other("Not connected".to_string()))?;
        let sftp = sftp.lock().unwrap();

        let full_path = self.full_path(path);
        let mut file = sftp.open(&full_path)?;
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer)?;

        Ok(buffer)
    }

    async fn write_full_file(&self, path: &Path, data: &[u8]) -> SyncResult<()> {
        let sftp = self.sftp.as_ref().ok_or_else(|| SyncError::Other("Not connected".to_string()))?;
        let sftp = sftp.lock().unwrap();

        let full_path = self.full_path(path);
        if let Some(parent) = full_path.parent() {
            sftp.mkdir(parent, 0o755).ok();
        }

        let mut file = sftp.create(&full_path)?;
        file.write_all(data)?;
        file.fsync()?;

        Ok(())
    }
}
