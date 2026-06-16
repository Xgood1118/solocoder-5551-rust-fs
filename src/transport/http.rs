use crate::chunk::Chunk;
use crate::SyncError;
use crate::SyncResult;
use crate::transport::Transport;
use async_trait::async_trait;
use bytes::Buf;
use serde::{Deserialize, Serialize};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use warp::Filter;

#[derive(Debug, Serialize, Deserialize)]
struct ChunkRequest {
    path: String,
    offset: u64,
    size: u32,
}

#[derive(Debug, Serialize, Deserialize)]
struct WriteChunkRequest {
    path: String,
    offset: u64,
    data: Vec<u8>,
}

#[derive(Clone)]
pub struct HttpTransport {
    base_url: String,
    client: reqwest::Client,
}

impl HttpTransport {
    pub fn new(base_url: String) -> Self {
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(120))
            .connect_timeout(std::time::Duration::from_secs(30))
            .build()
            .unwrap();

        Self { base_url, client }
    }

    fn build_url(&self, endpoint: &str) -> String {
        format!("{}/{}", self.base_url.trim_end_matches('/'), endpoint)
    }
}

#[async_trait]
impl Transport for HttpTransport {
    async fn connect(&mut self) -> SyncResult<()> {
        let url = self.build_url("health");
        let response = self.client.get(&url).send().await?;
        if !response.status().is_success() {
            return Err(SyncError::Other("HTTP server health check failed".to_string()));
        }
        Ok(())
    }

    async fn disconnect(&mut self) -> SyncResult<()> {
        Ok(())
    }

    async fn read_chunk(&self, path: &Path, chunk: &Chunk) -> SyncResult<Vec<u8>> {
        let url = self.build_url("chunk");
        let request = ChunkRequest {
            path: path.to_string_lossy().to_string(),
            offset: chunk.offset,
            size: chunk.size,
        };

        let response = self.client.get(&url).json(&request).send().await?;
        if !response.status().is_success() {
            return Err(SyncError::Other(format!(
                "Failed to read chunk: {}",
                response.status()
            )));
        }

        let data = response.bytes().await?;
        Ok(data.to_vec())
    }

    async fn write_chunk(&self, path: &Path, chunk: &Chunk, data: &[u8]) -> SyncResult<()> {
        let url = self.build_url("chunk");
        let request = WriteChunkRequest {
            path: path.to_string_lossy().to_string(),
            offset: chunk.offset,
            data: data.to_vec(),
        };

        let response = self.client.post(&url).json(&request).send().await?;
        if !response.status().is_success() {
            return Err(SyncError::Other(format!(
                "Failed to write chunk: {}",
                response.status()
            )));
        }

        Ok(())
    }

    async fn create_dir_all(&self, path: &Path) -> SyncResult<()> {
        let url = self.build_url("mkdir");
        let params = [("path", path.to_string_lossy().to_string())];
        let response = self.client.post(&url).form(&params).send().await?;
        if !response.status().is_success() {
            return Err(SyncError::Other(format!(
                "Failed to create directory: {}",
                response.status()
            )));
        }
        Ok(())
    }

    async fn file_exists(&self, path: &Path) -> SyncResult<bool> {
        let url = self.build_url("exists");
        let params = [("path", path.to_string_lossy().to_string())];
        let response = self.client.get(&url).query(&params).send().await?;
        if !response.status().is_success() {
            return Ok(false);
        }
        let exists: bool = response.json().await?;
        Ok(exists)
    }

    async fn get_file_size(&self, path: &Path) -> SyncResult<u64> {
        let url = self.build_url("size");
        let params = [("path", path.to_string_lossy().to_string())];
        let response = self.client.get(&url).query(&params).send().await?;
        if !response.status().is_success() {
            return Err(SyncError::Other(format!(
                "Failed to get file size: {}",
                response.status()
            )));
        }
        let size: u64 = response.json().await?;
        Ok(size)
    }

    async fn remove_file(&self, path: &Path) -> SyncResult<()> {
        let url = self.build_url("remove");
        let params = [("path", path.to_string_lossy().to_string())];
        let response = self.client.delete(&url).form(&params).send().await?;
        if !response.status().is_success() {
            return Err(SyncError::Other(format!(
                "Failed to remove file: {}",
                response.status()
            )));
        }
        Ok(())
    }

    async fn remove_dir(&self, path: &Path) -> SyncResult<()> {
        let url = self.build_url("rmdir");
        let params = [("path", path.to_string_lossy().to_string())];
        let response = self.client.delete(&url).form(&params).send().await?;
        if !response.status().is_success() {
            return Err(SyncError::Other(format!(
                "Failed to remove directory: {}",
                response.status()
            )));
        }
        Ok(())
    }

    async fn list_files(&self, path: &Path) -> SyncResult<Vec<PathBuf>> {
        let url = self.build_url("list");
        let params = [("path", path.to_string_lossy().to_string())];
        let response = self.client.get(&url).query(&params).send().await?;
        if !response.status().is_success() {
            return Err(SyncError::Other(format!(
                "Failed to list files: {}",
                response.status()
            )));
        }
        let files: Vec<String> = response.json().await?;
        Ok(files.into_iter().map(PathBuf::from).collect())
    }

    async fn read_full_file(&self, path: &Path) -> SyncResult<Vec<u8>> {
        let url = self.build_url("file");
        let params = [("path", path.to_string_lossy().to_string())];
        let response = self.client.get(&url).query(&params).send().await?;
        if !response.status().is_success() {
            return Err(SyncError::Other(format!(
                "Failed to read file: {}",
                response.status()
            )));
        }
        let data = response.bytes().await?;
        Ok(data.to_vec())
    }

    async fn write_full_file(&self, path: &Path, data: &[u8]) -> SyncResult<()> {
        let url = self.build_url("file");
        let form = reqwest::multipart::Form::new()
            .text("path", path.to_string_lossy().to_string())
            .part("data", reqwest::multipart::Part::bytes(data.to_vec()));

        let response = self.client.post(&url).multipart(form).send().await?;
        if !response.status().is_success() {
            return Err(SyncError::Other(format!(
                "Failed to write file: {}",
                response.status()
            )));
        }
        Ok(())
    }
}

pub async fn start_http_server(base_path: PathBuf, port: u16) -> SyncResult<()> {
    let base_path = std::sync::Arc::new(base_path);

    let health = warp::path("health")
        .and(warp::get())
        .map(|| warp::reply::json(&"ok"));

    let read_chunk = warp::path("chunk")
        .and(warp::get())
        .and(warp::query::<ChunkRequest>())
        .and(with_base(base_path.clone()))
        .and_then(handle_read_chunk);

    let write_chunk = warp::path("chunk")
        .and(warp::post())
        .and(warp::body::json())
        .and(with_base(base_path.clone()))
        .and_then(handle_write_chunk);

    let mkdir = warp::path("mkdir")
        .and(warp::post())
        .and(warp::body::form())
        .and(with_base(base_path.clone()))
        .and_then(handle_mkdir);

    let exists = warp::path("exists")
        .and(warp::get())
        .and(warp::query::<std::collections::HashMap<String, String>>())
        .and(with_base(base_path.clone()))
        .and_then(handle_exists);

    let size = warp::path("size")
        .and(warp::get())
        .and(warp::query::<std::collections::HashMap<String, String>>())
        .and(with_base(base_path.clone()))
        .and_then(handle_size);

    let remove = warp::path("remove")
        .and(warp::delete())
        .and(warp::body::form())
        .and(with_base(base_path.clone()))
        .and_then(handle_remove);

    let rmdir = warp::path("rmdir")
        .and(warp::delete())
        .and(warp::body::form())
        .and(with_base(base_path.clone()))
        .and_then(handle_rmdir);

    let list = warp::path("list")
        .and(warp::get())
        .and(warp::query::<std::collections::HashMap<String, String>>())
        .and(with_base(base_path.clone()))
        .and_then(handle_list);

    let read_file = warp::path("file")
        .and(warp::get())
        .and(warp::query::<std::collections::HashMap<String, String>>())
        .and(with_base(base_path.clone()))
        .and_then(handle_read_file);

    let write_file = warp::path("file")
        .and(warp::post())
        .and(warp::multipart::form().max_length(100 * 1024 * 1024))
        .and(with_base(base_path.clone()))
        .and_then(handle_write_file);

    let routes = health
        .or(read_chunk)
        .or(write_chunk)
        .or(mkdir)
        .or(exists)
        .or(size)
        .or(remove)
        .or(rmdir)
        .or(list)
        .or(read_file)
        .or(write_file);

    tracing::info!("Starting HTTP sync server on port {}", port);
    warp::serve(routes).run(([0, 0, 0, 0], port)).await;
    Ok(())
}

fn with_base(
    base: std::sync::Arc<PathBuf>,
) -> impl Filter<Extract = (std::sync::Arc<PathBuf>,), Error = std::convert::Infallible> + Clone {
    warp::any().map(move || base.clone())
}

async fn handle_read_chunk(
    req: ChunkRequest,
    base: std::sync::Arc<PathBuf>,
) -> Result<warp::reply::WithStatus<Vec<u8>>, warp::Rejection> {
    let full_path = base.join(&req.path);
    match std::fs::File::open(&full_path) {
        Ok(mut file) => {
            if let Err(e) = file.seek(SeekFrom::Start(req.offset)) {
                return Ok(warp::reply::with_status(
                    e.to_string().into_bytes(),
                    warp::http::StatusCode::INTERNAL_SERVER_ERROR,
                ));
            }
            let mut buffer = vec![0u8; req.size as usize];
            match file.read_exact(&mut buffer) {
                Ok(_) => Ok(warp::reply::with_status(
                    buffer,
                    warp::http::StatusCode::OK,
                )),
                Err(e) => Ok(warp::reply::with_status(
                    e.to_string().into_bytes(),
                    warp::http::StatusCode::INTERNAL_SERVER_ERROR,
                )),
            }
        }
        Err(e) => Ok(warp::reply::with_status(
            e.to_string().into_bytes(),
            warp::http::StatusCode::NOT_FOUND,
        )),
    }
}

async fn handle_write_chunk(
    req: WriteChunkRequest,
    base: std::sync::Arc<PathBuf>,
) -> Result<warp::reply::WithStatus<Vec<u8>>, warp::Rejection> {
    let full_path = base.join(&req.path);
    if let Some(parent) = full_path.parent() {
        if let Err(e) = std::fs::create_dir_all(parent) {
            return Ok(warp::reply::with_status(
                e.to_string().into_bytes(),
                warp::http::StatusCode::INTERNAL_SERVER_ERROR,
            ));
        }
    }

    match std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .open(&full_path)
    {
        Ok(mut file) => {
            if let Err(e) = file.seek(SeekFrom::Start(req.offset)) {
                return Ok(warp::reply::with_status(
                    e.to_string().into_bytes(),
                    warp::http::StatusCode::INTERNAL_SERVER_ERROR,
                ));
            }
            match file.write_all(&req.data) {
                Ok(_) => Ok(warp::reply::with_status(
                    b"ok".to_vec(),
                    warp::http::StatusCode::OK,
                )),
                Err(e) => Ok(warp::reply::with_status(
                    e.to_string().into_bytes(),
                    warp::http::StatusCode::INTERNAL_SERVER_ERROR,
                )),
            }
        }
        Err(e) => Ok(warp::reply::with_status(
            e.to_string().into_bytes(),
            warp::http::StatusCode::INTERNAL_SERVER_ERROR,
        )),
    }
}

async fn handle_mkdir(
    params: std::collections::HashMap<String, String>,
    base: std::sync::Arc<PathBuf>,
) -> Result<warp::reply::WithStatus<Vec<u8>>, warp::Rejection> {
    let path_str = params.get("path").cloned().unwrap_or_default();
    let full_path = base.join(path_str);
    match std::fs::create_dir_all(full_path) {
        Ok(_) => Ok(warp::reply::with_status(b"ok".to_vec(), warp::http::StatusCode::OK)),
        Err(e) => Ok(warp::reply::with_status(
            e.to_string().into_bytes(),
            warp::http::StatusCode::INTERNAL_SERVER_ERROR,
        )),
    }
}

async fn handle_exists(
    params: std::collections::HashMap<String, String>,
    base: std::sync::Arc<PathBuf>,
) -> Result<impl warp::Reply, warp::Rejection> {
    let path_str = params.get("path").cloned().unwrap_or_default();
    let full_path = base.join(path_str);
    Ok(warp::reply::json(&full_path.exists()))
}

async fn handle_size(
    params: std::collections::HashMap<String, String>,
    base: std::sync::Arc<PathBuf>,
) -> Result<impl warp::Reply, warp::Rejection> {
    let path_str = params.get("path").cloned().unwrap_or_default();
    let full_path = base.join(path_str);
    match std::fs::metadata(full_path) {
        Ok(meta) => Ok(warp::reply::json(&meta.len())),
        Err(_) => Ok(warp::reply::json(&0u64)),
    }
}

async fn handle_remove(
    params: std::collections::HashMap<String, String>,
    base: std::sync::Arc<PathBuf>,
) -> Result<warp::reply::WithStatus<Vec<u8>>, warp::Rejection> {
    let path_str = params.get("path").cloned().unwrap_or_default();
    let full_path = base.join(path_str);
    match std::fs::remove_file(full_path) {
        Ok(_) => Ok(warp::reply::with_status(b"ok".to_vec(), warp::http::StatusCode::OK)),
        Err(e) => Ok(warp::reply::with_status(
            e.to_string().into_bytes(),
            warp::http::StatusCode::INTERNAL_SERVER_ERROR,
        )),
    }
}

async fn handle_rmdir(
    params: std::collections::HashMap<String, String>,
    base: std::sync::Arc<PathBuf>,
) -> Result<warp::reply::WithStatus<Vec<u8>>, warp::Rejection> {
    let path_str = params.get("path").cloned().unwrap_or_default();
    let full_path = base.join(path_str);
    match std::fs::remove_dir_all(full_path) {
        Ok(_) => Ok(warp::reply::with_status(b"ok".to_vec(), warp::http::StatusCode::OK)),
        Err(e) => Ok(warp::reply::with_status(
            e.to_string().into_bytes(),
            warp::http::StatusCode::INTERNAL_SERVER_ERROR,
        )),
    }
}

async fn handle_list(
    params: std::collections::HashMap<String, String>,
    base: std::sync::Arc<PathBuf>,
) -> Result<impl warp::Reply, warp::Rejection> {
    let path_str = params.get("path").cloned().unwrap_or_default();
    let full_path = base.join(path_str);
    let mut files = Vec::new();

    if full_path.exists() {
        for entry in walkdir::WalkDir::new(&full_path) {
            if let Ok(entry) = entry {
                if entry.file_type().is_file() {
                    if let Ok(rel) = entry.path().strip_prefix(&full_path) {
                        files.push(rel.to_string_lossy().to_string());
                    }
                }
            }
        }
    }

    Ok(warp::reply::json(&files))
}

async fn handle_read_file(
    params: std::collections::HashMap<String, String>,
    base: std::sync::Arc<PathBuf>,
) -> Result<warp::reply::WithStatus<Vec<u8>>, warp::Rejection> {
    let path_str = params.get("path").cloned().unwrap_or_default();
    let full_path = base.join(path_str);
    match std::fs::read(full_path) {
        Ok(data) => Ok(warp::reply::with_status(
            data,
            warp::http::StatusCode::OK,
        )),
        Err(e) => Ok(warp::reply::with_status(
            e.to_string().into_bytes(),
            warp::http::StatusCode::NOT_FOUND,
        )),
    }
}

async fn handle_write_file(
    mut form: warp::multipart::FormData,
    base: std::sync::Arc<PathBuf>,
) -> Result<warp::reply::WithStatus<Vec<u8>>, warp::Rejection> {
    use futures_util::StreamExt;

    let mut path = String::new();
    let mut data: Option<Vec<u8>> = None;

    while let Some(part) = form.next().await {
        let mut part = match part {
            Ok(p) => p,
            Err(e) => {
                return Ok(warp::reply::with_status(
                    e.to_string().into_bytes(),
                    warp::http::StatusCode::BAD_REQUEST,
                ));
            }
        };

        let name = part.name().to_string();
        let mut value = Vec::new();

        while let Some(chunk) = part.data().await {
            match chunk {
                Ok(mut c) => {
                    
                    while c.has_remaining() {
                        let chunk_slice = c.chunk();
                        value.extend_from_slice(chunk_slice);
                        c.advance(chunk_slice.len());
                    }
                }
                Err(e) => {
                    return Ok(warp::reply::with_status(
                        e.to_string().into_bytes(),
                        warp::http::StatusCode::BAD_REQUEST,
                    ));
                }
            }
        }

        if name == "path" {
            path = String::from_utf8_lossy(&value).to_string();
        } else if name == "data" {
            data = Some(value);
        }
    }

    if path.is_empty() || data.is_none() {
        return Ok(warp::reply::with_status(
            b"Missing path or data".to_vec(),
            warp::http::StatusCode::BAD_REQUEST,
        ));
    }

    let full_path = base.join(path);
    if let Some(parent) = full_path.parent() {
        if let Err(e) = std::fs::create_dir_all(parent) {
            return Ok(warp::reply::with_status(
                e.to_string().into_bytes(),
                warp::http::StatusCode::INTERNAL_SERVER_ERROR,
            ));
        }
    }

    match std::fs::write(full_path, data.unwrap()) {
        Ok(_) => Ok(warp::reply::with_status(b"ok".to_vec(), warp::http::StatusCode::OK)),
        Err(e) => Ok(warp::reply::with_status(
            e.to_string().into_bytes(),
            warp::http::StatusCode::INTERNAL_SERVER_ERROR,
        )),
    }
}
