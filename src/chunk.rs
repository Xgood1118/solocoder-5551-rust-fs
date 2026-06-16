use crate::SyncResult;
use crate::util::crc32_bytes;
use serde::{Deserialize, Serialize};
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;

pub const CHUNK_SIZE: u64 = 4 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ChunkState {
    Pending,
    InProgress,
    Done,
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Chunk {
    pub id: u64,
    pub offset: u64,
    pub size: u32,
    pub crc32: u32,
    pub state: ChunkState,
    pub retry_count: u32,
}

impl Chunk {
    pub fn new(id: u64, offset: u64, size: u32) -> Self {
        Self {
            id,
            offset,
            size,
            crc32: 0,
            state: ChunkState::Pending,
            retry_count: 0,
        }
    }

    pub fn compute_crc(&mut self, data: &[u8]) {
        self.crc32 = crc32_bytes(data);
    }

    pub fn verify_crc(&self, data: &[u8]) -> bool {
        crc32_bytes(data) == self.crc32
    }
}

pub fn chunk_file(path: &Path) -> SyncResult<Vec<Chunk>> {
    let file_size = std::fs::metadata(path)?.len();
    let mut chunks = Vec::new();
    let mut id = 0u64;
    let mut offset = 0u64;

    while offset < file_size {
        let remaining = file_size - offset;
        let size = if remaining > CHUNK_SIZE {
            CHUNK_SIZE as u32
        } else {
            remaining as u32
        };
        chunks.push(Chunk::new(id, offset, size));
        id += 1;
        offset += size as u64;
    }

    let mut file = std::fs::File::open(path)?;
    for chunk in &mut chunks {
        let mut buffer = vec![0u8; chunk.size as usize];
        file.seek(SeekFrom::Start(chunk.offset))?;
        file.read_exact(&mut buffer)?;
        chunk.compute_crc(&buffer);
    }

    Ok(chunks)
}

pub fn read_chunk(path: &Path, chunk: &Chunk) -> SyncResult<Vec<u8>> {
    let mut file = std::fs::File::open(path)?;
    let mut buffer = vec![0u8; chunk.size as usize];
    file.seek(SeekFrom::Start(chunk.offset))?;
    file.read_exact(&mut buffer)?;
    Ok(buffer)
}

pub fn write_chunk(path: &Path, chunk: &Chunk, data: &[u8]) -> SyncResult<()> {
    use std::io::Write;
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .open(path)?;
    file.seek(SeekFrom::Start(chunk.offset))?;
    file.write_all(data)?;
    file.sync_all()?;
    Ok(())
}
