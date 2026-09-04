use crate::SegmentId;
use ketebe_core::{CollectionId, SequenceNumber};
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

const MAGIC: [u8; 4] = *b"KTCP";
const VERSION: u8 = 1;
const FILE_NAME: &str = "checkpoint.ktcp";
const TEMP_FILE_NAME: &str = "checkpoint.ktcp.tmp";
const HEADER_LEN: usize = 20;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Checkpoint {
    collection_id: CollectionId,
    segments: Vec<SegmentId>,
    sequence_number: SequenceNumber,
}

impl Checkpoint {
    #[must_use]
    pub fn new(
        collection_id: CollectionId,
        mut segments: Vec<SegmentId>,
        sequence_number: SequenceNumber,
    ) -> Self {
        segments.sort_unstable();
        segments.dedup();
        Self {
            collection_id,
            segments,
            sequence_number,
        }
    }

    #[must_use]
    pub fn collection_id(&self) -> &CollectionId {
        &self.collection_id
    }

    #[must_use]
    pub fn segments(&self) -> &[SegmentId] {
        &self.segments
    }

    #[must_use]
    pub const fn sequence_number(&self) -> SequenceNumber {
        self.sequence_number
    }
}

#[derive(Debug, Clone)]
pub struct CheckpointStore {
    directory: PathBuf,
}

impl CheckpointStore {
    pub fn open(directory: impl AsRef<Path>) -> Result<Self, CheckpointError> {
        fs::create_dir_all(directory.as_ref())?;
        Ok(Self {
            directory: directory.as_ref().to_path_buf(),
        })
    }

    pub fn load(&self) -> Result<Option<Checkpoint>, CheckpointError> {
        let path = self.directory.join(FILE_NAME);
        if !path.exists() {
            return Ok(None);
        }
        let mut bytes = Vec::new();
        File::open(path)?.read_to_end(&mut bytes)?;
        decode_checkpoint(&bytes).map(Some)
    }

    pub fn publish(&self, checkpoint: &Checkpoint) -> Result<(), CheckpointError> {
        let final_path = self.directory.join(FILE_NAME);
        let temp_path = self.directory.join(TEMP_FILE_NAME);
        if temp_path.exists() {
            fs::remove_file(&temp_path)?;
        }

        let bytes = encode_checkpoint(checkpoint)?;
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temp_path)?;
        file.write_all(&bytes)?;
        file.flush()?;
        file.sync_data()?;
        drop(file);

        fs::rename(&temp_path, &final_path)?;
        sync_directory(&self.directory)?;
        Ok(())
    }
}

#[derive(Debug)]
pub enum CheckpointError {
    Io(std::io::Error),
    InvalidMagic,
    UnsupportedVersion(u8),
    ChecksumMismatch,
    Corrupt(&'static str),
    Domain(String),
}

impl fmt::Display for CheckpointError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(f, "checkpoint I/O error: {error}"),
            Self::InvalidMagic => f.write_str("invalid checkpoint magic"),
            Self::UnsupportedVersion(version) => {
                write!(f, "unsupported checkpoint version: {version}")
            }
            Self::ChecksumMismatch => f.write_str("checkpoint checksum mismatch"),
            Self::Corrupt(message) => write!(f, "corrupt checkpoint: {message}"),
            Self::Domain(message) => write!(f, "invalid checkpoint domain value: {message}"),
        }
    }
}

impl std::error::Error for CheckpointError {}

impl From<std::io::Error> for CheckpointError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

fn encode_checkpoint(checkpoint: &Checkpoint) -> Result<Vec<u8>, CheckpointError> {
    let collection = checkpoint.collection_id.as_str().as_bytes();
    let collection_len = u32::try_from(collection.len())
        .map_err(|_| CheckpointError::Corrupt("collection ID too long"))?;
    let segment_count = u32::try_from(checkpoint.segments.len())
        .map_err(|_| CheckpointError::Corrupt("too many segment IDs"))?;

    let mut payload = Vec::new();
    payload.extend_from_slice(&collection_len.to_le_bytes());
    payload.extend_from_slice(collection);
    payload.extend_from_slice(&checkpoint.sequence_number.get().to_le_bytes());
    payload.extend_from_slice(&segment_count.to_le_bytes());
    for segment in &checkpoint.segments {
        payload.extend_from_slice(&segment.get().to_le_bytes());
    }

    let payload_len = u64::try_from(payload.len())
        .map_err(|_| CheckpointError::Corrupt("checkpoint payload too large"))?;
    let mut output = Vec::with_capacity(HEADER_LEN + payload.len());
    output.extend_from_slice(&MAGIC);
    output.push(VERSION);
    output.extend_from_slice(&[0, 0, 0]);
    output.extend_from_slice(&payload_len.to_le_bytes());
    output.extend_from_slice(&crc32(&payload).to_le_bytes());
    output.extend_from_slice(&payload);
    Ok(output)
}

fn decode_checkpoint(bytes: &[u8]) -> Result<Checkpoint, CheckpointError> {
    if bytes.len() < HEADER_LEN {
        return Err(CheckpointError::Corrupt("truncated header"));
    }
    if bytes[0..4] != MAGIC {
        return Err(CheckpointError::InvalidMagic);
    }
    if bytes[4] != VERSION {
        return Err(CheckpointError::UnsupportedVersion(bytes[4]));
    }

    let payload_len = u64::from_le_bytes(bytes[8..16].try_into().expect("fixed slice"));
    let payload_len = usize::try_from(payload_len)
        .map_err(|_| CheckpointError::Corrupt("payload length overflow"))?;
    if bytes.len() != HEADER_LEN + payload_len {
        return Err(CheckpointError::Corrupt("payload length mismatch"));
    }
    let checksum = u32::from_le_bytes(bytes[16..20].try_into().expect("fixed slice"));
    let payload = &bytes[HEADER_LEN..];
    if crc32(payload) != checksum {
        return Err(CheckpointError::ChecksumMismatch);
    }

    let mut cursor = Cursor::new(payload);
    let collection_len = cursor.read_u32()? as usize;
    let collection_bytes = cursor.read_exact(collection_len)?;
    let collection = std::str::from_utf8(collection_bytes)
        .map_err(|_| CheckpointError::Corrupt("collection ID is not UTF-8"))?;
    let collection_id = CollectionId::new(collection.to_string())
        .map_err(|error| CheckpointError::Domain(error.to_string()))?;
    let sequence_number = SequenceNumber::new(cursor.read_u64()?);
    let segment_count = cursor.read_u32()? as usize;
    let mut segments = Vec::with_capacity(segment_count);
    for _ in 0..segment_count {
        segments.push(SegmentId::new(cursor.read_u64()?));
    }
    if !cursor.is_empty() {
        return Err(CheckpointError::Corrupt("trailing payload bytes"));
    }
    Ok(Checkpoint::new(collection_id, segments, sequence_number))
}

fn sync_directory(path: &Path) -> Result<(), CheckpointError> {
    #[cfg(unix)]
    File::open(path)?.sync_all()?;
    Ok(())
}

struct Cursor<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Cursor<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn read_exact(&mut self, len: usize) -> Result<&'a [u8], CheckpointError> {
        let end = self
            .offset
            .checked_add(len)
            .ok_or(CheckpointError::Corrupt("cursor overflow"))?;
        if end > self.bytes.len() {
            return Err(CheckpointError::Corrupt("truncated payload"));
        }
        let value = &self.bytes[self.offset..end];
        self.offset = end;
        Ok(value)
    }

    fn read_u32(&mut self) -> Result<u32, CheckpointError> {
        Ok(u32::from_le_bytes(
            self.read_exact(4)?.try_into().expect("fixed slice"),
        ))
    }

    fn read_u64(&mut self) -> Result<u64, CheckpointError> {
        Ok(u64::from_le_bytes(
            self.read_exact(8)?.try_into().expect("fixed slice"),
        ))
    }

    fn is_empty(&self) -> bool {
        self.offset == self.bytes.len()
    }
}

fn crc32(bytes: &[u8]) -> u32 {
    let mut crc = !0u32;
    for byte in bytes {
        crc ^= u32::from(*byte);
        for _ in 0..8 {
            let mask = 0u32.wrapping_sub(crc & 1);
            crc = (crc >> 1) ^ (0xedb8_8320 & mask);
        }
    }
    !crc
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_dir() -> PathBuf {
        std::env::temp_dir().join(format!(
            "ketebe-checkpoint-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("time")
                .as_nanos()
        ))
    }

    #[test]
    fn checkpoint_round_trips_and_ignores_stale_temp_file() {
        let dir = temp_dir();
        let store = CheckpointStore::open(&dir).expect("store");
        let checkpoint = Checkpoint::new(
            CollectionId::new("docs").expect("collection"),
            vec![SegmentId::new(2), SegmentId::new(1), SegmentId::new(2)],
            SequenceNumber::new(12),
        );
        store.publish(&checkpoint).expect("publish");
        fs::write(dir.join(TEMP_FILE_NAME), b"stale").expect("temp");
        assert_eq!(store.load().expect("load"), Some(checkpoint));
        fs::remove_dir_all(dir).expect("cleanup");
    }
}
