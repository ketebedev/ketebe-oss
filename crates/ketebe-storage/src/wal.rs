use ketebe_core::{
    CollectionId, Metadata, MetadataValue, Record, RecordId, SequenceNumber, Vector,
};
use std::collections::BTreeMap;
use std::fmt;
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

const MAGIC: [u8; 4] = *b"KTWL";
const VERSION: u8 = 1;
const HEADER_LEN: usize = 24;
const OP_UPSERT: u8 = 1;
const OP_DELETE: u8 = 2;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SyncPolicy {
    #[default]
    Durable,
    Buffered,
}

#[derive(Debug, Clone, PartialEq)]
pub enum WalMutation {
    Upsert {
        collection_id: CollectionId,
        record: Record,
    },
    Delete {
        collection_id: CollectionId,
        record_id: RecordId,
        sequence_number: SequenceNumber,
    },
}

impl WalMutation {
    #[must_use]
    pub fn sequence_number(&self) -> SequenceNumber {
        match self {
            Self::Upsert { record, .. } => record.sequence_number(),
            Self::Delete {
                sequence_number, ..
            } => *sequence_number,
        }
    }
}

#[derive(Debug)]
pub enum WalError {
    Io(std::io::Error),
    UnsupportedVersion(u8),
    InvalidMagic,
    InvalidOperation(u8),
    ChecksumMismatch { sequence: u64 },
    CorruptFrame(&'static str),
    SequenceRegression { previous: u64, current: u64 },
    Domain(String),
}

impl fmt::Display for WalError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(f, "WAL I/O error: {error}"),
            Self::UnsupportedVersion(version) => write!(f, "unsupported WAL version: {version}"),
            Self::InvalidMagic => f.write_str("invalid WAL frame magic"),
            Self::InvalidOperation(operation) => write!(f, "invalid WAL operation: {operation}"),
            Self::ChecksumMismatch { sequence } => {
                write!(f, "WAL checksum mismatch at sequence {sequence}")
            }
            Self::CorruptFrame(message) => write!(f, "corrupt WAL frame: {message}"),
            Self::SequenceRegression { previous, current } => write!(
                f,
                "WAL sequence regression: previous={previous}, current={current}"
            ),
            Self::Domain(message) => write!(f, "invalid WAL domain value: {message}"),
        }
    }
}

impl std::error::Error for WalError {}

impl From<std::io::Error> for WalError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

pub struct Wal {
    path: PathBuf,
    file: File,
    sync_policy: SyncPolicy,
    last_sequence: Option<SequenceNumber>,
}

impl Wal {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, WalError> {
        Self::open_with_policy(path, SyncPolicy::Durable)
    }

    pub fn open_with_policy(
        path: impl AsRef<Path>,
        sync_policy: SyncPolicy,
    ) -> Result<Self, WalError> {
        let path = path.as_ref().to_path_buf();
        let replay = replay_path(&path)?;
        let last_sequence = replay.entries.last().map(WalMutation::sequence_number);
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(&path)?;
        Ok(Self {
            path,
            file,
            sync_policy,
            last_sequence,
        })
    }

    pub fn append(&mut self, mutation: &WalMutation) -> Result<(), WalError> {
        self.append_batch(std::slice::from_ref(mutation))
    }

    pub fn append_batch(&mut self, mutations: &[WalMutation]) -> Result<(), WalError> {
        if mutations.is_empty() {
            return Ok(());
        }

        let mut previous = self.last_sequence;
        let mut encoded = Vec::new();
        for mutation in mutations {
            let current = mutation.sequence_number();
            if let Some(previous_sequence) = previous
                && current <= previous_sequence
            {
                return Err(WalError::SequenceRegression {
                    previous: previous_sequence.get(),
                    current: current.get(),
                });
            }
            encoded.extend_from_slice(&encode_frame(mutation)?);
            previous = Some(current);
        }

        self.file.write_all(&encoded)?;
        self.file.flush()?;
        if self.sync_policy == SyncPolicy::Durable {
            self.file.sync_data()?;
        }
        self.last_sequence = previous;
        Ok(())
    }

    pub fn replay(&self) -> Result<ReplayResult, WalError> {
        replay_path(&self.path)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ReplayResult {
    pub entries: Vec<WalMutation>,
    pub ignored_tail_bytes: usize,
}

pub fn replay_wal_path(path: impl AsRef<Path>) -> Result<ReplayResult, WalError> {
    replay_path(path.as_ref())
}

fn replay_path(path: &Path) -> Result<ReplayResult, WalError> {
    if !path.exists() {
        return Ok(ReplayResult {
            entries: Vec::new(),
            ignored_tail_bytes: 0,
        });
    }

    let mut bytes = Vec::new();
    File::open(path)?.read_to_end(&mut bytes)?;
    decode_log(&bytes)
}

fn decode_log(bytes: &[u8]) -> Result<ReplayResult, WalError> {
    let mut offset = 0usize;
    let mut entries = Vec::new();
    let mut previous = None;

    while offset < bytes.len() {
        let remaining = bytes.len() - offset;
        if remaining < HEADER_LEN {
            return Ok(ReplayResult {
                entries,
                ignored_tail_bytes: remaining,
            });
        }

        let header = &bytes[offset..offset + HEADER_LEN];
        validate_header(header)?;
        let operation = header[5];
        let payload_len =
            u32::from_le_bytes(header[8..12].try_into().expect("fixed slice")) as usize;
        let sequence = u64::from_le_bytes(header[12..20].try_into().expect("fixed slice"));
        let checksum = u32::from_le_bytes(header[20..24].try_into().expect("fixed slice"));
        let frame_len = HEADER_LEN
            .checked_add(payload_len)
            .ok_or(WalError::CorruptFrame("payload length overflow"))?;

        if remaining < frame_len {
            return Ok(ReplayResult {
                entries,
                ignored_tail_bytes: remaining,
            });
        }
        if let Some(previous_sequence) = previous
            && sequence <= previous_sequence
        {
            return Err(WalError::SequenceRegression {
                previous: previous_sequence,
                current: sequence,
            });
        }

        let payload = &bytes[offset + HEADER_LEN..offset + frame_len];
        if crc32(payload) != checksum {
            return Err(WalError::ChecksumMismatch { sequence });
        }
        entries.push(decode_payload(operation, sequence, payload)?);
        previous = Some(sequence);
        offset += frame_len;
    }

    Ok(ReplayResult {
        entries,
        ignored_tail_bytes: 0,
    })
}

fn validate_header(header: &[u8]) -> Result<(), WalError> {
    if header[0..4] != MAGIC {
        return Err(WalError::InvalidMagic);
    }
    if header[4] != VERSION {
        return Err(WalError::UnsupportedVersion(header[4]));
    }
    Ok(())
}

fn encode_frame(mutation: &WalMutation) -> Result<Vec<u8>, WalError> {
    let (operation, payload) = encode_payload(mutation)?;
    let payload_len =
        u32::try_from(payload.len()).map_err(|_| WalError::CorruptFrame("payload too large"))?;
    let mut frame = Vec::with_capacity(HEADER_LEN + payload.len());
    frame.extend_from_slice(&MAGIC);
    frame.push(VERSION);
    frame.push(operation);
    frame.extend_from_slice(&0u16.to_le_bytes());
    frame.extend_from_slice(&payload_len.to_le_bytes());
    frame.extend_from_slice(&mutation.sequence_number().get().to_le_bytes());
    frame.extend_from_slice(&crc32(&payload).to_le_bytes());
    frame.extend_from_slice(&payload);
    Ok(frame)
}

fn encode_payload(mutation: &WalMutation) -> Result<(u8, Vec<u8>), WalError> {
    let mut output = Vec::new();
    match mutation {
        WalMutation::Upsert {
            collection_id,
            record,
        } => {
            write_string(&mut output, collection_id.as_str())?;
            write_record_id(&mut output, record.id())?;
            write_vector(&mut output, record.vector())?;
            write_metadata(&mut output, record.metadata())?;
            Ok((OP_UPSERT, output))
        }
        WalMutation::Delete {
            collection_id,
            record_id,
            ..
        } => {
            write_string(&mut output, collection_id.as_str())?;
            write_record_id(&mut output, record_id)?;
            Ok((OP_DELETE, output))
        }
    }
}

fn decode_payload(operation: u8, sequence: u64, payload: &[u8]) -> Result<WalMutation, WalError> {
    let mut cursor = Cursor::new(payload);
    let collection_id = CollectionId::new(cursor.read_string()?)
        .map_err(|error| WalError::Domain(error.to_string()))?;
    let record_id = cursor.read_record_id()?;
    let sequence_number = SequenceNumber::new(sequence);

    let mutation = match operation {
        OP_UPSERT => WalMutation::Upsert {
            collection_id,
            record: Record::new(
                record_id,
                cursor.read_vector()?,
                cursor.read_metadata()?,
                sequence_number,
            ),
        },
        OP_DELETE => WalMutation::Delete {
            collection_id,
            record_id,
            sequence_number,
        },
        other => return Err(WalError::InvalidOperation(other)),
    };

    if !cursor.is_finished() {
        return Err(WalError::CorruptFrame("trailing payload bytes"));
    }
    Ok(mutation)
}

fn write_string(output: &mut Vec<u8>, value: &str) -> Result<(), WalError> {
    let len = u32::try_from(value.len()).map_err(|_| WalError::CorruptFrame("string too large"))?;
    output.extend_from_slice(&len.to_le_bytes());
    output.extend_from_slice(value.as_bytes());
    Ok(())
}

fn write_record_id(output: &mut Vec<u8>, id: &RecordId) -> Result<(), WalError> {
    match id {
        RecordId::String(value) => {
            output.push(0);
            write_string(output, value)
        }
        RecordId::Unsigned(value) => {
            output.push(1);
            output.extend_from_slice(&value.to_le_bytes());
            Ok(())
        }
    }
}

fn write_vector(output: &mut Vec<u8>, vector: &Vector) -> Result<(), WalError> {
    let len =
        u32::try_from(vector.len()).map_err(|_| WalError::CorruptFrame("vector too large"))?;
    output.extend_from_slice(&len.to_le_bytes());
    for value in vector.as_slice() {
        output.extend_from_slice(&value.to_bits().to_le_bytes());
    }
    Ok(())
}

fn write_metadata(output: &mut Vec<u8>, metadata: &Metadata) -> Result<(), WalError> {
    let len =
        u32::try_from(metadata.len()).map_err(|_| WalError::CorruptFrame("metadata too large"))?;
    output.extend_from_slice(&len.to_le_bytes());
    for (key, value) in metadata {
        write_string(output, key)?;
        write_metadata_value(output, value)?;
    }
    Ok(())
}

fn write_metadata_value(output: &mut Vec<u8>, value: &MetadataValue) -> Result<(), WalError> {
    match value {
        MetadataValue::Null => output.push(0),
        MetadataValue::Bool(value) => {
            output.push(1);
            output.push(u8::from(*value));
        }
        MetadataValue::Number(value) => {
            output.push(2);
            output.extend_from_slice(&value.to_bits().to_le_bytes());
        }
        MetadataValue::String(value) => {
            output.push(3);
            write_string(output, value)?;
        }
        MetadataValue::Array(values) => {
            output.push(4);
            let len = u32::try_from(values.len())
                .map_err(|_| WalError::CorruptFrame("metadata array too large"))?;
            output.extend_from_slice(&len.to_le_bytes());
            for value in values {
                write_metadata_value(output, value)?;
            }
        }
        MetadataValue::Object(values) => {
            output.push(5);
            write_metadata(output, values)?;
        }
    }
    Ok(())
}

struct Cursor<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Cursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn is_finished(&self) -> bool {
        self.offset == self.bytes.len()
    }

    fn take(&mut self, len: usize) -> Result<&'a [u8], WalError> {
        let end = self
            .offset
            .checked_add(len)
            .ok_or(WalError::CorruptFrame("cursor overflow"))?;
        if end > self.bytes.len() {
            return Err(WalError::CorruptFrame("unexpected payload end"));
        }
        let slice = &self.bytes[self.offset..end];
        self.offset = end;
        Ok(slice)
    }

    fn read_u8(&mut self) -> Result<u8, WalError> {
        Ok(self.take(1)?[0])
    }

    fn read_u32(&mut self) -> Result<u32, WalError> {
        Ok(u32::from_le_bytes(
            self.take(4)?.try_into().expect("fixed slice"),
        ))
    }

    fn read_u64(&mut self) -> Result<u64, WalError> {
        Ok(u64::from_le_bytes(
            self.take(8)?.try_into().expect("fixed slice"),
        ))
    }

    fn read_string(&mut self) -> Result<String, WalError> {
        let len = self.read_u32()? as usize;
        String::from_utf8(self.take(len)?.to_vec())
            .map_err(|_| WalError::CorruptFrame("invalid UTF-8"))
    }

    fn read_record_id(&mut self) -> Result<RecordId, WalError> {
        match self.read_u8()? {
            0 => RecordId::string(self.read_string()?)
                .map_err(|error| WalError::Domain(error.to_string())),
            1 => Ok(RecordId::unsigned(self.read_u64()?)),
            _ => Err(WalError::CorruptFrame("invalid record id tag")),
        }
    }

    fn read_vector(&mut self) -> Result<Vector, WalError> {
        let len = self.read_u32()? as usize;
        let mut values = Vec::with_capacity(len);
        for _ in 0..len {
            let bits = u32::from_le_bytes(self.take(4)?.try_into().expect("fixed slice"));
            values.push(f32::from_bits(bits));
        }
        Vector::new(values).map_err(|error| WalError::Domain(error.to_string()))
    }

    fn read_metadata(&mut self) -> Result<Metadata, WalError> {
        let len = self.read_u32()? as usize;
        let mut metadata = BTreeMap::new();
        for _ in 0..len {
            metadata.insert(self.read_string()?, self.read_metadata_value()?);
        }
        Ok(metadata)
    }

    fn read_metadata_value(&mut self) -> Result<MetadataValue, WalError> {
        match self.read_u8()? {
            0 => Ok(MetadataValue::Null),
            1 => match self.read_u8()? {
                0 => Ok(MetadataValue::Bool(false)),
                1 => Ok(MetadataValue::Bool(true)),
                _ => Err(WalError::CorruptFrame("invalid boolean")),
            },
            2 => Ok(MetadataValue::Number(f64::from_bits(self.read_u64()?))),
            3 => Ok(MetadataValue::String(self.read_string()?)),
            4 => {
                let len = self.read_u32()? as usize;
                let mut values = Vec::with_capacity(len);
                for _ in 0..len {
                    values.push(self.read_metadata_value()?);
                }
                Ok(MetadataValue::Array(values))
            }
            5 => Ok(MetadataValue::Object(self.read_metadata()?)),
            _ => Err(WalError::CorruptFrame("invalid metadata tag")),
        }
    }
}

fn crc32(bytes: &[u8]) -> u32 {
    let mut crc = 0xffff_ffffu32;
    for byte in bytes {
        crc ^= u32::from(*byte);
        for _ in 0..8 {
            let mask = (crc & 1).wrapping_neg();
            crc = (crc >> 1) ^ (0xedb8_8320 & mask);
        }
    }
    !crc
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_path(name: &str) -> PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time")
            .as_nanos();
        std::env::temp_dir().join(format!("ketebe-{name}-{}-{nonce}.wal", std::process::id()))
    }

    fn collection() -> CollectionId {
        CollectionId::new("test").expect("collection")
    }

    fn upsert(sequence: u64, id: RecordId) -> WalMutation {
        let mut metadata = Metadata::new();
        metadata.insert("source".into(), MetadataValue::String("test".into()));
        WalMutation::Upsert {
            collection_id: collection(),
            record: Record::new(
                id,
                Vector::new(vec![1.0, 2.0, 3.0]).expect("vector"),
                metadata,
                SequenceNumber::new(sequence),
            ),
        }
    }

    #[test]
    fn append_reopen_and_replay_round_trip() {
        let path = temp_path("roundtrip");
        {
            let mut wal = Wal::open(&path).expect("open");
            wal.append(&upsert(1, RecordId::string("r1").expect("id")))
                .expect("append");
            wal.append(&WalMutation::Delete {
                collection_id: collection(),
                record_id: RecordId::unsigned(42),
                sequence_number: SequenceNumber::new(2),
            })
            .expect("append delete");
        }
        let replay = Wal::open(&path).expect("reopen").replay().expect("replay");
        assert_eq!(replay.entries.len(), 2);
        assert_eq!(replay.ignored_tail_bytes, 0);
        let _ = fs::remove_file(path);
    }

    #[test]
    fn append_batch_round_trips_in_order() {
        let path = temp_path("batch-roundtrip");
        let batch = vec![
            upsert(1, RecordId::unsigned(1)),
            upsert(2, RecordId::unsigned(2)),
            WalMutation::Delete {
                collection_id: collection(),
                record_id: RecordId::unsigned(1),
                sequence_number: SequenceNumber::new(3),
            },
        ];
        let mut wal = Wal::open(&path).expect("open");
        wal.append_batch(&batch).expect("append batch");
        drop(wal);
        let replay = Wal::open(&path).expect("reopen").replay().expect("replay");
        assert_eq!(replay.entries, batch);
        assert_eq!(replay.ignored_tail_bytes, 0);
        let _ = fs::remove_file(path);
    }

    #[test]
    fn invalid_batch_sequence_is_rejected_before_any_write() {
        let path = temp_path("batch-sequence");
        let mut wal = Wal::open(&path).expect("open");
        wal.append(&upsert(1, RecordId::unsigned(1))).expect("seed");
        let before = fs::read(&path).expect("before");
        let batch = vec![
            upsert(2, RecordId::unsigned(2)),
            upsert(2, RecordId::unsigned(3)),
        ];
        assert!(matches!(
            wal.append_batch(&batch),
            Err(WalError::SequenceRegression {
                previous: 2,
                current: 2
            })
        ));
        assert_eq!(fs::read(&path).expect("after"), before);
        let _ = fs::remove_file(path);
    }

    #[test]
    fn empty_batch_is_a_noop() {
        let path = temp_path("empty-batch");
        let mut wal = Wal::open(&path).expect("open");
        wal.append_batch(&[]).expect("empty batch");
        assert!(fs::read(&path).expect("wal").is_empty());
        let _ = fs::remove_file(path);
    }

    #[test]
    fn partial_final_frame_is_ignored() {
        let first = encode_frame(&upsert(1, RecordId::unsigned(1))).expect("frame");
        let second = encode_frame(&upsert(2, RecordId::unsigned(2))).expect("frame");
        let mut bytes = first;
        bytes.extend_from_slice(&second[..second.len() / 2]);
        let replay = decode_log(&bytes).expect("replay");
        assert_eq!(replay.entries.len(), 1);
        assert!(replay.ignored_tail_bytes > 0);
    }

    #[test]
    fn checksum_corruption_is_rejected() {
        let mut frame = encode_frame(&upsert(1, RecordId::unsigned(1))).expect("frame");
        *frame.last_mut().expect("payload") ^= 0xff;
        assert!(matches!(
            decode_log(&frame),
            Err(WalError::ChecksumMismatch { sequence: 1 })
        ));
    }

    #[test]
    fn unsupported_version_is_rejected() {
        let mut frame = encode_frame(&upsert(1, RecordId::unsigned(1))).expect("frame");
        frame[4] = VERSION + 1;
        assert!(matches!(
            decode_log(&frame),
            Err(WalError::UnsupportedVersion(_))
        ));
    }

    #[test]
    fn sequence_regression_is_rejected_on_append() {
        let path = temp_path("sequence");
        let mut wal = Wal::open(&path).expect("open");
        wal.append(&upsert(2, RecordId::unsigned(2)))
            .expect("append");
        assert!(matches!(
            wal.append(&upsert(1, RecordId::unsigned(1))),
            Err(WalError::SequenceRegression {
                previous: 2,
                current: 1
            })
        ));
        let _ = fs::remove_file(path);
    }

    #[test]
    fn sequence_regression_is_rejected_during_replay() {
        let mut bytes = encode_frame(&upsert(2, RecordId::unsigned(2))).expect("frame");
        bytes.extend_from_slice(&encode_frame(&upsert(1, RecordId::unsigned(1))).expect("frame"));
        assert!(matches!(
            decode_log(&bytes),
            Err(WalError::SequenceRegression {
                previous: 2,
                current: 1
            })
        ));
    }

    #[test]
    fn record_id_types_round_trip_distinctly() {
        let string = upsert(1, RecordId::string("42").expect("id"));
        let numeric = upsert(2, RecordId::unsigned(42));
        let string_frame = encode_frame(&string).expect("frame");
        let numeric_frame = encode_frame(&numeric).expect("frame");
        assert_eq!(
            decode_log(&string_frame).expect("decode").entries[0],
            string
        );
        assert_eq!(
            decode_log(&numeric_frame).expect("decode").entries[0],
            numeric
        );
    }

    #[test]
    fn reopen_recovers_last_sequence() {
        let path = temp_path("last-sequence");
        {
            let mut wal = Wal::open(&path).expect("open");
            wal.append(&upsert(7, RecordId::unsigned(7)))
                .expect("append");
        }
        let mut wal = Wal::open(&path).expect("reopen");
        assert!(matches!(
            wal.append(&upsert(7, RecordId::unsigned(8))),
            Err(WalError::SequenceRegression {
                previous: 7,
                current: 7
            })
        ));
        let _ = fs::remove_file(path);
    }
}
