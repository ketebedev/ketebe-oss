use crate::{
    LocalFilesystemBackend, SegmentLocation, StorageBackend, StorageBackendError, WalMutation,
};
use ketebe_core::{
    CollectionId, Metadata, MetadataValue, Record, RecordId, SequenceNumber, Vector,
};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

const MAGIC: [u8; 4] = *b"KTSG";
const VERSION: u8 = 1;
const EXTENSION: &str = "kseg";
const HEADER_LEN: usize = 20;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SegmentId(u64);

impl SegmentId {
    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Tombstone {
    record_id: RecordId,
    sequence_number: SequenceNumber,
}

impl Tombstone {
    #[must_use]
    pub fn new(record_id: RecordId, sequence_number: SequenceNumber) -> Self {
        Self {
            record_id,
            sequence_number,
        }
    }

    #[must_use]
    pub fn record_id(&self) -> &RecordId {
        &self.record_id
    }

    #[must_use]
    pub const fn sequence_number(&self) -> SequenceNumber {
        self.sequence_number
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Segment {
    id: SegmentId,
    collection_id: CollectionId,
    min_sequence: SequenceNumber,
    max_sequence: SequenceNumber,
    records: Vec<Record>,
    tombstones: Vec<Tombstone>,
}

impl Segment {
    pub fn from_mutations(id: SegmentId, mutations: &[WalMutation]) -> Result<Self, SegmentError> {
        let first = mutations.first().ok_or(SegmentError::EmptyMutationSet)?;
        let collection_id = mutation_collection(first).clone();
        let mut previous: Option<SequenceNumber> = None;
        let mut records = BTreeMap::<RecordId, Record>::new();
        let mut tombstones = BTreeMap::<RecordId, SequenceNumber>::new();
        let mut min_sequence = None;
        let mut max_sequence = None;

        for mutation in mutations {
            if mutation_collection(mutation) != &collection_id {
                return Err(SegmentError::CollectionMismatch);
            }

            let sequence = mutation.sequence_number();
            if let Some(previous_sequence) = previous
                && sequence <= previous_sequence
            {
                return Err(SegmentError::SequenceRegression {
                    previous: previous_sequence.get(),
                    current: sequence.get(),
                });
            }

            min_sequence.get_or_insert(sequence);
            max_sequence = Some(sequence);
            previous = Some(sequence);

            match mutation {
                WalMutation::Upsert { record, .. } => {
                    tombstones.remove(record.id());
                    records.insert(record.id().clone(), record.clone());
                }
                WalMutation::Delete {
                    record_id,
                    sequence_number,
                    ..
                } => {
                    records.remove(record_id);
                    tombstones.insert(record_id.clone(), *sequence_number);
                }
            }
        }

        Ok(Self {
            id,
            collection_id,
            min_sequence: min_sequence.expect("non-empty mutations"),
            max_sequence: max_sequence.expect("non-empty mutations"),
            records: records.into_values().collect(),
            tombstones: tombstones
                .into_iter()
                .map(|(record_id, sequence_number)| Tombstone::new(record_id, sequence_number))
                .collect(),
        })
    }

    #[must_use]
    pub const fn id(&self) -> SegmentId {
        self.id
    }

    #[must_use]
    pub fn collection_id(&self) -> &CollectionId {
        &self.collection_id
    }

    #[must_use]
    pub const fn min_sequence(&self) -> SequenceNumber {
        self.min_sequence
    }

    #[must_use]
    pub const fn max_sequence(&self) -> SequenceNumber {
        self.max_sequence
    }

    #[must_use]
    pub fn records(&self) -> &[Record] {
        &self.records
    }

    #[must_use]
    pub fn tombstones(&self) -> &[Tombstone] {
        &self.tombstones
    }

    #[must_use]
    pub fn record_count(&self) -> usize {
        self.records.len()
    }
}

#[derive(Debug)]
pub enum SegmentError {
    Io(std::io::Error),
    EmptyMutationSet,
    CollectionMismatch,
    SequenceRegression { previous: u64, current: u64 },
    InvalidMagic,
    UnsupportedVersion(u8),
    ChecksumMismatch,
    Corrupt(&'static str),
    Domain(String),
    AlreadyExists(PathBuf),
    Backend(StorageBackendError),
}

impl fmt::Display for SegmentError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(f, "segment I/O error: {error}"),
            Self::EmptyMutationSet => f.write_str("cannot build a segment from no mutations"),
            Self::CollectionMismatch => {
                f.write_str("segment mutations belong to different collections")
            }
            Self::SequenceRegression { previous, current } => write!(
                f,
                "segment sequence regression: previous={previous}, current={current}"
            ),
            Self::InvalidMagic => f.write_str("invalid segment magic"),
            Self::UnsupportedVersion(version) => {
                write!(f, "unsupported segment version: {version}")
            }
            Self::ChecksumMismatch => f.write_str("segment checksum mismatch"),
            Self::Corrupt(message) => write!(f, "corrupt segment: {message}"),
            Self::Domain(message) => write!(f, "invalid segment domain value: {message}"),
            Self::AlreadyExists(path) => {
                write!(f, "segment already exists: {}", path.display())
            }
            Self::Backend(error) => write!(f, "segment storage backend error: {error}"),
        }
    }
}

impl std::error::Error for SegmentError {}

impl From<std::io::Error> for SegmentError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

impl From<StorageBackendError> for SegmentError {
    fn from(value: StorageBackendError) -> Self {
        Self::Backend(value)
    }
}

pub struct SegmentStore {
    backend: Arc<dyn StorageBackend>,
}

impl SegmentStore {
    pub fn open(directory: impl AsRef<Path>) -> Result<Self, SegmentError> {
        let backend = LocalFilesystemBackend::open(directory)?;
        Self::from_backend(Arc::new(backend))
    }

    pub fn from_backend(backend: Arc<dyn StorageBackend>) -> Result<Self, SegmentError> {
        backend.capabilities().validate_segment_store()?;
        Ok(Self { backend })
    }

    #[must_use]
    pub fn capabilities(&self) -> crate::BackendCapabilities {
        self.backend.capabilities()
    }

    pub fn publish(&self, segment: &Segment) -> Result<SegmentLocation, SegmentError> {
        let location = self.location(segment.id());
        if self.backend.exists(&location)? {
            return Err(StorageBackendError::AlreadyExists(location).into());
        }
        let bytes = encode_segment(segment)?;
        self.backend.publish_atomic(&location, &bytes)?;
        Ok(location)
    }

    pub fn open_segment(&self, id: SegmentId) -> Result<Segment, SegmentError> {
        let bytes = self.backend.read(&self.location(id))?;
        decode_segment(&bytes)
    }

    pub fn discover(&self) -> Result<Vec<Segment>, SegmentError> {
        let mut segments = Vec::new();
        for location in self.backend.list()? {
            if !location.key().ends_with(".kseg") {
                continue;
            }
            let bytes = self.backend.read(&location)?;
            segments.push(decode_segment(&bytes)?);
        }
        segments.sort_by_key(|segment| (segment.min_sequence(), segment.id()));
        Ok(segments)
    }

    pub fn list_segment_ids(&self) -> Result<Vec<SegmentId>, SegmentError> {
        let mut ids = Vec::new();
        for location in self.backend.list()? {
            let Some(stem) = location.key().strip_suffix(".kseg") else {
                continue;
            };
            let value = stem
                .parse::<u64>()
                .map_err(|_| SegmentError::Corrupt("invalid segment location key"))?;
            ids.push(SegmentId::new(value));
        }
        ids.sort_unstable();
        Ok(ids)
    }

    pub fn delete_segment(&self, id: SegmentId) -> Result<bool, SegmentError> {
        Ok(self.backend.delete(&self.location(id))?)
    }

    #[must_use]
    pub fn location(&self, id: SegmentId) -> SegmentLocation {
        SegmentLocation::new(format!("{:020}.{EXTENSION}", id.get()))
            .expect("generated segment location is valid")
    }
}

fn mutation_collection(mutation: &WalMutation) -> &CollectionId {
    match mutation {
        WalMutation::Upsert { collection_id, .. } | WalMutation::Delete { collection_id, .. } => {
            collection_id
        }
    }
}

fn encode_segment(segment: &Segment) -> Result<Vec<u8>, SegmentError> {
    let mut payload = Vec::new();
    write_string(&mut payload, segment.collection_id.as_str())?;
    payload.extend_from_slice(&segment.id.get().to_le_bytes());
    payload.extend_from_slice(&segment.min_sequence.get().to_le_bytes());
    payload.extend_from_slice(&segment.max_sequence.get().to_le_bytes());

    write_len(&mut payload, segment.records.len(), "too many records")?;
    for record in &segment.records {
        write_record(&mut payload, record)?;
    }

    write_len(
        &mut payload,
        segment.tombstones.len(),
        "too many tombstones",
    )?;
    for tombstone in &segment.tombstones {
        write_record_id(&mut payload, tombstone.record_id())?;
        payload.extend_from_slice(&tombstone.sequence_number().get().to_le_bytes());
    }

    let mut output = Vec::new();
    output.extend_from_slice(&MAGIC);
    output.push(VERSION);
    output.extend_from_slice(&[0, 0, 0]);
    let payload_len = u64::try_from(payload.len())
        .map_err(|_| SegmentError::Corrupt("segment payload too large"))?;
    output.extend_from_slice(&payload_len.to_le_bytes());
    output.extend_from_slice(&crc32(&payload).to_le_bytes());
    output.extend_from_slice(&payload);
    Ok(output)
}

fn decode_segment(bytes: &[u8]) -> Result<Segment, SegmentError> {
    if bytes.len() < HEADER_LEN {
        return Err(SegmentError::Corrupt("truncated header"));
    }
    if bytes[0..4] != MAGIC {
        return Err(SegmentError::InvalidMagic);
    }
    if bytes[4] != VERSION {
        return Err(SegmentError::UnsupportedVersion(bytes[4]));
    }

    let payload_len = u64::from_le_bytes(bytes[8..16].try_into().expect("fixed slice"));
    let payload_len = usize::try_from(payload_len)
        .map_err(|_| SegmentError::Corrupt("payload length overflow"))?;
    let expected_len = HEADER_LEN
        .checked_add(payload_len)
        .ok_or(SegmentError::Corrupt("segment length overflow"))?;
    if bytes.len() != expected_len {
        return Err(SegmentError::Corrupt("segment length mismatch"));
    }

    let expected_crc = u32::from_le_bytes(bytes[16..20].try_into().expect("fixed slice"));
    let payload = &bytes[HEADER_LEN..];
    if crc32(payload) != expected_crc {
        return Err(SegmentError::ChecksumMismatch);
    }

    let mut cursor = Cursor::new(payload);
    let collection_id = CollectionId::new(cursor.read_string()?)
        .map_err(|error| SegmentError::Domain(error.to_string()))?;
    let id = SegmentId::new(cursor.read_u64()?);
    let min_sequence = SequenceNumber::new(cursor.read_u64()?);
    let max_sequence = SequenceNumber::new(cursor.read_u64()?);
    if min_sequence > max_sequence {
        return Err(SegmentError::Corrupt("invalid sequence range"));
    }

    let record_count = cursor.read_u32()? as usize;
    let mut records = Vec::with_capacity(record_count);
    let mut seen = BTreeSet::new();
    for _ in 0..record_count {
        let record = cursor.read_record()?;
        if !seen.insert(record.id().clone()) {
            return Err(SegmentError::Corrupt("duplicate record id"));
        }
        records.push(record);
    }

    let tombstone_count = cursor.read_u32()? as usize;
    let mut tombstones = Vec::with_capacity(tombstone_count);
    for _ in 0..tombstone_count {
        let record_id = cursor.read_record_id()?;
        if seen.contains(&record_id) {
            return Err(SegmentError::Corrupt("record and tombstone overlap"));
        }
        tombstones.push(Tombstone::new(
            record_id,
            SequenceNumber::new(cursor.read_u64()?),
        ));
    }

    if !cursor.is_finished() {
        return Err(SegmentError::Corrupt("trailing payload bytes"));
    }

    Ok(Segment {
        id,
        collection_id,
        min_sequence,
        max_sequence,
        records,
        tombstones,
    })
}

fn write_record(output: &mut Vec<u8>, record: &Record) -> Result<(), SegmentError> {
    write_record_id(output, record.id())?;
    output.extend_from_slice(&record.sequence_number().get().to_le_bytes());
    write_len(output, record.vector().len(), "vector too large")?;
    for value in record.vector().as_slice() {
        output.extend_from_slice(&value.to_bits().to_le_bytes());
    }
    write_metadata(output, record.metadata())
}

fn write_record_id(output: &mut Vec<u8>, id: &RecordId) -> Result<(), SegmentError> {
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

fn write_metadata(output: &mut Vec<u8>, metadata: &Metadata) -> Result<(), SegmentError> {
    write_len(output, metadata.len(), "metadata too large")?;
    for (key, value) in metadata {
        write_string(output, key)?;
        write_metadata_value(output, value)?;
    }
    Ok(())
}

fn write_metadata_value(output: &mut Vec<u8>, value: &MetadataValue) -> Result<(), SegmentError> {
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
            write_len(output, values.len(), "metadata array too large")?;
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

fn write_string(output: &mut Vec<u8>, value: &str) -> Result<(), SegmentError> {
    write_len(output, value.len(), "string too large")?;
    output.extend_from_slice(value.as_bytes());
    Ok(())
}

fn write_len(output: &mut Vec<u8>, len: usize, message: &'static str) -> Result<(), SegmentError> {
    let len = u32::try_from(len).map_err(|_| SegmentError::Corrupt(message))?;
    output.extend_from_slice(&len.to_le_bytes());
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

    fn take(&mut self, len: usize) -> Result<&'a [u8], SegmentError> {
        let end = self
            .offset
            .checked_add(len)
            .ok_or(SegmentError::Corrupt("cursor overflow"))?;
        if end > self.bytes.len() {
            return Err(SegmentError::Corrupt("unexpected payload end"));
        }
        let slice = &self.bytes[self.offset..end];
        self.offset = end;
        Ok(slice)
    }

    fn read_u8(&mut self) -> Result<u8, SegmentError> {
        Ok(self.take(1)?[0])
    }

    fn read_u32(&mut self) -> Result<u32, SegmentError> {
        Ok(u32::from_le_bytes(
            self.take(4)?.try_into().expect("fixed slice"),
        ))
    }

    fn read_u64(&mut self) -> Result<u64, SegmentError> {
        Ok(u64::from_le_bytes(
            self.take(8)?.try_into().expect("fixed slice"),
        ))
    }

    fn read_string(&mut self) -> Result<String, SegmentError> {
        let len = self.read_u32()? as usize;
        String::from_utf8(self.take(len)?.to_vec())
            .map_err(|_| SegmentError::Corrupt("invalid UTF-8"))
    }

    fn read_record_id(&mut self) -> Result<RecordId, SegmentError> {
        match self.read_u8()? {
            0 => RecordId::string(self.read_string()?)
                .map_err(|error| SegmentError::Domain(error.to_string())),
            1 => Ok(RecordId::unsigned(self.read_u64()?)),
            _ => Err(SegmentError::Corrupt("invalid record id tag")),
        }
    }

    fn read_record(&mut self) -> Result<Record, SegmentError> {
        let id = self.read_record_id()?;
        let sequence = SequenceNumber::new(self.read_u64()?);
        let vector_len = self.read_u32()? as usize;
        let mut values = Vec::with_capacity(vector_len);
        for _ in 0..vector_len {
            let bits = u32::from_le_bytes(self.take(4)?.try_into().expect("fixed slice"));
            values.push(f32::from_bits(bits));
        }
        let vector =
            Vector::new(values).map_err(|error| SegmentError::Domain(error.to_string()))?;
        let metadata = self.read_metadata()?;
        Ok(Record::new(id, vector, metadata, sequence))
    }

    fn read_metadata(&mut self) -> Result<Metadata, SegmentError> {
        let len = self.read_u32()? as usize;
        let mut metadata = BTreeMap::new();
        for _ in 0..len {
            metadata.insert(self.read_string()?, self.read_metadata_value()?);
        }
        Ok(metadata)
    }

    fn read_metadata_value(&mut self) -> Result<MetadataValue, SegmentError> {
        match self.read_u8()? {
            0 => Ok(MetadataValue::Null),
            1 => match self.read_u8()? {
                0 => Ok(MetadataValue::Bool(false)),
                1 => Ok(MetadataValue::Bool(true)),
                _ => Err(SegmentError::Corrupt("invalid boolean")),
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
            _ => Err(SegmentError::Corrupt("invalid metadata tag")),
        }
    }
}

fn crc32(bytes: &[u8]) -> u32 {
    let mut crc = 0xffff_ffffu32;
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
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_dir(name: &str) -> PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time")
            .as_nanos();
        std::env::temp_dir().join(format!(
            "ketebe-segment-{name}-{}-{nonce}",
            std::process::id()
        ))
    }

    fn collection() -> CollectionId {
        CollectionId::new("products").expect("collection")
    }

    fn upsert(sequence: u64, id: RecordId, value: f32) -> WalMutation {
        WalMutation::Upsert {
            collection_id: collection(),
            record: Record::new(
                id,
                Vector::new(vec![value, value + 1.0]).expect("vector"),
                BTreeMap::from([(
                    "name".to_string(),
                    MetadataValue::String("item".to_string()),
                )]),
                SequenceNumber::new(sequence),
            ),
        }
    }

    fn delete(sequence: u64, id: RecordId) -> WalMutation {
        WalMutation::Delete {
            collection_id: collection(),
            record_id: id,
            sequence_number: SequenceNumber::new(sequence),
        }
    }

    #[test]
    fn segment_round_trip_preserves_records_and_tombstones() {
        let id = RecordId::string("a").expect("id");
        let segment = Segment::from_mutations(
            SegmentId::new(7),
            &[
                upsert(1, id.clone(), 1.0),
                delete(2, RecordId::unsigned(42)),
            ],
        )
        .expect("segment");
        let encoded = encode_segment(&segment).expect("encode");
        assert_eq!(decode_segment(&encoded).expect("decode"), segment);
    }

    #[test]
    fn latest_update_wins_within_segment() {
        let id = RecordId::string("a").expect("id");
        let segment = Segment::from_mutations(
            SegmentId::new(1),
            &[upsert(1, id.clone(), 1.0), upsert(2, id, 9.0)],
        )
        .expect("segment");
        assert_eq!(segment.records().len(), 1);
        assert_eq!(
            segment.records()[0].sequence_number(),
            SequenceNumber::new(2)
        );
        assert_eq!(segment.records()[0].vector().as_slice()[0], 9.0);
    }

    #[test]
    fn delete_removes_live_record_and_creates_tombstone() {
        let id = RecordId::string("a").expect("id");
        let segment = Segment::from_mutations(
            SegmentId::new(1),
            &[upsert(1, id.clone(), 1.0), delete(2, id.clone())],
        )
        .expect("segment");
        assert!(segment.records().is_empty());
        assert_eq!(segment.tombstones()[0].record_id(), &id);
    }

    #[test]
    fn publish_and_reopen_are_durable() {
        let directory = temp_dir("publish");
        let store = SegmentStore::open(&directory).expect("store");
        let segment =
            Segment::from_mutations(SegmentId::new(3), &[upsert(1, RecordId::unsigned(1), 1.0)])
                .expect("segment");
        store.publish(&segment).expect("publish");
        assert_eq!(
            store.open_segment(SegmentId::new(3)).expect("open"),
            segment
        );
        fs::remove_dir_all(directory).ok();
    }

    #[test]
    fn discovery_ignores_abandoned_temp_files() {
        let directory = temp_dir("discover");
        let store = SegmentStore::open(&directory).expect("store");
        fs::write(directory.join("00000000000000000001.kseg.tmp"), b"partial").expect("temp");
        let segment =
            Segment::from_mutations(SegmentId::new(2), &[upsert(1, RecordId::unsigned(2), 1.0)])
                .expect("segment");
        store.publish(&segment).expect("publish");
        assert_eq!(store.discover().expect("discover"), vec![segment]);
        fs::remove_dir_all(directory).ok();
    }

    #[test]
    fn checksum_corruption_is_rejected() {
        let segment =
            Segment::from_mutations(SegmentId::new(1), &[upsert(1, RecordId::unsigned(1), 1.0)])
                .expect("segment");
        let mut bytes = encode_segment(&segment).expect("encode");
        let last = bytes.len() - 1;
        bytes[last] ^= 0xff;
        assert!(matches!(
            decode_segment(&bytes),
            Err(SegmentError::ChecksumMismatch)
        ));
    }

    #[test]
    fn unsupported_version_is_rejected() {
        let segment =
            Segment::from_mutations(SegmentId::new(1), &[upsert(1, RecordId::unsigned(1), 1.0)])
                .expect("segment");
        let mut bytes = encode_segment(&segment).expect("encode");
        bytes[4] = VERSION + 1;
        assert!(matches!(
            decode_segment(&bytes),
            Err(SegmentError::UnsupportedVersion(_))
        ));
    }

    #[test]
    fn sequence_regression_is_rejected() {
        let mutations = [
            upsert(2, RecordId::unsigned(2), 2.0),
            upsert(1, RecordId::unsigned(1), 1.0),
        ];
        assert!(matches!(
            Segment::from_mutations(SegmentId::new(1), &mutations),
            Err(SegmentError::SequenceRegression { .. })
        ));
    }

    #[test]
    fn collection_mismatch_is_rejected() {
        let first = upsert(1, RecordId::unsigned(1), 1.0);
        let second = WalMutation::Delete {
            collection_id: CollectionId::new("other").expect("collection"),
            record_id: RecordId::unsigned(1),
            sequence_number: SequenceNumber::new(2),
        };
        assert!(matches!(
            Segment::from_mutations(SegmentId::new(1), &[first, second]),
            Err(SegmentError::CollectionMismatch)
        ));
    }
}
