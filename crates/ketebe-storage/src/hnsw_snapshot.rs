use crate::hnsw::{HnswNativeGraph, HnswNativeNode};
use crate::{Checkpoint, HnswConfig, HnswError, HnswIndex, Segment, SegmentId, WalMutation};
use ketebe_core::{
    CollectionId, DistanceMetric, Metadata, MetadataValue, Record, RecordId, SequenceNumber, Vector,
};
#[cfg(test)]
use std::collections::BTreeMap;
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

const MAGIC: [u8; 4] = *b"KTHI";
const LEGACY_VERSION: u8 = 1;
const NATIVE_VERSION: u8 = 2;
const HEADER_LEN: usize = 20;
const FILE_NAME: &str = "hnsw.kthi";
const TEMP_FILE_NAME: &str = "hnsw.kthi.tmp";

#[derive(Debug)]
pub enum HnswLoadResult {
    Loaded(HnswIndex),
    Missing,
    Stale,
}

#[derive(Debug)]
pub enum HnswSnapshotError {
    Io(std::io::Error),
    InvalidMagic,
    UnsupportedVersion(u8),
    ChecksumMismatch,
    Corrupt(&'static str),
    Domain(String),
    Hnsw(HnswError),
}

impl fmt::Display for HnswSnapshotError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(f, "HNSW snapshot I/O error: {error}"),
            Self::InvalidMagic => f.write_str("invalid HNSW snapshot magic"),
            Self::UnsupportedVersion(version) => {
                write!(f, "unsupported HNSW snapshot version: {version}")
            }
            Self::ChecksumMismatch => f.write_str("HNSW snapshot checksum mismatch"),
            Self::Corrupt(message) => write!(f, "corrupt HNSW snapshot: {message}"),
            Self::Domain(message) => write!(f, "invalid HNSW snapshot domain value: {message}"),
            Self::Hnsw(error) => write!(f, "HNSW snapshot restore failed: {error}"),
        }
    }
}

impl std::error::Error for HnswSnapshotError {}

impl From<std::io::Error> for HnswSnapshotError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

impl From<HnswError> for HnswSnapshotError {
    fn from(value: HnswError) -> Self {
        Self::Hnsw(value)
    }
}

pub struct HnswIndexStore {
    directory: PathBuf,
}

impl HnswIndexStore {
    pub fn open(collection_directory: impl AsRef<Path>) -> Result<Self, HnswSnapshotError> {
        let directory = collection_directory.as_ref().join("indexes");
        fs::create_dir_all(&directory)?;
        Ok(Self { directory })
    }

    pub fn load(
        &self,
        checkpoint: &Checkpoint,
        metric: DistanceMetric,
        config: HnswConfig,
    ) -> Result<HnswLoadResult, HnswSnapshotError> {
        let path = self.directory.join(FILE_NAME);
        if !path.exists() {
            return Ok(HnswLoadResult::Missing);
        }

        let snapshot = read_snapshot(&path)?;
        let expected = fingerprint(checkpoint, metric, config);
        let index = match snapshot {
            DecodedSnapshot::Legacy(snapshot) => {
                if snapshot.fingerprint != expected
                    || snapshot.collection_id != *checkpoint.collection_id()
                    || snapshot.metric != metric
                    || snapshot.config != config
                {
                    return Ok(HnswLoadResult::Stale);
                }
                restore_index(&snapshot)?
            }
            DecodedSnapshot::Native(snapshot) => {
                if snapshot.fingerprint != expected
                    || snapshot.graph.collection_id != *checkpoint.collection_id()
                    || snapshot.graph.metric != metric
                    || snapshot.graph.config != config
                {
                    return Ok(HnswLoadResult::Stale);
                }
                HnswIndex::from_native_graph(snapshot.graph)?
            }
        };
        Ok(HnswLoadResult::Loaded(index))
    }

    pub fn rebuild_and_publish(
        &self,
        checkpoint: &Checkpoint,
        metric: DistanceMetric,
        config: HnswConfig,
        segments: &[Segment],
    ) -> Result<HnswIndex, HnswSnapshotError> {
        let index = HnswIndex::build(segments, checkpoint.collection_id(), metric, config)?;
        let snapshot = HnswNativeSnapshot {
            fingerprint: fingerprint(checkpoint, metric, config),
            graph: index.native_graph(),
        };
        self.publish(&snapshot)?;
        Ok(index)
    }

    pub fn remove(&self) -> Result<(), HnswSnapshotError> {
        let path = self.directory.join(FILE_NAME);
        if path.exists() {
            fs::remove_file(path)?;
            sync_directory(&self.directory)?;
        }
        Ok(())
    }

    fn publish(&self, snapshot: &HnswNativeSnapshot) -> Result<(), HnswSnapshotError> {
        let final_path = self.directory.join(FILE_NAME);
        let temp_path = self.directory.join(TEMP_FILE_NAME);
        if temp_path.exists() {
            fs::remove_file(&temp_path)?;
        }

        let bytes = encode_native_snapshot(snapshot)?;
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
struct HnswNativeSnapshot {
    fingerprint: u64,
    graph: HnswNativeGraph,
}

#[derive(Debug)]
enum DecodedSnapshot {
    Legacy(HnswSnapshot),
    Native(HnswNativeSnapshot),
}

#[derive(Debug)]
struct HnswSnapshot {
    fingerprint: u64,
    collection_id: CollectionId,
    metric: DistanceMetric,
    config: HnswConfig,
    records: Vec<Record>,
}

#[cfg(test)]
struct VisibleVersion {
    sequence: SequenceNumber,
    record: Option<Record>,
}

#[cfg(test)]
fn fold_visible_records(
    segments: &[Segment],
    collection_id: &CollectionId,
) -> BTreeMap<RecordId, Record> {
    let mut latest = BTreeMap::<RecordId, VisibleVersion>::new();
    for segment in segments {
        if segment.collection_id() != collection_id {
            continue;
        }
        for record in segment.records() {
            apply_version(
                &mut latest,
                record.id().clone(),
                record.sequence_number(),
                Some(record.clone()),
            );
        }
        for tombstone in segment.tombstones() {
            apply_version(
                &mut latest,
                tombstone.record_id().clone(),
                tombstone.sequence_number(),
                None,
            );
        }
    }
    latest
        .into_iter()
        .filter_map(|(id, version)| version.record.map(|record| (id, record)))
        .collect()
}

#[cfg(test)]
fn apply_version(
    latest: &mut BTreeMap<RecordId, VisibleVersion>,
    id: RecordId,
    sequence: SequenceNumber,
    record: Option<Record>,
) {
    match latest.get(&id) {
        Some(existing) if existing.sequence >= sequence => {}
        _ => {
            latest.insert(id, VisibleVersion { sequence, record });
        }
    }
}

fn restore_index(snapshot: &HnswSnapshot) -> Result<HnswIndex, HnswSnapshotError> {
    if snapshot.records.is_empty() {
        return HnswIndex::build(
            &[],
            &snapshot.collection_id,
            snapshot.metric,
            snapshot.config,
        )
        .map_err(Into::into);
    }

    let mut records = snapshot.records.clone();
    records.sort_by_key(Record::sequence_number);
    let mutations = records
        .into_iter()
        .map(|record| WalMutation::Upsert {
            collection_id: snapshot.collection_id.clone(),
            record,
        })
        .collect::<Vec<_>>();
    let segment = Segment::from_mutations(SegmentId::new(u64::MAX - 1), &mutations)
        .map_err(|error| HnswSnapshotError::Domain(error.to_string()))?;
    HnswIndex::build(
        &[segment],
        &snapshot.collection_id,
        snapshot.metric,
        snapshot.config,
    )
    .map_err(Into::into)
}

#[must_use]
pub fn hnsw_checkpoint_fingerprint(
    checkpoint: &Checkpoint,
    metric: DistanceMetric,
    config: HnswConfig,
) -> u64 {
    fingerprint(checkpoint, metric, config)
}

fn fingerprint(checkpoint: &Checkpoint, metric: DistanceMetric, config: HnswConfig) -> u64 {
    let mut hash = 0xcbf29ce484222325_u64;
    for byte in checkpoint.collection_id().as_str().bytes() {
        hash_byte(&mut hash, byte);
    }
    for byte in checkpoint.sequence_number().get().to_le_bytes() {
        hash_byte(&mut hash, byte);
    }
    for segment in checkpoint.segments() {
        for byte in segment.get().to_le_bytes() {
            hash_byte(&mut hash, byte);
        }
    }
    hash_byte(&mut hash, metric_tag(metric));
    for value in [config.m, config.ef_construction, config.ef_search] {
        for byte in u64::try_from(value).unwrap_or(u64::MAX).to_le_bytes() {
            hash_byte(&mut hash, byte);
        }
    }
    hash
}

fn hash_byte(hash: &mut u64, byte: u8) {
    *hash ^= u64::from(byte);
    *hash = hash.wrapping_mul(0x100000001b3);
}

#[cfg(test)]
fn encode_legacy_snapshot(snapshot: &HnswSnapshot) -> Result<Vec<u8>, HnswSnapshotError> {
    let mut payload = Vec::new();
    payload.extend_from_slice(&snapshot.fingerprint.to_le_bytes());
    write_string(&mut payload, snapshot.collection_id.as_str())?;
    payload.push(metric_tag(snapshot.metric));
    write_usize(&mut payload, snapshot.config.m)?;
    write_usize(&mut payload, snapshot.config.ef_construction)?;
    write_usize(&mut payload, snapshot.config.ef_search)?;
    write_len(
        &mut payload,
        snapshot.records.len(),
        "too many HNSW records",
    )?;
    for record in &snapshot.records {
        write_record(&mut payload, record)?;
    }

    let mut output = Vec::with_capacity(HEADER_LEN + payload.len());
    output.extend_from_slice(&MAGIC);
    output.push(LEGACY_VERSION);
    output.extend_from_slice(&[0, 0, 0]);
    let payload_len = u64::try_from(payload.len())
        .map_err(|_| HnswSnapshotError::Corrupt("snapshot payload too large"))?;
    output.extend_from_slice(&payload_len.to_le_bytes());
    output.extend_from_slice(&crc32(&payload).to_le_bytes());
    output.extend_from_slice(&payload);
    Ok(output)
}

fn read_snapshot(path: &Path) -> Result<DecodedSnapshot, HnswSnapshotError> {
    let mut bytes = Vec::new();
    File::open(path)?.read_to_end(&mut bytes)?;
    match bytes.get(4).copied() {
        Some(LEGACY_VERSION) => decode_legacy_snapshot(&bytes).map(DecodedSnapshot::Legacy),
        Some(NATIVE_VERSION) => decode_native_snapshot(&bytes).map(DecodedSnapshot::Native),
        Some(version) => Err(HnswSnapshotError::UnsupportedVersion(version)),
        None => Err(HnswSnapshotError::Corrupt("truncated header")),
    }
}

fn decode_legacy_snapshot(bytes: &[u8]) -> Result<HnswSnapshot, HnswSnapshotError> {
    if bytes.len() < HEADER_LEN {
        return Err(HnswSnapshotError::Corrupt("truncated header"));
    }
    if bytes[0..4] != MAGIC {
        return Err(HnswSnapshotError::InvalidMagic);
    }
    if bytes[4] != LEGACY_VERSION {
        return Err(HnswSnapshotError::UnsupportedVersion(bytes[4]));
    }
    let payload_len = u64::from_le_bytes(bytes[8..16].try_into().expect("fixed slice"));
    let payload_len = usize::try_from(payload_len)
        .map_err(|_| HnswSnapshotError::Corrupt("payload length overflow"))?;
    let expected_len = HEADER_LEN
        .checked_add(payload_len)
        .ok_or(HnswSnapshotError::Corrupt("snapshot length overflow"))?;
    if bytes.len() != expected_len {
        return Err(HnswSnapshotError::Corrupt("snapshot length mismatch"));
    }
    let expected_crc = u32::from_le_bytes(bytes[16..20].try_into().expect("fixed slice"));
    let payload = &bytes[HEADER_LEN..];
    if crc32(payload) != expected_crc {
        return Err(HnswSnapshotError::ChecksumMismatch);
    }

    let mut cursor = Cursor::new(payload);
    let fingerprint = cursor.read_u64()?;
    let collection_id = CollectionId::new(cursor.read_string()?)
        .map_err(|error| HnswSnapshotError::Domain(error.to_string()))?;
    let metric = read_metric(cursor.read_u8()?)?;
    let config = HnswConfig {
        m: cursor.read_usize()?,
        ef_construction: cursor.read_usize()?,
        ef_search: cursor.read_usize()?,
    };
    config.validate()?;
    let count = cursor.read_u32()? as usize;
    let mut records = Vec::with_capacity(count);
    for _ in 0..count {
        records.push(cursor.read_record()?);
    }
    if !cursor.is_finished() {
        return Err(HnswSnapshotError::Corrupt("trailing payload bytes"));
    }
    Ok(HnswSnapshot {
        fingerprint,
        collection_id,
        metric,
        config,
        records,
    })
}

fn encode_native_snapshot(snapshot: &HnswNativeSnapshot) -> Result<Vec<u8>, HnswSnapshotError> {
    let graph = &snapshot.graph;
    let mut payload = Vec::new();
    payload.extend_from_slice(&snapshot.fingerprint.to_le_bytes());
    write_string(&mut payload, graph.collection_id.as_str())?;
    payload.push(metric_tag(graph.metric));
    write_usize(&mut payload, graph.config.m)?;
    write_usize(&mut payload, graph.config.ef_construction)?;
    write_usize(&mut payload, graph.config.ef_search)?;
    payload.extend_from_slice(
        &graph
            .dimension
            .map(|v| u64::try_from(v).unwrap_or(u64::MAX - 1))
            .unwrap_or(u64::MAX)
            .to_le_bytes(),
    );
    payload.extend_from_slice(
        &graph
            .entry_point
            .map(|v| u64::try_from(v).unwrap_or(u64::MAX - 1))
            .unwrap_or(u64::MAX)
            .to_le_bytes(),
    );
    write_usize(&mut payload, graph.max_level)?;
    write_len(&mut payload, graph.nodes.len(), "too many HNSW nodes")?;
    for node in &graph.nodes {
        write_record(&mut payload, &node.record)?;
        write_usize(&mut payload, node.level)?;
        write_len(&mut payload, node.neighbors.len(), "too many HNSW layers")?;
        for layer in &node.neighbors {
            write_len(&mut payload, layer.len(), "too many HNSW neighbors")?;
            for &neighbor in layer {
                write_usize(&mut payload, neighbor)?;
            }
        }
    }
    let mut output = Vec::with_capacity(HEADER_LEN + payload.len());
    output.extend_from_slice(&MAGIC);
    output.push(NATIVE_VERSION);
    output.extend_from_slice(&[0, 0, 0]);
    let payload_len = u64::try_from(payload.len())
        .map_err(|_| HnswSnapshotError::Corrupt("snapshot payload too large"))?;
    output.extend_from_slice(&payload_len.to_le_bytes());
    output.extend_from_slice(&crc32(&payload).to_le_bytes());
    output.extend_from_slice(&payload);
    Ok(output)
}

fn decode_native_snapshot(bytes: &[u8]) -> Result<HnswNativeSnapshot, HnswSnapshotError> {
    if bytes.len() < HEADER_LEN {
        return Err(HnswSnapshotError::Corrupt("truncated header"));
    }
    if bytes[0..4] != MAGIC {
        return Err(HnswSnapshotError::InvalidMagic);
    }
    if bytes[4] != NATIVE_VERSION {
        return Err(HnswSnapshotError::UnsupportedVersion(bytes[4]));
    }
    let payload_len = usize::try_from(u64::from_le_bytes(
        bytes[8..16].try_into().expect("fixed slice"),
    ))
    .map_err(|_| HnswSnapshotError::Corrupt("payload length overflow"))?;
    if bytes.len()
        != HEADER_LEN
            .checked_add(payload_len)
            .ok_or(HnswSnapshotError::Corrupt("snapshot length overflow"))?
    {
        return Err(HnswSnapshotError::Corrupt("snapshot length mismatch"));
    }
    let expected_crc = u32::from_le_bytes(bytes[16..20].try_into().expect("fixed slice"));
    let payload = &bytes[HEADER_LEN..];
    if crc32(payload) != expected_crc {
        return Err(HnswSnapshotError::ChecksumMismatch);
    }
    let mut cursor = Cursor::new(payload);
    let fingerprint = cursor.read_u64()?;
    let collection_id = CollectionId::new(cursor.read_string()?)
        .map_err(|e| HnswSnapshotError::Domain(e.to_string()))?;
    let metric = read_metric(cursor.read_u8()?)?;
    let config = HnswConfig {
        m: cursor.read_usize()?,
        ef_construction: cursor.read_usize()?,
        ef_search: cursor.read_usize()?,
    };
    config.validate()?;
    let dimension_raw = cursor.read_u64()?;
    let dimension = if dimension_raw == u64::MAX {
        None
    } else {
        Some(
            usize::try_from(dimension_raw)
                .map_err(|_| HnswSnapshotError::Corrupt("dimension overflow"))?,
        )
    };
    let entry_raw = cursor.read_u64()?;
    let entry_point = if entry_raw == u64::MAX {
        None
    } else {
        Some(
            usize::try_from(entry_raw)
                .map_err(|_| HnswSnapshotError::Corrupt("entry point overflow"))?,
        )
    };
    let max_level = cursor.read_usize()?;
    let node_count = cursor.read_u32()? as usize;
    let mut nodes = Vec::with_capacity(node_count);
    for _ in 0..node_count {
        let record = cursor.read_record()?;
        let level = cursor.read_usize()?;
        let layer_count = cursor.read_u32()? as usize;
        let mut neighbors = Vec::with_capacity(layer_count);
        for _ in 0..layer_count {
            let neighbor_count = cursor.read_u32()? as usize;
            let mut layer = Vec::with_capacity(neighbor_count);
            for _ in 0..neighbor_count {
                layer.push(cursor.read_usize()?);
            }
            neighbors.push(layer);
        }
        nodes.push(HnswNativeNode {
            record,
            level,
            neighbors,
        });
    }
    if !cursor.is_finished() {
        return Err(HnswSnapshotError::Corrupt("trailing payload bytes"));
    }
    let graph = HnswNativeGraph {
        collection_id,
        metric,
        config,
        dimension,
        nodes,
        entry_point,
        max_level,
    };
    HnswIndex::from_native_graph(graph.clone())?;
    Ok(HnswNativeSnapshot { fingerprint, graph })
}

fn metric_tag(metric: DistanceMetric) -> u8 {
    match metric {
        DistanceMetric::Cosine => 0,
        DistanceMetric::Dot => 1,
        DistanceMetric::L2 => 2,
    }
}

fn read_metric(value: u8) -> Result<DistanceMetric, HnswSnapshotError> {
    match value {
        0 => Ok(DistanceMetric::Cosine),
        1 => Ok(DistanceMetric::Dot),
        2 => Ok(DistanceMetric::L2),
        _ => Err(HnswSnapshotError::Corrupt("unknown distance metric")),
    }
}

fn write_record(output: &mut Vec<u8>, record: &Record) -> Result<(), HnswSnapshotError> {
    write_record_id(output, record.id())?;
    output.extend_from_slice(&record.sequence_number().get().to_le_bytes());
    write_len(output, record.vector().len(), "vector too large")?;
    for value in record.vector().as_slice() {
        output.extend_from_slice(&value.to_bits().to_le_bytes());
    }
    write_metadata(output, record.metadata())
}

fn write_record_id(output: &mut Vec<u8>, id: &RecordId) -> Result<(), HnswSnapshotError> {
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

fn write_metadata(output: &mut Vec<u8>, metadata: &Metadata) -> Result<(), HnswSnapshotError> {
    write_len(output, metadata.len(), "metadata too large")?;
    for (key, value) in metadata {
        write_string(output, key)?;
        write_metadata_value(output, value)?;
    }
    Ok(())
}

fn write_metadata_value(
    output: &mut Vec<u8>,
    value: &MetadataValue,
) -> Result<(), HnswSnapshotError> {
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

fn write_string(output: &mut Vec<u8>, value: &str) -> Result<(), HnswSnapshotError> {
    write_len(output, value.len(), "string too large")?;
    output.extend_from_slice(value.as_bytes());
    Ok(())
}

fn write_usize(output: &mut Vec<u8>, value: usize) -> Result<(), HnswSnapshotError> {
    let value =
        u64::try_from(value).map_err(|_| HnswSnapshotError::Corrupt("usize value too large"))?;
    output.extend_from_slice(&value.to_le_bytes());
    Ok(())
}

fn write_len(
    output: &mut Vec<u8>,
    len: usize,
    message: &'static str,
) -> Result<(), HnswSnapshotError> {
    let len = u32::try_from(len).map_err(|_| HnswSnapshotError::Corrupt(message))?;
    output.extend_from_slice(&len.to_le_bytes());
    Ok(())
}

fn sync_directory(path: &Path) -> Result<(), HnswSnapshotError> {
    #[cfg(unix)]
    File::open(path)?.sync_all()?;
    Ok(())
}

struct Cursor<'a> {
    bytes: &'a [u8],
    position: usize,
}

impl<'a> Cursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, position: 0 }
    }

    fn is_finished(&self) -> bool {
        self.position == self.bytes.len()
    }

    fn take(&mut self, len: usize) -> Result<&'a [u8], HnswSnapshotError> {
        let end = self
            .position
            .checked_add(len)
            .ok_or(HnswSnapshotError::Corrupt("cursor overflow"))?;
        if end > self.bytes.len() {
            return Err(HnswSnapshotError::Corrupt("truncated payload"));
        }
        let slice = &self.bytes[self.position..end];
        self.position = end;
        Ok(slice)
    }

    fn read_u8(&mut self) -> Result<u8, HnswSnapshotError> {
        Ok(self.take(1)?[0])
    }

    fn read_u32(&mut self) -> Result<u32, HnswSnapshotError> {
        Ok(u32::from_le_bytes(
            self.take(4)?.try_into().expect("fixed slice"),
        ))
    }

    fn read_u64(&mut self) -> Result<u64, HnswSnapshotError> {
        Ok(u64::from_le_bytes(
            self.take(8)?.try_into().expect("fixed slice"),
        ))
    }

    fn read_usize(&mut self) -> Result<usize, HnswSnapshotError> {
        usize::try_from(self.read_u64()?)
            .map_err(|_| HnswSnapshotError::Corrupt("usize value overflow"))
    }

    fn read_string(&mut self) -> Result<String, HnswSnapshotError> {
        let len = self.read_u32()? as usize;
        String::from_utf8(self.take(len)?.to_vec())
            .map_err(|_| HnswSnapshotError::Corrupt("invalid UTF-8 string"))
    }

    fn read_record_id(&mut self) -> Result<RecordId, HnswSnapshotError> {
        match self.read_u8()? {
            0 => RecordId::string(self.read_string()?)
                .map_err(|error| HnswSnapshotError::Domain(error.to_string())),
            1 => Ok(RecordId::unsigned(self.read_u64()?)),
            _ => Err(HnswSnapshotError::Corrupt("unknown record ID tag")),
        }
    }

    fn read_record(&mut self) -> Result<Record, HnswSnapshotError> {
        let id = self.read_record_id()?;
        let sequence = SequenceNumber::new(self.read_u64()?);
        let vector_len = self.read_u32()? as usize;
        let mut values = Vec::with_capacity(vector_len);
        for _ in 0..vector_len {
            let bits = u32::from_le_bytes(self.take(4)?.try_into().expect("fixed slice"));
            values.push(f32::from_bits(bits));
        }
        let vector =
            Vector::new(values).map_err(|error| HnswSnapshotError::Domain(error.to_string()))?;
        let metadata = self.read_metadata()?;
        Ok(Record::new(id, vector, metadata, sequence))
    }

    fn read_metadata(&mut self) -> Result<Metadata, HnswSnapshotError> {
        let count = self.read_u32()? as usize;
        let mut metadata = Metadata::new();
        for _ in 0..count {
            metadata.insert(self.read_string()?, self.read_metadata_value()?);
        }
        Ok(metadata)
    }

    fn read_metadata_value(&mut self) -> Result<MetadataValue, HnswSnapshotError> {
        match self.read_u8()? {
            0 => Ok(MetadataValue::Null),
            1 => match self.read_u8()? {
                0 => Ok(MetadataValue::Bool(false)),
                1 => Ok(MetadataValue::Bool(true)),
                _ => Err(HnswSnapshotError::Corrupt("invalid bool value")),
            },
            2 => Ok(MetadataValue::Number(f64::from_bits(self.read_u64()?))),
            3 => Ok(MetadataValue::String(self.read_string()?)),
            4 => {
                let count = self.read_u32()? as usize;
                let mut values = Vec::with_capacity(count);
                for _ in 0..count {
                    values.push(self.read_metadata_value()?);
                }
                Ok(MetadataValue::Array(values))
            }
            5 => Ok(MetadataValue::Object(self.read_metadata()?)),
            _ => Err(HnswSnapshotError::Corrupt("unknown metadata tag")),
        }
    }
}

fn crc32(bytes: &[u8]) -> u32 {
    let mut crc = !0_u32;
    for &byte in bytes {
        crc ^= u32::from(byte);
        for _ in 0..8 {
            let mask = (crc & 1).wrapping_neg();
            crc = (crc >> 1) ^ (0xedb88320 & mask);
        }
    }
    !crc
}

#[cfg(test)]
mod tests {
    use super::*;
    use ketebe_core::Metadata;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_dir() -> PathBuf {
        std::env::temp_dir().join(format!(
            "ketebe-hnsw-snapshot-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("time")
                .as_nanos()
        ))
    }

    fn collection() -> CollectionId {
        CollectionId::new("docs").expect("collection")
    }

    fn segments() -> Vec<Segment> {
        let collection = collection();
        let mutations = (1..=8_u64)
            .map(|id| WalMutation::Upsert {
                collection_id: collection.clone(),
                record: Record::new(
                    RecordId::unsigned(id),
                    Vector::new(vec![id as f32, 1.0]).expect("vector"),
                    Metadata::new(),
                    SequenceNumber::new(id),
                ),
            })
            .collect::<Vec<_>>();
        vec![Segment::from_mutations(SegmentId::new(1), &mutations).expect("segment")]
    }

    #[test]
    fn matching_snapshot_restores_equivalent_index() {
        let dir = temp_dir();
        fs::create_dir_all(&dir).expect("dir");
        let collection = collection();
        let checkpoint = Checkpoint::new(
            collection.clone(),
            vec![SegmentId::new(1)],
            SequenceNumber::new(8),
        );
        let config = HnswConfig::default();
        let store = HnswIndexStore::open(&dir).expect("store");
        let built = store
            .rebuild_and_publish(&checkpoint, DistanceMetric::L2, config, &segments())
            .expect("publish");
        let native_bytes = fs::read(dir.join("indexes/hnsw.kthi")).expect("native snapshot");
        assert_eq!(native_bytes[4], NATIVE_VERSION);
        let loaded = match store
            .load(&checkpoint, DistanceMetric::L2, config)
            .expect("load")
        {
            HnswLoadResult::Loaded(index) => index,
            other => panic!("expected loaded index, got {other:?}"),
        };
        assert_eq!(
            built.search(&[4.0, 1.0], 3).expect("built search"),
            loaded.search(&[4.0, 1.0], 3).expect("loaded search")
        );
        fs::remove_dir_all(dir).expect("cleanup");
    }

    #[test]
    fn legacy_v1_snapshot_remains_loadable() {
        let dir = temp_dir();
        fs::create_dir_all(&dir).expect("dir");
        let collection = collection();
        let checkpoint = Checkpoint::new(
            collection.clone(),
            vec![SegmentId::new(1)],
            SequenceNumber::new(8),
        );
        let config = HnswConfig::default();
        let legacy = HnswSnapshot {
            fingerprint: fingerprint(&checkpoint, DistanceMetric::L2, config),
            collection_id: collection,
            metric: DistanceMetric::L2,
            config,
            records: fold_visible_records(&segments(), checkpoint.collection_id())
                .into_values()
                .collect(),
        };
        let store = HnswIndexStore::open(&dir).expect("store");
        fs::write(
            dir.join("indexes/hnsw.kthi"),
            encode_legacy_snapshot(&legacy).expect("encode legacy"),
        )
        .expect("write legacy");
        assert!(matches!(
            store
                .load(&checkpoint, DistanceMetric::L2, config)
                .expect("load"),
            HnswLoadResult::Loaded(_)
        ));
        fs::remove_dir_all(dir).expect("cleanup");
    }

    #[test]
    fn changed_checkpoint_is_stale() {
        let dir = temp_dir();
        fs::create_dir_all(&dir).expect("dir");
        let collection = collection();
        let first = Checkpoint::new(
            collection.clone(),
            vec![SegmentId::new(1)],
            SequenceNumber::new(8),
        );
        let second = Checkpoint::new(collection, vec![SegmentId::new(2)], SequenceNumber::new(9));
        let store = HnswIndexStore::open(&dir).expect("store");
        store
            .rebuild_and_publish(
                &first,
                DistanceMetric::L2,
                HnswConfig::default(),
                &segments(),
            )
            .expect("publish");
        assert!(matches!(
            store
                .load(&second, DistanceMetric::L2, HnswConfig::default())
                .expect("load"),
            HnswLoadResult::Stale
        ));
        fs::remove_dir_all(dir).expect("cleanup");
    }
}
