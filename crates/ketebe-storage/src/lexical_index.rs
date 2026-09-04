use crate::{Checkpoint, QueryControl, QueryControlError, Segment};
use ketebe_core::{
    CollectionId, FieldPath, LexicalAnalyzerConfig, LexicalAnalyzerKind, Metadata, MetadataValue,
    Predicate, Record, RecordId, SequenceNumber,
};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

pub const LEXICAL_ANALYZER_VERSION: u8 = 1;
pub const LEXICAL_INDEX_VERSION: u8 = 2;
pub const DEFAULT_BM25_K1: f32 = 1.2;
pub const DEFAULT_BM25_B: f32 = 0.75;

const MAGIC: [u8; 4] = *b"KTLI";
const HEADER_LEN: usize = 20;

#[derive(Debug, Clone, PartialEq)]
pub struct LexicalIndexHit {
    record: Record,
    score: f32,
}

impl LexicalIndexHit {
    #[must_use]
    pub fn record(&self) -> &Record {
        &self.record
    }

    #[must_use]
    pub const fn score(&self) -> f32 {
        self.score
    }
}

#[derive(Debug, Clone)]
struct IndexedDocument {
    record: Record,
    length: u32,
}

#[derive(Debug, Clone, Copy)]
struct Posting {
    document: u32,
    term_frequency: u32,
}

#[derive(Debug, Clone)]
pub struct LexicalIndex {
    collection_id: CollectionId,
    fields: Vec<FieldPath>,
    source_fingerprint: u64,
    analyzer: LexicalAnalyzerConfig,
    documents: Vec<IndexedDocument>,
    postings: BTreeMap<String, Vec<Posting>>,
    average_length: f32,
}

impl LexicalIndex {
    pub fn build(
        segments: &[Segment],
        collection_id: &CollectionId,
        fields: Vec<FieldPath>,
        analyzer: LexicalAnalyzerConfig,
        source_fingerprint: u64,
    ) -> Result<Self, LexicalIndexError> {
        if fields.is_empty() {
            return Err(LexicalIndexError::EmptyFields);
        }
        let records = fold_visible_records(segments, collection_id);
        let mut documents = Vec::with_capacity(records.len());
        let mut postings = BTreeMap::<String, Vec<Posting>>::new();
        let mut total_length = 0_u64;

        for record in records.into_values() {
            let mut tokens = Vec::new();
            for field in &fields {
                if let Some(MetadataValue::String(value)) = resolve(field, record.metadata()) {
                    tokens.extend(analyze(value, analyzer));
                }
            }
            let length = u32::try_from(tokens.len())
                .map_err(|_| LexicalIndexError::Corrupt("document token count exceeds u32"))?;
            let document = u32::try_from(documents.len())
                .map_err(|_| LexicalIndexError::Corrupt("document count exceeds u32"))?;
            total_length = total_length.saturating_add(u64::from(length));

            let mut frequencies = BTreeMap::<String, u32>::new();
            for token in tokens {
                let frequency = frequencies.entry(token).or_default();
                *frequency = frequency
                    .checked_add(1)
                    .ok_or(LexicalIndexError::Corrupt("term frequency overflow"))?;
            }
            for (term, term_frequency) in frequencies {
                postings.entry(term).or_default().push(Posting {
                    document,
                    term_frequency,
                });
            }
            documents.push(IndexedDocument { record, length });
        }

        let average_length = if documents.is_empty() {
            0.0
        } else {
            total_length as f32 / documents.len() as f32
        };
        Ok(Self {
            collection_id: collection_id.clone(),
            fields,
            source_fingerprint,
            analyzer,
            documents,
            postings,
            average_length,
        })
    }

    pub fn search(
        &self,
        text: &str,
        top_k: usize,
        predicate: Option<&Predicate>,
    ) -> Result<Vec<LexicalIndexHit>, LexicalIndexError> {
        self.search_with_control(text, top_k, predicate, &QueryControl::unbounded())
    }

    pub fn search_with_control(
        &self,
        text: &str,
        top_k: usize,
        predicate: Option<&Predicate>,
        control: &QueryControl,
    ) -> Result<Vec<LexicalIndexHit>, LexicalIndexError> {
        control.check()?;
        if top_k == 0 {
            return Err(LexicalIndexError::InvalidTopK);
        }
        let query_terms = analyze(text, self.analyzer)
            .into_iter()
            .collect::<BTreeSet<_>>();
        if query_terms.is_empty() {
            return Err(LexicalIndexError::EmptyQuery);
        }
        if self.documents.is_empty() {
            return Ok(Vec::new());
        }

        let document_count = self.documents.len() as f32;
        let mut scores = BTreeMap::<u32, f32>::new();
        for term in query_terms {
            control.check()?;
            let Some(postings) = self.postings.get(&term) else {
                continue;
            };
            let df = postings.len() as f32;
            let idf = (1.0 + (document_count - df + 0.5) / (df + 0.5)).ln();
            for (posting_index, posting) in postings.iter().enumerate() {
                if posting_index % 256 == 0 {
                    control.check()?;
                }
                let document = self.documents.get(posting.document as usize).ok_or(
                    LexicalIndexError::Corrupt("posting references missing document"),
                )?;
                let tf = posting.term_frequency as f32;
                let norm = if self.average_length > 0.0 {
                    document.length as f32 / self.average_length
                } else {
                    0.0
                };
                let denominator =
                    tf + DEFAULT_BM25_K1 * (1.0 - DEFAULT_BM25_B + DEFAULT_BM25_B * norm);
                *scores.entry(posting.document).or_default() +=
                    idf * (tf * (DEFAULT_BM25_K1 + 1.0)) / denominator;
            }
        }

        let mut hits = Vec::with_capacity(scores.len());
        for (position, (document, score)) in scores.into_iter().enumerate() {
            if position % 256 == 0 {
                control.check()?;
            }
            let indexed =
                self.documents
                    .get(document as usize)
                    .ok_or(LexicalIndexError::Corrupt(
                        "score references missing document",
                    ))?;
            if let Some(predicate) = predicate
                && !predicate
                    .evaluate(indexed.record.metadata())
                    .map_err(|error| LexicalIndexError::Predicate(error.to_string()))?
            {
                continue;
            }
            if score > 0.0 {
                hits.push(LexicalIndexHit {
                    record: indexed.record.clone(),
                    score,
                });
            }
        }
        control.check()?;
        hits.sort_by(|left, right| {
            right
                .score
                .total_cmp(&left.score)
                .then_with(|| left.record.id().cmp(right.record.id()))
        });
        hits.truncate(top_k);
        Ok(hits)
    }

    #[must_use]
    pub fn collection_id(&self) -> &CollectionId {
        &self.collection_id
    }

    #[must_use]
    pub fn fields(&self) -> &[FieldPath] {
        &self.fields
    }

    #[must_use]
    pub const fn source_fingerprint(&self) -> u64 {
        self.source_fingerprint
    }

    #[must_use]
    pub const fn analyzer(&self) -> LexicalAnalyzerConfig {
        self.analyzer
    }

    #[must_use]
    pub fn document_count(&self) -> usize {
        self.documents.len()
    }

    #[must_use]
    pub fn term_count(&self) -> usize {
        self.postings.len()
    }
}

#[derive(Debug)]
pub enum LexicalLoadResult {
    Loaded(LexicalIndex),
    Missing,
    Stale,
}

pub struct LexicalIndexStore {
    directory: PathBuf,
}

impl LexicalIndexStore {
    pub fn open(collection_directory: impl AsRef<Path>) -> Result<Self, LexicalIndexError> {
        let directory = collection_directory
            .as_ref()
            .join("indexes")
            .join("lexical");
        fs::create_dir_all(&directory)?;
        Ok(Self { directory })
    }

    pub fn load(
        &self,
        checkpoint: &Checkpoint,
        fields: &[FieldPath],
        analyzer: LexicalAnalyzerConfig,
        segments: &[Segment],
    ) -> Result<LexicalLoadResult, LexicalIndexError> {
        let fingerprint = lexical_checkpoint_fingerprint(checkpoint, fields, analyzer);
        let path = self.path_for(fingerprint);
        if !path.exists() {
            return Ok(LexicalLoadResult::Missing);
        }
        let snapshot = read_snapshot(&path)?;
        if snapshot.fingerprint != fingerprint
            || snapshot.collection_id != *checkpoint.collection_id()
            || snapshot.fields != fields
            || snapshot.analyzer != analyzer
        {
            return Ok(LexicalLoadResult::Stale);
        }
        match hydrate(snapshot, segments, checkpoint.collection_id())? {
            Some(index) => Ok(LexicalLoadResult::Loaded(index)),
            None => Ok(LexicalLoadResult::Stale),
        }
    }

    pub fn rebuild_and_publish(
        &self,
        checkpoint: &Checkpoint,
        fields: Vec<FieldPath>,
        analyzer: LexicalAnalyzerConfig,
        segments: &[Segment],
    ) -> Result<LexicalIndex, LexicalIndexError> {
        let fingerprint = lexical_checkpoint_fingerprint(checkpoint, &fields, analyzer);
        let index = LexicalIndex::build(
            segments,
            checkpoint.collection_id(),
            fields,
            analyzer,
            fingerprint,
        )?;
        let snapshot = Snapshot::from_index(&index)?;
        self.publish(&snapshot)?;
        Ok(index)
    }

    pub fn garbage_collect(&self, keep_fingerprint: u64) -> Result<usize, LexicalIndexError> {
        let keep_path = self.path_for(keep_fingerprint);
        let mut removed = 0;
        for entry in fs::read_dir(&self.directory)? {
            let entry = entry?;
            if !entry.file_type()?.is_file() {
                continue;
            }
            let path = entry.path();
            if path == keep_path
                || path.extension().and_then(|value| value.to_str()) != Some("ktli")
            {
                continue;
            }
            fs::remove_file(path)?;
            removed += 1;
        }
        if removed > 0 {
            sync_directory(&self.directory)?;
        }
        Ok(removed)
    }

    pub fn remove_snapshot(&self, fingerprint: u64) -> Result<bool, LexicalIndexError> {
        let path = self.path_for(fingerprint);
        if !path.exists() {
            return Ok(false);
        }
        fs::remove_file(path)?;
        sync_directory(&self.directory)?;
        Ok(true)
    }

    fn path_for(&self, fingerprint: u64) -> PathBuf {
        self.directory.join(format!("{fingerprint:016x}.ktli"))
    }

    fn publish(&self, snapshot: &Snapshot) -> Result<(), LexicalIndexError> {
        let final_path = self.path_for(snapshot.fingerprint);
        if final_path.exists() {
            return Ok(());
        }
        let temp_path = final_path.with_extension("ktli.tmp");
        if temp_path.exists() {
            fs::remove_file(&temp_path)?;
        }
        let bytes = encode_snapshot(snapshot)?;
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
pub enum LexicalIndexError {
    Io(std::io::Error),
    InvalidMagic,
    UnsupportedVersion(u8),
    ChecksumMismatch,
    Corrupt(&'static str),
    Domain(String),
    EmptyFields,
    EmptyQuery,
    InvalidTopK,
    Predicate(String),
    Control(QueryControlError),
}

impl fmt::Display for LexicalIndexError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(f, "lexical index I/O error: {error}"),
            Self::InvalidMagic => f.write_str("invalid lexical index magic"),
            Self::UnsupportedVersion(version) => {
                write!(f, "unsupported lexical index version: {version}")
            }
            Self::ChecksumMismatch => f.write_str("lexical index checksum mismatch"),
            Self::Corrupt(message) => write!(f, "corrupt lexical index: {message}"),
            Self::Domain(message) => write!(f, "invalid lexical index domain value: {message}"),
            Self::EmptyFields => f.write_str("lexical index requires at least one field"),
            Self::EmptyQuery => f.write_str("lexical query must contain at least one token"),
            Self::InvalidTopK => f.write_str("top_k must be greater than zero"),
            Self::Predicate(message) => write!(f, "predicate evaluation failed: {message}"),
            Self::Control(error) => write!(f, "query control stopped lexical search: {error}"),
        }
    }
}

impl std::error::Error for LexicalIndexError {}
impl From<QueryControlError> for LexicalIndexError {
    fn from(value: QueryControlError) -> Self {
        Self::Control(value)
    }
}
impl From<std::io::Error> for LexicalIndexError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

#[derive(Debug)]
struct SnapshotDocument {
    id: RecordId,
    sequence: SequenceNumber,
    length: u32,
}

#[derive(Debug)]
struct Snapshot {
    fingerprint: u64,
    collection_id: CollectionId,
    fields: Vec<FieldPath>,
    analyzer: LexicalAnalyzerConfig,
    documents: Vec<SnapshotDocument>,
    postings: BTreeMap<String, Vec<Posting>>,
    average_length: f32,
}

impl Snapshot {
    fn from_index(index: &LexicalIndex) -> Result<Self, LexicalIndexError> {
        let documents = index
            .documents
            .iter()
            .map(|document| SnapshotDocument {
                id: document.record.id().clone(),
                sequence: document.record.sequence_number(),
                length: document.length,
            })
            .collect();
        Ok(Self {
            fingerprint: index.source_fingerprint,
            collection_id: index.collection_id.clone(),
            fields: index.fields.clone(),
            analyzer: index.analyzer,
            documents,
            postings: index.postings.clone(),
            average_length: index.average_length,
        })
    }
}

fn hydrate(
    snapshot: Snapshot,
    segments: &[Segment],
    collection_id: &CollectionId,
) -> Result<Option<LexicalIndex>, LexicalIndexError> {
    let visible = fold_visible_records(segments, collection_id);
    let mut documents = Vec::with_capacity(snapshot.documents.len());
    for document in snapshot.documents {
        let Some(record) = visible.get(&document.id) else {
            return Ok(None);
        };
        if record.sequence_number() != document.sequence {
            return Ok(None);
        }
        documents.push(IndexedDocument {
            record: record.clone(),
            length: document.length,
        });
    }
    if documents.len() != visible.len() {
        return Ok(None);
    }
    Ok(Some(LexicalIndex {
        collection_id: snapshot.collection_id,
        fields: snapshot.fields,
        source_fingerprint: snapshot.fingerprint,
        analyzer: snapshot.analyzer,
        documents,
        postings: snapshot.postings,
        average_length: snapshot.average_length,
    }))
}

#[must_use]
pub fn lexical_checkpoint_fingerprint(
    checkpoint: &Checkpoint,
    fields: &[FieldPath],
    analyzer: LexicalAnalyzerConfig,
) -> u64 {
    let mut hash = 0xcbf29ce484222325_u64;
    hash_bytes(&mut hash, checkpoint.collection_id().as_str().as_bytes());
    hash_bytes(&mut hash, &checkpoint.sequence_number().get().to_le_bytes());
    for segment in checkpoint.segments() {
        hash_bytes(&mut hash, &segment.get().to_le_bytes());
    }
    hash_bytes(
        &mut hash,
        &[LEXICAL_ANALYZER_VERSION, LEXICAL_INDEX_VERSION],
    );
    hash_bytes(
        &mut hash,
        &[match analyzer.kind() {
            LexicalAnalyzerKind::Standard => 1,
        }],
    );
    hash_bytes(&mut hash, &[u8::from(analyzer.lowercase())]);
    hash_bytes(&mut hash, &DEFAULT_BM25_K1.to_bits().to_le_bytes());
    hash_bytes(&mut hash, &DEFAULT_BM25_B.to_bits().to_le_bytes());
    for field in fields {
        hash_bytes(&mut hash, &[0xff]);
        for segment in field.segments() {
            hash_bytes(&mut hash, segment.as_bytes());
            hash_bytes(&mut hash, &[0]);
        }
    }
    hash
}

fn hash_bytes(hash: &mut u64, bytes: &[u8]) {
    for byte in bytes {
        *hash ^= u64::from(*byte);
        *hash = hash.wrapping_mul(0x100000001b3);
    }
}

pub(crate) fn analyze(text: &str, analyzer: LexicalAnalyzerConfig) -> Vec<String> {
    let mut tokens = Vec::new();
    let mut token = String::new();
    for character in text.chars() {
        if character.is_alphanumeric() {
            if analyzer.lowercase() {
                token.extend(character.to_lowercase());
            } else {
                token.push(character);
            }
        } else if !token.is_empty() {
            tokens.push(std::mem::take(&mut token));
        }
    }
    if !token.is_empty() {
        tokens.push(token);
    }
    tokens
}

fn resolve<'a>(path: &FieldPath, metadata: &'a Metadata) -> Option<&'a MetadataValue> {
    let mut segments = path.segments().iter();
    let mut current = metadata.get(segments.next()?)?;
    for segment in segments {
        current = match current {
            MetadataValue::Object(object) => object.get(segment)?,
            _ => return None,
        };
    }
    Some(current)
}

struct VisibleVersion {
    sequence: SequenceNumber,
    record: Option<Record>,
}

fn fold_visible_records(
    segments: &[Segment],
    collection_id: &CollectionId,
) -> BTreeMap<RecordId, Record> {
    let mut latest = BTreeMap::<RecordId, VisibleVersion>::new();
    for segment in segments
        .iter()
        .filter(|segment| segment.collection_id() == collection_id)
    {
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

fn encode_snapshot(snapshot: &Snapshot) -> Result<Vec<u8>, LexicalIndexError> {
    let mut payload = Vec::new();
    payload.extend_from_slice(&snapshot.fingerprint.to_le_bytes());
    write_string(&mut payload, snapshot.collection_id.as_str())?;
    write_len(
        &mut payload,
        snapshot.fields.len(),
        "too many lexical fields",
    )?;
    for field in &snapshot.fields {
        write_len(
            &mut payload,
            field.segments().len(),
            "too many field path segments",
        )?;
        for segment in field.segments() {
            write_string(&mut payload, segment)?;
        }
    }
    payload.push(match snapshot.analyzer.kind() {
        LexicalAnalyzerKind::Standard => 1,
    });
    payload.push(u8::from(snapshot.analyzer.lowercase()));
    payload.extend_from_slice(&snapshot.average_length.to_bits().to_le_bytes());
    write_len(
        &mut payload,
        snapshot.documents.len(),
        "too many lexical documents",
    )?;
    for document in &snapshot.documents {
        write_record_id(&mut payload, &document.id)?;
        payload.extend_from_slice(&document.sequence.get().to_le_bytes());
        payload.extend_from_slice(&document.length.to_le_bytes());
    }
    write_len(
        &mut payload,
        snapshot.postings.len(),
        "too many lexical terms",
    )?;
    for (term, postings) in &snapshot.postings {
        write_string(&mut payload, term)?;
        write_len(&mut payload, postings.len(), "too many postings")?;
        for posting in postings {
            payload.extend_from_slice(&posting.document.to_le_bytes());
            payload.extend_from_slice(&posting.term_frequency.to_le_bytes());
        }
    }

    let mut output = Vec::with_capacity(HEADER_LEN + payload.len());
    output.extend_from_slice(&MAGIC);
    output.push(LEXICAL_INDEX_VERSION);
    output.extend_from_slice(&[0, 0, 0]);
    output.extend_from_slice(
        &u64::try_from(payload.len())
            .map_err(|_| LexicalIndexError::Corrupt("lexical payload too large"))?
            .to_le_bytes(),
    );
    output.extend_from_slice(&crc32(&payload).to_le_bytes());
    output.extend_from_slice(&payload);
    Ok(output)
}

fn read_snapshot(path: &Path) -> Result<Snapshot, LexicalIndexError> {
    let mut bytes = Vec::new();
    File::open(path)?.read_to_end(&mut bytes)?;
    decode_snapshot(&bytes)
}

fn decode_snapshot(bytes: &[u8]) -> Result<Snapshot, LexicalIndexError> {
    if bytes.len() < HEADER_LEN {
        return Err(LexicalIndexError::Corrupt("truncated lexical header"));
    }
    if bytes[0..4] != MAGIC {
        return Err(LexicalIndexError::InvalidMagic);
    }
    if bytes[4] != LEXICAL_INDEX_VERSION {
        return Err(LexicalIndexError::UnsupportedVersion(bytes[4]));
    }
    let payload_len = usize::try_from(u64::from_le_bytes(
        bytes[8..16].try_into().expect("fixed lexical header slice"),
    ))
    .map_err(|_| LexicalIndexError::Corrupt("lexical payload length overflow"))?;
    let expected_len = HEADER_LEN
        .checked_add(payload_len)
        .ok_or(LexicalIndexError::Corrupt(
            "lexical snapshot length overflow",
        ))?;
    if bytes.len() != expected_len {
        return Err(LexicalIndexError::Corrupt(
            "lexical snapshot length mismatch",
        ));
    }
    let expected_crc = u32::from_le_bytes(
        bytes[16..20]
            .try_into()
            .expect("fixed lexical checksum slice"),
    );
    let payload = &bytes[HEADER_LEN..];
    if crc32(payload) != expected_crc {
        return Err(LexicalIndexError::ChecksumMismatch);
    }

    let mut cursor = Cursor::new(payload);
    let fingerprint = cursor.read_u64()?;
    let collection_id = CollectionId::new(cursor.read_string()?)
        .map_err(|error| LexicalIndexError::Domain(error.to_string()))?;
    let field_count = cursor.read_u32()? as usize;
    let mut fields = Vec::with_capacity(field_count);
    for _ in 0..field_count {
        let segment_count = cursor.read_u32()? as usize;
        let mut segments = Vec::with_capacity(segment_count);
        for _ in 0..segment_count {
            segments.push(cursor.read_string()?);
        }
        fields.push(
            FieldPath::new(segments)
                .map_err(|error| LexicalIndexError::Domain(error.to_string()))?,
        );
    }
    let analyzer_kind = match cursor.read_u8()? {
        1 => LexicalAnalyzerKind::Standard,
        _ => return Err(LexicalIndexError::Corrupt("unknown lexical analyzer kind")),
    };
    let lowercase = match cursor.read_u8()? {
        0 => false,
        1 => true,
        _ => return Err(LexicalIndexError::Corrupt("invalid lexical lowercase flag")),
    };
    let analyzer = LexicalAnalyzerConfig::standard(lowercase);
    debug_assert_eq!(analyzer.kind(), analyzer_kind);
    let average_length = f32::from_bits(cursor.read_u32()?);
    if !average_length.is_finite() || average_length < 0.0 {
        return Err(LexicalIndexError::Corrupt(
            "invalid average document length",
        ));
    }
    let document_count = cursor.read_u32()? as usize;
    let mut documents = Vec::with_capacity(document_count);
    for _ in 0..document_count {
        documents.push(SnapshotDocument {
            id: cursor.read_record_id()?,
            sequence: SequenceNumber::new(cursor.read_u64()?),
            length: cursor.read_u32()?,
        });
    }
    let term_count = cursor.read_u32()? as usize;
    let mut postings = BTreeMap::new();
    for _ in 0..term_count {
        let term = cursor.read_string()?;
        let posting_count = cursor.read_u32()? as usize;
        let mut list = Vec::with_capacity(posting_count);
        let mut previous = None;
        for _ in 0..posting_count {
            let posting = Posting {
                document: cursor.read_u32()?,
                term_frequency: cursor.read_u32()?,
            };
            if posting.term_frequency == 0 || posting.document as usize >= document_count {
                return Err(LexicalIndexError::Corrupt("invalid lexical posting"));
            }
            if previous.is_some_and(|value| posting.document <= value) {
                return Err(LexicalIndexError::Corrupt(
                    "lexical postings are not ordered",
                ));
            }
            previous = Some(posting.document);
            list.push(posting);
        }
        if postings.insert(term, list).is_some() {
            return Err(LexicalIndexError::Corrupt("duplicate lexical term"));
        }
    }
    if !cursor.finished() {
        return Err(LexicalIndexError::Corrupt("trailing lexical payload bytes"));
    }
    Ok(Snapshot {
        fingerprint,
        collection_id,
        fields,
        analyzer,
        documents,
        postings,
        average_length,
    })
}

fn write_record_id(output: &mut Vec<u8>, id: &RecordId) -> Result<(), LexicalIndexError> {
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

fn write_string(output: &mut Vec<u8>, value: &str) -> Result<(), LexicalIndexError> {
    write_len(output, value.len(), "lexical string too large")?;
    output.extend_from_slice(value.as_bytes());
    Ok(())
}

fn write_len(
    output: &mut Vec<u8>,
    value: usize,
    message: &'static str,
) -> Result<(), LexicalIndexError> {
    output.extend_from_slice(
        &u32::try_from(value)
            .map_err(|_| LexicalIndexError::Corrupt(message))?
            .to_le_bytes(),
    );
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

    fn take(&mut self, length: usize) -> Result<&'a [u8], LexicalIndexError> {
        let end = self
            .offset
            .checked_add(length)
            .ok_or(LexicalIndexError::Corrupt("lexical cursor overflow"))?;
        if end > self.bytes.len() {
            return Err(LexicalIndexError::Corrupt("truncated lexical payload"));
        }
        let value = &self.bytes[self.offset..end];
        self.offset = end;
        Ok(value)
    }

    fn read_u8(&mut self) -> Result<u8, LexicalIndexError> {
        Ok(self.take(1)?[0])
    }

    fn read_u32(&mut self) -> Result<u32, LexicalIndexError> {
        Ok(u32::from_le_bytes(
            self.take(4)?.try_into().expect("fixed u32 slice"),
        ))
    }

    fn read_u64(&mut self) -> Result<u64, LexicalIndexError> {
        Ok(u64::from_le_bytes(
            self.take(8)?.try_into().expect("fixed u64 slice"),
        ))
    }

    fn read_string(&mut self) -> Result<String, LexicalIndexError> {
        let length = self.read_u32()? as usize;
        String::from_utf8(self.take(length)?.to_vec())
            .map_err(|_| LexicalIndexError::Corrupt("invalid UTF-8 in lexical snapshot"))
    }

    fn read_record_id(&mut self) -> Result<RecordId, LexicalIndexError> {
        match self.read_u8()? {
            0 => RecordId::string(self.read_string()?)
                .map_err(|error| LexicalIndexError::Domain(error.to_string())),
            1 => Ok(RecordId::unsigned(self.read_u64()?)),
            _ => Err(LexicalIndexError::Corrupt("unknown record id tag")),
        }
    }

    fn finished(&self) -> bool {
        self.offset == self.bytes.len()
    }
}

fn crc32(bytes: &[u8]) -> u32 {
    let mut crc = 0xffff_ffff_u32;
    for byte in bytes {
        crc ^= u32::from(*byte);
        for _ in 0..8 {
            let mask = 0_u32.wrapping_sub(crc & 1);
            crc = (crc >> 1) ^ (0xedb8_8320 & mask);
        }
    }
    !crc
}

fn sync_directory(directory: &Path) -> Result<(), LexicalIndexError> {
    #[cfg(unix)]
    {
        File::open(directory)?.sync_all()?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{CheckpointStore, SegmentId, WalMutation};
    use ketebe_core::{MetadataValue, Vector};
    use std::time::{SystemTime, UNIX_EPOCH};

    fn path(parts: &[&str]) -> FieldPath {
        FieldPath::new(parts.iter().copied()).unwrap()
    }

    fn record(id: u64, sequence: u64, title: &str) -> WalMutation {
        let mut metadata = Metadata::new();
        metadata.insert("title".into(), MetadataValue::String(title.into()));
        WalMutation::Upsert {
            collection_id: CollectionId::new("docs").unwrap(),
            record: Record::new(
                RecordId::unsigned(id),
                Vector::new(vec![id as f32]).unwrap(),
                metadata,
                SequenceNumber::new(sequence),
            ),
        }
    }

    fn temp_dir() -> PathBuf {
        std::env::temp_dir().join(format!(
            "ketebe-lexical-index-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("time")
                .as_nanos()
        ))
    }

    fn fixture() -> (Vec<Segment>, Checkpoint) {
        let collection = CollectionId::new("docs").unwrap();
        let first = Segment::from_mutations(
            SegmentId::new(1),
            &[
                record(1, 1, "rust database"),
                record(2, 2, "rust rust search"),
            ],
        )
        .unwrap();
        let second =
            Segment::from_mutations(SegmentId::new(2), &[record(1, 3, "python database")]).unwrap();
        let checkpoint = Checkpoint::new(
            collection,
            vec![SegmentId::new(1), SegmentId::new(2)],
            SequenceNumber::new(3),
        );
        (vec![first, second], checkpoint)
    }

    #[test]
    fn inverted_index_uses_latest_visible_versions() {
        let (segments, checkpoint) = fixture();
        let fields = vec![path(&["title"])];
        let index = LexicalIndex::build(
            &segments,
            checkpoint.collection_id(),
            fields,
            LexicalAnalyzerConfig::default(),
            lexical_checkpoint_fingerprint(
                &checkpoint,
                &[path(&["title"])],
                LexicalAnalyzerConfig::default(),
            ),
        )
        .unwrap();
        let rust = index.search("rust", 10, None).unwrap();
        assert_eq!(rust.len(), 1);
        assert_eq!(rust[0].record().id(), &RecordId::unsigned(2));
        let python = index.search("python", 10, None).unwrap();
        assert_eq!(python[0].record().id(), &RecordId::unsigned(1));
    }

    #[test]
    fn persistent_restart_preserves_scores_and_order() {
        let directory = temp_dir();
        fs::create_dir_all(&directory).unwrap();
        let (segments, checkpoint) = fixture();
        let fields = vec![path(&["title"])];
        let store = LexicalIndexStore::open(&directory).unwrap();
        let built = store
            .rebuild_and_publish(
                &checkpoint,
                fields.clone(),
                LexicalAnalyzerConfig::default(),
                &segments,
            )
            .unwrap();
        let before = built.search("rust search", 10, None).unwrap();
        let loaded = match store
            .load(
                &checkpoint,
                &fields,
                LexicalAnalyzerConfig::default(),
                &segments,
            )
            .unwrap()
        {
            LexicalLoadResult::Loaded(index) => index,
            other => panic!("unexpected load result: {other:?}"),
        };
        let after = loaded.search("rust search", 10, None).unwrap();
        assert_eq!(before, after);
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn changed_checkpoint_does_not_reuse_old_snapshot() {
        let directory = temp_dir();
        fs::create_dir_all(&directory).unwrap();
        let (segments, checkpoint) = fixture();
        let fields = vec![path(&["title"])];
        let store = LexicalIndexStore::open(&directory).unwrap();
        store
            .rebuild_and_publish(
                &checkpoint,
                fields.clone(),
                LexicalAnalyzerConfig::default(),
                &segments,
            )
            .unwrap();
        let newer = Checkpoint::new(
            checkpoint.collection_id().clone(),
            checkpoint.segments().to_vec(),
            SequenceNumber::new(4),
        );
        assert!(matches!(
            store
                .load(&newer, &fields, LexicalAnalyzerConfig::default(), &segments)
                .unwrap(),
            LexicalLoadResult::Missing
        ));
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn corrupted_snapshot_is_rejected() {
        let directory = temp_dir();
        fs::create_dir_all(&directory).unwrap();
        let (segments, checkpoint) = fixture();
        let fields = vec![path(&["title"])];
        let store = LexicalIndexStore::open(&directory).unwrap();
        let index = store
            .rebuild_and_publish(
                &checkpoint,
                fields.clone(),
                LexicalAnalyzerConfig::default(),
                &segments,
            )
            .unwrap();
        let path = store.path_for(index.source_fingerprint());
        let mut bytes = fs::read(&path).unwrap();
        let last = bytes.len() - 1;
        bytes[last] ^= 0xff;
        fs::write(&path, bytes).unwrap();
        assert!(matches!(
            store.load(
                &checkpoint,
                &fields,
                LexicalAnalyzerConfig::default(),
                &segments
            ),
            Err(LexicalIndexError::ChecksumMismatch)
        ));
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn garbage_collection_keeps_only_current_snapshot() {
        let directory = temp_dir();
        fs::create_dir_all(&directory).unwrap();
        let (segments, checkpoint) = fixture();
        let fields = vec![path(&["title"])];
        let store = LexicalIndexStore::open(&directory).unwrap();
        let first = store
            .rebuild_and_publish(
                &checkpoint,
                fields.clone(),
                LexicalAnalyzerConfig::default(),
                &segments,
            )
            .unwrap();
        let newer = Checkpoint::new(
            checkpoint.collection_id().clone(),
            checkpoint.segments().to_vec(),
            SequenceNumber::new(4),
        );
        let second = store
            .rebuild_and_publish(&newer, fields, LexicalAnalyzerConfig::default(), &segments)
            .unwrap();
        assert_ne!(first.source_fingerprint(), second.source_fingerprint());
        assert_eq!(
            store.garbage_collect(second.source_fingerprint()).unwrap(),
            1
        );
        assert!(!store.path_for(first.source_fingerprint()).exists());
        assert!(store.path_for(second.source_fingerprint()).exists());
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn checkpoint_store_fixture_compiles_with_lexical_lifecycle() {
        let directory = temp_dir();
        fs::create_dir_all(&directory).unwrap();
        let (_, checkpoint) = fixture();
        CheckpointStore::open(&directory)
            .unwrap()
            .publish(&checkpoint)
            .unwrap();
        assert_eq!(
            CheckpointStore::open(&directory)
                .unwrap()
                .load()
                .unwrap()
                .unwrap()
                .sequence_number(),
            checkpoint.sequence_number()
        );
        fs::remove_dir_all(directory).unwrap();
    }
}
