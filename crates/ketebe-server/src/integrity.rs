use crate::runtime::{PersistedCollection, PersistedMetric};
use ketebe_core::{CollectionConfig, CollectionId, FieldPath};
use ketebe_storage::{
    CheckpointStore, HnswConfig, HnswIndexStore, HnswLoadResult, LexicalIndexStore,
    LexicalLoadResult, Segment, SegmentId, SegmentStore, WalMutation, replay_wal_path,
};
use serde::Serialize;
use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum IntegrityStatus {
    Healthy,
    Degraded,
    Corrupt,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum IntegrityClass {
    Authoritative,
    Derived,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum IntegrityOutcome {
    Ok,
    Warning,
    Error,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct IntegrityCheck {
    pub code: String,
    pub class: IntegrityClass,
    pub outcome: IntegrityOutcome,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct IntegrityReport {
    pub collection_id: String,
    pub status: IntegrityStatus,
    pub authoritative_ok: bool,
    pub derived_ok: bool,
    pub checks: Vec<IntegrityCheck>,
}

impl IntegrityReport {
    fn new(collection_id: &CollectionId) -> Self {
        Self {
            collection_id: collection_id.as_str().to_string(),
            status: IntegrityStatus::Healthy,
            authoritative_ok: true,
            derived_ok: true,
            checks: Vec::new(),
        }
    }

    fn push(
        &mut self,
        code: impl Into<String>,
        class: IntegrityClass,
        outcome: IntegrityOutcome,
        message: impl Into<String>,
    ) {
        if outcome != IntegrityOutcome::Ok {
            match class {
                IntegrityClass::Authoritative => {
                    if outcome == IntegrityOutcome::Error {
                        self.authoritative_ok = false;
                        self.status = IntegrityStatus::Corrupt;
                    } else if self.status == IntegrityStatus::Healthy {
                        self.status = IntegrityStatus::Degraded;
                    }
                }
                IntegrityClass::Derived => {
                    self.derived_ok = false;
                    if self.status == IntegrityStatus::Healthy {
                        self.status = IntegrityStatus::Degraded;
                    }
                }
            }
        }
        self.checks.push(IntegrityCheck {
            code: code.into(),
            class,
            outcome,
            message: message.into(),
        });
    }
}

#[derive(Debug)]
pub enum IntegrityError {
    CollectionNotFound(CollectionId),
    Scope(String),
    Io(std::io::Error),
}

impl std::fmt::Display for IntegrityError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::CollectionNotFound(id) => write!(f, "collection not found: {id}"),
            Self::Scope(message) => write!(f, "integrity scope error: {message}"),
            Self::Io(error) => write!(f, "integrity verifier I/O error: {error}"),
        }
    }
}
impl std::error::Error for IntegrityError {}
impl From<std::io::Error> for IntegrityError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

#[derive(Debug, Clone)]
pub struct IntegrityVerifier {
    data_dir: PathBuf,
}

impl IntegrityVerifier {
    #[must_use]
    pub fn new(data_dir: impl Into<PathBuf>) -> Self {
        Self {
            data_dir: data_dir.into(),
        }
    }

    pub fn verify_collection(
        &self,
        collection_id: &CollectionId,
    ) -> Result<IntegrityReport, IntegrityError> {
        let legacy_dir = self
            .data_dir
            .join("collections")
            .join(collection_id.as_str());
        let collection_dir = match crate::CollectionNamespaceCatalog::open(&self.data_dir)
            .ok()
            .and_then(|catalog| {
                catalog
                    .find_scope_by_collection_id(collection_id)
                    .ok()
                    .flatten()
            }) {
            Some(scope) => {
                match ketebe_storage::ScopedStorageNamespace::open_existing(&self.data_dir, scope) {
                    Ok(namespace) => namespace.root().to_path_buf(),
                    Err(ketebe_storage::NamespaceError::MissingNamespace(_))
                        if !collection_id.as_str().starts_with("c_") && legacy_dir.is_dir() =>
                    {
                        legacy_dir
                    }
                    Err(error) => return Err(IntegrityError::Scope(error.to_string())),
                }
            }
            None if legacy_dir.is_dir() => legacy_dir,
            None => return Err(IntegrityError::CollectionNotFound(collection_id.clone())),
        };
        let mut report = IntegrityReport::new(collection_id);

        let config = match self.verify_metadata(&collection_dir, collection_id, &mut report) {
            Some(config) => config,
            None => return Ok(report),
        };

        let checkpoint = match CheckpointStore::open(&collection_dir).and_then(|store| store.load())
        {
            Ok(value) => {
                report.push(
                    "checkpoint_decode",
                    IntegrityClass::Authoritative,
                    IntegrityOutcome::Ok,
                    "checkpoint framing and checksum are valid",
                );
                value
            }
            Err(error) => {
                report.push(
                    "checkpoint_decode",
                    IntegrityClass::Authoritative,
                    IntegrityOutcome::Error,
                    error.to_string(),
                );
                None
            }
        };

        if let Some(value) = &checkpoint
            && value.collection_id() != collection_id
        {
            report.push(
                "checkpoint_collection",
                IntegrityClass::Authoritative,
                IntegrityOutcome::Error,
                "checkpoint belongs to a different collection",
            );
        }

        let segments = self.verify_segments(
            &collection_dir,
            collection_id,
            &config,
            checkpoint.as_ref(),
            &mut report,
        );
        self.verify_wal(
            &collection_dir,
            collection_id,
            &config,
            checkpoint.as_ref(),
            &mut report,
        );

        if let (Some(checkpoint), Some(segments)) = (checkpoint.as_ref(), segments.as_ref()) {
            self.verify_derived(&collection_dir, checkpoint, &config, segments, &mut report);
        } else {
            report.push(
                "derived_verification",
                IntegrityClass::Derived,
                IntegrityOutcome::Warning,
                "derived indexes cannot be compatibility-checked without a valid checkpoint and segments",
            );
        }

        Ok(report)
    }

    fn verify_metadata(
        &self,
        collection_dir: &Path,
        collection_id: &CollectionId,
        report: &mut IntegrityReport,
    ) -> Option<CollectionConfig> {
        let path = collection_dir.join("collection.json");
        let bytes = match fs::read(&path) {
            Ok(value) => value,
            Err(error) => {
                report.push(
                    "collection_metadata",
                    IntegrityClass::Authoritative,
                    IntegrityOutcome::Error,
                    format!("cannot read collection metadata: {error}"),
                );
                return None;
            }
        };
        let persisted: PersistedCollection = match serde_json::from_slice(&bytes) {
            Ok(value) => value,
            Err(error) => {
                report.push(
                    "collection_metadata",
                    IntegrityClass::Authoritative,
                    IntegrityOutcome::Error,
                    format!("invalid collection metadata JSON: {error}"),
                );
                return None;
            }
        };
        if !matches!(persisted.version, 1..=6) {
            report.push(
                "collection_metadata_version",
                IntegrityClass::Authoritative,
                IntegrityOutcome::Error,
                format!(
                    "unsupported collection metadata version {}",
                    persisted.version
                ),
            );
            return None;
        }
        if persisted.id != collection_id.as_str() {
            report.push(
                "collection_metadata_identity",
                IntegrityClass::Authoritative,
                IntegrityOutcome::Error,
                format!(
                    "collection metadata id '{}' does not match directory id '{}'",
                    persisted.id,
                    collection_id.as_str()
                ),
            );
            return None;
        }
        let lexical_fields = match persisted
            .lexical_fields
            .into_iter()
            .map(FieldPath::new)
            .collect::<Result<Vec<_>, _>>()
        {
            Ok(value) => value,
            Err(error) => {
                report.push(
                    "collection_metadata_schema",
                    IntegrityClass::Authoritative,
                    IntegrityOutcome::Error,
                    error.to_string(),
                );
                return None;
            }
        };
        let metric = match persisted.metric {
            PersistedMetric::Cosine => ketebe_core::DistanceMetric::Cosine,
            PersistedMetric::Dot => ketebe_core::DistanceMetric::Dot,
            PersistedMetric::L2 => ketebe_core::DistanceMetric::L2,
        };
        let config = match CollectionConfig::new(collection_id.clone(), persisted.dimension, metric)
        {
            Ok(value) => value
                .with_lexical_fields(lexical_fields)
                .with_lexical_analyzer(persisted.lexical_analyzer.into_domain()),
            Err(error) => {
                report.push(
                    "collection_metadata_schema",
                    IntegrityClass::Authoritative,
                    IntegrityOutcome::Error,
                    error.to_string(),
                );
                return None;
            }
        };
        report.push(
            "collection_metadata",
            IntegrityClass::Authoritative,
            IntegrityOutcome::Ok,
            "collection metadata is valid",
        );
        Some(config)
    }

    fn verify_segments(
        &self,
        collection_dir: &Path,
        collection_id: &CollectionId,
        config: &CollectionConfig,
        checkpoint: Option<&ketebe_storage::Checkpoint>,
        report: &mut IntegrityReport,
    ) -> Option<Vec<Segment>> {
        let segment_dir = collection_dir.join("segments");
        if !segment_dir.is_dir() {
            let required = checkpoint.is_some_and(|value| !value.segments().is_empty());
            report.push(
                "segment_directory",
                IntegrityClass::Authoritative,
                if required {
                    IntegrityOutcome::Error
                } else {
                    IntegrityOutcome::Ok
                },
                if required {
                    "checkpoint references segments but segment directory is missing"
                } else {
                    "segment directory is absent and checkpoint references no segments"
                },
            );
            return if required { None } else { Some(Vec::new()) };
        }

        let store = match SegmentStore::open(&segment_dir) {
            Ok(value) => value,
            Err(error) => {
                report.push(
                    "segment_store",
                    IntegrityClass::Authoritative,
                    IntegrityOutcome::Error,
                    error.to_string(),
                );
                return None;
            }
        };
        let mut disk_ids = BTreeSet::new();
        if let Ok(entries) = fs::read_dir(&segment_dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.extension().and_then(|value| value.to_str()) != Some("kseg") {
                    continue;
                }
                if let Some(id) = path
                    .file_stem()
                    .and_then(|value| value.to_str())
                    .and_then(|value| value.parse::<u64>().ok())
                {
                    disk_ids.insert(SegmentId::new(id));
                }
            }
        }
        if let Some(checkpoint) = checkpoint {
            let referenced = checkpoint
                .segments()
                .iter()
                .copied()
                .collect::<BTreeSet<_>>();
            for missing in referenced.difference(&disk_ids) {
                report.push(
                    "missing_segment_reference",
                    IntegrityClass::Authoritative,
                    IntegrityOutcome::Error,
                    format!("checkpoint references missing segment {}", missing.get()),
                );
            }
            for orphan in disk_ids.difference(&referenced) {
                report.push(
                    "orphan_segment",
                    IntegrityClass::Authoritative,
                    IntegrityOutcome::Warning,
                    format!(
                        "segment {} exists but is not referenced by checkpoint",
                        orphan.get()
                    ),
                );
            }
        }

        let discovered = match store.discover() {
            Ok(value) => value,
            Err(error) => {
                report.push(
                    "segment_decode",
                    IntegrityClass::Authoritative,
                    IntegrityOutcome::Error,
                    error.to_string(),
                );
                return None;
            }
        };
        let mut authoritative = Vec::new();
        for segment in discovered {
            if segment.collection_id() != collection_id {
                report.push(
                    "segment_collection",
                    IntegrityClass::Authoritative,
                    IntegrityOutcome::Error,
                    format!(
                        "segment {} belongs to another collection",
                        segment.id().get()
                    ),
                );
                continue;
            }
            let referenced = checkpoint
                .map(|value| value.segments().contains(&segment.id()))
                .unwrap_or(true);
            if !referenced {
                continue;
            }
            if let Some(value) = checkpoint
                && segment.max_sequence() > value.sequence_number()
            {
                report.push(
                    "segment_checkpoint_sequence",
                    IntegrityClass::Authoritative,
                    IntegrityOutcome::Error,
                    format!(
                        "segment {} max sequence {} exceeds checkpoint sequence {}",
                        segment.id().get(),
                        segment.max_sequence().get(),
                        value.sequence_number().get()
                    ),
                );
            }
            for record in segment.records() {
                if record.sequence_number() < segment.min_sequence()
                    || record.sequence_number() > segment.max_sequence()
                {
                    report.push(
                        "segment_record_sequence",
                        IntegrityClass::Authoritative,
                        IntegrityOutcome::Error,
                        format!(
                            "record sequence is outside segment {} range",
                            segment.id().get()
                        ),
                    );
                }
                if let Err(error) = config.validate_record(record) {
                    report.push(
                        "segment_record_domain",
                        IntegrityClass::Authoritative,
                        IntegrityOutcome::Error,
                        format!("segment {}: {error}", segment.id().get()),
                    );
                }
            }
            for tombstone in segment.tombstones() {
                if tombstone.sequence_number() < segment.min_sequence()
                    || tombstone.sequence_number() > segment.max_sequence()
                {
                    report.push(
                        "segment_tombstone_sequence",
                        IntegrityClass::Authoritative,
                        IntegrityOutcome::Error,
                        format!(
                            "tombstone sequence is outside segment {} range",
                            segment.id().get()
                        ),
                    );
                }
            }
            authoritative.push(segment);
        }
        report.push(
            "segment_decode",
            IntegrityClass::Authoritative,
            IntegrityOutcome::Ok,
            format!("verified {} referenced segment(s)", authoritative.len()),
        );
        Some(authoritative)
    }

    fn verify_wal(
        &self,
        collection_dir: &Path,
        collection_id: &CollectionId,
        config: &CollectionConfig,
        checkpoint: Option<&ketebe_storage::Checkpoint>,
        report: &mut IntegrityReport,
    ) {
        let wal_path = collection_dir.join("wal.log");
        let replay = match replay_wal_path(&wal_path) {
            Ok(value) => value,
            Err(error) => {
                report.push(
                    "wal_decode",
                    IntegrityClass::Authoritative,
                    IntegrityOutcome::Error,
                    error.to_string(),
                );
                return;
            }
        };
        if replay.ignored_tail_bytes > 0 {
            report.push(
                "wal_truncated_tail",
                IntegrityClass::Authoritative,
                IntegrityOutcome::Warning,
                format!(
                    "WAL has {} ignored trailing byte(s) from an incomplete final frame",
                    replay.ignored_tail_bytes
                ),
            );
        }
        let checkpoint_sequence = checkpoint
            .map(|value| value.sequence_number().get())
            .unwrap_or(0);
        for mutation in &replay.entries {
            let mutation_collection = match mutation {
                WalMutation::Upsert { collection_id, .. }
                | WalMutation::Delete { collection_id, .. } => collection_id,
            };
            if mutation_collection != collection_id {
                report.push(
                    "wal_collection",
                    IntegrityClass::Authoritative,
                    IntegrityOutcome::Error,
                    format!(
                        "WAL sequence {} belongs to another collection",
                        mutation.sequence_number().get()
                    ),
                );
            }
            if let WalMutation::Upsert { record, .. } = mutation
                && let Err(error) = config.validate_record(record)
            {
                report.push(
                    "wal_record_domain",
                    IntegrityClass::Authoritative,
                    IntegrityOutcome::Error,
                    format!("WAL sequence {}: {error}", mutation.sequence_number().get()),
                );
            }
        }
        let live_entries = replay
            .entries
            .iter()
            .filter(|mutation| mutation.sequence_number().get() > checkpoint_sequence)
            .count();
        report.push(
            "wal_decode",
            IntegrityClass::Authoritative,
            IntegrityOutcome::Ok,
            format!(
                "verified {} WAL frame(s); {} frame(s) are newer than checkpoint",
                replay.entries.len(),
                live_entries
            ),
        );
    }

    fn verify_derived(
        &self,
        collection_dir: &Path,
        checkpoint: &ketebe_storage::Checkpoint,
        config: &CollectionConfig,
        segments: &[Segment],
        report: &mut IntegrityReport,
    ) {
        let hnsw_path = collection_dir.join("indexes").join("hnsw.kthi");
        if !hnsw_path.exists() {
            report.push(
                "hnsw_compatibility",
                IntegrityClass::Derived,
                IntegrityOutcome::Warning,
                "HNSW snapshot is missing and can be rebuilt",
            );
        } else {
            match HnswIndexStore::open(collection_dir).and_then(|store| {
                store.load(checkpoint, config.distance_metric(), HnswConfig::default())
            }) {
                Ok(HnswLoadResult::Loaded(_)) => report.push(
                    "hnsw_compatibility",
                    IntegrityClass::Derived,
                    IntegrityOutcome::Ok,
                    "HNSW snapshot matches the checkpoint fingerprint",
                ),
                Ok(HnswLoadResult::Missing | HnswLoadResult::Stale) => report.push(
                    "hnsw_compatibility",
                    IntegrityClass::Derived,
                    IntegrityOutcome::Warning,
                    "HNSW snapshot is missing or stale and can be rebuilt",
                ),
                Err(error) => report.push(
                    "hnsw_compatibility",
                    IntegrityClass::Derived,
                    IntegrityOutcome::Error,
                    format!("HNSW snapshot is invalid but rebuildable: {error}"),
                ),
            }
        }

        if config.lexical_fields().is_empty() {
            report.push(
                "lexical_compatibility",
                IntegrityClass::Derived,
                IntegrityOutcome::Ok,
                "lexical indexing is disabled",
            );
            return;
        }
        let fingerprint = ketebe_storage::lexical_checkpoint_fingerprint(
            checkpoint,
            config.lexical_fields(),
            config.lexical_analyzer(),
        );
        let lexical_path = collection_dir
            .join("indexes")
            .join("lexical")
            .join(format!("{fingerprint:016x}.ktli"));
        if !lexical_path.exists() {
            report.push(
                "lexical_compatibility",
                IntegrityClass::Derived,
                IntegrityOutcome::Warning,
                "lexical snapshot is missing and can be rebuilt",
            );
            return;
        }
        match LexicalIndexStore::open(collection_dir).and_then(|store| {
            store.load(
                checkpoint,
                config.lexical_fields(),
                config.lexical_analyzer(),
                segments,
            )
        }) {
            Ok(LexicalLoadResult::Loaded(_)) => report.push(
                "lexical_compatibility",
                IntegrityClass::Derived,
                IntegrityOutcome::Ok,
                "lexical snapshot matches the checkpoint fingerprint",
            ),
            Ok(LexicalLoadResult::Missing | LexicalLoadResult::Stale) => report.push(
                "lexical_compatibility",
                IntegrityClass::Derived,
                IntegrityOutcome::Warning,
                "lexical snapshot is missing or stale and can be rebuilt",
            ),
            Err(error) => report.push(
                "lexical_compatibility",
                IntegrityClass::Derived,
                IntegrityOutcome::Error,
                format!("lexical snapshot is invalid but rebuildable: {error}"),
            ),
        }
    }
}
