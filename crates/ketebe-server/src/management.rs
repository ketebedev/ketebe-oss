use ketebe_core::{
    CollectionId, CollectionIngestionConfig, DistanceMetric, FieldPath, LexicalAnalyzerConfig,
    RecordId, SequenceNumber,
};
use ketebe_storage::{HnswConfig, Segment};
use std::collections::BTreeMap;
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::runtime::{AppState, LexicalBuildState};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HnswState {
    Ready,
    Unavailable,
}

#[derive(Debug, Clone)]
pub struct CollectionInfo {
    pub id: CollectionId,
    pub dimension: usize,
    pub metric: DistanceMetric,
    pub live_records: usize,
    pub tombstones: usize,
    pub immutable_segments: usize,
    pub mutable_mutations: usize,
    pub checkpoint_sequence: Option<u64>,
    pub next_sequence: u64,
    pub hnsw_state: HnswState,
    pub hnsw_config: Option<HnswConfig>,
    pub lexical_fields: Vec<FieldPath>,
    pub lexical_analyzer: LexicalAnalyzerConfig,
    pub lexical_state: LexicalBuildState,
    pub ingestion: Option<CollectionIngestionConfig>,
}

#[derive(Clone)]
pub struct CollectionService {
    state: AppState,
}

impl CollectionService {
    #[must_use]
    pub fn new(state: AppState) -> Self {
        Self { state }
    }

    pub async fn list(&self) -> Result<Vec<CollectionInfo>, ManagementError> {
        let catalog = self.state.catalog.read().await;
        catalog
            .collections
            .iter()
            .map(|(id, runtime)| collection_info(id, runtime))
            .collect()
    }

    pub async fn get(&self, id: &CollectionId) -> Result<CollectionInfo, ManagementError> {
        let catalog = self.state.catalog.read().await;
        let runtime = catalog
            .collections
            .get(id)
            .ok_or_else(|| ManagementError::CollectionNotFound(id.clone()))?;
        collection_info(id, runtime)
    }

    pub async fn delete(&self, id: &CollectionId) -> Result<(), ManagementError> {
        let collections_dir = self.state.data_dir.join("collections");
        let deleting_dir = self.state.data_dir.join("deleting");
        fs::create_dir_all(&deleting_dir)?;
        let legacy_source = collections_dir.join(id.as_str());
        let (source, source_parent) =
            match crate::data_plane_request::scope_for_collection_id(&self.state, id)
                .map_err(|error| ManagementError::Scope(error.to_string()))?
            {
                Some(scope) => match ketebe_storage::ScopedStorageNamespace::open_existing(
                    &*self.state.data_dir,
                    scope,
                ) {
                    Ok(namespace) => {
                        let source = namespace.root().to_path_buf();
                        let parent = source
                            .parent()
                            .ok_or_else(|| {
                                ManagementError::Scope(
                                    "scoped collection directory has no parent".to_string(),
                                )
                            })?
                            .to_path_buf();
                        (source, parent)
                    }
                    Err(ketebe_storage::NamespaceError::MissingNamespace(_))
                        if !id.as_str().starts_with("c_") =>
                    {
                        (legacy_source.clone(), collections_dir.clone())
                    }
                    Err(error) => return Err(ManagementError::Scope(error.to_string())),
                },
                None if id.as_str().starts_with("c_") => {
                    return Err(ManagementError::Scope(
                        "stable collection identity has no project namespace binding".to_string(),
                    ));
                }
                None => (legacy_source.clone(), collections_dir.clone()),
            };
        let deleting = deleting_path(&deleting_dir, id);

        {
            let mut catalog = self.state.catalog.write().await;
            let runtime = catalog
                .collections
                .remove(id)
                .ok_or_else(|| ManagementError::CollectionNotFound(id.clone()))?;
            self.state.lexical_scheduler.cancel(id);

            if let Err(error) = fs::rename(&source, &deleting) {
                catalog.collections.insert(id.clone(), runtime);
                return Err(ManagementError::Io(error));
            }

            if let Err(error) = sync_directory(&source_parent) {
                return Err(ManagementError::Io(error));
            }
            if let Err(error) = sync_directory(&deleting_dir) {
                return Err(ManagementError::Io(error));
            }
        }

        if deleting.exists() {
            fs::remove_dir_all(&deleting)?;
            sync_directory(&deleting_dir)?;
        }

        // A pre-scope collection can acquire a durable default-project binding while its
        // original directory still exists for compatibility. Once the scoped namespace is
        // authoritative, deleting only that namespace would leave stale legacy data behind and
        // allow a later recovery to rediscover the deleted collection. Remove the compatibility
        // directory as part of the same logical delete when it is distinct from the source that
        // was already moved through the crash-safe deleting directory.
        if legacy_source != source && legacy_source.exists() {
            fs::remove_dir_all(&legacy_source)?;
            sync_directory(&collections_dir)?;
        }
        Ok(())
    }
}

fn collection_info(
    id: &CollectionId,
    runtime: &crate::runtime::CollectionRuntime,
) -> Result<CollectionInfo, ManagementError> {
    let config = runtime
        .config
        .as_ref()
        .ok_or(ManagementError::CollectionNotManageable)?;
    let segments = runtime.query_segments()?;
    let (live_records, tombstones) = latest_state_counts(&segments, id);
    let hnsw = runtime.query_hnsw();

    Ok(CollectionInfo {
        id: id.clone(),
        dimension: config.dimension(),
        metric: config.distance_metric(),
        live_records,
        tombstones,
        immutable_segments: runtime.segments.len(),
        mutable_mutations: runtime.mutable.len(),
        checkpoint_sequence: runtime
            .checkpoint
            .as_ref()
            .map(|checkpoint| checkpoint.sequence_number().get()),
        next_sequence: runtime.next_sequence,
        hnsw_state: if hnsw.is_some() {
            HnswState::Ready
        } else {
            HnswState::Unavailable
        },
        hnsw_config: hnsw.map(|index| index.config()),
        lexical_fields: config.lexical_fields().to_vec(),
        lexical_analyzer: config.lexical_analyzer(),
        lexical_state: runtime.lexical_state(),
        ingestion: config.ingestion().cloned(),
    })
}

#[derive(Clone, Copy)]
struct LogicalVersion {
    sequence: SequenceNumber,
    live: bool,
}

fn latest_state_counts(segments: &[Segment], collection_id: &CollectionId) -> (usize, usize) {
    let mut latest = BTreeMap::<RecordId, LogicalVersion>::new();
    for segment in segments {
        if segment.collection_id() != collection_id {
            continue;
        }
        for record in segment.records() {
            apply_version(
                &mut latest,
                record.id().clone(),
                LogicalVersion {
                    sequence: record.sequence_number(),
                    live: true,
                },
            );
        }
        for tombstone in segment.tombstones() {
            apply_version(
                &mut latest,
                tombstone.record_id().clone(),
                LogicalVersion {
                    sequence: tombstone.sequence_number(),
                    live: false,
                },
            );
        }
    }

    latest.values().fold((0, 0), |(live, deleted), version| {
        if version.live {
            (live + 1, deleted)
        } else {
            (live, deleted + 1)
        }
    })
}

fn apply_version(
    latest: &mut BTreeMap<RecordId, LogicalVersion>,
    id: RecordId,
    version: LogicalVersion,
) {
    match latest.get(&id) {
        Some(existing) if existing.sequence >= version.sequence => {}
        _ => {
            latest.insert(id, version);
        }
    }
}

fn deleting_path(deleting_dir: &Path, id: &CollectionId) -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0);
    deleting_dir.join(format!("{}-{nonce}", id.as_str()))
}

fn sync_directory(path: &Path) -> Result<(), std::io::Error> {
    #[cfg(unix)]
    File::open(path)?.sync_all()?;
    Ok(())
}

#[derive(Debug)]
pub enum ManagementError {
    Io(std::io::Error),
    Segment(ketebe_storage::SegmentError),
    CollectionNotFound(CollectionId),
    CollectionNotManageable,
    Scope(String),
}

impl std::fmt::Display for ManagementError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(error) => write!(f, "collection management I/O error: {error}"),
            Self::Segment(error) => write!(f, "collection management segment error: {error}"),
            Self::CollectionNotFound(id) => write!(f, "collection not found: {id}"),
            Self::CollectionNotManageable => {
                f.write_str("collection does not have manageable runtime configuration")
            }
            Self::Scope(message) => write!(f, "collection management scope error: {message}"),
        }
    }
}

impl std::error::Error for ManagementError {}

impl From<std::io::Error> for ManagementError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

impl From<ketebe_storage::SegmentError> for ManagementError {
    fn from(error: ketebe_storage::SegmentError) -> Self {
        Self::Segment(error)
    }
}
