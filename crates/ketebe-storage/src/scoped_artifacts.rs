use crate::{
    Checkpoint, CheckpointError, CheckpointStore, NamespaceError, ScopedStorageNamespace, Segment,
    SegmentError, SegmentId, SegmentLocation, SegmentStore,
};
use ketebe_core::DataPlaneScope;
use std::fmt;
use std::path::Path;

/// Segment access bound to one validated project+collection namespace.
pub struct ScopedSegmentStore {
    namespace: ScopedStorageNamespace,
    inner: SegmentStore,
}

impl ScopedSegmentStore {
    pub fn open(
        data_dir: impl AsRef<Path>,
        scope: DataPlaneScope,
    ) -> Result<Self, ScopedArtifactError> {
        let namespace = ScopedStorageNamespace::open(data_dir, scope)?;
        let inner = SegmentStore::open(namespace.segments_dir())?;
        let store = Self { namespace, inner };
        store.validate_discovered()?;
        Ok(store)
    }

    pub fn open_existing(
        data_dir: impl AsRef<Path>,
        scope: DataPlaneScope,
    ) -> Result<Self, ScopedArtifactError> {
        let namespace = ScopedStorageNamespace::open_existing(data_dir, scope)?;
        let inner = SegmentStore::open(namespace.segments_dir())?;
        let store = Self { namespace, inner };
        store.validate_discovered()?;
        Ok(store)
    }

    #[must_use]
    pub fn scope(&self) -> &DataPlaneScope {
        self.namespace.scope()
    }

    pub fn publish(&self, segment: &Segment) -> Result<SegmentLocation, ScopedArtifactError> {
        validate_collection(self.scope(), segment.collection_id().as_str(), "segment")?;
        self.inner
            .publish(segment)
            .map_err(ScopedArtifactError::Segment)
    }

    pub fn open_segment(&self, id: SegmentId) -> Result<Segment, ScopedArtifactError> {
        let segment = self.inner.open_segment(id)?;
        validate_collection(self.scope(), segment.collection_id().as_str(), "segment")?;
        Ok(segment)
    }

    pub fn discover(&self) -> Result<Vec<Segment>, ScopedArtifactError> {
        let segments = self.inner.discover()?;
        for segment in &segments {
            validate_collection(self.scope(), segment.collection_id().as_str(), "segment")?;
        }
        Ok(segments)
    }

    pub fn garbage_collect(
        &self,
        keep: &[SegmentId],
    ) -> Result<Vec<SegmentId>, ScopedArtifactError> {
        self.validate_discovered()?;
        crate::garbage_collect_segment_store(&self.inner, keep)
            .map_err(ScopedArtifactError::Compaction)
    }

    fn validate_discovered(&self) -> Result<(), ScopedArtifactError> {
        self.discover().map(|_| ())
    }
}

/// Checkpoint access bound to one validated project+collection namespace.
pub struct ScopedCheckpointStore {
    namespace: ScopedStorageNamespace,
    inner: CheckpointStore,
}

impl ScopedCheckpointStore {
    pub fn open(
        data_dir: impl AsRef<Path>,
        scope: DataPlaneScope,
    ) -> Result<Self, ScopedArtifactError> {
        let namespace = ScopedStorageNamespace::open(data_dir, scope)?;
        let inner = CheckpointStore::open(namespace.root())?;
        let store = Self { namespace, inner };
        let _ = store.load()?;
        Ok(store)
    }

    pub fn open_existing(
        data_dir: impl AsRef<Path>,
        scope: DataPlaneScope,
    ) -> Result<Self, ScopedArtifactError> {
        let namespace = ScopedStorageNamespace::open_existing(data_dir, scope)?;
        let inner = CheckpointStore::open(namespace.root())?;
        let store = Self { namespace, inner };
        let _ = store.load()?;
        Ok(store)
    }

    #[must_use]
    pub fn scope(&self) -> &DataPlaneScope {
        self.namespace.scope()
    }

    #[must_use]
    pub fn collection_root(&self) -> &Path {
        self.namespace.root()
    }

    pub fn publish(&self, checkpoint: &Checkpoint) -> Result<(), ScopedArtifactError> {
        validate_collection(
            self.scope(),
            checkpoint.collection_id().as_str(),
            "checkpoint",
        )?;
        self.inner
            .publish(checkpoint)
            .map_err(ScopedArtifactError::Checkpoint)
    }

    pub fn load(&self) -> Result<Option<Checkpoint>, ScopedArtifactError> {
        let checkpoint = self.inner.load()?;
        if let Some(value) = &checkpoint {
            validate_collection(self.scope(), value.collection_id().as_str(), "checkpoint")?;
        }
        Ok(checkpoint)
    }
}

fn validate_collection(
    scope: &DataPlaneScope,
    actual_collection: &str,
    artifact: &'static str,
) -> Result<(), ScopedArtifactError> {
    if actual_collection != scope.collection_id().as_str() {
        return Err(ScopedArtifactError::CollectionMismatch {
            artifact,
            expected: scope.collection_id().as_str().to_string(),
            actual: actual_collection.to_string(),
        });
    }
    Ok(())
}

#[derive(Debug)]
pub enum ScopedArtifactError {
    Namespace(NamespaceError),
    Segment(SegmentError),
    Compaction(crate::CompactionError),
    Checkpoint(CheckpointError),
    CollectionMismatch {
        artifact: &'static str,
        expected: String,
        actual: String,
    },
}

impl fmt::Display for ScopedArtifactError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Namespace(error) => error.fmt(formatter),
            Self::Segment(error) => error.fmt(formatter),
            Self::Compaction(error) => error.fmt(formatter),
            Self::Checkpoint(error) => error.fmt(formatter),
            Self::CollectionMismatch {
                artifact,
                expected,
                actual,
            } => write!(
                formatter,
                "{artifact} collection scope mismatch: expected {expected}, found {actual}"
            ),
        }
    }
}

impl std::error::Error for ScopedArtifactError {}

impl From<NamespaceError> for ScopedArtifactError {
    fn from(error: NamespaceError) -> Self {
        Self::Namespace(error)
    }
}

impl From<SegmentError> for ScopedArtifactError {
    fn from(error: SegmentError) -> Self {
        Self::Segment(error)
    }
}

impl From<CheckpointError> for ScopedArtifactError {
    fn from(error: CheckpointError) -> Self {
        Self::Checkpoint(error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::WalMutation;
    use ketebe_core::{CollectionId, ProjectId, Record, RecordId, SequenceNumber, Vector};
    use std::fs;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn scope(project: &str, collection: &str) -> DataPlaneScope {
        DataPlaneScope::new(
            ProjectId::new(project).unwrap(),
            CollectionId::new(collection).unwrap(),
        )
    }

    fn temp_root(label: &str) -> PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("ketebe-{label}-{}-{nonce}", std::process::id()))
    }

    fn mutation(collection: &str) -> WalMutation {
        WalMutation::Upsert {
            collection_id: CollectionId::new(collection).unwrap(),
            record: Record::new(
                RecordId::string("same-record").unwrap(),
                Vector::new(vec![1.0, 2.0]).unwrap(),
                Default::default(),
                SequenceNumber::new(1),
            ),
        }
    }

    #[test]
    fn segment_publish_rejects_cross_scope_artifact() {
        let root = temp_root("segment-scope");
        let a = scope("project-a", "collection-a");
        let store = ScopedSegmentStore::open(&root, a).unwrap();
        let foreign =
            Segment::from_mutations(SegmentId::new(1), &[mutation("collection-b")]).unwrap();
        assert!(matches!(
            store.publish(&foreign),
            Err(ScopedArtifactError::CollectionMismatch {
                artifact: "segment",
                ..
            })
        ));
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn checkpoint_reopen_rejects_injected_foreign_collection() {
        let root = temp_root("checkpoint-scope");
        let a = scope("project-a", "collection-a");
        let b = scope("project-b", "collection-b");
        let a_namespace = ScopedStorageNamespace::open(&root, a.clone()).unwrap();
        let b_store = ScopedCheckpointStore::open(&root, b).unwrap();
        b_store
            .publish(&Checkpoint::new(
                CollectionId::new("collection-b").unwrap(),
                vec![],
                SequenceNumber::new(1),
            ))
            .unwrap();
        fs::copy(
            b_store.collection_root().join("checkpoint.ktcp"),
            a_namespace.root().join("checkpoint.ktcp"),
        )
        .unwrap();
        let error = match ScopedCheckpointStore::open_existing(&root, a) {
            Ok(_) => panic!("foreign checkpoint must fail"),
            Err(error) => error,
        };
        assert!(matches!(
            error,
            ScopedArtifactError::CollectionMismatch {
                artifact: "checkpoint",
                ..
            }
        ));
        fs::remove_dir_all(root).unwrap();
    }
}
