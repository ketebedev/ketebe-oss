use crate::{CompactionError, Segment, SegmentId, compact_segments};
use ketebe_core::DataPlaneScope;
use std::fmt;

/// Compacts only segments belonging to the supplied project+collection data-plane scope.
pub fn compact_scoped_segments(
    scope: &DataPlaneScope,
    replacement_id: SegmentId,
    segments: &[Segment],
) -> Result<Segment, ScopedMaintenanceError> {
    for segment in segments {
        if segment.collection_id() != scope.collection_id() {
            return Err(ScopedMaintenanceError::CollectionMismatch {
                expected: scope.collection_id().as_str().to_string(),
                actual: segment.collection_id().as_str().to_string(),
            });
        }
    }
    let replacement = compact_segments(replacement_id, segments)?;
    if replacement.collection_id() != scope.collection_id() {
        return Err(ScopedMaintenanceError::CollectionMismatch {
            expected: scope.collection_id().as_str().to_string(),
            actual: replacement.collection_id().as_str().to_string(),
        });
    }
    Ok(replacement)
}

#[derive(Debug)]
pub enum ScopedMaintenanceError {
    CollectionMismatch { expected: String, actual: String },
    Compaction(CompactionError),
}

impl fmt::Display for ScopedMaintenanceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CollectionMismatch { expected, actual } => write!(
                formatter,
                "maintenance collection scope mismatch: expected {expected}, found {actual}"
            ),
            Self::Compaction(error) => error.fmt(formatter),
        }
    }
}

impl std::error::Error for ScopedMaintenanceError {}

impl From<CompactionError> for ScopedMaintenanceError {
    fn from(error: CompactionError) -> Self {
        Self::Compaction(error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::WalMutation;
    use ketebe_core::{CollectionId, ProjectId, Record, RecordId, SequenceNumber, Vector};

    fn scope(project: &str, collection: &str) -> DataPlaneScope {
        DataPlaneScope::new(
            ProjectId::new(project).unwrap(),
            CollectionId::new(collection).unwrap(),
        )
    }

    fn segment(collection: &str, id: u64, sequence: u64) -> Segment {
        Segment::from_mutations(
            SegmentId::new(id),
            &[WalMutation::Upsert {
                collection_id: CollectionId::new(collection).unwrap(),
                record: Record::new(
                    RecordId::string(format!("record-{id}")).unwrap(),
                    Vector::new(vec![id as f32]).unwrap(),
                    Default::default(),
                    SequenceNumber::new(sequence),
                ),
            }],
        )
        .unwrap()
    }

    #[test]
    fn compaction_preserves_expected_scope() {
        let expected = scope("project-a", "collection-a");
        let compacted = compact_scoped_segments(
            &expected,
            SegmentId::new(3),
            &[segment("collection-a", 1, 1), segment("collection-a", 2, 2)],
        )
        .unwrap();
        assert_eq!(compacted.collection_id(), expected.collection_id());
    }

    #[test]
    fn compaction_rejects_cross_scope_segment_injection() {
        let expected = scope("project-a", "collection-a");
        let error = compact_scoped_segments(
            &expected,
            SegmentId::new(3),
            &[segment("collection-a", 1, 1), segment("collection-b", 2, 2)],
        )
        .unwrap_err();
        assert!(matches!(
            error,
            ScopedMaintenanceError::CollectionMismatch { .. }
        ));
    }
}
