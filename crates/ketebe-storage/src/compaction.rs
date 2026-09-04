use crate::{Segment, SegmentError, SegmentId, SegmentStore, WalMutation};
use ketebe_core::{Record, RecordId, SequenceNumber};
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

pub fn compact_segments(
    replacement_id: SegmentId,
    segments: &[Segment],
) -> Result<Segment, CompactionError> {
    if segments.len() < 2 {
        return Err(CompactionError::InsufficientSegments);
    }

    let collection_id = segments[0].collection_id().clone();
    let mut latest = BTreeMap::<RecordId, LatestValue>::new();

    for segment in segments {
        if segment.collection_id() != &collection_id {
            return Err(CompactionError::CollectionMismatch);
        }

        for record in segment.records() {
            apply_latest(
                &mut latest,
                record.id().clone(),
                record.sequence_number(),
                LatestKind::Record(record.clone()),
            );
        }
        for tombstone in segment.tombstones() {
            apply_latest(
                &mut latest,
                tombstone.record_id().clone(),
                tombstone.sequence_number(),
                LatestKind::Tombstone,
            );
        }
    }

    let mut mutations = latest
        .into_iter()
        .map(|(record_id, value)| match value.kind {
            LatestKind::Record(record) => WalMutation::Upsert {
                collection_id: collection_id.clone(),
                record,
            },
            LatestKind::Tombstone => WalMutation::Delete {
                collection_id: collection_id.clone(),
                record_id,
                sequence_number: value.sequence,
            },
        })
        .collect::<Vec<_>>();
    mutations.sort_by_key(WalMutation::sequence_number);

    Segment::from_mutations(replacement_id, &mutations).map_err(CompactionError::Segment)
}

pub fn garbage_collect_segments(
    directory: impl AsRef<Path>,
    retained: &[SegmentId],
) -> Result<Vec<SegmentId>, CompactionError> {
    let store = SegmentStore::open(directory).map_err(CompactionError::Segment)?;
    garbage_collect_segment_store(&store, retained)
}

pub fn garbage_collect_segment_store(
    store: &SegmentStore,
    retained: &[SegmentId],
) -> Result<Vec<SegmentId>, CompactionError> {
    let retained = retained.iter().copied().collect::<BTreeSet<_>>();
    let mut removed = Vec::new();
    for id in store.list_segment_ids().map_err(CompactionError::Segment)? {
        if retained.contains(&id) {
            continue;
        }
        if store.delete_segment(id).map_err(CompactionError::Segment)? {
            removed.push(id);
        }
    }
    removed.sort_unstable();
    Ok(removed)
}

fn apply_latest(
    latest: &mut BTreeMap<RecordId, LatestValue>,
    id: RecordId,
    sequence: SequenceNumber,
    kind: LatestKind,
) {
    match latest.get(&id) {
        Some(existing) if existing.sequence >= sequence => {}
        _ => {
            latest.insert(id, LatestValue { sequence, kind });
        }
    }
}

struct LatestValue {
    sequence: SequenceNumber,
    kind: LatestKind,
}

enum LatestKind {
    Record(Record),
    Tombstone,
}

#[derive(Debug)]
pub enum CompactionError {
    Io(std::io::Error),
    Segment(SegmentError),
    InsufficientSegments,
    CollectionMismatch,
    InvalidSegmentFilename(PathBuf),
}

impl std::fmt::Display for CompactionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(error) => write!(f, "compaction I/O error: {error}"),
            Self::Segment(error) => write!(f, "compaction segment error: {error}"),
            Self::InsufficientSegments => f.write_str("compaction requires at least two segments"),
            Self::CollectionMismatch => {
                f.write_str("cannot compact segments from different collections")
            }
            Self::InvalidSegmentFilename(path) => {
                write!(f, "invalid segment filename: {}", path.display())
            }
        }
    }
}

impl std::error::Error for CompactionError {}

impl From<std::io::Error> for CompactionError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ketebe_core::{CollectionId, Metadata, Vector};
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn collection() -> CollectionId {
        CollectionId::new("docs").expect("collection")
    }

    fn upsert(id: &str, sequence: u64, value: f32) -> WalMutation {
        WalMutation::Upsert {
            collection_id: collection(),
            record: Record::new(
                RecordId::string(id).expect("id"),
                Vector::new(vec![value]).expect("vector"),
                Metadata::new(),
                SequenceNumber::new(sequence),
            ),
        }
    }

    fn delete(id: &str, sequence: u64) -> WalMutation {
        WalMutation::Delete {
            collection_id: collection(),
            record_id: RecordId::string(id).expect("id"),
            sequence_number: SequenceNumber::new(sequence),
        }
    }

    fn temp_dir() -> PathBuf {
        std::env::temp_dir().join(format!(
            "ketebe-compaction-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("time")
                .as_nanos()
        ))
    }

    #[test]
    fn compaction_keeps_only_latest_logical_state() {
        let first = Segment::from_mutations(
            SegmentId::new(1),
            &[upsert("a", 1, 1.0), upsert("b", 2, 2.0)],
        )
        .expect("first");
        let second = Segment::from_mutations(
            SegmentId::new(2),
            &[upsert("a", 3, 3.0), delete("b", 4), upsert("c", 5, 5.0)],
        )
        .expect("second");

        let compacted = compact_segments(SegmentId::new(3), &[first, second]).expect("compact");
        assert_eq!(compacted.records().len(), 2);
        assert_eq!(compacted.tombstones().len(), 1);
        assert_eq!(compacted.max_sequence().get(), 5);
        assert!(compacted.records().iter().any(|record| {
            record.id() == &RecordId::string("a").expect("id")
                && record.sequence_number().get() == 3
        }));
        assert_eq!(compacted.tombstones()[0].sequence_number().get(), 4);
    }

    #[test]
    fn garbage_collection_keeps_authoritative_segments() {
        let dir = temp_dir();
        fs::create_dir_all(&dir).expect("dir");
        for id in [1_u64, 2, 3] {
            fs::write(dir.join(format!("{id:020}.kseg")), b"segment").expect("file");
        }

        let removed = garbage_collect_segments(&dir, &[SegmentId::new(3)]).expect("gc");
        assert_eq!(removed, vec![SegmentId::new(1), SegmentId::new(2)]);
        assert!(dir.join("00000000000000000003.kseg").exists());
        fs::remove_dir_all(dir).expect("cleanup");
    }
}
