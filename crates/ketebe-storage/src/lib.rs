#![forbid(unsafe_code)]

mod checkpoint;
mod compaction;
#[allow(clippy::possible_missing_else)]
mod cost_planner;
mod encryption;
mod filter;
mod hnsw;
mod hnsw_snapshot;
mod hybrid;
mod lexical_index;
mod namespace;
mod query_control;
mod scoped_artifacts;
mod scoped_maintenance;
mod scoped_wal;
mod search;
mod segment;
mod storage_backend;
mod wal;
mod wal_reclaim;

use ketebe_core::BuildInfo;

pub use checkpoint::{Checkpoint, CheckpointError, CheckpointStore};
pub use compaction::{
    CompactionError, compact_segments, garbage_collect_segment_store, garbage_collect_segments,
};
pub use cost_planner::{
    CostReason, DEFAULT_ANN_MIN_RECORDS, ExecutionPreference, ExecutionStrategy, PlanReason,
    PlannedSearchHit, PlannerConfig, PlannerError, QueryRequest, QueryResponse, SearchExplain,
    execute_query, execute_query_with_config, execute_query_with_config_and_control,
    execute_query_with_control,
};
pub use encryption::{
    StorageEncryptionArtifact, StorageEncryptionContext, StorageEncryptionPolicyProvider,
    UnencryptedStoragePolicy,
};
pub use filter::{
    FilteredSearchError, exact_search_filtered_segments,
    exact_search_filtered_segments_with_control, hnsw_search_filtered,
    hnsw_search_filtered_with_control,
};
pub use hnsw::{HnswConfig, HnswError, HnswHit, HnswIndex};
pub use hnsw_snapshot::{
    HnswIndexStore, HnswLoadResult, HnswSnapshotError, hnsw_checkpoint_fingerprint,
};
pub use hybrid::{
    DEFAULT_RRF_K, HybridError, HybridExplain, HybridHit, HybridOptions, HybridResponse,
    LexicalHit, LexicalQuery, MAX_HYBRID_CANDIDATES, execute_hybrid_query,
    execute_hybrid_query_with_index, execute_hybrid_query_with_index_and_options,
    execute_hybrid_query_with_index_and_options_and_control, execute_hybrid_query_with_options,
    lexical_search, lexical_search_index, lexical_search_index_with_control,
};
// Persistent lexical index lifecycle, checkpoint identity, snapshot validation, and BM25 primitives.
pub use lexical_index::{
    DEFAULT_BM25_B, DEFAULT_BM25_K1, LEXICAL_ANALYZER_VERSION, LEXICAL_INDEX_VERSION, LexicalIndex,
    LexicalIndexError, LexicalIndexHit, LexicalIndexStore, LexicalLoadResult,
    lexical_checkpoint_fingerprint,
};
pub use namespace::{NamespaceError, ScopedStorageNamespace};
pub use query_control::{QueryControl, QueryControlError};
pub use scoped_artifacts::{ScopedArtifactError, ScopedCheckpointStore, ScopedSegmentStore};
pub use scoped_maintenance::{ScopedMaintenanceError, compact_scoped_segments};
pub use scoped_wal::{ScopedWal, ScopedWalError};
pub use search::{
    SearchAfter, SearchError, SearchHit, exact_search, exact_search_segments,
    exact_search_segments_after_with_control, exact_search_segments_with_control,
};
pub use segment::{Segment, SegmentError, SegmentId, SegmentStore, Tombstone};
pub use storage_backend::{
    BackendCapabilities, LocalFilesystemBackend, SegmentLocation, StorageBackend,
    StorageBackendError,
};
pub use wal::{ReplayResult, SyncPolicy, Wal, WalError, WalMutation, replay_wal_path};
pub use wal_reclaim::{WalReclaimError, reclaim_wal};

/// Returns the Ketebe build information visible to the storage layer.
#[must_use]
pub const fn build_info() -> BuildInfo {
    ketebe_core::build_info()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn storage_uses_core_build_info() {
        assert_eq!(build_info().name, "ketebe");
    }
}
