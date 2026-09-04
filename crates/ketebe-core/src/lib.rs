#![forbid(unsafe_code)]

mod collection;
mod data_encryption;
mod error;
mod identifiers;
mod metadata;
mod predicate;
mod record;
mod scope;
mod source;
mod vector;

pub use collection::{
    ChunkingPolicy, ChunkingStructure, CollectionConfig, CollectionIngestionConfig, DistanceMetric,
    LexicalAnalyzerConfig, LexicalAnalyzerKind, SemanticChunkingPolicy, TokenChunkingPolicy,
    TokenizerKind,
};
pub use data_encryption::{
    DataEncryptionError, DataEncryptionKeyRef, DataEncryptionKeyResolver, DataEncryptionKeyVersion,
    DataEncryptionOwnership, DataEncryptionPolicy, LocalDataEncryptionKeyResolver,
    ResolvedDataEncryptionKey,
};
pub use error::DomainError;
pub use identifiers::{CollectionId, RecordId, SequenceNumber};
pub use metadata::{Metadata, MetadataValue};
pub use predicate::{FieldPath, Predicate, PredicateError};
pub use record::Record;
pub use scope::{CollectionName, DataPlaneScope, ProjectId, ScopeError};
pub use source::{DocumentSource, SourceIdentity, SourceKind, SourceRevision};
pub use vector::Vector;

/// Static build information shared by Ketebe components.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BuildInfo {
    /// Package name.
    pub name: &'static str,
    /// Package version.
    pub version: &'static str,
}

/// Returns build information for the Ketebe core package.
#[must_use]
pub const fn build_info() -> BuildInfo {
    BuildInfo {
        name: "ketebe",
        version: env!("CARGO_PKG_VERSION"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_info_is_stable() {
        let info = build_info();

        assert_eq!(info.name, "ketebe");
        assert!(!info.version.is_empty());
    }
}
