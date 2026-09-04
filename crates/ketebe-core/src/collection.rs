use crate::{CollectionId, DomainError, FieldPath, Record, Vector};

/// Distance/similarity function configured for a collection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DistanceMetric {
    Cosine,
    Dot,
    L2,
}

/// Tokenizer/analyzer family used by the lexical index.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum LexicalAnalyzerKind {
    #[default]
    Standard,
}

/// Immutable analyzer configuration owned by collection schema.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct LexicalAnalyzerConfig {
    kind: LexicalAnalyzerKind,
    lowercase: bool,
}

impl LexicalAnalyzerConfig {
    #[must_use]
    pub const fn standard(lowercase: bool) -> Self {
        Self {
            kind: LexicalAnalyzerKind::Standard,
            lowercase,
        }
    }

    #[must_use]
    pub const fn kind(self) -> LexicalAnalyzerKind {
        self.kind
    }

    #[must_use]
    pub const fn lowercase(self) -> bool {
        self.lowercase
    }
}

impl Default for LexicalAnalyzerConfig {
    fn default() -> Self {
        Self::standard(true)
    }
}

/// Deterministic character-based chunking policy owned by a collection ingestion schema.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ChunkingPolicy {
    max_chars: usize,
    overlap_chars: usize,
}

impl ChunkingPolicy {
    pub fn new(max_chars: usize, overlap_chars: usize) -> Result<Self, DomainError> {
        if max_chars == 0 {
            return Err(DomainError::InvalidIngestionConfig {
                message: "chunking max_chars must be greater than zero".to_string(),
            });
        }
        if overlap_chars >= max_chars {
            return Err(DomainError::InvalidIngestionConfig {
                message: "chunking overlap_chars must be smaller than max_chars".to_string(),
            });
        }
        Ok(Self {
            max_chars,
            overlap_chars,
        })
    }

    #[must_use]
    pub const fn max_chars(self) -> usize {
        self.max_chars
    }

    #[must_use]
    pub const fn overlap_chars(self) -> usize {
        self.overlap_chars
    }
}

/// Stable tokenizer identity used by token-aware chunking policies.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum TokenizerKind {
    #[default]
    UnicodeWordsV1,
}

/// Structural boundary preference for token-aware chunking.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum ChunkingStructure {
    #[default]
    Tokens,
    Sentences,
    Paragraphs,
    Markdown,
    Html,
}

/// Deterministic token-aware chunking policy owned by collection ingestion schema.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TokenChunkingPolicy {
    structure: ChunkingStructure,
    max_tokens: usize,
    token_overlap: usize,
    tokenizer: TokenizerKind,
}

impl TokenChunkingPolicy {
    pub fn new(
        structure: ChunkingStructure,
        max_tokens: usize,
        token_overlap: usize,
        tokenizer: TokenizerKind,
    ) -> Result<Self, DomainError> {
        if max_tokens == 0 {
            return Err(DomainError::InvalidIngestionConfig {
                message: "chunking max_tokens must be greater than zero".to_string(),
            });
        }
        if token_overlap >= max_tokens {
            return Err(DomainError::InvalidIngestionConfig {
                message: "chunking token_overlap must be smaller than max_tokens".to_string(),
            });
        }
        Ok(Self {
            structure,
            max_tokens,
            token_overlap,
            tokenizer,
        })
    }

    #[must_use]
    pub const fn structure(self) -> ChunkingStructure {
        self.structure
    }

    #[must_use]
    pub const fn max_tokens(self) -> usize {
        self.max_tokens
    }

    #[must_use]
    pub const fn token_overlap(self) -> usize {
        self.token_overlap
    }

    #[must_use]
    pub const fn tokenizer(self) -> TokenizerKind {
        self.tokenizer
    }
}

/// Deterministic semantic chunking policy owned by a collection ingestion schema.
///
/// Similarity is stored as thousandths so policy identity remains Eq/Hash stable on disk and in
/// provenance. A boundary is preferred when adjacent context similarity is below the threshold.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SemanticChunkingPolicy {
    max_tokens: usize,
    token_overlap: usize,
    min_tokens: usize,
    breakpoint_threshold_milli: u16,
    tokenizer: TokenizerKind,
}

impl SemanticChunkingPolicy {
    pub fn new(
        max_tokens: usize,
        token_overlap: usize,
        min_tokens: usize,
        breakpoint_threshold_milli: u16,
        tokenizer: TokenizerKind,
    ) -> Result<Self, DomainError> {
        if max_tokens == 0 {
            return Err(DomainError::InvalidIngestionConfig {
                message: "semantic chunking max_tokens must be greater than zero".to_string(),
            });
        }
        if token_overlap >= max_tokens {
            return Err(DomainError::InvalidIngestionConfig {
                message: "semantic chunking token_overlap must be smaller than max_tokens"
                    .to_string(),
            });
        }
        if min_tokens == 0 || min_tokens > max_tokens {
            return Err(DomainError::InvalidIngestionConfig {
                message: "semantic chunking min_tokens must be in 1..=max_tokens".to_string(),
            });
        }
        if breakpoint_threshold_milli > 1000 {
            return Err(DomainError::InvalidIngestionConfig {
                message: "semantic chunking breakpoint threshold must be in 0..=1000".to_string(),
            });
        }
        Ok(Self {
            max_tokens,
            token_overlap,
            min_tokens,
            breakpoint_threshold_milli,
            tokenizer,
        })
    }

    #[must_use]
    pub const fn max_tokens(self) -> usize {
        self.max_tokens
    }
    #[must_use]
    pub const fn token_overlap(self) -> usize {
        self.token_overlap
    }
    #[must_use]
    pub const fn min_tokens(self) -> usize {
        self.min_tokens
    }
    #[must_use]
    pub const fn breakpoint_threshold_milli(self) -> u16 {
        self.breakpoint_threshold_milli
    }
    #[must_use]
    pub const fn tokenizer(self) -> TokenizerKind {
        self.tokenizer
    }
}

/// Durable document-ingestion behavior owned by collection schema.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CollectionIngestionConfig {
    embedding_profile: String,
    chunking: Option<ChunkingPolicy>,
    token_chunking: Option<TokenChunkingPolicy>,
    semantic_chunking: Option<SemanticChunkingPolicy>,
    index_chunk_text: bool,
}

impl CollectionIngestionConfig {
    pub fn new(
        embedding_profile: impl Into<String>,
        chunking: Option<ChunkingPolicy>,
        index_chunk_text: bool,
    ) -> Result<Self, DomainError> {
        Self::build(embedding_profile, chunking, None, None, index_chunk_text)
    }

    pub fn new_tokenized(
        embedding_profile: impl Into<String>,
        chunking: TokenChunkingPolicy,
        index_chunk_text: bool,
    ) -> Result<Self, DomainError> {
        Self::build(
            embedding_profile,
            None,
            Some(chunking),
            None,
            index_chunk_text,
        )
    }

    pub fn new_semantic(
        embedding_profile: impl Into<String>,
        chunking: SemanticChunkingPolicy,
        index_chunk_text: bool,
    ) -> Result<Self, DomainError> {
        Self::build(
            embedding_profile,
            None,
            None,
            Some(chunking),
            index_chunk_text,
        )
    }

    fn build(
        embedding_profile: impl Into<String>,
        chunking: Option<ChunkingPolicy>,
        token_chunking: Option<TokenChunkingPolicy>,
        semantic_chunking: Option<SemanticChunkingPolicy>,
        index_chunk_text: bool,
    ) -> Result<Self, DomainError> {
        let embedding_profile = embedding_profile.into();
        if embedding_profile.trim().is_empty() {
            return Err(DomainError::InvalidIngestionConfig {
                message: "embedding_profile must not be empty".to_string(),
            });
        }
        if embedding_profile.trim() != embedding_profile {
            return Err(DomainError::InvalidIngestionConfig {
                message: "embedding_profile must not contain leading or trailing whitespace"
                    .to_string(),
            });
        }
        let configured_modes = usize::from(chunking.is_some())
            + usize::from(token_chunking.is_some())
            + usize::from(semantic_chunking.is_some());
        if configured_modes > 1 {
            return Err(DomainError::InvalidIngestionConfig {
                message: "character, token and semantic chunking policies are mutually exclusive"
                    .to_string(),
            });
        }
        if index_chunk_text && configured_modes == 0 {
            return Err(DomainError::InvalidIngestionConfig {
                message: "index_chunk_text requires chunking to be configured".to_string(),
            });
        }
        Ok(Self {
            embedding_profile,
            chunking,
            token_chunking,
            semantic_chunking,
            index_chunk_text,
        })
    }

    #[must_use]
    pub fn embedding_profile(&self) -> &str {
        &self.embedding_profile
    }

    #[must_use]
    pub const fn chunking(&self) -> Option<ChunkingPolicy> {
        self.chunking
    }

    #[must_use]
    pub const fn token_chunking(&self) -> Option<TokenChunkingPolicy> {
        self.token_chunking
    }

    #[must_use]
    pub const fn semantic_chunking(&self) -> Option<SemanticChunkingPolicy> {
        self.semantic_chunking
    }

    #[must_use]
    pub const fn has_chunking(&self) -> bool {
        self.chunking.is_some() || self.token_chunking.is_some() || self.semantic_chunking.is_some()
    }

    #[must_use]
    pub const fn index_chunk_text(&self) -> bool {
        self.index_chunk_text
    }
}

/// Immutable v0 collection configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CollectionConfig {
    id: CollectionId,
    dimension: usize,
    distance_metric: DistanceMetric,
    lexical_fields: Vec<FieldPath>,
    lexical_analyzer: LexicalAnalyzerConfig,
    ingestion: Option<CollectionIngestionConfig>,
}

impl CollectionConfig {
    pub fn new(
        id: CollectionId,
        dimension: usize,
        distance_metric: DistanceMetric,
    ) -> Result<Self, DomainError> {
        if dimension == 0 {
            return Err(DomainError::InvalidDimension { dimension });
        }

        Ok(Self {
            id,
            dimension,
            distance_metric,
            lexical_fields: Vec::new(),
            lexical_analyzer: LexicalAnalyzerConfig::default(),
            ingestion: None,
        })
    }

    #[must_use]
    pub fn id(&self) -> &CollectionId {
        &self.id
    }

    #[must_use]
    pub const fn dimension(&self) -> usize {
        self.dimension
    }

    #[must_use]
    pub const fn distance_metric(&self) -> DistanceMetric {
        self.distance_metric
    }

    /// Configures the canonical metadata field set used by the collection lexical index.
    /// Duplicate paths are removed and ordering is normalized for stable index identity.
    /// An empty field set disables persistent lexical-index prebuilds for the collection unless
    /// the ingestion schema requires chunk text to be indexed.
    #[must_use]
    pub fn with_lexical_fields(mut self, mut fields: Vec<FieldPath>) -> Self {
        fields.sort();
        fields.dedup();
        self.lexical_fields = fields;
        self.ensure_ingestion_lexical_field();
        self
    }

    #[must_use]
    pub fn lexical_fields(&self) -> &[FieldPath] {
        &self.lexical_fields
    }

    #[must_use]
    pub fn with_lexical_analyzer(mut self, analyzer: LexicalAnalyzerConfig) -> Self {
        self.lexical_analyzer = analyzer;
        self
    }

    #[must_use]
    pub const fn lexical_analyzer(&self) -> LexicalAnalyzerConfig {
        self.lexical_analyzer
    }

    #[must_use]
    pub fn with_ingestion(mut self, ingestion: CollectionIngestionConfig) -> Self {
        self.ingestion = Some(ingestion);
        self.ensure_ingestion_lexical_field();
        self
    }

    #[must_use]
    pub fn ingestion(&self) -> Option<&CollectionIngestionConfig> {
        self.ingestion.as_ref()
    }

    fn ensure_ingestion_lexical_field(&mut self) {
        if self
            .ingestion
            .as_ref()
            .is_some_and(CollectionIngestionConfig::index_chunk_text)
        {
            let field = FieldPath::new(["_ketebe_chunk", "text"])
                .expect("Ketebe chunk text field path is valid");
            if !self.lexical_fields.contains(&field) {
                self.lexical_fields.push(field);
                self.lexical_fields.sort();
                self.lexical_fields.dedup();
            }
        }
    }

    pub fn validate_vector(&self, vector: &Vector) -> Result<(), DomainError> {
        if vector.len() != self.dimension {
            return Err(DomainError::VectorDimensionMismatch {
                expected: self.dimension,
                actual: vector.len(),
            });
        }
        Ok(())
    }

    pub fn validate_record(&self, record: &Record) -> Result<(), DomainError> {
        self.validate_vector(record.vector())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{RecordId, SequenceNumber};

    fn collection() -> CollectionConfig {
        CollectionConfig::new(
            CollectionId::new("docs").expect("valid id"),
            3,
            DistanceMetric::Cosine,
        )
        .expect("valid config")
    }

    #[test]
    fn zero_dimension_is_rejected() {
        assert_eq!(
            CollectionConfig::new(
                CollectionId::new("docs").expect("valid id"),
                0,
                DistanceMetric::Cosine,
            )
            .expect_err("zero dimension must fail"),
            DomainError::InvalidDimension { dimension: 0 }
        );
    }

    #[test]
    fn vector_dimension_must_match_collection() {
        let vector = Vector::new(vec![1.0, 2.0]).expect("valid vector values");
        assert_eq!(
            collection()
                .validate_vector(&vector)
                .expect_err("dimension mismatch must fail"),
            DomainError::VectorDimensionMismatch {
                expected: 3,
                actual: 2,
            }
        );
    }

    #[test]
    fn lexical_fields_are_canonicalized() {
        let title = FieldPath::new(["title"]).unwrap();
        let body = FieldPath::new(["body"]).unwrap();
        let config = collection().with_lexical_fields(vec![title.clone(), body.clone(), title]);
        assert_eq!(
            config.lexical_fields(),
            &[body, FieldPath::new(["title"]).unwrap()]
        );
    }

    #[test]
    fn ingestion_schema_can_require_chunk_text_lexical_field() {
        let ingestion = CollectionIngestionConfig::new(
            "docs-profile",
            Some(ChunkingPolicy::new(800, 100).unwrap()),
            true,
        )
        .unwrap();
        let config = collection()
            .with_ingestion(ingestion)
            .with_lexical_fields(Vec::new());
        assert_eq!(
            config.lexical_fields(),
            &[FieldPath::new(["_ketebe_chunk", "text"]).unwrap()]
        );
    }

    #[test]
    fn token_chunking_schema_is_durable_and_lexically_indexable() {
        let policy = TokenChunkingPolicy::new(
            ChunkingStructure::Markdown,
            256,
            32,
            TokenizerKind::UnicodeWordsV1,
        )
        .unwrap();
        let ingestion =
            CollectionIngestionConfig::new_tokenized("docs-profile", policy, true).unwrap();
        assert_eq!(ingestion.token_chunking(), Some(policy));
        assert!(ingestion.has_chunking());
        let config = collection().with_ingestion(ingestion);
        assert_eq!(
            config.lexical_fields(),
            &[FieldPath::new(["_ketebe_chunk", "text"]).unwrap()]
        );
    }

    #[test]
    fn invalid_ingestion_schema_is_rejected() {
        assert!(CollectionIngestionConfig::new("", None, false).is_err());
        assert!(CollectionIngestionConfig::new("default", None, true).is_err());
        assert!(ChunkingPolicy::new(4, 4).is_err());
        assert!(
            TokenChunkingPolicy::new(
                ChunkingStructure::Tokens,
                4,
                4,
                TokenizerKind::UnicodeWordsV1,
            )
            .is_err()
        );
    }

    #[test]
    fn compatible_record_is_valid() {
        let record = Record::new(
            RecordId::string("doc-1").expect("valid record id"),
            Vector::new(vec![0.1, 0.2, 0.3]).expect("valid vector"),
            Default::default(),
            SequenceNumber::new(1),
        );
        collection()
            .validate_record(&record)
            .expect("record is compatible");
    }
}
