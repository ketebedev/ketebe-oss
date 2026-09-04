use axum::http::StatusCode;
use ketebe_core::{
    ChunkingPolicy, ChunkingStructure, CollectionIngestionConfig, DistanceMetric, FieldPath,
    LexicalAnalyzerConfig, Metadata, MetadataValue, Predicate, RecordId, SemanticChunkingPolicy,
    TokenChunkingPolicy, TokenizerKind,
};
use ketebe_storage::{DEFAULT_RRF_K, ExecutionPreference, ExecutionStrategy, PlanReason};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Number, Value};
use std::collections::BTreeMap;

use crate::chunking::ChunkingConfig;
use crate::http::ApiError;

#[derive(Debug, Deserialize)]
pub(crate) struct QueryBody {
    pub(crate) vector: Vec<f32>,
    pub(crate) metric: MetricDto,
    pub(crate) top_k: usize,
    #[serde(default)]
    pub(crate) predicate: Option<PredicateDto>,
    #[serde(default)]
    pub(crate) execution: ExecutionDto,
    #[serde(default)]
    pub(crate) lexical: Option<LexicalDto>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct LexicalDto {
    pub(crate) text: String,
    #[serde(default)]
    pub(crate) fields: Vec<Vec<String>>,

    #[serde(default = "default_rrf_k")]
    pub(crate) rrf_k: u32,
}
fn default_rrf_k() -> u32 {
    DEFAULT_RRF_K
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum MetricDto {
    Cosine,
    Dot,
    L2,
}
impl From<MetricDto> for DistanceMetric {
    fn from(value: MetricDto) -> Self {
        match value {
            MetricDto::Cosine => Self::Cosine,
            MetricDto::Dot => Self::Dot,
            MetricDto::L2 => Self::L2,
        }
    }
}
impl From<DistanceMetric> for MetricDto {
    fn from(value: DistanceMetric) -> Self {
        match value {
            DistanceMetric::Cosine => Self::Cosine,
            DistanceMetric::Dot => Self::Dot,
            DistanceMetric::L2 => Self::L2,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, Deserialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum ExecutionDto {
    #[default]
    Auto,
    Exact,
    Hnsw,
}
impl From<ExecutionDto> for ExecutionPreference {
    fn from(value: ExecutionDto) -> Self {
        match value {
            ExecutionDto::Auto => Self::Auto,
            ExecutionDto::Exact => Self::Exact,
            ExecutionDto::Hnsw => Self::Hnsw,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub(crate) enum PredicateDto {
    Eq {
        path: Vec<String>,
        value: Value,
    },
    Ne {
        path: Vec<String>,
        value: Value,
    },
    Lt {
        path: Vec<String>,
        value: Value,
    },
    Lte {
        path: Vec<String>,
        value: Value,
    },
    Gt {
        path: Vec<String>,
        value: Value,
    },
    Gte {
        path: Vec<String>,
        value: Value,
    },
    Exists {
        path: Vec<String>,
    },
    In {
        path: Vec<String>,
        values: Vec<Value>,
    },
    Contains {
        path: Vec<String>,
        value: Value,
    },
    And {
        predicates: Vec<PredicateDto>,
    },
    Or {
        predicates: Vec<PredicateDto>,
    },
    Not {
        predicate: Box<PredicateDto>,
    },
}
impl PredicateDto {
    pub(crate) fn into_domain(self) -> Result<Predicate, ApiError> {
        match self {
            Self::Eq { path, value } => {
                Ok(Predicate::Eq(field_path(path)?, json_to_metadata(value)?))
            }
            Self::Ne { path, value } => {
                Ok(Predicate::Ne(field_path(path)?, json_to_metadata(value)?))
            }
            Self::Lt { path, value } => {
                Ok(Predicate::Lt(field_path(path)?, json_to_metadata(value)?))
            }
            Self::Lte { path, value } => {
                Ok(Predicate::Lte(field_path(path)?, json_to_metadata(value)?))
            }
            Self::Gt { path, value } => {
                Ok(Predicate::Gt(field_path(path)?, json_to_metadata(value)?))
            }
            Self::Gte { path, value } => {
                Ok(Predicate::Gte(field_path(path)?, json_to_metadata(value)?))
            }
            Self::Exists { path } => Ok(Predicate::Exists(field_path(path)?)),
            Self::In { path, values } => Ok(Predicate::In(
                field_path(path)?,
                values
                    .into_iter()
                    .map(json_to_metadata)
                    .collect::<Result<Vec<_>, _>>()?,
            )),
            Self::Contains { path, value } => Ok(Predicate::Contains(
                field_path(path)?,
                json_to_metadata(value)?,
            )),
            Self::And { predicates } => Ok(Predicate::And(
                predicates
                    .into_iter()
                    .map(Self::into_domain)
                    .collect::<Result<Vec<_>, _>>()?,
            )),
            Self::Or { predicates } => Ok(Predicate::Or(
                predicates
                    .into_iter()
                    .map(Self::into_domain)
                    .collect::<Result<Vec<_>, _>>()?,
            )),
            Self::Not { predicate } => Ok(Predicate::Not(Box::new(predicate.into_domain()?))),
        }
    }
}

pub(crate) fn field_path(path: Vec<String>) -> Result<FieldPath, ApiError> {
    FieldPath::new(path).map_err(|error| {
        ApiError::new(
            StatusCode::BAD_REQUEST,
            "invalid_field_path",
            error.to_string(),
        )
    })
}

pub(crate) fn json_to_metadata(value: Value) -> Result<MetadataValue, ApiError> {
    match value {
        Value::Null => Ok(MetadataValue::Null),
        Value::Bool(value) => Ok(MetadataValue::Bool(value)),
        Value::Number(value) => value.as_f64().map(MetadataValue::Number).ok_or_else(|| {
            ApiError::new(
                StatusCode::BAD_REQUEST,
                "invalid_metadata_value",
                "JSON number cannot be represented as f64",
            )
        }),
        Value::String(value) => Ok(MetadataValue::String(value)),
        Value::Array(values) => Ok(MetadataValue::Array(
            values
                .into_iter()
                .map(json_to_metadata)
                .collect::<Result<Vec<_>, _>>()?,
        )),
        Value::Object(values) => {
            let mut object = BTreeMap::new();
            for (key, value) in values {
                object.insert(key, json_to_metadata(value)?);
            }
            Ok(MetadataValue::Object(object))
        }
    }
}

pub(crate) fn json_object_to_metadata(value: Option<Value>) -> Result<Metadata, ApiError> {
    match value.unwrap_or_else(|| Value::Object(Map::new())) {
        Value::Object(values) => {
            let mut metadata = Metadata::new();
            for (key, value) in values {
                metadata.insert(key, json_to_metadata(value)?);
            }
            Ok(metadata)
        }
        _ => Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            "invalid_metadata",
            "metadata must be a JSON object",
        )),
    }
}

#[derive(Debug, Serialize)]
pub(crate) struct QueryResponseDto {
    pub(crate) hits: Vec<HitDto>,
    pub(crate) explain: ExplainDto,
}
#[derive(Debug, Serialize)]
pub(crate) struct HitDto {
    pub(crate) id: RecordIdDto,
    pub(crate) score: f32,
    pub(crate) sequence_number: u64,
    pub(crate) metadata: Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) dense_rank: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) lexical_rank: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) dense_score: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) lexical_score: Option<f32>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "type", content = "value", rename_all = "lowercase")]
pub(crate) enum RecordIdDto {
    String(String),
    U64(u64),
}
impl RecordIdDto {
    pub(crate) fn into_domain(self) -> Result<RecordId, ApiError> {
        match self {
            Self::String(value) => RecordId::string(value).map_err(|error| {
                ApiError::new(
                    StatusCode::BAD_REQUEST,
                    "invalid_record_id",
                    error.to_string(),
                )
            }),
            Self::U64(value) => Ok(RecordId::unsigned(value)),
        }
    }
}
impl From<&RecordId> for RecordIdDto {
    fn from(value: &RecordId) -> Self {
        match value {
            RecordId::String(value) => Self::String(value.clone()),
            RecordId::Unsigned(value) => Self::U64(*value),
        }
    }
}

#[derive(Debug, Serialize)]
pub(crate) struct ExplainDto {
    pub(crate) strategy: &'static str,
    pub(crate) reason: &'static str,
    pub(crate) collection_id: String,
    pub(crate) metric: MetricDto,
    pub(crate) top_k: usize,
    pub(crate) has_predicate: bool,
    pub(crate) candidate_limit: Option<usize>,
    pub(crate) fallback: bool,
    pub(crate) hybrid: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) dense_candidates: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) lexical_candidates: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) rrf_k: Option<u32>,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
pub(crate) struct LexicalAnalyzerDto {
    #[serde(default = "default_analyzer_kind")]
    pub(crate) kind: AnalyzerKindDto,
    #[serde(default = "default_true")]
    pub(crate) lowercase: bool,
}

#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum AnalyzerKindDto {
    #[default]
    Standard,
}

impl Default for LexicalAnalyzerDto {
    fn default() -> Self {
        Self {
            kind: AnalyzerKindDto::Standard,
            lowercase: true,
        }
    }
}
fn default_analyzer_kind() -> AnalyzerKindDto {
    AnalyzerKindDto::Standard
}
fn default_true() -> bool {
    true
}
impl From<LexicalAnalyzerDto> for LexicalAnalyzerConfig {
    fn from(value: LexicalAnalyzerDto) -> Self {
        match value.kind {
            AnalyzerKindDto::Standard => LexicalAnalyzerConfig::standard(value.lowercase),
        }
    }
}
impl From<LexicalAnalyzerConfig> for LexicalAnalyzerDto {
    fn from(value: LexicalAnalyzerConfig) -> Self {
        Self {
            kind: AnalyzerKindDto::Standard,
            lowercase: value.lowercase(),
        }
    }
}

#[derive(Debug, Deserialize)]
pub(crate) struct UpdateLexicalSchemaBody {
    #[serde(default)]
    pub(crate) lexical_fields: Vec<Vec<String>>,
    #[serde(default)]
    pub(crate) analyzer: LexicalAnalyzerDto,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum TokenChunkingStructureDto {
    Tokens,
    Sentences,
    Paragraphs,
    Markdown,
    Html,
}

#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum TokenizerKindDto {
    #[default]
    UnicodeWordsV1,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
pub(crate) struct TokenChunkingDto {
    pub(crate) strategy: TokenChunkingStructureDto,
    pub(crate) max_tokens: usize,
    #[serde(default)]
    pub(crate) token_overlap: usize,
    #[serde(default)]
    pub(crate) tokenizer: TokenizerKindDto,
}

impl TokenChunkingDto {
    fn into_domain(self) -> Result<TokenChunkingPolicy, ApiError> {
        let structure = match self.strategy {
            TokenChunkingStructureDto::Tokens => ChunkingStructure::Tokens,
            TokenChunkingStructureDto::Sentences => ChunkingStructure::Sentences,
            TokenChunkingStructureDto::Paragraphs => ChunkingStructure::Paragraphs,
            TokenChunkingStructureDto::Markdown => ChunkingStructure::Markdown,
            TokenChunkingStructureDto::Html => ChunkingStructure::Html,
        };
        let tokenizer = match self.tokenizer {
            TokenizerKindDto::UnicodeWordsV1 => TokenizerKind::UnicodeWordsV1,
        };
        TokenChunkingPolicy::new(structure, self.max_tokens, self.token_overlap, tokenizer).map_err(
            |error| {
                ApiError::new(
                    StatusCode::BAD_REQUEST,
                    "invalid_ingestion_schema",
                    error.to_string(),
                )
            },
        )
    }
}

impl From<TokenChunkingPolicy> for TokenChunkingDto {
    fn from(value: TokenChunkingPolicy) -> Self {
        let strategy = match value.structure() {
            ChunkingStructure::Tokens => TokenChunkingStructureDto::Tokens,
            ChunkingStructure::Sentences => TokenChunkingStructureDto::Sentences,
            ChunkingStructure::Paragraphs => TokenChunkingStructureDto::Paragraphs,
            ChunkingStructure::Markdown => TokenChunkingStructureDto::Markdown,
            ChunkingStructure::Html => TokenChunkingStructureDto::Html,
        };
        let tokenizer = match value.tokenizer() {
            TokenizerKind::UnicodeWordsV1 => TokenizerKindDto::UnicodeWordsV1,
        };
        Self {
            strategy,
            max_tokens: value.max_tokens(),
            token_overlap: value.token_overlap(),
            tokenizer,
        }
    }
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
pub(crate) struct SemanticChunkingDto {
    pub(crate) max_tokens: usize,
    #[serde(default)]
    pub(crate) token_overlap: usize,
    pub(crate) min_tokens: usize,
    pub(crate) breakpoint_threshold: f32,
    #[serde(default)]
    pub(crate) tokenizer: TokenizerKindDto,
}

impl SemanticChunkingDto {
    fn into_domain(self) -> Result<SemanticChunkingPolicy, ApiError> {
        if !self.breakpoint_threshold.is_finite()
            || !(0.0..=1.0).contains(&self.breakpoint_threshold)
        {
            return Err(ApiError::new(
                StatusCode::BAD_REQUEST,
                "invalid_ingestion_schema",
                "semantic breakpoint_threshold must be in 0..=1",
            ));
        }
        let threshold = (self.breakpoint_threshold * 1000.0).round() as u16;
        SemanticChunkingPolicy::new(
            self.max_tokens,
            self.token_overlap,
            self.min_tokens,
            threshold,
            match self.tokenizer {
                TokenizerKindDto::UnicodeWordsV1 => TokenizerKind::UnicodeWordsV1,
            },
        )
        .map_err(|e| {
            ApiError::new(
                StatusCode::BAD_REQUEST,
                "invalid_ingestion_schema",
                e.to_string(),
            )
        })
    }
}
impl From<SemanticChunkingPolicy> for SemanticChunkingDto {
    fn from(value: SemanticChunkingPolicy) -> Self {
        Self {
            max_tokens: value.max_tokens(),
            token_overlap: value.token_overlap(),
            min_tokens: value.min_tokens(),
            breakpoint_threshold: f32::from(value.breakpoint_threshold_milli()) / 1000.0,
            tokenizer: TokenizerKindDto::UnicodeWordsV1,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub(crate) struct IngestionSchemaDto {
    pub(crate) embedding_profile: String,
    #[serde(default)]
    pub(crate) chunking: Option<ChunkingConfig>,
    #[serde(default)]
    pub(crate) token_chunking: Option<TokenChunkingDto>,
    #[serde(default)]
    pub(crate) semantic_chunking: Option<SemanticChunkingDto>,
    #[serde(default)]
    pub(crate) index_chunk_text: bool,
}

impl IngestionSchemaDto {
    pub(crate) fn into_domain(self) -> Result<CollectionIngestionConfig, ApiError> {
        let configured_modes = usize::from(self.chunking.is_some())
            + usize::from(self.token_chunking.is_some())
            + usize::from(self.semantic_chunking.is_some());
        if configured_modes > 1 {
            return Err(ApiError::new(
                StatusCode::BAD_REQUEST,
                "invalid_ingestion_schema",
                "character, token_chunking and semantic_chunking are mutually exclusive",
            ));
        }
        if let Some(semantic) = self.semantic_chunking {
            return CollectionIngestionConfig::new_semantic(
                self.embedding_profile,
                semantic.into_domain()?,
                self.index_chunk_text,
            )
            .map_err(|e| {
                ApiError::new(
                    StatusCode::BAD_REQUEST,
                    "invalid_ingestion_schema",
                    e.to_string(),
                )
            });
        }
        if let Some(token_chunking) = self.token_chunking {
            return CollectionIngestionConfig::new_tokenized(
                self.embedding_profile,
                token_chunking.into_domain()?,
                self.index_chunk_text,
            )
            .map_err(|error| {
                ApiError::new(
                    StatusCode::BAD_REQUEST,
                    "invalid_ingestion_schema",
                    error.to_string(),
                )
            });
        }
        let chunking = self
            .chunking
            .map(|value| {
                value
                    .validate()
                    .map_err(|error| {
                        ApiError::new(
                            StatusCode::BAD_REQUEST,
                            "invalid_ingestion_schema",
                            error.to_string(),
                        )
                    })
                    .and_then(|value| {
                        ChunkingPolicy::new(value.max_chars, value.overlap_chars).map_err(|error| {
                            ApiError::new(
                                StatusCode::BAD_REQUEST,
                                "invalid_ingestion_schema",
                                error.to_string(),
                            )
                        })
                    })
            })
            .transpose()?;
        CollectionIngestionConfig::new(self.embedding_profile, chunking, self.index_chunk_text)
            .map_err(|error| {
                ApiError::new(
                    StatusCode::BAD_REQUEST,
                    "invalid_ingestion_schema",
                    error.to_string(),
                )
            })
    }
}

impl From<&CollectionIngestionConfig> for IngestionSchemaDto {
    fn from(value: &CollectionIngestionConfig) -> Self {
        Self {
            embedding_profile: value.embedding_profile().to_string(),
            chunking: value.chunking().map(ChunkingConfig::from),
            token_chunking: value.token_chunking().map(TokenChunkingDto::from),
            semantic_chunking: value.semantic_chunking().map(SemanticChunkingDto::from),
            index_chunk_text: value.index_chunk_text(),
        }
    }
}

#[derive(Debug, Deserialize)]
pub(crate) struct CreateCollectionBody {
    pub(crate) id: String,
    pub(crate) dimension: usize,
    pub(crate) metric: MetricDto,
    #[serde(default)]
    pub(crate) lexical_fields: Vec<Vec<String>>,
    #[serde(default)]
    pub(crate) analyzer: LexicalAnalyzerDto,
    #[serde(default)]
    pub(crate) ingestion: Option<IngestionSchemaDto>,
}
#[derive(Debug, Serialize)]
pub(crate) struct CollectionDto {
    pub(crate) id: String,
    pub(crate) dimension: usize,
    pub(crate) metric: MetricDto,
    pub(crate) lexical_fields: Vec<Vec<String>>,
    pub(crate) analyzer: LexicalAnalyzerDto,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) ingestion: Option<IngestionSchemaDto>,
}
#[derive(Debug, Deserialize)]
pub(crate) struct UpsertBody {
    pub(crate) vector: Vec<f32>,
    #[serde(default)]
    pub(crate) metadata: Option<Value>,
}
#[derive(Debug, Deserialize)]
pub(crate) struct BatchUpsertBody {
    pub(crate) records: Vec<BatchRecordDto>,
}
#[derive(Debug, Deserialize)]
pub(crate) struct BatchRecordDto {
    pub(crate) id: RecordIdDto,
    pub(crate) vector: Vec<f32>,
    #[serde(default)]
    pub(crate) metadata: Option<Value>,
}
#[derive(Debug, Serialize)]
pub(crate) struct MutationDto {
    pub(crate) sequence_number: u64,
}
#[derive(Debug, Serialize)]
pub(crate) struct BatchMutationDto {
    pub(crate) sequence_numbers: Vec<u64>,
}

pub(crate) fn metadata_map_to_json(metadata: &Metadata) -> Result<Value, ApiError> {
    let mut map = Map::new();
    for (key, value) in metadata {
        map.insert(key.clone(), metadata_to_json(value)?);
    }
    Ok(Value::Object(map))
}
fn metadata_to_json(value: &MetadataValue) -> Result<Value, ApiError> {
    match value {
        MetadataValue::Null => Ok(Value::Null),
        MetadataValue::Bool(value) => Ok(Value::Bool(*value)),
        MetadataValue::Number(value) => Number::from_f64(*value)
            .map(Value::Number)
            .ok_or_else(|| ApiError::internal("metadata contains a non-finite number")),
        MetadataValue::String(value) => Ok(Value::String(value.clone())),
        MetadataValue::Array(values) => Ok(Value::Array(
            values
                .iter()
                .map(metadata_to_json)
                .collect::<Result<Vec<_>, _>>()?,
        )),
        MetadataValue::Object(values) => {
            let mut map = Map::new();
            for (key, value) in values {
                map.insert(key.clone(), metadata_to_json(value)?);
            }
            Ok(Value::Object(map))
        }
    }
}

pub(crate) fn metric_name(metric: DistanceMetric) -> &'static str {
    match metric {
        DistanceMetric::Cosine => "cosine",
        DistanceMetric::Dot => "dot",
        DistanceMetric::L2 => "l2",
    }
}
pub(crate) fn strategy_name(strategy: ExecutionStrategy) -> &'static str {
    match strategy {
        ExecutionStrategy::Exact => "exact",
        ExecutionStrategy::Hnsw => "hnsw",
        ExecutionStrategy::HnswPostFilter => "hnsw_post_filter",
    }
}
pub(crate) fn reason_name(reason: PlanReason) -> &'static str {
    match reason {
        PlanReason::ExplicitExact => "explicit_exact",
        PlanReason::ExplicitHnsw => "explicit_hnsw",
        PlanReason::ExplicitHnswWithPredicate => "explicit_hnsw_with_predicate",
        PlanReason::AutoHnswAvailable => "auto_hnsw_available",
        PlanReason::AutoHnswWithPredicate => "auto_hnsw_with_predicate",
        PlanReason::AutoExactFallbackNoHnsw => "auto_exact_fallback_no_hnsw",
    }
}
