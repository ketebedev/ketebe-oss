use crate::{
    HnswIndex, LexicalIndex, LexicalIndexError, PlannerError, QueryControl, QueryRequest,
    SearchExplain, Segment, execute_query_with_control,
};
use ketebe_core::{CollectionId, FieldPath, LexicalAnalyzerConfig, Predicate, Record, RecordId};
use std::collections::BTreeMap;
use std::fmt;

pub const DEFAULT_RRF_K: u32 = 60;
pub const MAX_HYBRID_CANDIDATES: usize = 10_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HybridOptions {
    pub top_k: usize,
    pub dense_k: usize,
    pub lexical_k: usize,
    pub rrf_k: u32,
}

impl HybridOptions {
    pub fn new(
        top_k: usize,
        dense_k: usize,
        lexical_k: usize,
        rrf_k: u32,
    ) -> Result<Self, HybridError> {
        if top_k == 0 || dense_k == 0 || lexical_k == 0 {
            return Err(HybridError::InvalidTopK);
        }
        if rrf_k == 0 {
            return Err(HybridError::InvalidRrfK);
        }
        if dense_k < top_k || lexical_k < top_k {
            return Err(HybridError::CandidateDepthBelowTopK);
        }
        if dense_k.saturating_add(lexical_k) > MAX_HYBRID_CANDIDATES {
            return Err(HybridError::CandidateBudgetExceeded {
                requested: dense_k.saturating_add(lexical_k),
                maximum: MAX_HYBRID_CANDIDATES,
            });
        }
        Ok(Self {
            top_k,
            dense_k,
            lexical_k,
            rrf_k,
        })
    }

    pub fn baseline(top_k: usize, rrf_k: u32) -> Result<Self, HybridError> {
        Self::new(top_k, top_k, top_k, rrf_k)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct LexicalQuery {
    text: String,
    fields: Vec<FieldPath>,
    analyzer: LexicalAnalyzerConfig,
}

impl LexicalQuery {
    pub fn new(text: impl Into<String>, fields: Vec<FieldPath>) -> Result<Self, HybridError> {
        let text = text.into();
        if crate::lexical_index::analyze(&text, LexicalAnalyzerConfig::default()).is_empty() {
            return Err(HybridError::EmptyLexicalQuery);
        }
        if fields.is_empty() {
            return Err(HybridError::EmptyLexicalFields);
        }
        Ok(Self {
            text,
            fields,
            analyzer: LexicalAnalyzerConfig::default(),
        })
    }

    #[must_use]
    pub fn text(&self) -> &str {
        &self.text
    }

    #[must_use]
    pub fn fields(&self) -> &[FieldPath] {
        &self.fields
    }

    #[must_use]
    pub fn with_analyzer(mut self, analyzer: LexicalAnalyzerConfig) -> Self {
        self.analyzer = analyzer;
        self
    }

    #[must_use]
    pub const fn analyzer(&self) -> LexicalAnalyzerConfig {
        self.analyzer
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct LexicalHit {
    record: Record,
    score: f32,
}

impl LexicalHit {
    #[must_use]
    pub fn record(&self) -> &Record {
        &self.record
    }

    #[must_use]
    pub const fn score(&self) -> f32 {
        self.score
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct HybridHit {
    record: Record,
    score: f32,
    dense_rank: Option<usize>,
    lexical_rank: Option<usize>,
    dense_score: Option<f32>,
    lexical_score: Option<f32>,
}

impl HybridHit {
    #[must_use]
    pub fn record(&self) -> &Record {
        &self.record
    }
    #[must_use]
    pub const fn score(&self) -> f32 {
        self.score
    }
    #[must_use]
    pub const fn dense_rank(&self) -> Option<usize> {
        self.dense_rank
    }
    #[must_use]
    pub const fn lexical_rank(&self) -> Option<usize> {
        self.lexical_rank
    }
    #[must_use]
    pub const fn dense_score(&self) -> Option<f32> {
        self.dense_score
    }
    #[must_use]
    pub const fn lexical_score(&self) -> Option<f32> {
        self.lexical_score
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct HybridExplain {
    dense: SearchExplain,
    dense_candidates: usize,
    lexical_candidates: usize,
    requested_dense_candidates: usize,
    requested_lexical_candidates: usize,
    top_k: usize,
    rrf_k: u32,
}

impl HybridExplain {
    #[must_use]
    pub const fn dense(&self) -> &SearchExplain {
        &self.dense
    }
    #[must_use]
    pub const fn dense_candidates(&self) -> usize {
        self.dense_candidates
    }
    #[must_use]
    pub const fn lexical_candidates(&self) -> usize {
        self.lexical_candidates
    }
    #[must_use]
    pub const fn requested_dense_candidates(&self) -> usize {
        self.requested_dense_candidates
    }
    #[must_use]
    pub const fn requested_lexical_candidates(&self) -> usize {
        self.requested_lexical_candidates
    }
    #[must_use]
    pub const fn top_k(&self) -> usize {
        self.top_k
    }
    #[must_use]
    pub const fn rrf_k(&self) -> u32 {
        self.rrf_k
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct HybridResponse {
    hits: Vec<HybridHit>,
    explain: HybridExplain,
}

impl HybridResponse {
    #[must_use]
    pub fn hits(&self) -> &[HybridHit] {
        &self.hits
    }
    #[must_use]
    pub const fn explain(&self) -> &HybridExplain {
        &self.explain
    }
}

#[derive(Debug)]
pub enum HybridError {
    EmptyLexicalQuery,
    EmptyLexicalFields,
    InvalidTopK,
    InvalidRrfK,
    CandidateDepthBelowTopK,
    CandidateBudgetExceeded { requested: usize, maximum: usize },
    LexicalIndexMismatch,
    Predicate(String),
    Index(LexicalIndexError),
    Dense(PlannerError),
}

impl fmt::Display for HybridError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyLexicalQuery => {
                f.write_str("lexical query must contain at least one alphanumeric token")
            }
            Self::EmptyLexicalFields => {
                f.write_str("lexical query must contain at least one field path")
            }
            Self::InvalidTopK => {
                f.write_str("top_k and candidate depths must be greater than zero")
            }
            Self::InvalidRrfK => f.write_str("rrf_k must be greater than zero"),
            Self::CandidateDepthBelowTopK => {
                f.write_str("dense_k and lexical_k must be greater than or equal to top_k")
            }
            Self::CandidateBudgetExceeded { requested, maximum } => write!(
                f,
                "hybrid candidate budget {requested} exceeds maximum {maximum}"
            ),
            Self::LexicalIndexMismatch => {
                f.write_str("lexical index does not match query collection or fields")
            }
            Self::Predicate(message) => write!(f, "predicate evaluation failed: {message}"),
            Self::Index(error) => write!(f, "lexical index execution failed: {error}"),
            Self::Dense(error) => write!(f, "dense execution failed: {error}"),
        }
    }
}

impl std::error::Error for HybridError {}
impl From<PlannerError> for HybridError {
    fn from(value: PlannerError) -> Self {
        Self::Dense(value)
    }
}
impl From<LexicalIndexError> for HybridError {
    fn from(value: LexicalIndexError) -> Self {
        match value {
            LexicalIndexError::EmptyFields => Self::EmptyLexicalFields,
            LexicalIndexError::EmptyQuery => Self::EmptyLexicalQuery,
            LexicalIndexError::InvalidTopK => Self::InvalidTopK,
            LexicalIndexError::Predicate(message) => Self::Predicate(message),
            other => Self::Index(other),
        }
    }
}

pub fn lexical_search(
    segments: &[Segment],
    collection_id: &CollectionId,
    query: &LexicalQuery,
    top_k: usize,
    predicate: Option<&Predicate>,
) -> Result<Vec<LexicalHit>, HybridError> {
    let index = LexicalIndex::build(
        segments,
        collection_id,
        query.fields().to_vec(),
        query.analyzer(),
        0,
    )?;
    lexical_search_index(&index, query, top_k, predicate)
}

pub fn lexical_search_index(
    index: &LexicalIndex,
    query: &LexicalQuery,
    top_k: usize,
    predicate: Option<&Predicate>,
) -> Result<Vec<LexicalHit>, HybridError> {
    lexical_search_index_with_control(index, query, top_k, predicate, &QueryControl::unbounded())
}

pub fn lexical_search_index_with_control(
    index: &LexicalIndex,
    query: &LexicalQuery,
    top_k: usize,
    predicate: Option<&Predicate>,
    control: &QueryControl,
) -> Result<Vec<LexicalHit>, HybridError> {
    control.check().map_err(LexicalIndexError::from)?;
    if index.fields() != query.fields() || index.analyzer() != query.analyzer() {
        return Err(HybridError::LexicalIndexMismatch);
    }
    index
        .search_with_control(query.text(), top_k, predicate, control)?
        .into_iter()
        .map(|hit| {
            Ok(LexicalHit {
                record: hit.record().clone(),
                score: hit.score(),
            })
        })
        .collect()
}

pub fn execute_hybrid_query(
    dense_request: &QueryRequest,
    lexical_query: &LexicalQuery,
    segments: &[Segment],
    hnsw: Option<&HnswIndex>,
    rrf_k: u32,
) -> Result<HybridResponse, HybridError> {
    let options = HybridOptions::baseline(dense_request.top_k(), rrf_k)?;
    execute_hybrid_query_with_options(dense_request, lexical_query, segments, hnsw, options)
}

pub fn execute_hybrid_query_with_options(
    dense_request: &QueryRequest,
    lexical_query: &LexicalQuery,
    segments: &[Segment],
    hnsw: Option<&HnswIndex>,
    options: HybridOptions,
) -> Result<HybridResponse, HybridError> {
    let index = LexicalIndex::build(
        segments,
        dense_request.collection_id(),
        lexical_query.fields().to_vec(),
        lexical_query.analyzer(),
        0,
    )?;
    execute_hybrid_query_with_index_and_options(
        dense_request,
        lexical_query,
        &index,
        segments,
        hnsw,
        options,
    )
}

pub fn execute_hybrid_query_with_index(
    dense_request: &QueryRequest,
    lexical_query: &LexicalQuery,
    lexical_index: &LexicalIndex,
    segments: &[Segment],
    hnsw: Option<&HnswIndex>,
    rrf_k: u32,
) -> Result<HybridResponse, HybridError> {
    let options = HybridOptions::baseline(dense_request.top_k(), rrf_k)?;
    execute_hybrid_query_with_index_and_options(
        dense_request,
        lexical_query,
        lexical_index,
        segments,
        hnsw,
        options,
    )
}

pub fn execute_hybrid_query_with_index_and_options(
    dense_request: &QueryRequest,
    lexical_query: &LexicalQuery,
    lexical_index: &LexicalIndex,
    segments: &[Segment],
    hnsw: Option<&HnswIndex>,
    options: HybridOptions,
) -> Result<HybridResponse, HybridError> {
    execute_hybrid_query_with_index_and_options_and_control(
        dense_request,
        lexical_query,
        lexical_index,
        segments,
        hnsw,
        options,
        &QueryControl::unbounded(),
    )
}

pub fn execute_hybrid_query_with_index_and_options_and_control(
    dense_request: &QueryRequest,
    lexical_query: &LexicalQuery,
    lexical_index: &LexicalIndex,
    segments: &[Segment],
    hnsw: Option<&HnswIndex>,
    options: HybridOptions,
    control: &QueryControl,
) -> Result<HybridResponse, HybridError> {
    control.check().map_err(LexicalIndexError::from)?;
    let options = HybridOptions::new(
        options.top_k,
        options.dense_k,
        options.lexical_k,
        options.rrf_k,
    )?;
    if lexical_index.collection_id() != dense_request.collection_id()
        || lexical_index.fields() != lexical_query.fields()
    {
        return Err(HybridError::LexicalIndexMismatch);
    }

    let mut expanded_dense = QueryRequest::new(
        dense_request.collection_id().clone(),
        dense_request.vector().to_vec(),
        dense_request.metric(),
        options.dense_k,
    )
    .with_preference(dense_request.preference());
    if let Some(predicate) = dense_request.predicate().cloned() {
        expanded_dense = expanded_dense.with_predicate(predicate);
    }
    let dense = execute_query_with_control(&expanded_dense, segments, hnsw, control)?;
    control.check().map_err(LexicalIndexError::from)?;
    let lexical = lexical_search_index_with_control(
        lexical_index,
        lexical_query,
        options.lexical_k,
        dense_request.predicate(),
        control,
    )?;

    #[derive(Clone)]
    struct FusionEntry {
        record: Record,
        score: f32,
        dense_rank: Option<usize>,
        lexical_rank: Option<usize>,
        dense_score: Option<f32>,
        lexical_score: Option<f32>,
    }
    let mut fused = BTreeMap::<RecordId, FusionEntry>::new();
    for (index, hit) in dense.hits().iter().enumerate() {
        if index % 128 == 0 {
            control.check().map_err(LexicalIndexError::from)?;
        }
        let rank = index + 1;
        let score = 1.0 / (options.rrf_k as f32 + rank as f32);
        fused.insert(
            hit.record().id().clone(),
            FusionEntry {
                record: hit.record().clone(),
                score,
                dense_rank: Some(rank),
                lexical_rank: None,
                dense_score: Some(hit.score()),
                lexical_score: None,
            },
        );
    }
    for (index, hit) in lexical.iter().enumerate() {
        if index % 128 == 0 {
            control.check().map_err(LexicalIndexError::from)?;
        }
        let rank = index + 1;
        let score = 1.0 / (options.rrf_k as f32 + rank as f32);
        fused
            .entry(hit.record().id().clone())
            .and_modify(|entry| {
                entry.score += score;
                entry.lexical_rank = Some(rank);
                entry.lexical_score = Some(hit.score());
            })
            .or_insert_with(|| FusionEntry {
                record: hit.record().clone(),
                score,
                dense_rank: None,
                lexical_rank: Some(rank),
                dense_score: None,
                lexical_score: Some(hit.score()),
            });
    }
    let mut hits = fused
        .into_values()
        .map(|entry| HybridHit {
            record: entry.record,
            score: entry.score,
            dense_rank: entry.dense_rank,
            lexical_rank: entry.lexical_rank,
            dense_score: entry.dense_score,
            lexical_score: entry.lexical_score,
        })
        .collect::<Vec<_>>();
    hits.sort_by(|left, right| {
        right
            .score
            .total_cmp(&left.score)
            .then_with(|| left.record.id().cmp(right.record.id()))
    });
    hits.truncate(options.top_k);
    Ok(HybridResponse {
        explain: HybridExplain {
            dense: dense.explain().clone(),
            dense_candidates: dense.hits().len(),
            lexical_candidates: lexical.len(),
            requested_dense_candidates: options.dense_k,
            requested_lexical_candidates: options.lexical_k,
            top_k: options.top_k,
            rrf_k: options.rrf_k,
        },
        hits,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ExecutionPreference, SegmentId, WalMutation};
    use ketebe_core::{DistanceMetric, Metadata, MetadataValue, RecordId, SequenceNumber, Vector};

    fn path(parts: &[&str]) -> FieldPath {
        FieldPath::new(parts.iter().copied()).unwrap()
    }

    fn record(id: u64, sequence: u64, vector: f32, title: &str) -> WalMutation {
        let mut metadata = Metadata::new();
        metadata.insert("title".into(), MetadataValue::String(title.into()));
        WalMutation::Upsert {
            collection_id: CollectionId::new("docs").unwrap(),
            record: Record::new(
                RecordId::unsigned(id),
                Vector::new(vec![vector]).unwrap(),
                metadata,
                SequenceNumber::new(sequence),
            ),
        }
    }

    #[test]
    fn lexical_fallback_preserves_latest_visibility() {
        let collection = CollectionId::new("docs").unwrap();
        let first = Segment::from_mutations(
            SegmentId::new(1),
            &[
                record(1, 1, 1.0, "rust database"),
                record(2, 2, 2.0, "rust rust database"),
            ],
        )
        .unwrap();
        let second =
            Segment::from_mutations(SegmentId::new(2), &[record(1, 3, 1.0, "python database")])
                .unwrap();
        let query = LexicalQuery::new("rust", vec![path(&["title"])]).unwrap();
        let hits = lexical_search(&[first, second], &collection, &query, 10, None).unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].record().id(), &RecordId::unsigned(2));
    }

    #[test]
    fn persistent_index_path_fuses_dense_and_lexical_rankings() {
        let collection = CollectionId::new("docs").unwrap();
        let segment = Segment::from_mutations(
            SegmentId::new(1),
            &[record(1, 1, 10.0, "rust guide"), record(2, 2, 1.0, "other")],
        )
        .unwrap();
        let fields = vec![path(&["title"])];
        let lexical_query = LexicalQuery::new("rust", fields.clone()).unwrap();
        let index = LexicalIndex::build(
            std::slice::from_ref(&segment),
            &collection,
            fields,
            ketebe_core::LexicalAnalyzerConfig::default(),
            123,
        )
        .unwrap();
        let dense = QueryRequest::new(collection, vec![10.0], DistanceMetric::Dot, 2)
            .with_preference(ExecutionPreference::Exact);
        let response = execute_hybrid_query_with_index(
            &dense,
            &lexical_query,
            &index,
            &[segment],
            None,
            DEFAULT_RRF_K,
        )
        .unwrap();
        assert_eq!(response.hits().len(), 2);
        assert_eq!(response.hits()[0].record().id(), &RecordId::unsigned(1));
        assert_eq!(response.hits()[0].dense_rank(), Some(1));
        assert_eq!(response.hits()[0].lexical_rank(), Some(1));
    }

    #[test]
    fn candidate_depths_are_independent_and_explained() {
        let collection = CollectionId::new("docs").unwrap();
        let segment = Segment::from_mutations(
            SegmentId::new(1),
            &[
                record(1, 1, 10.0, "rust"),
                record(2, 2, 9.0, "rust"),
                record(3, 3, 8.0, "other"),
                record(4, 4, 7.0, "other"),
            ],
        )
        .unwrap();
        let fields = vec![path(&["title"])];
        let lexical_query = LexicalQuery::new("rust", fields.clone()).unwrap();
        let index = LexicalIndex::build(
            std::slice::from_ref(&segment),
            &collection,
            fields,
            ketebe_core::LexicalAnalyzerConfig::default(),
            1,
        )
        .unwrap();
        let dense = QueryRequest::new(collection, vec![10.0], DistanceMetric::Dot, 1)
            .with_preference(ExecutionPreference::Exact);
        let options = HybridOptions::new(1, 4, 2, DEFAULT_RRF_K).unwrap();
        let response = execute_hybrid_query_with_index_and_options(
            &dense,
            &lexical_query,
            &index,
            &[segment],
            None,
            options,
        )
        .unwrap();
        assert_eq!(response.hits().len(), 1);
        assert_eq!(response.explain().requested_dense_candidates(), 4);
        assert_eq!(response.explain().requested_lexical_candidates(), 2);
        assert_eq!(response.explain().dense_candidates(), 4);
        assert_eq!(response.explain().lexical_candidates(), 2);
        assert_eq!(response.explain().top_k(), 1);
    }

    #[test]
    fn fusion_ties_are_deterministic_by_record_id() {
        let collection = CollectionId::new("docs").unwrap();
        let segment = Segment::from_mutations(
            SegmentId::new(1),
            &[record(2, 1, 1.0, "x"), record(1, 2, 1.0, "x")],
        )
        .unwrap();
        let fields = vec![path(&["title"])];
        let lexical_query = LexicalQuery::new("x", fields.clone()).unwrap();
        let index = LexicalIndex::build(
            std::slice::from_ref(&segment),
            &collection,
            fields,
            ketebe_core::LexicalAnalyzerConfig::default(),
            1,
        )
        .unwrap();
        let dense = QueryRequest::new(collection, vec![1.0], DistanceMetric::Dot, 2)
            .with_preference(ExecutionPreference::Exact);
        let response = execute_hybrid_query_with_index_and_options(
            &dense,
            &lexical_query,
            &index,
            &[segment],
            None,
            HybridOptions::new(2, 2, 2, DEFAULT_RRF_K).unwrap(),
        )
        .unwrap();
        if response.hits()[0]
            .score()
            .total_cmp(&response.hits()[1].score())
            .is_eq()
        {
            assert!(response.hits()[0].record().id() < response.hits()[1].record().id());
        }
    }

    #[test]
    fn candidate_budget_is_bounded() {
        assert!(matches!(
            HybridOptions::new(1, MAX_HYBRID_CANDIDATES, 1, DEFAULT_RRF_K),
            Err(HybridError::CandidateBudgetExceeded { .. })
        ));
    }

    #[test]
    fn mismatched_field_index_is_rejected() {
        let collection = CollectionId::new("docs").unwrap();
        let segment =
            Segment::from_mutations(SegmentId::new(1), &[record(1, 1, 1.0, "rust")]).unwrap();
        let index = LexicalIndex::build(
            std::slice::from_ref(&segment),
            &collection,
            vec![path(&["title"])],
            ketebe_core::LexicalAnalyzerConfig::default(),
            1,
        )
        .unwrap();
        let query = LexicalQuery::new("rust", vec![path(&["description"])]).unwrap();
        let dense = QueryRequest::new(collection, vec![1.0], DistanceMetric::Dot, 1);
        assert!(matches!(
            execute_hybrid_query_with_index(&dense, &query, &index, &[segment], None, 60),
            Err(HybridError::LexicalIndexMismatch)
        ));
    }
}
