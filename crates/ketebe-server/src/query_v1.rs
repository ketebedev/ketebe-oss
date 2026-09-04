use ketebe_core::{CollectionId, FieldPath, Metadata, Predicate, Record, RecordId};
use ketebe_storage::{
    DEFAULT_RRF_K, ExecutionPreference, FilteredSearchError, HnswError, HybridError, HybridOptions,
    LexicalIndex, LexicalIndexError, LexicalQuery, PlannerError, QueryControl, QueryControlError,
    QueryRequest, SearchAfter, SearchError, exact_search_segments_after_with_control,
    execute_hybrid_query_with_index_and_options_and_control, execute_query_with_control,
    lexical_search_index_with_control,
};
use std::collections::BTreeMap;
use std::fmt;
use std::time::Duration;

use crate::cursor::{
    CursorError, CursorQueryBinding, cursor_query_hash, decode_cursor, encode_cursor,
    validate_cursor,
};
use crate::query_runtime::{QueryAdmissionError, QueryAdmissionRequest, QueryRuntime};
use crate::reranking::{
    CandidateProjection, RerankFailurePolicy, RerankingError, RerankingService,
};
use crate::runtime::AppState;
use crate::search_profiles::{
    DEFAULT_QUERY_TOP_K, SearchProfile, SearchProfileError, SearchProfileStore,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryModeV1 {
    Dense,
    Lexical,
    Hybrid,
}

#[derive(Debug, Clone)]
pub struct QueryRerankV1 {
    pub profile: String,
    pub query: Option<String>,
    pub top_n: usize,
    pub text_fields: Vec<FieldPath>,
    pub include_metadata: bool,
    pub failure_policy: RerankFailurePolicy,
}

#[derive(Debug, Clone)]
pub struct QueryV1Request {
    pub collection_id: CollectionId,
    pub vector: Option<Vec<f32>>,
    pub text: Option<String>,
    pub top_k: usize,
    pub predicate: Option<Predicate>,
    pub execution: ExecutionPreference,
    pub dense_candidates: Option<usize>,
    pub lexical_candidates: Option<usize>,
    pub rrf_k: u32,
    pub search_profile: Option<String>,
    pub include_metadata: bool,
    pub include_provenance: bool,
    pub explain: bool,
    pub timeout_ms: Option<u64>,
    pub rerank: Option<QueryRerankV1>,
}

impl QueryV1Request {
    pub fn validate(&self) -> Result<QueryModeV1, QueryV1Error> {
        if self.top_k == 0 {
            return Err(QueryV1Error::Invalid(
                "top_k must be greater than zero".into(),
            ));
        }
        if self.timeout_ms == Some(0) {
            return Err(QueryV1Error::Invalid(
                "timeout_ms must be greater than zero when provided".into(),
            ));
        }
        if self.dense_candidates == Some(0) || self.lexical_candidates == Some(0) {
            return Err(QueryV1Error::Invalid(
                "candidate limits must be greater than zero when provided".into(),
            ));
        }
        if self.rrf_k == 0 {
            return Err(QueryV1Error::Invalid(
                "rrf_k must be greater than zero".into(),
            ));
        }
        if let Some(rerank) = &self.rerank {
            if rerank.profile.trim().is_empty() {
                return Err(QueryV1Error::Invalid(
                    "rerank profile must not be empty".into(),
                ));
            }
            if rerank.top_n < self.top_k {
                return Err(QueryV1Error::Invalid(
                    "rerank top_n must be greater than or equal to top_k".into(),
                ));
            }
            if rerank.text_fields.is_empty() {
                return Err(QueryV1Error::Invalid(
                    "rerank text_fields must contain at least one field".into(),
                ));
            }
            let has_rerank_query = rerank
                .query
                .as_ref()
                .is_some_and(|value| !value.trim().is_empty())
                || self
                    .text
                    .as_ref()
                    .is_some_and(|value| !value.trim().is_empty());
            if !has_rerank_query {
                return Err(QueryV1Error::Invalid(
                    "reranking requires rerank.query or query text".into(),
                ));
            }
        }
        let has_vector = self.vector.as_ref().is_some_and(|value| !value.is_empty());
        let has_text = self
            .text
            .as_ref()
            .is_some_and(|value| !value.trim().is_empty());
        match (has_vector, has_text) {
            (true, false) => Ok(QueryModeV1::Dense),
            (false, true) => Ok(QueryModeV1::Lexical),
            (true, true) => Ok(QueryModeV1::Hybrid),
            (false, false) => Err(QueryV1Error::Invalid(
                "query requires a non-empty vector and/or query text".into(),
            )),
        }
    }
}

impl Default for QueryV1Request {
    fn default() -> Self {
        Self {
            collection_id: CollectionId::new("default").expect("static collection id"),
            vector: None,
            text: None,
            top_k: 10,
            predicate: None,
            execution: ExecutionPreference::Auto,
            dense_candidates: None,
            lexical_candidates: None,
            rrf_k: DEFAULT_RRF_K,
            search_profile: None,
            include_metadata: true,
            include_provenance: false,
            explain: false,
            timeout_ms: None,
            rerank: None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct QueryV1Hit {
    pub id: RecordId,
    pub score: f32,
    pub sequence_number: u64,
    pub metadata: Metadata,
    pub dense_rank: Option<usize>,
    pub lexical_rank: Option<usize>,
    pub dense_score: Option<f32>,
    pub lexical_score: Option<f32>,
    pub rerank_score: Option<f32>,
    pub original_rank: Option<usize>,
}

impl QueryV1Hit {
    fn from_record(record: &Record, score: f32) -> Self {
        Self {
            id: record.id().clone(),
            score,
            sequence_number: record.sequence_number().get(),
            metadata: record.metadata().clone(),
            dense_rank: None,
            lexical_rank: None,
            dense_score: None,
            lexical_score: None,
            rerank_score: None,
            original_rank: None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct QueryRerankExplainV1 {
    pub profile: String,
    pub provider: String,
    pub input_candidates: usize,
    pub applied: bool,
    pub fallback_reason: Option<String>,
}

#[derive(Debug, Clone)]
pub struct QueryV1Explain {
    pub mode: QueryModeV1,
    pub strategy: String,
    pub reason: String,
    pub top_k: usize,
    pub dense_candidates: Option<usize>,
    pub lexical_candidates: Option<usize>,
    pub rrf_k: Option<u32>,
    pub has_predicate: bool,
    pub search_profile: String,
    pub timeout_ms: Option<u64>,
    pub rerank: Option<QueryRerankExplainV1>,
}

#[derive(Debug, Clone)]
pub struct QueryV1Response {
    pub hits: Vec<QueryV1Hit>,
    pub explain: Option<QueryV1Explain>,
}

#[derive(Debug, Clone, Default)]
pub struct QueryPaginationV1 {
    pub enabled: bool,
    pub cursor: Option<String>,
}

#[derive(Debug, Clone)]
pub struct QueryV1Page {
    pub response: QueryV1Response,
    pub next_cursor: Option<String>,
}

#[derive(Debug)]
pub enum QueryV1Error {
    Invalid(String),
    CollectionNotFound(String),
    LexicalNotConfigured,
    UnsupportedSearchProfile(String),
    RerankerProfileNotFound(String),
    Reranking(RerankingError),
    ResourceLimit(String),
    Overloaded,
    DeadlineExceeded,
    Cancelled,
    Cursor(CursorError),
    CursorUnsupported(String),
    Search(SearchError),
    Planner(PlannerError),
    Hybrid(HybridError),
    Internal(String),
}

impl fmt::Display for QueryV1Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Invalid(message) => f.write_str(message),
            Self::CollectionNotFound(id) => write!(f, "collection '{id}' was not found"),
            Self::LexicalNotConfigured => {
                f.write_str("collection has no lexical fields configured")
            }
            Self::UnsupportedSearchProfile(profile) => {
                write!(f, "search profile '{profile}' is not available")
            }
            Self::RerankerProfileNotFound(profile) => {
                write!(f, "reranker profile '{profile}' is not available")
            }
            Self::Reranking(error) => write!(f, "reranking failed: {error}"),
            Self::ResourceLimit(message) => f.write_str(message),
            Self::Overloaded => f.write_str("query concurrency limit reached"),
            Self::DeadlineExceeded => f.write_str("query execution deadline exceeded"),
            Self::Cancelled => f.write_str("query execution was cancelled"),
            Self::Cursor(error) => write!(f, "{error}"),
            Self::CursorUnsupported(message) => f.write_str(message),
            Self::Search(error) => write!(f, "cursor exact search failed: {error}"),
            Self::Planner(error) => write!(f, "query execution failed: {error}"),
            Self::Hybrid(error) => write!(f, "hybrid/lexical execution failed: {error}"),
            Self::Internal(message) => f.write_str(message),
        }
    }
}
impl std::error::Error for QueryV1Error {}

#[tracing::instrument(skip_all, name = "ketebe.query.v1", fields(component = "query"))]
pub async fn execute_query_v1(
    state: &AppState,
    request: QueryV1Request,
) -> Result<QueryV1Response, QueryV1Error> {
    Ok(
        execute_query_v1_page(state, request, QueryPaginationV1::default())
            .await?
            .response,
    )
}

pub async fn execute_query_v1_page(
    state: &AppState,
    request: QueryV1Request,
    pagination: QueryPaginationV1,
) -> Result<QueryV1Page, QueryV1Error> {
    let mut request = resolve_search_profile(state, request)?;
    let mode = request.validate()?;
    let pagination_enabled = pagination.enabled || pagination.cursor.is_some();
    if pagination_enabled {
        if mode != QueryModeV1::Dense {
            return Err(QueryV1Error::CursorUnsupported(
                "cursor pagination v0 supports dense queries (with optional metadata filter) only"
                    .into(),
            ));
        }
        if request.rerank.is_some() {
            return Err(QueryV1Error::CursorUnsupported(
                "cursor pagination v0 does not support reranking".into(),
            ));
        }
        if request.execution == ExecutionPreference::Hnsw {
            return Err(QueryV1Error::CursorUnsupported(
                "cursor pagination requires exact or auto execution; explicit hnsw is unsupported"
                    .into(),
            ));
        }
    }
    let mut next_cursor = None;
    let retrieval_top_k = request
        .rerank
        .as_ref()
        .map_or(request.top_k, |rerank| rerank.top_n)
        .max(request.top_k);
    let dense_candidate_k = match mode {
        QueryModeV1::Dense | QueryModeV1::Hybrid => request
            .dense_candidates
            .unwrap_or(retrieval_top_k)
            .max(retrieval_top_k),
        QueryModeV1::Lexical => 0,
    };
    let lexical_candidate_k = match mode {
        QueryModeV1::Lexical | QueryModeV1::Hybrid => request
            .lexical_candidates
            .unwrap_or(retrieval_top_k)
            .max(retrieval_top_k),
        QueryModeV1::Dense => 0,
    };
    let candidate_budget = dense_candidate_k.saturating_add(lexical_candidate_k);
    let query_runtime = state.query_runtime();
    let admission = query_runtime
        .admit(QueryAdmissionRequest {
            top_k: request.top_k,
            candidate_budget,
            timeout_ms: request.timeout_ms,
        })
        .map_err(map_admission_error)?;
    request.timeout_ms = Some(admission.timeout_ms());
    let control = QueryControl::with_timeout(Duration::from_millis(admission.timeout_ms()));

    // Build the candidate set while holding the catalog read lock. The lock is
    // intentionally released before any external reranker request is awaited.
    let mut response = {
        let catalog = state.catalog.read().await;
        let runtime = catalog
            .collections
            .get(&request.collection_id)
            .ok_or_else(|| QueryV1Error::CollectionNotFound(request.collection_id.to_string()))?;
        let segments = runtime.query_segments().map_err(|error| {
            QueryV1Error::Internal(format!("failed to build query overlay: {error}"))
        })?;

        match mode {
            QueryModeV1::Dense => {
                if pagination_enabled {
                    let vector = request.vector.clone().expect("validated vector");
                    let snapshot_sequence = runtime.next_sequence.saturating_sub(1);
                    let checkpoint_sequence = runtime
                        .checkpoint
                        .as_ref()
                        .map(|checkpoint| checkpoint.sequence_number().get());
                    let query_hash = cursor_query_hash(&CursorQueryBinding {
                        collection_id: &request.collection_id,
                        vector: &vector,
                        predicate: request.predicate.as_ref(),
                        execution: request.execution,
                        search_profile: request.search_profile.as_deref(),
                        metric: runtime.metric,
                    });
                    let after = pagination
                        .cursor
                        .as_deref()
                        .map(decode_cursor)
                        .transpose()
                        .map_err(QueryV1Error::Cursor)?
                        .map(|payload| {
                            validate_cursor(
                                &payload,
                                &request.collection_id,
                                &query_hash,
                                snapshot_sequence,
                            )
                            .map_err(QueryV1Error::Cursor)?;
                            Ok::<SearchAfter, QueryV1Error>(SearchAfter::new(
                                payload.score,
                                payload
                                    .record_id
                                    .to_record_id()
                                    .map_err(QueryV1Error::Cursor)?,
                            ))
                        })
                        .transpose()?;
                    let page_limit = request.top_k.saturating_add(1);
                    let raw = exact_search_segments_after_with_control(
                        &segments,
                        &request.collection_id,
                        &vector,
                        runtime.metric,
                        page_limit,
                        request.predicate.as_ref(),
                        after.as_ref(),
                        &control,
                    )
                    .map_err(|error| match error {
                        SearchError::Control(control) => map_control_error(control, &query_runtime),
                        other => QueryV1Error::Search(other),
                    })?;
                    let mut hits = raw
                        .iter()
                        .map(|hit| QueryV1Hit::from_record(hit.record(), hit.score()))
                        .collect::<Vec<_>>();
                    if hits.len() > request.top_k {
                        let boundary = &hits[request.top_k - 1];
                        next_cursor = Some(
                            encode_cursor(
                                &request.collection_id,
                                query_hash,
                                snapshot_sequence,
                                checkpoint_sequence,
                                boundary.score,
                                &boundary.id,
                            )
                            .map_err(QueryV1Error::Cursor)?,
                        );
                        hits.truncate(request.top_k);
                    }
                    let explain = request.explain.then(|| QueryV1Explain {
                        mode,
                        strategy: "exact_cursor".into(),
                        reason: if after.is_some() {
                            "cursor_continuation".into()
                        } else {
                            "cursor_first_page".into()
                        },
                        top_k: request.top_k,
                        dense_candidates: None,
                        lexical_candidates: None,
                        rrf_k: None,
                        has_predicate: request.predicate.is_some(),
                        search_profile: request
                            .search_profile
                            .clone()
                            .unwrap_or_else(|| "default@1".into()),
                        timeout_ms: request.timeout_ms,
                        rerank: None,
                    });
                    QueryV1Response { hits, explain }
                } else {
                    let vector = request.vector.clone().expect("validated vector");
                    let candidate_top_k = dense_candidate_k;
                    let mut dense = QueryRequest::new(
                        request.collection_id.clone(),
                        vector,
                        runtime.metric,
                        candidate_top_k,
                    )
                    .with_preference(request.execution);
                    if let Some(predicate) = request.predicate.clone() {
                        dense = dense.with_predicate(predicate);
                    }
                    let result = execute_query_with_control(
                        &dense,
                        &segments,
                        runtime.query_hnsw(),
                        &control,
                    )
                    .map_err(|error| map_planner_execution_error(error, &query_runtime))?;
                    let hits = result
                        .hits()
                        .iter()
                        .take(retrieval_top_k)
                        .map(|hit| QueryV1Hit::from_record(hit.record(), hit.score()))
                        .collect::<Vec<_>>();
                    let explain = request.explain.then(|| QueryV1Explain {
                        mode,
                        strategy: format!("{:?}", result.explain().strategy()).to_lowercase(),
                        reason: format!("{:?}", result.explain().reason()).to_lowercase(),
                        top_k: request.top_k,
                        dense_candidates: Some(candidate_top_k),
                        lexical_candidates: None,
                        rrf_k: None,
                        has_predicate: request.predicate.is_some(),
                        search_profile: request
                            .search_profile
                            .clone()
                            .unwrap_or_else(|| "default@1".into()),
                        timeout_ms: request.timeout_ms,
                        rerank: None,
                    });
                    QueryV1Response { hits, explain }
                }
            }
            QueryModeV1::Lexical => {
                let lexical_query =
                    lexical_query(runtime, request.text.clone().expect("validated text"))?;
                let candidate_top_k = lexical_candidate_k;
                let collection_directory =
                    scoped_collection_directory(state, &request.collection_id)?;
                let persistent = runtime
                    .query_lexical_index(&collection_directory, lexical_query.fields())
                    .map_err(|error| {
                        QueryV1Error::Internal(format!("lexical index lifecycle failure: {error}"))
                    })?;
                control
                    .check()
                    .map_err(|error| map_control_error(error, &query_runtime))?;
                let index = if let Some(index) = persistent {
                    index
                } else {
                    let index = LexicalIndex::build(
                        &segments,
                        &request.collection_id,
                        lexical_query.fields().to_vec(),
                        lexical_query.analyzer(),
                        0,
                    )
                    .map_err(|error| {
                        map_hybrid_execution_error(HybridError::from(error), &query_runtime)
                    })?;
                    control
                        .check()
                        .map_err(|error| map_control_error(error, &query_runtime))?;
                    std::sync::Arc::new(index)
                };
                let lexical_hits = lexical_search_index_with_control(
                    &index,
                    &lexical_query,
                    candidate_top_k,
                    request.predicate.as_ref(),
                    &control,
                )
                .map_err(|error| map_hybrid_execution_error(error, &query_runtime))?;
                let hits = lexical_hits
                    .iter()
                    .take(retrieval_top_k)
                    .enumerate()
                    .map(|(index, hit)| {
                        let mut value = QueryV1Hit::from_record(hit.record(), hit.score());
                        value.lexical_rank = Some(index + 1);
                        value.lexical_score = Some(hit.score());
                        value
                    })
                    .collect::<Vec<_>>();
                let explain = request.explain.then(|| QueryV1Explain {
                    mode,
                    strategy: "lexical".into(),
                    reason: "query_text_only".into(),
                    top_k: request.top_k,
                    dense_candidates: None,
                    lexical_candidates: Some(candidate_top_k),
                    rrf_k: None,
                    has_predicate: request.predicate.is_some(),
                    search_profile: request
                        .search_profile
                        .clone()
                        .unwrap_or_else(|| "default@1".into()),
                    timeout_ms: request.timeout_ms,
                    rerank: None,
                });
                QueryV1Response { hits, explain }
            }
            QueryModeV1::Hybrid => {
                let lexical_query =
                    lexical_query(runtime, request.text.clone().expect("validated text"))?;
                let dense_k = dense_candidate_k;
                let lexical_k = lexical_candidate_k;
                let options =
                    HybridOptions::new(retrieval_top_k, dense_k, lexical_k, request.rrf_k)
                        .map_err(QueryV1Error::Hybrid)?;
                let mut dense = QueryRequest::new(
                    request.collection_id.clone(),
                    request.vector.clone().expect("validated vector"),
                    runtime.metric,
                    retrieval_top_k,
                )
                .with_preference(request.execution);
                if let Some(predicate) = request.predicate.clone() {
                    dense = dense.with_predicate(predicate);
                }
                let collection_directory =
                    scoped_collection_directory(state, &request.collection_id)?;
                let persistent = runtime
                    .query_lexical_index(&collection_directory, lexical_query.fields())
                    .map_err(|error| {
                        QueryV1Error::Internal(format!("lexical index lifecycle failure: {error}"))
                    })?;
                control
                    .check()
                    .map_err(|error| map_control_error(error, &query_runtime))?;
                let index = if let Some(index) = persistent {
                    index
                } else {
                    let index = LexicalIndex::build(
                        &segments,
                        &request.collection_id,
                        lexical_query.fields().to_vec(),
                        lexical_query.analyzer(),
                        0,
                    )
                    .map_err(|error| {
                        map_hybrid_execution_error(HybridError::from(error), &query_runtime)
                    })?;
                    control
                        .check()
                        .map_err(|error| map_control_error(error, &query_runtime))?;
                    std::sync::Arc::new(index)
                };
                let result = execute_hybrid_query_with_index_and_options_and_control(
                    &dense,
                    &lexical_query,
                    &index,
                    &segments,
                    runtime.query_hnsw(),
                    options,
                    &control,
                )
                .map_err(|error| map_hybrid_execution_error(error, &query_runtime))?;
                let hits = result
                    .hits()
                    .iter()
                    .take(retrieval_top_k)
                    .map(|hit| QueryV1Hit {
                        id: hit.record().id().clone(),
                        score: hit.score(),
                        sequence_number: hit.record().sequence_number().get(),
                        metadata: hit.record().metadata().clone(),
                        dense_rank: hit.dense_rank(),
                        lexical_rank: hit.lexical_rank(),
                        dense_score: hit.dense_score(),
                        lexical_score: hit.lexical_score(),
                        rerank_score: None,
                        original_rank: None,
                    })
                    .collect::<Vec<_>>();
                let dense_explain = result.explain().dense();
                let explain = request.explain.then(|| QueryV1Explain {
                    mode,
                    strategy: format!("{:?}", dense_explain.strategy()).to_lowercase(),
                    reason: format!("{:?}", dense_explain.reason()).to_lowercase(),
                    top_k: request.top_k,
                    dense_candidates: Some(result.explain().requested_dense_candidates()),
                    lexical_candidates: Some(result.explain().requested_lexical_candidates()),
                    rrf_k: Some(request.rrf_k),
                    has_predicate: request.predicate.is_some(),
                    search_profile: request
                        .search_profile
                        .clone()
                        .unwrap_or_else(|| "default@1".into()),
                    timeout_ms: request.timeout_ms,
                    rerank: None,
                });
                QueryV1Response { hits, explain }
            }
        }
    };

    if let Some(rerank) = &request.rerank {
        apply_reranking(
            state,
            &request,
            rerank,
            &mut response,
            &control,
            &query_runtime,
        )
        .await?;
    } else {
        response.hits.truncate(request.top_k);
    }
    apply_projection(&mut response.hits, &request);
    Ok(QueryV1Page {
        response,
        next_cursor,
    })
}

fn scoped_collection_directory(
    state: &AppState,
    collection_id: &CollectionId,
) -> Result<std::path::PathBuf, QueryV1Error> {
    let legacy = state
        .data_dir
        .join("collections")
        .join(collection_id.as_str());
    match crate::data_plane_request::scope_for_collection_id(state, collection_id)
        .map_err(|error| QueryV1Error::Internal(error.to_string()))?
    {
        Some(scope) => {
            match ketebe_storage::ScopedStorageNamespace::open_existing(&*state.data_dir, scope) {
                Ok(namespace) => Ok(namespace.root().to_path_buf()),
                Err(ketebe_storage::NamespaceError::MissingNamespace(_))
                    if !collection_id.as_str().starts_with("c_") =>
                {
                    Ok(legacy)
                }
                Err(error) => Err(QueryV1Error::Internal(error.to_string())),
            }
        }
        None if collection_id.as_str().starts_with("c_") => Err(QueryV1Error::Internal(
            "stable collection identity has no project namespace binding".to_string(),
        )),
        None => Ok(legacy),
    }
}

fn map_admission_error(error: QueryAdmissionError) -> QueryV1Error {
    match error {
        QueryAdmissionError::Overloaded => QueryV1Error::Overloaded,
        QueryAdmissionError::TopKExceeded { .. }
        | QueryAdmissionError::CandidateBudgetExceeded { .. }
        | QueryAdmissionError::TimeoutExceeded { .. } => {
            QueryV1Error::ResourceLimit(error.to_string())
        }
        QueryAdmissionError::InvalidConfiguration(_) => QueryV1Error::Internal(error.to_string()),
    }
}

fn map_control_error(error: QueryControlError, runtime: &QueryRuntime) -> QueryV1Error {
    match error {
        QueryControlError::DeadlineExceeded => {
            runtime.mark_timeout();
            QueryV1Error::DeadlineExceeded
        }
        QueryControlError::Cancelled => {
            runtime.mark_cancelled();
            QueryV1Error::Cancelled
        }
    }
}

fn planner_control_error(error: &PlannerError) -> Option<QueryControlError> {
    match error {
        PlannerError::Exact(SearchError::Control(error)) => Some(*error),
        PlannerError::Hnsw(HnswError::Control(error)) => Some(*error),
        PlannerError::Filtered(FilteredSearchError::Exact(SearchError::Control(error))) => {
            Some(*error)
        }
        PlannerError::Filtered(FilteredSearchError::Hnsw(HnswError::Control(error))) => {
            Some(*error)
        }
        _ => None,
    }
}

fn hybrid_control_error(error: &HybridError) -> Option<QueryControlError> {
    match error {
        HybridError::Dense(error) => planner_control_error(error),
        HybridError::Index(LexicalIndexError::Control(error)) => Some(*error),
        _ => None,
    }
}

fn map_planner_execution_error(error: PlannerError, runtime: &QueryRuntime) -> QueryV1Error {
    if let Some(control) = planner_control_error(&error) {
        map_control_error(control, runtime)
    } else {
        QueryV1Error::Planner(error)
    }
}

fn map_hybrid_execution_error(error: HybridError, runtime: &QueryRuntime) -> QueryV1Error {
    if let Some(control) = hybrid_control_error(&error) {
        map_control_error(control, runtime)
    } else {
        QueryV1Error::Hybrid(error)
    }
}

fn resolve_search_profile(
    state: &AppState,
    mut request: QueryV1Request,
) -> Result<QueryV1Request, QueryV1Error> {
    let selector = request
        .search_profile
        .clone()
        .unwrap_or_else(|| "default".to_string());
    let profile = if selector == "default" {
        SearchProfile::default()
    } else {
        SearchProfileStore::new(state.data_dir.as_ref().clone())
            .get(request.collection_id.as_str(), &selector)
            .map_err(|error| map_search_profile_error(&selector, error))?
    };
    let pinned_id = profile.pinned_id();

    if request.top_k == DEFAULT_QUERY_TOP_K {
        request.top_k = profile.final_top_k;
    }
    if request.execution == ExecutionPreference::Auto {
        request.execution = profile.execution.into();
    }
    if request.dense_candidates.is_none() {
        request.dense_candidates = profile.dense_candidates;
    }
    if request.lexical_candidates.is_none() {
        request.lexical_candidates = profile.lexical_candidates;
    }
    if request.rrf_k == DEFAULT_RRF_K {
        request.rrf_k = profile.rrf_k;
    }
    if request.timeout_ms.is_none() {
        request.timeout_ms = profile.timeout_ms;
    }
    if request.rerank.is_none()
        && let Some(rerank) = profile.rerank
    {
        let text_fields = rerank
            .text_fields
            .into_iter()
            .map(FieldPath::new)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| {
                QueryV1Error::Internal(format!(
                    "persisted search profile contains an invalid rerank field: {error}"
                ))
            })?;
        request.rerank = Some(QueryRerankV1 {
            profile: rerank.profile,
            query: None,
            top_n: rerank.top_n,
            text_fields,
            include_metadata: rerank.include_metadata,
            failure_policy: rerank.failure_policy.into(),
        });
    }
    request.search_profile = Some(pinned_id);
    Ok(request)
}

fn map_search_profile_error(selector: &str, error: SearchProfileError) -> QueryV1Error {
    match error {
        SearchProfileError::NotFound(_) => {
            QueryV1Error::UnsupportedSearchProfile(selector.to_string())
        }
        SearchProfileError::Invalid(message) => QueryV1Error::Invalid(message),
        SearchProfileError::AlreadyExists(_) => QueryV1Error::Internal(error.to_string()),
        SearchProfileError::Io(_) | SearchProfileError::Json(_) => {
            QueryV1Error::Internal(error.to_string())
        }
    }
}

async fn apply_reranking(
    state: &AppState,
    request: &QueryV1Request,
    rerank: &QueryRerankV1,
    response: &mut QueryV1Response,
    control: &QueryControl,
    query_runtime: &QueryRuntime,
) -> Result<(), QueryV1Error> {
    control
        .check()
        .map_err(|error| map_control_error(error, query_runtime))?;
    let provider = state
        .reranker_profile(&rerank.profile)
        .await
        .ok_or_else(|| QueryV1Error::RerankerProfileNotFound(rerank.profile.clone()))?;
    let projection = CandidateProjection {
        text_fields: rerank.text_fields.clone(),
        include_metadata: rerank.include_metadata,
    };
    let candidates = response
        .hits
        .iter()
        .map(|hit| projection.project(hit.id.clone(), &hit.metadata, hit.score))
        .collect::<Result<Vec<_>, _>>()
        .map_err(QueryV1Error::Reranking)?;
    let query = rerank
        .query
        .as_deref()
        .or(request.text.as_deref())
        .expect("rerank query was validated");
    let remaining = control.remaining().unwrap_or(Duration::ZERO);
    if remaining.is_zero() {
        query_runtime.mark_timeout();
        return Err(QueryV1Error::DeadlineExceeded);
    }
    let result = tokio::time::timeout(
        remaining,
        RerankingService::new(provider, rerank.failure_policy.clone()).rerank(
            query,
            candidates,
            request.top_k,
        ),
    )
    .await
    .map_err(|_| {
        query_runtime.mark_timeout();
        QueryV1Error::DeadlineExceeded
    })?
    .map_err(QueryV1Error::Reranking)?;
    control
        .check()
        .map_err(|error| map_control_error(error, query_runtime))?;

    let mut raw_hits = response
        .hits
        .drain(..)
        .map(|hit| (hit.id.clone(), hit))
        .collect::<BTreeMap<_, _>>();
    response.hits = result
        .candidates
        .into_iter()
        .map(|value| {
            let mut hit = raw_hits
                .remove(&value.candidate.id)
                .expect("reranker returns only supplied candidates");
            hit.original_rank = Some(value.original_rank);
            hit.rerank_score = value.rerank_score;
            if let Some(score) = value.rerank_score {
                hit.score = score;
            }
            hit
        })
        .collect();

    if let Some(explain) = &mut response.explain {
        explain.rerank = Some(QueryRerankExplainV1 {
            profile: rerank.profile.clone(),
            provider: result.explain.provider,
            input_candidates: result.explain.input_candidates,
            applied: result.explain.applied,
            fallback_reason: result.explain.fallback_reason,
        });
    }
    Ok(())
}

fn lexical_query(
    runtime: &crate::runtime::CollectionRuntime,
    text: String,
) -> Result<LexicalQuery, QueryV1Error> {
    let fields = runtime.configured_lexical_fields().to_vec();
    if fields.is_empty() {
        return Err(QueryV1Error::LexicalNotConfigured);
    }
    LexicalQuery::new(text, fields)
        .map_err(QueryV1Error::Hybrid)
        .map(|query| query.with_analyzer(runtime.configured_lexical_analyzer()))
}

fn apply_projection(hits: &mut [QueryV1Hit], request: &QueryV1Request) {
    if !request.include_metadata {
        for hit in hits {
            hit.metadata.clear();
        }
        return;
    }
    if !request.include_provenance {
        for hit in hits {
            hit.metadata.retain(|key, _| !key.starts_with("_ketebe_"));
        }
    }
}
