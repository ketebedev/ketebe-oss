use crate::{
    FilteredSearchError, HnswError, HnswIndex, SearchError, Segment,
    exact_search_filtered_segments, exact_search_segments, hnsw_search_filtered,
};
use ketebe_core::{CollectionId, DistanceMetric, Predicate, Record};
use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionPreference {
    Auto,
    Exact,
    Hnsw,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionStrategy {
    Exact,
    Hnsw,
    HnswPostFilter,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlanReason {
    ExplicitExact,
    ExplicitHnsw,
    ExplicitHnswWithPredicate,
    AutoHnswAvailable,
    AutoHnswWithPredicate,
    AutoExactFallbackNoHnsw,
}

#[derive(Debug, Clone, PartialEq)]
pub struct QueryRequest {
    collection_id: CollectionId,
    vector: Vec<f32>,
    metric: DistanceMetric,
    top_k: usize,
    predicate: Option<Predicate>,
    preference: ExecutionPreference,
}

impl QueryRequest {
    #[must_use]
    pub fn new(
        collection_id: CollectionId,
        vector: Vec<f32>,
        metric: DistanceMetric,
        top_k: usize,
    ) -> Self {
        Self {
            collection_id,
            vector,
            metric,
            top_k,
            predicate: None,
            preference: ExecutionPreference::Auto,
        }
    }

    #[must_use]
    pub fn with_predicate(mut self, predicate: Predicate) -> Self {
        self.predicate = Some(predicate);
        self
    }

    #[must_use]
    pub const fn with_preference(mut self, preference: ExecutionPreference) -> Self {
        self.preference = preference;
        self
    }

    #[must_use]
    pub fn collection_id(&self) -> &CollectionId {
        &self.collection_id
    }

    #[must_use]
    pub fn vector(&self) -> &[f32] {
        &self.vector
    }

    #[must_use]
    pub const fn metric(&self) -> DistanceMetric {
        self.metric
    }

    #[must_use]
    pub const fn top_k(&self) -> usize {
        self.top_k
    }

    #[must_use]
    pub fn predicate(&self) -> Option<&Predicate> {
        self.predicate.as_ref()
    }

    #[must_use]
    pub const fn preference(&self) -> ExecutionPreference {
        self.preference
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchExplain {
    strategy: ExecutionStrategy,
    reason: PlanReason,
    collection_id: CollectionId,
    metric: DistanceMetric,
    top_k: usize,
    has_predicate: bool,
    candidate_limit: Option<usize>,
    fallback: bool,
}

impl SearchExplain {
    #[must_use]
    pub const fn strategy(&self) -> ExecutionStrategy {
        self.strategy
    }

    #[must_use]
    pub const fn reason(&self) -> PlanReason {
        self.reason
    }

    #[must_use]
    pub fn collection_id(&self) -> &CollectionId {
        &self.collection_id
    }

    #[must_use]
    pub const fn metric(&self) -> DistanceMetric {
        self.metric
    }

    #[must_use]
    pub const fn top_k(&self) -> usize {
        self.top_k
    }

    #[must_use]
    pub const fn has_predicate(&self) -> bool {
        self.has_predicate
    }

    #[must_use]
    pub const fn candidate_limit(&self) -> Option<usize> {
        self.candidate_limit
    }

    #[must_use]
    pub const fn fallback(&self) -> bool {
        self.fallback
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct PlannedSearchHit {
    record: Record,
    score: f32,
}

impl PlannedSearchHit {
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
pub struct QueryResponse {
    hits: Vec<PlannedSearchHit>,
    explain: SearchExplain,
}

impl QueryResponse {
    #[must_use]
    pub fn hits(&self) -> &[PlannedSearchHit] {
        &self.hits
    }

    #[must_use]
    pub const fn explain(&self) -> &SearchExplain {
        &self.explain
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct QueryPlan {
    strategy: ExecutionStrategy,
    reason: PlanReason,
    candidate_limit: Option<usize>,
    fallback: bool,
}

#[derive(Debug)]
pub enum PlannerError {
    MissingHnswIndex,
    HnswCollectionMismatch {
        query: CollectionId,
        index: CollectionId,
    },
    Exact(SearchError),
    Hnsw(HnswError),
    Filtered(FilteredSearchError),
}

impl fmt::Display for PlannerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingHnswIndex => {
                f.write_str("HNSW execution was requested but no HNSW index was supplied")
            }
            Self::HnswCollectionMismatch { query, index } => write!(
                f,
                "HNSW index collection does not match query: query={}, index={}",
                query.as_str(),
                index.as_str()
            ),
            Self::Exact(error) => write!(f, "exact execution failed: {error}"),
            Self::Hnsw(error) => write!(f, "HNSW execution failed: {error}"),
            Self::Filtered(error) => write!(f, "filtered execution failed: {error}"),
        }
    }
}

impl std::error::Error for PlannerError {}

impl From<SearchError> for PlannerError {
    fn from(value: SearchError) -> Self {
        Self::Exact(value)
    }
}

impl From<HnswError> for PlannerError {
    fn from(value: HnswError) -> Self {
        Self::Hnsw(value)
    }
}

impl From<FilteredSearchError> for PlannerError {
    fn from(value: FilteredSearchError) -> Self {
        Self::Filtered(value)
    }
}

pub fn execute_query(
    request: &QueryRequest,
    segments: &[Segment],
    hnsw: Option<&HnswIndex>,
) -> Result<QueryResponse, PlannerError> {
    if let Some(index) = hnsw
        && index.collection_id() != request.collection_id()
    {
        return Err(PlannerError::HnswCollectionMismatch {
            query: request.collection_id().clone(),
            index: index.collection_id().clone(),
        });
    }

    let plan = plan_query(request, hnsw)?;
    let hits = match plan.strategy {
        ExecutionStrategy::Exact => execute_exact(request, segments)?,
        ExecutionStrategy::Hnsw => {
            let index = hnsw.expect("HNSW plan requires an index");
            index
                .search(request.vector(), request.top_k())?
                .into_iter()
                .map(|hit| PlannedSearchHit {
                    record: hit.record().clone(),
                    score: hit.score(),
                })
                .collect()
        }
        ExecutionStrategy::HnswPostFilter => {
            let index = hnsw.expect("HNSW post-filter plan requires an index");
            let predicate = request
                .predicate()
                .expect("HNSW post-filter plan requires a predicate");
            hnsw_search_filtered(index, request.vector(), request.top_k(), predicate)?
                .into_iter()
                .map(|hit| PlannedSearchHit {
                    record: hit.record().clone(),
                    score: hit.score(),
                })
                .collect()
        }
    };

    Ok(QueryResponse {
        hits,
        explain: SearchExplain {
            strategy: plan.strategy,
            reason: plan.reason,
            collection_id: request.collection_id().clone(),
            metric: request.metric(),
            top_k: request.top_k(),
            has_predicate: request.predicate().is_some(),
            candidate_limit: plan.candidate_limit,
            fallback: plan.fallback,
        },
    })
}

fn plan_query(request: &QueryRequest, hnsw: Option<&HnswIndex>) -> Result<QueryPlan, PlannerError> {
    let has_predicate = request.predicate().is_some();
    match request.preference() {
        ExecutionPreference::Exact => Ok(QueryPlan {
            strategy: ExecutionStrategy::Exact,
            reason: PlanReason::ExplicitExact,
            candidate_limit: None,
            fallback: false,
        }),
        ExecutionPreference::Hnsw => {
            let index = hnsw.ok_or(PlannerError::MissingHnswIndex)?;
            Ok(QueryPlan {
                strategy: if has_predicate {
                    ExecutionStrategy::HnswPostFilter
                } else {
                    ExecutionStrategy::Hnsw
                },
                reason: if has_predicate {
                    PlanReason::ExplicitHnswWithPredicate
                } else {
                    PlanReason::ExplicitHnsw
                },
                candidate_limit: Some(index.config().ef_search),
                fallback: false,
            })
        }
        ExecutionPreference::Auto => match hnsw {
            Some(index) => Ok(QueryPlan {
                strategy: if has_predicate {
                    ExecutionStrategy::HnswPostFilter
                } else {
                    ExecutionStrategy::Hnsw
                },
                reason: if has_predicate {
                    PlanReason::AutoHnswWithPredicate
                } else {
                    PlanReason::AutoHnswAvailable
                },
                candidate_limit: Some(index.config().ef_search),
                fallback: false,
            }),
            None => Ok(QueryPlan {
                strategy: ExecutionStrategy::Exact,
                reason: PlanReason::AutoExactFallbackNoHnsw,
                candidate_limit: None,
                fallback: true,
            }),
        },
    }
}

fn execute_exact(
    request: &QueryRequest,
    segments: &[Segment],
) -> Result<Vec<PlannedSearchHit>, PlannerError> {
    let hits = match request.predicate() {
        Some(predicate) => exact_search_filtered_segments(
            segments,
            request.collection_id(),
            request.vector(),
            request.metric(),
            request.top_k(),
            predicate,
        )?,
        None => exact_search_segments(
            segments,
            request.collection_id(),
            request.vector(),
            request.metric(),
            request.top_k(),
        )?,
    };

    Ok(hits
        .into_iter()
        .map(|hit| PlannedSearchHit {
            record: hit.record().clone(),
            score: hit.score(),
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{HnswConfig, SegmentId, WalMutation};
    use ketebe_core::{FieldPath, Metadata, MetadataValue, RecordId, SequenceNumber, Vector};

    fn collection(name: &str) -> CollectionId {
        CollectionId::new(name).expect("collection")
    }

    fn record(id: u64, sequence: u64, value: f32, category: &str) -> Record {
        let mut metadata = Metadata::new();
        metadata.insert(
            "category".into(),
            MetadataValue::String(category.to_string()),
        );
        Record::new(
            RecordId::unsigned(id),
            Vector::new(vec![value]).expect("vector"),
            metadata,
            SequenceNumber::new(sequence),
        )
    }

    fn segment(collection: &CollectionId) -> Segment {
        let mutations = vec![
            WalMutation::Upsert {
                collection_id: collection.clone(),
                record: record(1, 1, 1.0, "book"),
            },
            WalMutation::Upsert {
                collection_id: collection.clone(),
                record: record(2, 2, 2.0, "game"),
            },
            WalMutation::Upsert {
                collection_id: collection.clone(),
                record: record(3, 3, 3.0, "book"),
            },
        ];
        Segment::from_mutations(SegmentId::new(1), &mutations).expect("segment")
    }

    fn index(segment: &Segment, collection: &CollectionId) -> HnswIndex {
        HnswIndex::build(
            std::slice::from_ref(segment),
            collection,
            DistanceMetric::L2,
            HnswConfig {
                m: 2,
                ef_construction: 8,
                ef_search: 8,
            },
        )
        .expect("index")
    }

    fn request(collection: &CollectionId) -> QueryRequest {
        QueryRequest::new(collection.clone(), vec![1.0], DistanceMetric::L2, 2)
    }

    #[test]
    fn auto_falls_back_to_exact_without_hnsw() {
        let collection = collection("docs");
        let segment = segment(&collection);
        let response = execute_query(&request(&collection), &[segment], None).expect("execute");
        assert_eq!(response.explain().strategy(), ExecutionStrategy::Exact);
        assert_eq!(
            response.explain().reason(),
            PlanReason::AutoExactFallbackNoHnsw
        );
        assert!(response.explain().fallback());
        assert_eq!(response.explain().candidate_limit(), None);
    }

    #[test]
    fn auto_uses_hnsw_when_available() {
        let collection = collection("docs");
        let segment = segment(&collection);
        let index = index(&segment, &collection);
        let response =
            execute_query(&request(&collection), &[segment], Some(&index)).expect("execute");
        assert_eq!(response.explain().strategy(), ExecutionStrategy::Hnsw);
        assert_eq!(response.explain().reason(), PlanReason::AutoHnswAvailable);
        assert_eq!(response.explain().candidate_limit(), Some(8));
        assert!(!response.explain().fallback());
    }

    #[test]
    fn auto_uses_hnsw_post_filter_with_predicate() {
        let collection = collection("docs");
        let segment = segment(&collection);
        let index = index(&segment, &collection);
        let predicate = Predicate::Eq(
            FieldPath::new(["category"]).expect("path"),
            MetadataValue::String("book".into()),
        );
        let request = request(&collection).with_predicate(predicate);
        let response = execute_query(&request, &[segment], Some(&index)).expect("execute");
        assert_eq!(
            response.explain().strategy(),
            ExecutionStrategy::HnswPostFilter
        );
        assert_eq!(
            response.explain().reason(),
            PlanReason::AutoHnswWithPredicate
        );
        assert!(response.explain().has_predicate());
        assert!(response.hits().iter().all(|hit| {
            hit.record().metadata().get("category") == Some(&MetadataValue::String("book".into()))
        }));
    }

    #[test]
    fn explicit_exact_overrides_available_hnsw() {
        let collection = collection("docs");
        let segment = segment(&collection);
        let index = index(&segment, &collection);
        let request = request(&collection).with_preference(ExecutionPreference::Exact);
        let response = execute_query(&request, &[segment], Some(&index)).expect("execute");
        assert_eq!(response.explain().strategy(), ExecutionStrategy::Exact);
        assert_eq!(response.explain().reason(), PlanReason::ExplicitExact);
        assert_eq!(response.explain().candidate_limit(), None);
    }

    #[test]
    fn explicit_hnsw_requires_index() {
        let collection = collection("docs");
        let request = request(&collection).with_preference(ExecutionPreference::Hnsw);
        let error = execute_query(&request, &[], None).expect_err("missing index");
        assert!(matches!(error, PlannerError::MissingHnswIndex));
    }

    #[test]
    fn explicit_hnsw_with_predicate_reports_post_filter() {
        let collection = collection("docs");
        let segment = segment(&collection);
        let index = index(&segment, &collection);
        let predicate = Predicate::Exists(FieldPath::new(["category"]).expect("path"));
        let request = request(&collection)
            .with_predicate(predicate)
            .with_preference(ExecutionPreference::Hnsw);
        let response = execute_query(&request, &[segment], Some(&index)).expect("execute");
        assert_eq!(
            response.explain().strategy(),
            ExecutionStrategy::HnswPostFilter
        );
        assert_eq!(
            response.explain().reason(),
            PlanReason::ExplicitHnswWithPredicate
        );
    }

    #[test]
    fn explain_reports_stable_query_inputs() {
        let collection = collection("docs");
        let segment = segment(&collection);
        let request = request(&collection).with_preference(ExecutionPreference::Exact);
        let response = execute_query(&request, &[segment], None).expect("execute");
        assert_eq!(response.explain().collection_id(), &collection);
        assert_eq!(response.explain().metric(), DistanceMetric::L2);
        assert_eq!(response.explain().top_k(), 2);
        assert!(!response.explain().has_predicate());
    }

    #[test]
    fn exact_plan_matches_existing_exact_api() {
        let collection = collection("docs");
        let segment = segment(&collection);
        let request = request(&collection).with_preference(ExecutionPreference::Exact);
        let planned =
            execute_query(&request, std::slice::from_ref(&segment), None).expect("execute");
        let direct = exact_search_segments(&[segment], &collection, &[1.0], DistanceMetric::L2, 2)
            .expect("direct");
        let planned_ids: Vec<_> = planned
            .hits()
            .iter()
            .map(|hit| hit.record().id().clone())
            .collect();
        let direct_ids: Vec<_> = direct.iter().map(|hit| hit.record().id().clone()).collect();
        assert_eq!(planned_ids, direct_ids);
    }

    #[test]
    fn invalid_query_errors_propagate_from_selected_engine() {
        let collection = collection("docs");
        let segment = segment(&collection);
        let request = QueryRequest::new(collection, vec![f32::NAN], DistanceMetric::L2, 1)
            .with_preference(ExecutionPreference::Exact);
        let error = execute_query(&request, &[segment], None).expect_err("invalid query");
        assert!(matches!(
            error,
            PlannerError::Exact(SearchError::NonFiniteQueryValue { .. })
        ));
    }

    #[test]
    fn rejects_hnsw_from_another_collection() {
        let docs = collection("docs");
        let other = collection("other");
        let other_segment = segment(&other);
        let index = index(&other_segment, &other);
        let error = execute_query(&request(&docs), &[], Some(&index)).expect_err("mismatch");
        assert!(matches!(error, PlannerError::HnswCollectionMismatch { .. }));
    }
}
