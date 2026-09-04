#[rustfmt::skip]
mod implementation {
use crate::{
    FilteredSearchError, HnswError, HnswIndex, QueryControl, SearchError, Segment,
    exact_search_filtered_segments_with_control, exact_search_segments_with_control,
    hnsw_search_filtered_with_control,
};
use ketebe_core::{CollectionId, DistanceMetric, Predicate, Record, RecordId, SequenceNumber};
use std::collections::BTreeMap;
use std::fmt;

pub const DEFAULT_ANN_MIN_RECORDS: usize = 1_024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PlannerConfig { ann_min_records: usize }
impl Default for PlannerConfig { fn default() -> Self { Self { ann_min_records: DEFAULT_ANN_MIN_RECORDS } } }
impl PlannerConfig {
    #[must_use] pub const fn new(ann_min_records: usize) -> Self { Self { ann_min_records } }
    #[must_use] pub const fn ann_min_records(self) -> usize { self.ann_min_records }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)] pub enum ExecutionPreference { Auto, Exact, Hnsw }
#[derive(Debug, Clone, Copy, PartialEq, Eq)] pub enum ExecutionStrategy { Exact, Hnsw, HnswPostFilter }
#[derive(Debug, Clone, Copy, PartialEq, Eq)] pub enum PlanReason { ExplicitExact, ExplicitHnsw, ExplicitHnswWithPredicate, AutoHnswAvailable, AutoHnswWithPredicate, AutoExactFallbackNoHnsw }
#[derive(Debug, Clone, Copy, PartialEq, Eq)] pub enum CostReason { NotApplicable, NoHnsw, SmallCorpus, FilteredExactConservative, AnnThresholdReached }

#[derive(Debug, Clone, PartialEq)]
pub struct QueryRequest { collection_id: CollectionId, vector: Vec<f32>, metric: DistanceMetric, top_k: usize, predicate: Option<Predicate>, preference: ExecutionPreference }
impl QueryRequest {
    #[must_use] pub fn new(collection_id: CollectionId, vector: Vec<f32>, metric: DistanceMetric, top_k: usize) -> Self { Self { collection_id, vector, metric, top_k, predicate: None, preference: ExecutionPreference::Auto } }
    #[must_use] pub fn with_predicate(mut self, predicate: Predicate) -> Self { self.predicate = Some(predicate); self }
    #[must_use] pub const fn with_preference(mut self, preference: ExecutionPreference) -> Self { self.preference = preference; self }
    #[must_use] pub fn collection_id(&self) -> &CollectionId { &self.collection_id }
    #[must_use] pub fn vector(&self) -> &[f32] { &self.vector }
    #[must_use] pub const fn metric(&self) -> DistanceMetric { self.metric }
    #[must_use] pub const fn top_k(&self) -> usize { self.top_k }
    #[must_use] pub fn predicate(&self) -> Option<&Predicate> { self.predicate.as_ref() }
    #[must_use] pub const fn preference(&self) -> ExecutionPreference { self.preference }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchExplain { strategy: ExecutionStrategy, reason: PlanReason, cost_reason: CostReason, collection_id: CollectionId, metric: DistanceMetric, top_k: usize, has_predicate: bool, candidate_limit: Option<usize>, fallback: bool, corpus_size: usize, ann_min_records: usize }
impl SearchExplain {
    #[must_use] pub const fn strategy(&self) -> ExecutionStrategy { self.strategy }
    #[must_use] pub const fn reason(&self) -> PlanReason { self.reason }
    #[must_use] pub const fn cost_reason(&self) -> CostReason { self.cost_reason }
    #[must_use] pub fn collection_id(&self) -> &CollectionId { &self.collection_id }
    #[must_use] pub const fn metric(&self) -> DistanceMetric { self.metric }
    #[must_use] pub const fn top_k(&self) -> usize { self.top_k }
    #[must_use] pub const fn has_predicate(&self) -> bool { self.has_predicate }
    #[must_use] pub const fn candidate_limit(&self) -> Option<usize> { self.candidate_limit }
    #[must_use] pub const fn fallback(&self) -> bool { self.fallback }
    #[must_use] pub const fn corpus_size(&self) -> usize { self.corpus_size }
    #[must_use] pub const fn ann_min_records(&self) -> usize { self.ann_min_records }
}

#[derive(Debug, Clone, PartialEq)] pub struct PlannedSearchHit { record: Record, score: f32 }
impl PlannedSearchHit { #[must_use] pub fn record(&self) -> &Record { &self.record } #[must_use] pub const fn score(&self) -> f32 { self.score } }
#[derive(Debug, Clone, PartialEq)] pub struct QueryResponse { hits: Vec<PlannedSearchHit>, explain: SearchExplain }
impl QueryResponse { #[must_use] pub fn hits(&self) -> &[PlannedSearchHit] { &self.hits } #[must_use] pub const fn explain(&self) -> &SearchExplain { &self.explain } }

#[derive(Debug, Clone, Copy)] struct QueryPlan { strategy: ExecutionStrategy, reason: PlanReason, cost_reason: CostReason, candidate_limit: Option<usize>, fallback: bool }
#[derive(Debug)] pub enum PlannerError { MissingHnswIndex, HnswCollectionMismatch { query: CollectionId, index: CollectionId }, Exact(SearchError), Hnsw(HnswError), Filtered(FilteredSearchError) }
impl fmt::Display for PlannerError { fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { match self { Self::MissingHnswIndex => f.write_str("HNSW execution was requested but no HNSW index was supplied"), Self::HnswCollectionMismatch { query, index } => write!(f, "HNSW index collection does not match query: query={}, index={}", query.as_str(), index.as_str()), Self::Exact(e) => write!(f, "exact execution failed: {e}"), Self::Hnsw(e) => write!(f, "HNSW execution failed: {e}"), Self::Filtered(e) => write!(f, "filtered execution failed: {e}") } } }
impl std::error::Error for PlannerError {}
impl From<SearchError> for PlannerError { fn from(v: SearchError) -> Self { Self::Exact(v) } }
impl From<HnswError> for PlannerError { fn from(v: HnswError) -> Self { Self::Hnsw(v) } }
impl From<FilteredSearchError> for PlannerError { fn from(v: FilteredSearchError) -> Self { Self::Filtered(v) } }

pub fn execute_query(request: &QueryRequest, segments: &[Segment], hnsw: Option<&HnswIndex>) -> Result<QueryResponse, PlannerError> { execute_query_with_config(request, segments, hnsw, PlannerConfig::default()) }
pub fn execute_query_with_config(request: &QueryRequest, segments: &[Segment], hnsw: Option<&HnswIndex>, config: PlannerConfig) -> Result<QueryResponse, PlannerError> { execute_query_with_config_and_control(request, segments, hnsw, config, &QueryControl::unbounded()) }
pub fn execute_query_with_control(request: &QueryRequest, segments: &[Segment], hnsw: Option<&HnswIndex>, control: &QueryControl) -> Result<QueryResponse, PlannerError> { execute_query_with_config_and_control(request, segments, hnsw, PlannerConfig::default(), control) }
pub fn execute_query_with_config_and_control(request: &QueryRequest, segments: &[Segment], hnsw: Option<&HnswIndex>, config: PlannerConfig, control: &QueryControl) -> Result<QueryResponse, PlannerError> {
    control.check().map_err(|error| PlannerError::Exact(SearchError::from(error)))?;
    if let Some(index) = hnsw && index.collection_id() != request.collection_id() { return Err(PlannerError::HnswCollectionMismatch { query: request.collection_id().clone(), index: index.collection_id().clone() }); }
    let corpus_size = visible_record_count(segments, request.collection_id());
    let plan = plan_query(request, hnsw, corpus_size, config)?;
    let hits = match plan.strategy {
        ExecutionStrategy::Exact => execute_exact_controlled(request, segments, control)?,
        ExecutionStrategy::Hnsw => hnsw.expect("HNSW plan requires index").search_with_control(request.vector(), request.top_k(), control)?.into_iter().map(|h| PlannedSearchHit { record: h.record().clone(), score: h.score() }).collect(),
        ExecutionStrategy::HnswPostFilter => hnsw_search_filtered_with_control(hnsw.expect("HNSW plan requires index"), request.vector(), request.top_k(), request.predicate().expect("predicate required"), control)?.into_iter().map(|h| PlannedSearchHit { record: h.record().clone(), score: h.score() }).collect(),
    };
    Ok(QueryResponse { hits, explain: SearchExplain { strategy: plan.strategy, reason: plan.reason, cost_reason: plan.cost_reason, collection_id: request.collection_id().clone(), metric: request.metric(), top_k: request.top_k(), has_predicate: request.predicate().is_some(), candidate_limit: plan.candidate_limit, fallback: plan.fallback, corpus_size, ann_min_records: config.ann_min_records() } })
}
fn plan_query(request: &QueryRequest, hnsw: Option<&HnswIndex>, corpus_size: usize, config: PlannerConfig) -> Result<QueryPlan, PlannerError> {
    let filtered = request.predicate().is_some();
    match request.preference() {
        ExecutionPreference::Exact => Ok(QueryPlan { strategy: ExecutionStrategy::Exact, reason: PlanReason::ExplicitExact, cost_reason: CostReason::NotApplicable, candidate_limit: None, fallback: false }),
        ExecutionPreference::Hnsw => { let index = hnsw.ok_or(PlannerError::MissingHnswIndex)?; Ok(QueryPlan { strategy: if filtered { ExecutionStrategy::HnswPostFilter } else { ExecutionStrategy::Hnsw }, reason: if filtered { PlanReason::ExplicitHnswWithPredicate } else { PlanReason::ExplicitHnsw }, cost_reason: CostReason::NotApplicable, candidate_limit: Some(index.config().ef_search), fallback: false }) }
        ExecutionPreference::Auto => { let Some(index) = hnsw else { return Ok(QueryPlan { strategy: ExecutionStrategy::Exact, reason: PlanReason::AutoExactFallbackNoHnsw, cost_reason: CostReason::NoHnsw, candidate_limit: None, fallback: true }); }; if filtered { return Ok(QueryPlan { strategy: ExecutionStrategy::Exact, reason: PlanReason::AutoHnswWithPredicate, cost_reason: CostReason::FilteredExactConservative, candidate_limit: None, fallback: true }); } if corpus_size < config.ann_min_records() { return Ok(QueryPlan { strategy: ExecutionStrategy::Exact, reason: PlanReason::AutoHnswAvailable, cost_reason: CostReason::SmallCorpus, candidate_limit: None, fallback: true }); } Ok(QueryPlan { strategy: ExecutionStrategy::Hnsw, reason: PlanReason::AutoHnswAvailable, cost_reason: CostReason::AnnThresholdReached, candidate_limit: Some(index.config().ef_search), fallback: false }) }
    }
}
fn visible_record_count(segments: &[Segment], collection: &CollectionId) -> usize { let mut latest = BTreeMap::<RecordId, (SequenceNumber, bool)>::new(); for segment in segments.iter().filter(|s| s.collection_id() == collection) { for record in segment.records() { let candidate = (record.sequence_number(), true); if latest.get(record.id()).is_none_or(|current| candidate.0 > current.0) { latest.insert(record.id().clone(), candidate); } } for tombstone in segment.tombstones() { let candidate = (tombstone.sequence_number(), false); if latest.get(tombstone.record_id()).is_none_or(|current| candidate.0 > current.0) { latest.insert(tombstone.record_id().clone(), candidate); } } } latest.values().filter(|(_, live)| *live).count() }
fn execute_exact_controlled(request: &QueryRequest, segments: &[Segment], control: &QueryControl) -> Result<Vec<PlannedSearchHit>, PlannerError> { let hits = match request.predicate() { Some(predicate) => exact_search_filtered_segments_with_control(segments, request.collection_id(), request.vector(), request.metric(), request.top_k(), predicate, control)?, None => exact_search_segments_with_control(segments, request.collection_id(), request.vector(), request.metric(), request.top_k(), control)? }; Ok(hits.into_iter().map(|h| PlannedSearchHit { record: h.record().clone(), score: h.score() }).collect()) }

#[cfg(test)] mod tests {
    use super::*; use crate::{HnswConfig, SegmentId, WalMutation}; use ketebe_core::{Metadata, RecordId, SequenceNumber, Vector};
    fn fixture(count: usize) -> (CollectionId, Segment, HnswIndex) { let id = CollectionId::new("docs").unwrap(); let mutations = (0..count).map(|n| WalMutation::Upsert { collection_id: id.clone(), record: Record::new(RecordId::unsigned(n as u64), Vector::new(vec![n as f32 + 1.0]).unwrap(), Metadata::new(), SequenceNumber::new(n as u64 + 1)) }).collect::<Vec<_>>(); let segment = Segment::from_mutations(SegmentId::new(1), &mutations).unwrap(); let index = HnswIndex::build(std::slice::from_ref(&segment), &id, DistanceMetric::L2, HnswConfig { m: 2, ef_construction: 8, ef_search: 8 }).unwrap(); (id, segment, index) }
    #[test] fn threshold_boundary_is_deterministic() { let (id, segment, index)=fixture(3); let request=QueryRequest::new(id, vec![1.0], DistanceMetric::L2, 1); let below=execute_query_with_config(&request,std::slice::from_ref(&segment),Some(&index),PlannerConfig::new(4)).unwrap(); assert_eq!(below.explain().strategy(),ExecutionStrategy::Exact); assert_eq!(below.explain().cost_reason(),CostReason::SmallCorpus); let at=execute_query_with_config(&request,&[segment],Some(&index),PlannerConfig::new(3)).unwrap(); assert_eq!(at.explain().strategy(),ExecutionStrategy::Hnsw); assert_eq!(at.explain().cost_reason(),CostReason::AnnThresholdReached); assert_eq!(at.explain().corpus_size(),3); assert_eq!(at.explain().ann_min_records(),3); }
    #[test] fn auto_filter_is_conservative_exact() { let (id,segment,index)=fixture(3); let request=QueryRequest::new(id,vec![1.0],DistanceMetric::L2,1).with_predicate(Predicate::And(vec![])); let response=execute_query_with_config(&request,&[segment],Some(&index),PlannerConfig::new(1)).unwrap(); assert_eq!(response.explain().strategy(),ExecutionStrategy::Exact); assert_eq!(response.explain().cost_reason(),CostReason::FilteredExactConservative); }
    #[test] fn explicit_modes_ignore_cost_threshold() { let (id,segment,index)=fixture(3); let exact=QueryRequest::new(id.clone(),vec![1.0],DistanceMetric::L2,1).with_preference(ExecutionPreference::Exact); assert_eq!(execute_query_with_config(&exact,std::slice::from_ref(&segment),Some(&index),PlannerConfig::new(0)).unwrap().explain().strategy(),ExecutionStrategy::Exact); let ann=QueryRequest::new(id,vec![1.0],DistanceMetric::L2,1).with_preference(ExecutionPreference::Hnsw); assert_eq!(execute_query_with_config(&ann,&[segment],Some(&index),PlannerConfig::new(usize::MAX)).unwrap().explain().strategy(),ExecutionStrategy::Hnsw); }
}
}

pub use implementation::*;
