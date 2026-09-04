use crate::{QueryControl, QueryControlError, Segment, SegmentStore};
use ketebe_core::{
    CollectionId, DistanceMetric, Predicate, PredicateError, Record, RecordId, SequenceNumber,
    Vector,
};
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::fmt;

#[derive(Debug, Clone, PartialEq)]
pub struct SearchHit {
    record: Record,
    score: f32,
}

impl SearchHit {
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
pub struct SearchAfter {
    score: f32,
    record_id: RecordId,
}

impl SearchAfter {
    #[must_use]
    pub fn new(score: f32, record_id: RecordId) -> Self {
        Self { score, record_id }
    }

    #[must_use]
    pub const fn score(&self) -> f32 {
        self.score
    }

    #[must_use]
    pub fn record_id(&self) -> &RecordId {
        &self.record_id
    }
}

#[derive(Debug)]
pub enum SearchError {
    Segment(crate::SegmentError),
    InvalidTopK,
    EmptyQueryVector,
    NonFiniteQueryValue { index: usize },
    DimensionMismatch { expected: usize, actual: usize },
    ZeroNormVector,
    Control(QueryControlError),
    Predicate(PredicateError),
}

impl fmt::Display for SearchError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Segment(error) => write!(f, "segment search error: {error}"),
            Self::InvalidTopK => f.write_str("top_k must be greater than zero"),
            Self::EmptyQueryVector => f.write_str("query vector must not be empty"),
            Self::NonFiniteQueryValue { index } => {
                write!(f, "query vector contains non-finite value at index {index}")
            }
            Self::DimensionMismatch { expected, actual } => write!(
                f,
                "query vector dimension mismatch: expected={expected}, actual={actual}"
            ),
            Self::ZeroNormVector => {
                f.write_str("cosine similarity is undefined for zero-norm vectors")
            }
            Self::Control(error) => write!(f, "query control stopped exact search: {error}"),
            Self::Predicate(error) => write!(f, "predicate evaluation failed: {error}"),
        }
    }
}

impl std::error::Error for SearchError {}

impl From<crate::SegmentError> for SearchError {
    fn from(value: crate::SegmentError) -> Self {
        Self::Segment(value)
    }
}

impl From<QueryControlError> for SearchError {
    fn from(value: QueryControlError) -> Self {
        Self::Control(value)
    }
}

impl From<PredicateError> for SearchError {
    fn from(value: PredicateError) -> Self {
        Self::Predicate(value)
    }
}

pub fn exact_search(
    store: &SegmentStore,
    collection_id: &CollectionId,
    query: &[f32],
    metric: DistanceMetric,
    top_k: usize,
) -> Result<Vec<SearchHit>, SearchError> {
    validate_query(query, top_k)?;
    let segments = store.discover()?;
    exact_search_segments(&segments, collection_id, query, metric, top_k)
}

pub fn exact_search_segments(
    segments: &[Segment],
    collection_id: &CollectionId,
    query: &[f32],
    metric: DistanceMetric,
    top_k: usize,
) -> Result<Vec<SearchHit>, SearchError> {
    exact_search_segments_with_control(
        segments,
        collection_id,
        query,
        metric,
        top_k,
        &QueryControl::unbounded(),
    )
}

pub fn exact_search_segments_with_control(
    segments: &[Segment],
    collection_id: &CollectionId,
    query: &[f32],
    metric: DistanceMetric,
    top_k: usize,
    control: &QueryControl,
) -> Result<Vec<SearchHit>, SearchError> {
    exact_search_segments_after_with_control(
        segments,
        collection_id,
        query,
        metric,
        top_k,
        None,
        None,
        control,
    )
}

// The primitive keeps each search invariant explicit at the call boundary.
#[allow(clippy::too_many_arguments)]
pub fn exact_search_segments_after_with_control(
    segments: &[Segment],
    collection_id: &CollectionId,
    query: &[f32],
    metric: DistanceMetric,
    top_k: usize,
    predicate: Option<&Predicate>,
    after: Option<&SearchAfter>,
    control: &QueryControl,
) -> Result<Vec<SearchHit>, SearchError> {
    control.check()?;
    validate_query(query, top_k)?;
    let visible = fold_visible_records(segments, collection_id);
    control.check()?;
    if visible.is_empty() {
        return Ok(Vec::new());
    }

    let expected = visible
        .values()
        .next()
        .expect("visible is non-empty")
        .vector()
        .len();
    if query.len() != expected {
        return Err(SearchError::DimensionMismatch {
            expected,
            actual: query.len(),
        });
    }
    if metric == DistanceMetric::Cosine && norm(query) == 0.0 {
        return Err(SearchError::ZeroNormVector);
    }

    let mut hits = Vec::with_capacity(top_k.min(visible.len()));
    for (index, record) in visible.into_values().enumerate() {
        if index % 256 == 0 {
            control.check()?;
        }
        if record.vector().len() != expected {
            return Err(SearchError::DimensionMismatch {
                expected,
                actual: record.vector().len(),
            });
        }
        if let Some(predicate) = predicate
            && !predicate.evaluate(record.metadata())?
        {
            continue;
        }
        let score = score(query, record.vector(), metric)?;
        if let Some(after) = after
            && !compare_score_and_id(score, record.id(), after.score, &after.record_id, metric)
                .is_gt()
        {
            continue;
        }
        hits.push(SearchHit { record, score });
        hits.sort_by(|left, right| compare_hits(left, right, metric));
        if hits.len() > top_k {
            hits.pop();
        }
    }
    control.check()?;
    Ok(hits)
}

fn validate_query(query: &[f32], top_k: usize) -> Result<(), SearchError> {
    if top_k == 0 {
        return Err(SearchError::InvalidTopK);
    }
    if query.is_empty() {
        return Err(SearchError::EmptyQueryVector);
    }
    for (index, value) in query.iter().enumerate() {
        if !value.is_finite() {
            return Err(SearchError::NonFiniteQueryValue { index });
        }
    }
    Ok(())
}

fn fold_visible_records(
    segments: &[Segment],
    collection_id: &CollectionId,
) -> BTreeMap<RecordId, Record> {
    let mut latest = BTreeMap::<RecordId, VisibleVersion>::new();

    for segment in segments {
        if segment.collection_id() != collection_id {
            continue;
        }

        for record in segment.records() {
            apply_version(
                &mut latest,
                record.id().clone(),
                record.sequence_number(),
                Some(record.clone()),
            );
        }

        for tombstone in segment.tombstones() {
            apply_version(
                &mut latest,
                tombstone.record_id().clone(),
                tombstone.sequence_number(),
                None,
            );
        }
    }

    latest
        .into_iter()
        .filter_map(|(id, version)| version.record.map(|record| (id, record)))
        .collect()
}

struct VisibleVersion {
    sequence: SequenceNumber,
    record: Option<Record>,
}

fn apply_version(
    latest: &mut BTreeMap<RecordId, VisibleVersion>,
    id: RecordId,
    sequence: SequenceNumber,
    record: Option<Record>,
) {
    match latest.get(&id) {
        Some(existing) if existing.sequence >= sequence => {}
        _ => {
            latest.insert(id, VisibleVersion { sequence, record });
        }
    }
}

fn score(query: &[f32], vector: &Vector, metric: DistanceMetric) -> Result<f32, SearchError> {
    let candidate = vector.as_slice();
    match metric {
        DistanceMetric::Dot => Ok(dot(query, candidate)),
        DistanceMetric::L2 => Ok(l2(query, candidate)),
        DistanceMetric::Cosine => {
            let query_norm = norm(query);
            let candidate_norm = norm(candidate);
            if query_norm == 0.0 || candidate_norm == 0.0 {
                return Err(SearchError::ZeroNormVector);
            }
            Ok(dot(query, candidate) / (query_norm * candidate_norm))
        }
    }
}

fn dot(left: &[f32], right: &[f32]) -> f32 {
    left.iter().zip(right).map(|(a, b)| a * b).sum()
}

fn l2(left: &[f32], right: &[f32]) -> f32 {
    left.iter()
        .zip(right)
        .map(|(a, b)| {
            let delta = a - b;
            delta * delta
        })
        .sum::<f32>()
        .sqrt()
}

fn norm(vector: &[f32]) -> f32 {
    vector.iter().map(|value| value * value).sum::<f32>().sqrt()
}

fn compare_hits(left: &SearchHit, right: &SearchHit, metric: DistanceMetric) -> Ordering {
    compare_score_and_id(
        left.score,
        left.record.id(),
        right.score,
        right.record.id(),
        metric,
    )
    .then_with(|| {
        left.record
            .sequence_number()
            .cmp(&right.record.sequence_number())
    })
}

fn compare_score_and_id(
    left_score: f32,
    left_id: &RecordId,
    right_score: f32,
    right_id: &RecordId,
    metric: DistanceMetric,
) -> Ordering {
    let score_order = match metric {
        DistanceMetric::L2 => left_score.total_cmp(&right_score),
        DistanceMetric::Cosine | DistanceMetric::Dot => right_score.total_cmp(&left_score),
    };
    score_order.then_with(|| left_id.cmp(right_id))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{SegmentId, Tombstone, WalMutation};
    use ketebe_core::Metadata;

    fn collection(name: &str) -> CollectionId {
        CollectionId::new(name).expect("collection")
    }

    fn record(id: RecordId, sequence: u64, values: &[f32]) -> Record {
        Record::new(
            id,
            Vector::new(values.to_vec()).expect("vector"),
            Metadata::default(),
            SequenceNumber::new(sequence),
        )
    }

    fn upsert(collection_id: &CollectionId, record: Record) -> WalMutation {
        WalMutation::Upsert {
            collection_id: collection_id.clone(),
            record,
        }
    }

    fn delete(collection_id: &CollectionId, id: RecordId, sequence: u64) -> WalMutation {
        WalMutation::Delete {
            collection_id: collection_id.clone(),
            record_id: id,
            sequence_number: SequenceNumber::new(sequence),
        }
    }

    fn segment(id: u64, mutations: Vec<WalMutation>) -> Segment {
        Segment::from_mutations(SegmentId::new(id), &mutations).expect("segment")
    }

    #[test]
    fn cosine_ranking_is_exact() {
        let c = collection("docs");
        let s = segment(
            1,
            vec![
                upsert(&c, record(RecordId::unsigned(1), 1, &[1.0, 0.0])),
                upsert(&c, record(RecordId::unsigned(2), 2, &[0.5, 0.5])),
                upsert(&c, record(RecordId::unsigned(3), 3, &[0.0, 1.0])),
            ],
        );
        let hits = exact_search_segments(&[s], &c, &[1.0, 0.0], DistanceMetric::Cosine, 3)
            .expect("search");
        assert_eq!(hits[0].record().id(), &RecordId::unsigned(1));
        assert_eq!(hits[2].record().id(), &RecordId::unsigned(3));
    }

    #[test]
    fn dot_and_l2_rank_correctly() {
        let c = collection("docs");
        let s = segment(
            1,
            vec![
                upsert(&c, record(RecordId::unsigned(1), 1, &[2.0, 0.0])),
                upsert(&c, record(RecordId::unsigned(2), 2, &[1.0, 0.0])),
            ],
        );
        let dot_hits = exact_search_segments(
            std::slice::from_ref(&s),
            &c,
            &[1.0, 0.0],
            DistanceMetric::Dot,
            2,
        )
        .expect("dot");
        assert_eq!(dot_hits[0].record().id(), &RecordId::unsigned(1));

        let l2_hits =
            exact_search_segments(&[s], &c, &[1.0, 0.0], DistanceMetric::L2, 2).expect("l2");
        assert_eq!(l2_hits[0].record().id(), &RecordId::unsigned(2));
    }

    #[test]
    fn top_k_and_tie_breaking_are_deterministic() {
        let c = collection("docs");
        let s = segment(
            1,
            vec![
                upsert(&c, record(RecordId::unsigned(2), 1, &[1.0, 0.0])),
                upsert(&c, record(RecordId::unsigned(1), 2, &[1.0, 0.0])),
            ],
        );
        let hits =
            exact_search_segments(&[s], &c, &[1.0, 0.0], DistanceMetric::Dot, 1).expect("search");
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].record().id(), &RecordId::unsigned(1));
    }

    #[test]
    fn latest_update_wins_across_segments() {
        let c = collection("docs");
        let id = RecordId::unsigned(1);
        let old = segment(1, vec![upsert(&c, record(id.clone(), 1, &[1.0, 0.0]))]);
        let new = segment(2, vec![upsert(&c, record(id.clone(), 2, &[0.0, 1.0]))]);
        let hits = exact_search_segments(&[old, new], &c, &[0.0, 1.0], DistanceMetric::Dot, 10)
            .expect("search");
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].record().sequence_number(), SequenceNumber::new(2));
    }

    #[test]
    fn tombstone_hides_older_record_and_later_upsert_resurrects_it() {
        let c = collection("docs");
        let id = RecordId::unsigned(1);
        let live = segment(1, vec![upsert(&c, record(id.clone(), 1, &[1.0, 0.0]))]);
        let deleted = segment(2, vec![delete(&c, id.clone(), 2)]);
        let no_hits = exact_search_segments(
            &[live.clone(), deleted.clone()],
            &c,
            &[1.0, 0.0],
            DistanceMetric::Dot,
            10,
        )
        .expect("search");
        assert!(no_hits.is_empty());

        let resurrected = segment(3, vec![upsert(&c, record(id, 3, &[1.0, 0.0]))]);
        let hits = exact_search_segments(
            &[live, deleted, resurrected],
            &c,
            &[1.0, 0.0],
            DistanceMetric::Dot,
            10,
        )
        .expect("search");
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].record().sequence_number(), SequenceNumber::new(3));
    }

    #[test]
    fn collection_isolation_and_empty_results_work() {
        let a = collection("a");
        let b = collection("b");
        let s = segment(
            1,
            vec![upsert(&a, record(RecordId::unsigned(1), 1, &[1.0]))],
        );
        let hits =
            exact_search_segments(&[s], &b, &[1.0], DistanceMetric::Dot, 10).expect("search");
        assert!(hits.is_empty());
    }

    #[test]
    fn search_after_preserves_score_and_typed_id_order() {
        let c = collection("docs");
        let s = segment(
            1,
            vec![
                upsert(&c, record(RecordId::string("1").unwrap(), 1, &[1.0])),
                upsert(&c, record(RecordId::unsigned(1), 2, &[1.0])),
                upsert(&c, record(RecordId::unsigned(2), 3, &[1.0])),
            ],
        );
        let first = exact_search_segments_after_with_control(
            std::slice::from_ref(&s),
            &c,
            &[1.0],
            DistanceMetric::Dot,
            2,
            None,
            None,
            &QueryControl::unbounded(),
        )
        .unwrap();
        assert_eq!(first.len(), 2);
        let boundary = SearchAfter::new(first[1].score(), first[1].record().id().clone());
        let next = exact_search_segments_after_with_control(
            &[s],
            &c,
            &[1.0],
            DistanceMetric::Dot,
            2,
            None,
            Some(&boundary),
            &QueryControl::unbounded(),
        )
        .unwrap();
        assert_eq!(next.len(), 1);
        assert!(
            first
                .iter()
                .all(|hit| hit.record().id() != next[0].record().id())
        );
    }

    #[test]
    fn invalid_queries_are_rejected() {
        let c = collection("docs");
        let s = segment(
            1,
            vec![upsert(&c, record(RecordId::unsigned(1), 1, &[1.0, 2.0]))],
        );
        assert!(matches!(
            exact_search_segments(std::slice::from_ref(&s), &c, &[1.0], DistanceMetric::Dot, 1),
            Err(SearchError::DimensionMismatch { .. })
        ));
        assert!(matches!(
            exact_search_segments(
                std::slice::from_ref(&s),
                &c,
                &[f32::NAN, 1.0],
                DistanceMetric::Dot,
                1
            ),
            Err(SearchError::NonFiniteQueryValue { .. })
        ));
        assert!(matches!(
            exact_search_segments(&[s], &c, &[1.0, 2.0], DistanceMetric::Dot, 0),
            Err(SearchError::InvalidTopK)
        ));
    }

    #[test]
    fn string_and_numeric_ids_remain_distinct() {
        let c = collection("docs");
        let s = segment(
            1,
            vec![
                upsert(&c, record(RecordId::string("42").expect("id"), 1, &[1.0])),
                upsert(&c, record(RecordId::unsigned(42), 2, &[2.0])),
            ],
        );
        let hits =
            exact_search_segments(&[s], &c, &[1.0], DistanceMetric::Dot, 10).expect("search");
        assert_eq!(hits.len(), 2);
    }

    #[test]
    fn tombstone_type_is_publicly_usable() {
        let tombstone = Tombstone::new(RecordId::unsigned(1), SequenceNumber::new(1));
        assert_eq!(tombstone.sequence_number(), SequenceNumber::new(1));
    }
}
