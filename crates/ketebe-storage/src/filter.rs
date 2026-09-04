use crate::{
    HnswError, HnswHit, HnswIndex, QueryControl, SearchError, SearchHit, Segment,
    exact_search_segments_with_control,
};
use ketebe_core::{CollectionId, DistanceMetric, Predicate, PredicateError};
use std::fmt;

#[derive(Debug)]
pub enum FilteredSearchError {
    Exact(SearchError),
    Hnsw(HnswError),
    Predicate(PredicateError),
}

impl fmt::Display for FilteredSearchError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Exact(error) => write!(f, "exact search failed: {error}"),
            Self::Hnsw(error) => write!(f, "HNSW search failed: {error}"),
            Self::Predicate(error) => write!(f, "predicate evaluation failed: {error}"),
        }
    }
}

impl std::error::Error for FilteredSearchError {}

impl From<SearchError> for FilteredSearchError {
    fn from(value: SearchError) -> Self {
        Self::Exact(value)
    }
}

impl From<HnswError> for FilteredSearchError {
    fn from(value: HnswError) -> Self {
        Self::Hnsw(value)
    }
}

impl From<PredicateError> for FilteredSearchError {
    fn from(value: PredicateError) -> Self {
        Self::Predicate(value)
    }
}

pub fn exact_search_filtered_segments(
    segments: &[Segment],
    collection_id: &CollectionId,
    query: &[f32],
    metric: DistanceMetric,
    top_k: usize,
    predicate: &Predicate,
) -> Result<Vec<SearchHit>, FilteredSearchError> {
    exact_search_filtered_segments_with_control(
        segments,
        collection_id,
        query,
        metric,
        top_k,
        predicate,
        &QueryControl::unbounded(),
    )
}

pub fn exact_search_filtered_segments_with_control(
    segments: &[Segment],
    collection_id: &CollectionId,
    query: &[f32],
    metric: DistanceMetric,
    top_k: usize,
    predicate: &Predicate,
    control: &QueryControl,
) -> Result<Vec<SearchHit>, FilteredSearchError> {
    if top_k == 0 {
        return Err(SearchError::InvalidTopK.into());
    }

    let hits = exact_search_segments_with_control(
        segments,
        collection_id,
        query,
        metric,
        usize::MAX,
        control,
    )?;
    let mut filtered = Vec::with_capacity(top_k.min(hits.len()));
    for (index, hit) in hits.into_iter().enumerate() {
        if index % 256 == 0 {
            control.check().map_err(SearchError::from)?;
        }
        if predicate.evaluate(hit.record().metadata())? {
            filtered.push(hit);
            if filtered.len() == top_k {
                break;
            }
        }
    }
    Ok(filtered)
}

pub fn hnsw_search_filtered(
    index: &HnswIndex,
    query: &[f32],
    top_k: usize,
    predicate: &Predicate,
) -> Result<Vec<HnswHit>, FilteredSearchError> {
    hnsw_search_filtered_with_control(index, query, top_k, predicate, &QueryControl::unbounded())
}

pub fn hnsw_search_filtered_with_control(
    index: &HnswIndex,
    query: &[f32],
    top_k: usize,
    predicate: &Predicate,
    control: &QueryControl,
) -> Result<Vec<HnswHit>, FilteredSearchError> {
    if top_k == 0 {
        return Err(HnswError::InvalidTopK.into());
    }

    let candidate_limit = index.config().ef_search;
    let candidates = index.search_with_control(query, candidate_limit, control)?;
    let mut filtered = Vec::with_capacity(top_k.min(candidates.len()));
    for (index, hit) in candidates.into_iter().enumerate() {
        if index % 64 == 0 {
            control.check().map_err(HnswError::from)?;
        }
        if predicate.evaluate(hit.record().metadata())? {
            filtered.push(hit);
            if filtered.len() == top_k {
                break;
            }
        }
    }
    Ok(filtered)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{HnswConfig, SegmentId, WalMutation};
    use ketebe_core::{
        FieldPath, Metadata, MetadataValue, Record, RecordId, SequenceNumber, Vector,
    };
    use std::collections::BTreeMap;

    fn collection() -> CollectionId {
        CollectionId::new("products").expect("collection")
    }

    fn path(parts: &[&str]) -> FieldPath {
        FieldPath::new(parts.iter().copied()).expect("path")
    }

    fn record(
        id: u64,
        sequence: u64,
        vector: &[f32],
        price: f64,
        category: &str,
        tags: &[&str],
    ) -> Record {
        let mut product = BTreeMap::new();
        product.insert(
            "category".to_string(),
            MetadataValue::String(category.to_string()),
        );
        let mut metadata = Metadata::new();
        metadata.insert("price".into(), MetadataValue::Number(price));
        metadata.insert("active".into(), MetadataValue::Bool(true));
        metadata.insert("nullable".into(), MetadataValue::Null);
        metadata.insert("product".into(), MetadataValue::Object(product));
        metadata.insert(
            "tags".into(),
            MetadataValue::Array(
                tags.iter()
                    .map(|tag| MetadataValue::String((*tag).to_string()))
                    .collect(),
            ),
        );
        Record::new(
            RecordId::unsigned(id),
            Vector::new(vector.to_vec()).expect("vector"),
            metadata,
            SequenceNumber::new(sequence),
        )
    }

    fn segment(id: u64, collection: &CollectionId, records: Vec<Record>) -> Segment {
        let mutations: Vec<_> = records
            .into_iter()
            .map(|record| WalMutation::Upsert {
                collection_id: collection.clone(),
                record,
            })
            .collect();
        Segment::from_mutations(SegmentId::new(id), &mutations).expect("segment")
    }

    #[test]
    fn exact_filter_applies_before_final_top_k_semantics() {
        let collection = collection();
        let segment = segment(
            1,
            &collection,
            vec![
                record(1, 1, &[10.0], 1000.0, "book", &["rust"]),
                record(2, 2, &[9.0], 100.0, "book", &["rust"]),
                record(3, 3, &[8.0], 90.0, "game", &["sale"]),
            ],
        );
        let predicate = Predicate::Lt(path(&["price"]), MetadataValue::Number(500.0));
        let hits = exact_search_filtered_segments(
            &[segment],
            &collection,
            &[1.0],
            DistanceMetric::Dot,
            1,
            &predicate,
        )
        .expect("search");
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].record().id(), &RecordId::unsigned(2));
    }

    #[test]
    fn nested_array_and_boolean_predicates_work_in_search() {
        let collection = collection();
        let segment = segment(
            1,
            &collection,
            vec![
                record(1, 1, &[2.0], 100.0, "book", &["rust", "db"]),
                record(2, 2, &[1.0], 100.0, "game", &["sale"]),
            ],
        );
        let predicate = Predicate::And(vec![
            Predicate::Eq(
                path(&["product", "category"]),
                MetadataValue::String("book".into()),
            ),
            Predicate::Contains(path(&["tags"]), MetadataValue::String("rust".into())),
            Predicate::Exists(path(&["nullable"])),
            Predicate::Not(Box::new(Predicate::Exists(path(&["missing"])))),
        ]);
        let hits = exact_search_filtered_segments(
            &[segment],
            &collection,
            &[1.0],
            DistanceMetric::Dot,
            10,
            &predicate,
        )
        .expect("search");
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].record().id(), &RecordId::unsigned(1));
    }

    #[test]
    fn hnsw_post_filter_uses_candidate_expansion() {
        let collection = collection();
        let records: Vec<_> = (1..=20)
            .map(|id| {
                let category = if id % 2 == 0 { "book" } else { "game" };
                record(id, id, &[id as f32], id as f64, category, &["tag"])
            })
            .collect();
        let segment = segment(1, &collection, records);
        let index = HnswIndex::build(
            std::slice::from_ref(&segment),
            &collection,
            DistanceMetric::L2,
            HnswConfig {
                m: 8,
                ef_construction: 20,
                ef_search: 20,
            },
        )
        .expect("build");
        let predicate = Predicate::Eq(
            path(&["product", "category"]),
            MetadataValue::String("book".into()),
        );
        let hits = hnsw_search_filtered(&index, &[1.0], 3, &predicate).expect("search");
        assert_eq!(hits.len(), 3);
        assert!(hits.iter().all(|hit| {
            predicate
                .evaluate(hit.record().metadata())
                .expect("predicate")
        }));
    }

    #[test]
    fn invalid_type_comparison_propagates_typed_error() {
        let collection = collection();
        let segment = segment(
            1,
            &collection,
            vec![record(1, 1, &[1.0], 100.0, "book", &["rust"])],
        );
        let predicate = Predicate::Lt(path(&["price"]), MetadataValue::String("500".to_string()));
        let error = exact_search_filtered_segments(
            &[segment],
            &collection,
            &[1.0],
            DistanceMetric::Dot,
            1,
            &predicate,
        )
        .expect_err("typed predicate error");
        assert!(matches!(error, FilteredSearchError::Predicate(_)));
    }
}
