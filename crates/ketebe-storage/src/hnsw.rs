use crate::{QueryControl, QueryControlError, SearchError, Segment, exact_search_segments};
use ketebe_core::{CollectionId, DistanceMetric, Record, RecordId, SequenceNumber};
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HnswConfig {
    pub m: usize,
    pub ef_construction: usize,
    pub ef_search: usize,
}

impl Default for HnswConfig {
    fn default() -> Self {
        Self {
            m: 16,
            ef_construction: 64,
            ef_search: 64,
        }
    }
}

impl HnswConfig {
    pub fn validate(self) -> Result<Self, HnswError> {
        if self.m == 0 {
            return Err(HnswError::InvalidConfig("m must be greater than zero"));
        }
        if self.ef_construction < self.m {
            return Err(HnswError::InvalidConfig("ef_construction must be >= m"));
        }
        if self.ef_search == 0 {
            return Err(HnswError::InvalidConfig(
                "ef_search must be greater than zero",
            ));
        }
        Ok(self)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct HnswHit {
    record: Record,
    score: f32,
}

impl HnswHit {
    #[must_use]
    pub fn record(&self) -> &Record {
        &self.record
    }

    #[must_use]
    pub const fn score(&self) -> f32 {
        self.score
    }
}

#[derive(Debug)]
pub enum HnswError {
    InvalidConfig(&'static str),
    InvalidTopK,
    EfSearchTooSmall { ef_search: usize, top_k: usize },
    EmptyQueryVector,
    NonFiniteQueryValue { index: usize },
    DimensionMismatch { expected: usize, actual: usize },
    ZeroNormVector,
    ExactSearch(String),
    InvalidGraph(&'static str),
    Control(QueryControlError),
}

impl fmt::Display for HnswError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfig(message) => write!(f, "invalid HNSW config: {message}"),
            Self::InvalidTopK => f.write_str("top_k must be greater than zero"),
            Self::EfSearchTooSmall { ef_search, top_k } => write!(
                f,
                "ef_search must be >= top_k: ef_search={ef_search}, top_k={top_k}"
            ),
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
            Self::ExactSearch(message) => write!(f, "exact search failed: {message}"),
            Self::InvalidGraph(message) => write!(f, "invalid persisted HNSW graph: {message}"),
            Self::Control(error) => write!(f, "query control stopped HNSW search: {error}"),
        }
    }
}

impl std::error::Error for HnswError {}

impl From<QueryControlError> for HnswError {
    fn from(value: QueryControlError) -> Self {
        Self::Control(value)
    }
}

#[derive(Debug, Clone)]
struct Node {
    record: Record,
    level: usize,
    neighbors: Vec<Vec<usize>>,
}

#[derive(Debug, Clone)]
pub(crate) struct HnswNativeNode {
    pub(crate) record: Record,
    pub(crate) level: usize,
    pub(crate) neighbors: Vec<Vec<usize>>,
}

#[derive(Debug, Clone)]
pub(crate) struct HnswNativeGraph {
    pub(crate) collection_id: CollectionId,
    pub(crate) metric: DistanceMetric,
    pub(crate) config: HnswConfig,
    pub(crate) dimension: Option<usize>,
    pub(crate) nodes: Vec<HnswNativeNode>,
    pub(crate) entry_point: Option<usize>,
    pub(crate) max_level: usize,
}

#[derive(Debug, Clone)]
pub struct HnswIndex {
    collection_id: CollectionId,
    metric: DistanceMetric,
    config: HnswConfig,
    dimension: Option<usize>,
    nodes: Vec<Node>,
    entry_point: Option<usize>,
    max_level: usize,
}

impl HnswIndex {
    pub fn build(
        segments: &[Segment],
        collection_id: &CollectionId,
        metric: DistanceMetric,
        config: HnswConfig,
    ) -> Result<Self, HnswError> {
        let config = config.validate()?;
        let mut records: Vec<Record> = fold_visible_records(segments, collection_id)
            .into_values()
            .collect();
        records.sort_by(|left, right| left.id().cmp(right.id()));

        let dimension = records.first().map(|record| record.vector().len());
        if let Some(expected) = dimension {
            for record in &records {
                if record.vector().len() != expected {
                    return Err(HnswError::DimensionMismatch {
                        expected,
                        actual: record.vector().len(),
                    });
                }
                if metric == DistanceMetric::Cosine && norm(record.vector().as_slice()) == 0.0 {
                    return Err(HnswError::ZeroNormVector);
                }
            }
        }

        let mut index = Self {
            collection_id: collection_id.clone(),
            metric,
            config,
            dimension,
            nodes: Vec::new(),
            entry_point: None,
            max_level: 0,
        };
        for record in records {
            index.insert(record)?;
        }
        Ok(index)
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    #[must_use]
    pub fn collection_id(&self) -> &CollectionId {
        &self.collection_id
    }

    #[must_use]
    pub const fn config(&self) -> HnswConfig {
        self.config
    }

    pub fn search(&self, query: &[f32], top_k: usize) -> Result<Vec<HnswHit>, HnswError> {
        self.search_with_control(query, top_k, &QueryControl::unbounded())
    }

    pub fn search_with_control(
        &self,
        query: &[f32],
        top_k: usize,
        control: &QueryControl,
    ) -> Result<Vec<HnswHit>, HnswError> {
        control.check()?;
        self.validate_query(query, top_k)?;
        let Some(mut current) = self.entry_point else {
            return Ok(Vec::new());
        };

        for level in (1..=self.max_level).rev() {
            control.check()?;
            current = self.greedy_search_level_controlled(query, current, level, control)?;
        }

        let candidates =
            self.search_layer_controlled(query, current, self.config.ef_search, 0, control)?;
        let mut hits = Vec::with_capacity(candidates.len());
        for (position, index) in candidates.into_iter().enumerate() {
            if position % 64 == 0 {
                control.check()?;
            }
            let record = self.nodes[index].record.clone();
            let score = score(query, record.vector().as_slice(), self.metric)
                .expect("validated vectors must be scorable");
            hits.push(HnswHit { record, score });
        }
        control.check()?;
        hits.sort_by(|left, right| compare_hits(left, right, self.metric));
        hits.truncate(top_k);
        Ok(hits)
    }

    pub fn recall_at_k(
        &self,
        segments: &[Segment],
        query: &[f32],
        k: usize,
    ) -> Result<f32, HnswError> {
        let exact = exact_search_segments(segments, &self.collection_id, query, self.metric, k)
            .map_err(map_search_error)?;
        if exact.is_empty() {
            return Ok(1.0);
        }

        let approximate = self.search(query, k)?;
        let expected: BTreeSet<RecordId> = exact
            .into_iter()
            .map(|hit| hit.record().id().clone())
            .collect();
        let matched = approximate
            .iter()
            .filter(|hit| expected.contains(hit.record().id()))
            .count();
        Ok(matched as f32 / expected.len() as f32)
    }

    pub(crate) fn native_graph(&self) -> HnswNativeGraph {
        HnswNativeGraph {
            collection_id: self.collection_id.clone(),
            metric: self.metric,
            config: self.config,
            dimension: self.dimension,
            nodes: self
                .nodes
                .iter()
                .map(|node| HnswNativeNode {
                    record: node.record.clone(),
                    level: node.level,
                    neighbors: node.neighbors.clone(),
                })
                .collect(),
            entry_point: self.entry_point,
            max_level: self.max_level,
        }
    }

    pub(crate) fn from_native_graph(graph: HnswNativeGraph) -> Result<Self, HnswError> {
        let config = graph.config.validate()?;
        if graph.nodes.is_empty() {
            if graph.entry_point.is_some() || graph.max_level != 0 || graph.dimension.is_some() {
                return Err(HnswError::InvalidGraph(
                    "empty graph has non-empty topology metadata",
                ));
            }
            return Ok(Self {
                collection_id: graph.collection_id,
                metric: graph.metric,
                config,
                dimension: None,
                nodes: Vec::new(),
                entry_point: None,
                max_level: 0,
            });
        }
        let dimension = graph.dimension.ok_or(HnswError::InvalidGraph(
            "non-empty graph is missing dimension",
        ))?;
        if dimension == 0 {
            return Err(HnswError::InvalidGraph("graph dimension is zero"));
        }
        let entry = graph.entry_point.ok_or(HnswError::InvalidGraph(
            "non-empty graph is missing entry point",
        ))?;
        if entry >= graph.nodes.len() {
            return Err(HnswError::InvalidGraph("entry point is out of bounds"));
        }
        let observed_max = graph.nodes.iter().map(|node| node.level).max().unwrap_or(0);
        if observed_max != graph.max_level || graph.nodes[entry].level != graph.max_level {
            return Err(HnswError::InvalidGraph(
                "max level or entry point level is inconsistent",
            ));
        }
        for (index, node) in graph.nodes.iter().enumerate() {
            if node.record.vector().len() != dimension {
                return Err(HnswError::DimensionMismatch {
                    expected: dimension,
                    actual: node.record.vector().len(),
                });
            }
            if graph.metric == DistanceMetric::Cosine
                && norm(node.record.vector().as_slice()) == 0.0
            {
                return Err(HnswError::ZeroNormVector);
            }
            if node.neighbors.len() != node.level + 1 {
                return Err(HnswError::InvalidGraph(
                    "node layer count does not match node level",
                ));
            }
            for (layer, neighbors) in node.neighbors.iter().enumerate() {
                let mut seen = BTreeSet::new();
                for &neighbor in neighbors {
                    if neighbor >= graph.nodes.len() {
                        return Err(HnswError::InvalidGraph("neighbor is out of bounds"));
                    }
                    if neighbor == index {
                        return Err(HnswError::InvalidGraph("node contains a self edge"));
                    }
                    if graph.nodes[neighbor].level < layer {
                        return Err(HnswError::InvalidGraph(
                            "neighbor does not exist at referenced layer",
                        ));
                    }
                    if !seen.insert(neighbor) {
                        return Err(HnswError::InvalidGraph("duplicate neighbor edge"));
                    }
                }
            }
        }
        Ok(Self {
            collection_id: graph.collection_id,
            metric: graph.metric,
            config,
            dimension: Some(dimension),
            nodes: graph
                .nodes
                .into_iter()
                .map(|node| Node {
                    record: node.record,
                    level: node.level,
                    neighbors: node.neighbors,
                })
                .collect(),
            entry_point: Some(entry),
            max_level: graph.max_level,
        })
    }

    fn insert(&mut self, record: Record) -> Result<(), HnswError> {
        let level = deterministic_level(record.id());
        let new_index = self.nodes.len();
        self.nodes.push(Node {
            record,
            level,
            neighbors: vec![Vec::new(); level + 1],
        });

        let Some(mut current) = self.entry_point else {
            self.entry_point = Some(new_index);
            self.max_level = level;
            return Ok(());
        };

        for layer in ((level + 1)..=self.max_level).rev() {
            let query = self.nodes[new_index].record.vector().as_slice();
            current = self.greedy_search_level(query, current, layer)?;
        }

        for layer in (0..=level.min(self.max_level)).rev() {
            let query = self.nodes[new_index].record.vector().as_slice().to_vec();
            let candidates =
                self.search_layer(&query, current, self.config.ef_construction, layer)?;
            let selected = self.select_neighbors(&query, candidates, self.config.m);
            if let Some(first) = selected.first().copied() {
                current = first;
            }
            self.nodes[new_index].neighbors[layer] = selected.clone();
            for neighbor in selected {
                self.connect(neighbor, new_index, layer);
            }
        }

        if level > self.max_level {
            self.entry_point = Some(new_index);
            self.max_level = level;
        }
        Ok(())
    }

    fn connect(&mut self, existing: usize, new_index: usize, layer: usize) {
        if self.nodes[existing].level < layer {
            return;
        }
        if !self.nodes[existing].neighbors[layer].contains(&new_index) {
            self.nodes[existing].neighbors[layer].push(new_index);
        }
        let query = self.nodes[existing].record.vector().as_slice().to_vec();
        let candidates = self.nodes[existing].neighbors[layer].clone();
        self.nodes[existing].neighbors[layer] =
            self.select_neighbors(&query, candidates, self.config.m);
    }

    fn greedy_search_level(
        &self,
        query: &[f32],
        current: usize,
        layer: usize,
    ) -> Result<usize, HnswError> {
        self.greedy_search_level_controlled(query, current, layer, &QueryControl::unbounded())
    }

    fn greedy_search_level_controlled(
        &self,
        query: &[f32],
        mut current: usize,
        layer: usize,
        control: &QueryControl,
    ) -> Result<usize, HnswError> {
        loop {
            control.check()?;
            let current_distance = self.node_distance(query, current)?;
            let next = self.nodes[current].neighbors[layer]
                .iter()
                .copied()
                .min_by(|left, right| self.compare_node_distance(query, *left, *right));
            let Some(candidate) = next else {
                return Ok(current);
            };
            let candidate_distance = self.node_distance(query, candidate)?;
            if candidate_distance < current_distance
                || (candidate_distance.total_cmp(&current_distance) == Ordering::Equal
                    && self.nodes[candidate].record.id() < self.nodes[current].record.id())
            {
                current = candidate;
            } else {
                return Ok(current);
            }
        }
    }

    fn search_layer(
        &self,
        query: &[f32],
        entry: usize,
        ef: usize,
        layer: usize,
    ) -> Result<Vec<usize>, HnswError> {
        self.search_layer_controlled(query, entry, ef, layer, &QueryControl::unbounded())
    }

    fn search_layer_controlled(
        &self,
        query: &[f32],
        entry: usize,
        ef: usize,
        layer: usize,
        control: &QueryControl,
    ) -> Result<Vec<usize>, HnswError> {
        control.check()?;
        let mut visited = BTreeSet::from([entry]);
        let mut frontier = vec![entry];
        let mut best = vec![entry];

        while let Some(current) = pop_nearest(&mut frontier, |left, right| {
            self.compare_node_distance(query, left, right)
        }) {
            control.check()?;
            let worst_distance = best
                .last()
                .map(|index| self.node_distance(query, *index))
                .transpose()?
                .unwrap_or(f32::INFINITY);
            if best.len() >= ef && self.node_distance(query, current)? > worst_distance {
                break;
            }

            for &neighbor in &self.nodes[current].neighbors[layer] {
                if !visited.insert(neighbor) {
                    continue;
                }
                frontier.push(neighbor);
                best.push(neighbor);
                best.sort_by(|left, right| self.compare_node_distance(query, *left, *right));
                best.truncate(ef);
            }
        }
        Ok(best)
    }

    fn select_neighbors(
        &self,
        query: &[f32],
        mut candidates: Vec<usize>,
        limit: usize,
    ) -> Vec<usize> {
        candidates.sort_by(|left, right| self.compare_node_distance(query, *left, *right));
        candidates.dedup();
        candidates.truncate(limit);
        candidates
    }

    fn node_distance(&self, query: &[f32], index: usize) -> Result<f32, HnswError> {
        distance(
            query,
            self.nodes[index].record.vector().as_slice(),
            self.metric,
        )
    }

    fn compare_node_distance(&self, query: &[f32], left: usize, right: usize) -> Ordering {
        let left_distance = self
            .node_distance(query, left)
            .expect("validated vectors must be scorable");
        let right_distance = self
            .node_distance(query, right)
            .expect("validated vectors must be scorable");
        left_distance.total_cmp(&right_distance).then_with(|| {
            self.nodes[left]
                .record
                .id()
                .cmp(self.nodes[right].record.id())
        })
    }

    fn validate_query(&self, query: &[f32], top_k: usize) -> Result<(), HnswError> {
        if top_k == 0 {
            return Err(HnswError::InvalidTopK);
        }
        if self.config.ef_search < top_k {
            return Err(HnswError::EfSearchTooSmall {
                ef_search: self.config.ef_search,
                top_k,
            });
        }
        if query.is_empty() {
            return Err(HnswError::EmptyQueryVector);
        }
        for (index, value) in query.iter().enumerate() {
            if !value.is_finite() {
                return Err(HnswError::NonFiniteQueryValue { index });
            }
        }
        if let Some(expected) = self.dimension
            && query.len() != expected
        {
            return Err(HnswError::DimensionMismatch {
                expected,
                actual: query.len(),
            });
        }
        if self.metric == DistanceMetric::Cosine && norm(query) == 0.0 {
            return Err(HnswError::ZeroNormVector);
        }
        Ok(())
    }
}

struct VisibleVersion {
    sequence: SequenceNumber,
    record: Option<Record>,
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

fn pop_nearest<F>(values: &mut Vec<usize>, compare: F) -> Option<usize>
where
    F: Fn(usize, usize) -> Ordering,
{
    let index = (0..values.len()).min_by(|left, right| compare(values[*left], values[*right]))?;
    Some(values.swap_remove(index))
}

fn deterministic_level(id: &RecordId) -> usize {
    let mut hash = 0xcbf29ce484222325_u64;
    let mut push = |byte: u8| {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x100000001b3);
    };
    match id {
        RecordId::String(value) => {
            push(0);
            for byte in value.bytes() {
                push(byte);
            }
        }
        RecordId::Unsigned(value) => {
            push(1);
            for byte in value.to_le_bytes() {
                push(byte);
            }
        }
    }
    ((hash.trailing_zeros() as usize) / 2).min(16)
}

fn distance(query: &[f32], vector: &[f32], metric: DistanceMetric) -> Result<f32, HnswError> {
    if query.len() != vector.len() {
        return Err(HnswError::DimensionMismatch {
            expected: vector.len(),
            actual: query.len(),
        });
    }
    match metric {
        DistanceMetric::L2 => Ok(l2(query, vector)),
        DistanceMetric::Dot => Ok(-dot(query, vector)),
        DistanceMetric::Cosine => Ok(-cosine(query, vector)?),
    }
}

fn score(query: &[f32], vector: &[f32], metric: DistanceMetric) -> Result<f32, HnswError> {
    match metric {
        DistanceMetric::L2 => Ok(l2(query, vector)),
        DistanceMetric::Dot => Ok(dot(query, vector)),
        DistanceMetric::Cosine => cosine(query, vector),
    }
}

fn cosine(left: &[f32], right: &[f32]) -> Result<f32, HnswError> {
    let left_norm = norm(left);
    let right_norm = norm(right);
    if left_norm == 0.0 || right_norm == 0.0 {
        return Err(HnswError::ZeroNormVector);
    }
    Ok(dot(left, right) / (left_norm * right_norm))
}

fn compare_hits(left: &HnswHit, right: &HnswHit, metric: DistanceMetric) -> Ordering {
    let score_order = match metric {
        DistanceMetric::L2 => left.score.total_cmp(&right.score),
        DistanceMetric::Cosine | DistanceMetric::Dot => right.score.total_cmp(&left.score),
    };
    score_order.then_with(|| left.record.id().cmp(right.record.id()))
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

fn map_search_error(error: SearchError) -> HnswError {
    HnswError::ExactSearch(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{SegmentId, WalMutation};
    use ketebe_core::{Metadata, Vector};

    fn collection() -> CollectionId {
        CollectionId::new("docs").expect("collection")
    }

    fn record(id: u64, sequence: u64, vector: Vec<f32>) -> Record {
        Record::new(
            RecordId::unsigned(id),
            Vector::new(vector).expect("vector"),
            Metadata::default(),
            SequenceNumber::new(sequence),
        )
    }

    fn segment(id: u64, collection_id: &CollectionId, records: Vec<Record>) -> Segment {
        let mutations: Vec<_> = records
            .into_iter()
            .map(|record| WalMutation::Upsert {
                collection_id: collection_id.clone(),
                record,
            })
            .collect();
        Segment::from_mutations(SegmentId::new(id), &mutations).expect("segment")
    }

    fn config() -> HnswConfig {
        HnswConfig {
            m: 8,
            ef_construction: 64,
            ef_search: 64,
        }
    }

    #[test]
    fn supports_cosine_dot_and_l2() {
        let collection = collection();
        let segment = segment(
            1,
            &collection,
            vec![record(1, 1, vec![1.0, 0.0]), record(2, 2, vec![0.0, 1.0])],
        );
        for metric in [
            DistanceMetric::Cosine,
            DistanceMetric::Dot,
            DistanceMetric::L2,
        ] {
            let index = HnswIndex::build(
                std::slice::from_ref(&segment),
                &collection,
                metric,
                config(),
            )
            .expect("build");
            let hits = index.search(&[1.0, 0.0], 1).expect("search");
            assert_eq!(hits[0].record().id(), &RecordId::unsigned(1));
        }
    }

    #[test]
    fn build_search_and_validation_are_deterministic() {
        let collection = collection();
        let records: Vec<_> = (0..32)
            .map(|id| record(id, id, vec![id as f32 + 1.0, 1.0]))
            .collect();
        let segment = segment(1, &collection, records);
        let first = HnswIndex::build(
            std::slice::from_ref(&segment),
            &collection,
            DistanceMetric::L2,
            config(),
        )
        .expect("build");
        let second = HnswIndex::build(
            std::slice::from_ref(&segment),
            &collection,
            DistanceMetric::L2,
            config(),
        )
        .expect("build");
        assert_eq!(
            first.search(&[10.0, 1.0], 5).expect("search"),
            second.search(&[10.0, 1.0], 5).expect("search")
        );
        assert!(matches!(
            first.search(&[1.0], 1),
            Err(HnswError::DimensionMismatch { .. })
        ));
    }

    #[test]
    fn tombstone_and_resurrection_match_latest_state() {
        let collection = collection();
        let live = segment(1, &collection, vec![record(1, 1, vec![1.0, 0.0])]);
        let deleted = Segment::from_mutations(
            SegmentId::new(2),
            &[WalMutation::Delete {
                collection_id: collection.clone(),
                record_id: RecordId::unsigned(1),
                sequence_number: SequenceNumber::new(2),
            }],
        )
        .expect("delete segment");
        let deleted_index = HnswIndex::build(
            &[live.clone(), deleted.clone()],
            &collection,
            DistanceMetric::Dot,
            config(),
        )
        .expect("build");
        assert!(deleted_index.is_empty());

        let resurrected = segment(3, &collection, vec![record(1, 3, vec![1.0, 0.0])]);
        let index = HnswIndex::build(
            &[live, deleted, resurrected],
            &collection,
            DistanceMetric::Dot,
            config(),
        )
        .expect("build");
        assert_eq!(index.len(), 1);
    }

    #[test]
    fn representative_recall_is_at_least_ninety_five_percent() {
        let collection = collection();
        let records: Vec<_> = (0..100)
            .map(|id| {
                let x = id as f32 / 10.0 + 0.1;
                record(
                    id,
                    id,
                    vec![x, x.sin() + 1.5, x.cos() + 1.5, (x * 0.37).sin() + 1.5],
                )
            })
            .collect();
        let segment = segment(1, &collection, records);
        let index = HnswIndex::build(
            std::slice::from_ref(&segment),
            &collection,
            DistanceMetric::L2,
            HnswConfig {
                m: 16,
                ef_construction: 100,
                ef_search: 100,
            },
        )
        .expect("build");

        for query_id in [7_u64, 21, 42, 78] {
            let x = query_id as f32 / 10.0 + 0.1;
            let query = [x, x.sin() + 1.5, x.cos() + 1.5, (x * 0.37).sin() + 1.5];
            assert!(
                index
                    .recall_at_k(std::slice::from_ref(&segment), &query, 10)
                    .expect("recall")
                    >= 0.95
            );
        }
    }
}
