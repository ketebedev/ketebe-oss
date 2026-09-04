use std::cmp::Ordering;

use rmcp::schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};

use crate::retrieval::AgentRecordId;

const DEFAULT_LIMIT: usize = 10;
const SOURCE_METADATA_KEY: &str = "_ketebe_source";
const SOURCE_TIMESTAMP_KEY: &str = "observed_at_unix_ms";

#[derive(Clone, Copy, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum SearchMode {
    Dense,
    Sparse,
    Hybrid,
}

#[derive(Clone, Debug, Deserialize, JsonSchema)]
pub struct SearchParams {
    pub collection: String,
    pub mode: SearchMode,
    #[serde(default)]
    pub text: Option<String>,
    #[serde(default)]
    pub vector: Option<Vec<f32>>,
    #[serde(default)]
    pub filter: Option<Value>,
    #[serde(default)]
    pub after: Option<u64>,
    #[serde(default)]
    pub before: Option<u64>,
    #[serde(default)]
    pub prefer_recent: bool,
    #[serde(default = "default_limit")]
    pub limit: usize,
    #[serde(default)]
    pub fields: Vec<String>,
    #[serde(default)]
    pub execution: Option<String>,
    #[serde(default)]
    pub dense_candidates: Option<usize>,
    #[serde(default)]
    pub sparse_candidates: Option<usize>,
    #[serde(default)]
    pub search_profile: Option<String>,
    #[serde(default)]
    pub timeout_ms: Option<u64>,
    #[serde(default)]
    pub explain: bool,
}

const fn default_limit() -> usize {
    DEFAULT_LIMIT
}

impl SearchParams {
    pub(crate) fn into_request(self) -> Result<(String, SearchMode, SearchRequest), String> {
        if self.collection.trim().is_empty() {
            return Err("Ketebe search request failed: invalid_collection".to_string());
        }
        if self.limit == 0 {
            return Err("Ketebe search request failed: invalid_limit".to_string());
        }
        if self.timeout_ms == Some(0) {
            return Err("Ketebe search request failed: invalid_timeout".to_string());
        }
        if self.dense_candidates == Some(0) || self.sparse_candidates == Some(0) {
            return Err("Ketebe search request failed: invalid_candidates".to_string());
        }
        if self
            .after
            .zip(self.before)
            .is_some_and(|(after, before)| after >= before)
        {
            return Err("Ketebe search request failed: invalid_time_range".to_string());
        }

        let text = self
            .text
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty());
        let vector = self.vector;
        match self.mode {
            SearchMode::Dense if vector.is_none() && text.is_none() => {
                return Err(
                    "Ketebe search request failed: dense_requires_vector_or_text".to_string(),
                );
            }
            SearchMode::Dense if vector.is_some() && text.is_some() => {
                return Err(
                    "Ketebe search request failed: dense_accepts_one_query_input".to_string(),
                );
            }
            SearchMode::Sparse if text.is_none() || vector.is_some() => {
                return Err("Ketebe search request failed: sparse_requires_text_only".to_string());
            }
            SearchMode::Hybrid if text.is_none() => {
                return Err("Ketebe search request failed: hybrid_requires_text".to_string());
            }
            _ => {}
        }

        let freshness_requested =
            self.after.is_some() || self.before.is_some() || self.prefer_recent;
        let include_metadata = metadata_requested(&self.fields)? || freshness_requested;
        let predicate = compose_temporal_filter(self.filter, self.after, self.before);
        let collection = self.collection;
        let mode = self.mode;
        Ok((
            collection,
            mode,
            SearchRequest {
                vector,
                text,
                top_k: self.limit,
                predicate,
                execution: self.execution,
                dense_candidates: self.dense_candidates,
                lexical_candidates: self.sparse_candidates,
                search_profile: self.search_profile,
                include_metadata,
                include_provenance: freshness_requested,
                timeout_ms: self.timeout_ms,
                explain: self.explain,
            },
        ))
    }
}

fn compose_temporal_filter(
    existing: Option<Value>,
    after: Option<u64>,
    before: Option<u64>,
) -> Option<Value> {
    let mut predicates = Vec::new();
    if let Some(existing) = existing {
        predicates.push(existing);
    }
    if let Some(after) = after {
        predicates.push(json!({
            "op": "gt",
            "path": [SOURCE_METADATA_KEY, SOURCE_TIMESTAMP_KEY],
            "value": after
        }));
    }
    if let Some(before) = before {
        predicates.push(json!({
            "op": "lt",
            "path": [SOURCE_METADATA_KEY, SOURCE_TIMESTAMP_KEY],
            "value": before
        }));
    }
    match predicates.len() {
        0 => None,
        1 => predicates.pop(),
        _ => Some(json!({"op":"and", "predicates":predicates})),
    }
}

fn metadata_requested(fields: &[String]) -> Result<bool, String> {
    if fields.is_empty() {
        return Ok(true);
    }
    let mut include_metadata = false;
    for field in fields {
        match field.as_str() {
            "id"
            | "score"
            | "sequence_number"
            | "source_timestamp_unix_ms"
            | "dense_rank"
            | "sparse_rank"
            | "dense_score"
            | "sparse_score"
            | "rerank_score"
            | "original_rank" => {}
            "metadata" => include_metadata = true,
            value if value.starts_with("metadata.") && value.len() > "metadata.".len() => {
                include_metadata = true;
            }
            _ => {
                return Err(format!(
                    "Ketebe search request failed: invalid_field {field}"
                ));
            }
        }
    }
    Ok(include_metadata)
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct SearchRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) vector: Option<Vec<f32>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) text: Option<String>,
    pub(crate) top_k: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) predicate: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) execution: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) dense_candidates: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) lexical_candidates: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) search_profile: Option<String>,
    pub(crate) include_metadata: bool,
    pub(crate) include_provenance: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) timeout_ms: Option<u64>,
    pub(crate) explain: bool,
}

#[derive(Clone, Debug, Deserialize)]
pub(crate) struct SearchResponse {
    pub(crate) hits: Vec<SearchResponseHit>,
    #[serde(default)]
    pub(crate) explain: Option<Value>,
}

#[derive(Clone, Debug, Deserialize)]
pub(crate) struct SearchResponseHit {
    pub(crate) id: ketebe_sdk::RecordId,
    pub(crate) score: f32,
    pub(crate) sequence_number: u64,
    #[serde(default)]
    pub(crate) metadata: Option<Value>,
    #[serde(default)]
    pub(crate) dense_rank: Option<usize>,
    #[serde(default)]
    pub(crate) lexical_rank: Option<usize>,
    #[serde(default)]
    pub(crate) dense_score: Option<f32>,
    #[serde(default)]
    pub(crate) lexical_score: Option<f32>,
    #[serde(default)]
    pub(crate) rerank_score: Option<f32>,
    #[serde(default)]
    pub(crate) original_rank: Option<usize>,
}

#[derive(Clone, Debug, Serialize, JsonSchema, PartialEq)]
pub struct SearchOutput {
    pub mode: SearchMode,
    pub hits: Vec<SearchHit>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub explain: Option<Value>,
}

#[derive(Clone, Debug, Serialize, JsonSchema, PartialEq)]
pub struct SearchHit {
    pub id: AgentRecordId,
    pub score: f32,
    pub sequence_number: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_timestamp_unix_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dense_rank: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sparse_rank: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dense_score: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sparse_score: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rerank_score: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub original_rank: Option<usize>,
}

impl SearchResponse {
    pub(crate) fn project(
        mut self,
        mode: SearchMode,
        fields: &[String],
        prefer_recent: bool,
    ) -> SearchOutput {
        if prefer_recent {
            self.hits.sort_by(compare_freshness);
        }
        SearchOutput {
            mode,
            hits: self
                .hits
                .into_iter()
                .map(|hit| {
                    let source_timestamp_unix_ms = source_timestamp(hit.metadata.as_ref());
                    SearchHit {
                        id: hit.id.into(),
                        score: hit.score,
                        sequence_number: hit.sequence_number,
                        source_timestamp_unix_ms,
                        metadata: project_metadata(hit.metadata, fields),
                        dense_rank: hit.dense_rank,
                        sparse_rank: hit.lexical_rank,
                        dense_score: hit.dense_score,
                        sparse_score: hit.lexical_score,
                        rerank_score: hit.rerank_score,
                        original_rank: hit.original_rank,
                    }
                })
                .collect(),
            explain: self.explain,
        }
    }
}

fn compare_freshness(left: &SearchResponseHit, right: &SearchResponseHit) -> Ordering {
    right
        .score
        .partial_cmp(&left.score)
        .unwrap_or(Ordering::Equal)
        .then_with(|| {
            source_timestamp(right.metadata.as_ref()).cmp(&source_timestamp(left.metadata.as_ref()))
        })
        .then_with(|| left.sequence_number.cmp(&right.sequence_number))
}

fn source_timestamp(metadata: Option<&Value>) -> Option<u64> {
    let value = metadata?
        .get(SOURCE_METADATA_KEY)?
        .get(SOURCE_TIMESTAMP_KEY)?;
    value.as_u64().or_else(|| {
        let value = value.as_f64()?;
        (value.is_finite() && value >= 0.0 && value.fract() == 0.0 && value <= u64::MAX as f64)
            .then_some(value as u64)
    })
}

fn project_metadata(metadata: Option<Value>, fields: &[String]) -> Option<Value> {
    let mut metadata = metadata?;
    if let Value::Object(object) = &mut metadata {
        object.remove(SOURCE_METADATA_KEY);
        object.remove("_ketebe_content");
    }
    if fields.is_empty() || fields.iter().any(|field| field == "metadata") {
        return Some(metadata);
    }
    let selected = fields
        .iter()
        .filter_map(|field| field.strip_prefix("metadata."))
        .collect::<Vec<_>>();
    if selected.is_empty() {
        return None;
    }
    let Value::Object(source) = metadata else {
        return Some(metadata);
    };
    let mut output = Map::new();
    for key in selected {
        if let Some(value) = source.get(key) {
            output.insert(key.to_string(), value.clone());
        }
    }
    Some(Value::Object(output))
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SearchError {
    Api { status: u16, code: String },
    Transport,
}

impl SearchError {
    #[must_use]
    pub fn stable_message(&self) -> String {
        match self {
            Self::Api { status, code } => format!("Ketebe search request failed: {status} {code}"),
            Self::Transport => "Ketebe search service unavailable".to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn base(mode: SearchMode) -> SearchParams {
        SearchParams {
            collection: "docs".into(),
            mode,
            text: None,
            vector: None,
            filter: None,
            after: None,
            before: None,
            prefer_recent: false,
            limit: 10,
            fields: Vec::new(),
            execution: None,
            dense_candidates: None,
            sparse_candidates: None,
            search_profile: None,
            timeout_ms: None,
            explain: false,
        }
    }

    fn response_hit(
        id: &str,
        score: f32,
        timestamp: Option<u64>,
        sequence: u64,
    ) -> SearchResponseHit {
        SearchResponseHit {
            id: ketebe_sdk::RecordId::String(id.into()),
            score,
            sequence_number: sequence,
            metadata: Some(json!({
                "title": id,
                "_ketebe_source": timestamp.map(|value| json!({"observed_at_unix_ms":value}))
            })),
            dense_rank: None,
            lexical_rank: None,
            dense_score: None,
            lexical_score: None,
            rerank_score: None,
            original_rank: None,
        }
    }

    #[test]
    fn modes_map_to_public_query_shapes() {
        let mut dense = base(SearchMode::Dense);
        dense.vector = Some(vec![1.0, 0.0]);
        let (_, _, dense) = dense.into_request().unwrap();
        assert!(dense.vector.is_some());
        assert!(dense.text.is_none());

        let mut embedded_dense = base(SearchMode::Dense);
        embedded_dense.text = Some("hello".into());
        let (_, _, embedded_dense) = embedded_dense.into_request().unwrap();
        assert!(embedded_dense.vector.is_none());
        assert_eq!(embedded_dense.text.as_deref(), Some("hello"));

        let mut sparse = base(SearchMode::Sparse);
        sparse.text = Some("hello".into());
        let (_, _, sparse) = sparse.into_request().unwrap();
        assert!(sparse.vector.is_none());
        assert_eq!(sparse.text.as_deref(), Some("hello"));

        let mut hybrid = base(SearchMode::Hybrid);
        hybrid.text = Some("hello".into());
        let (_, _, hybrid) = hybrid.into_request().unwrap();
        assert!(hybrid.vector.is_none());
        assert!(hybrid.text.is_some());
    }

    #[test]
    fn filter_is_forwarded_without_rewriting_when_freshness_is_absent() {
        let filter = json!({
            "op": "and",
            "predicates": [
                {"op":"eq", "path":["tenant"], "value":"a"},
                {"op":"gte", "path":["year"], "value":2024}
            ]
        });
        let mut params = base(SearchMode::Sparse);
        params.text = Some("query".into());
        params.filter = Some(filter.clone());
        let (_, _, request) = params.into_request().unwrap();
        assert_eq!(request.predicate, Some(filter));
        assert!(!request.include_provenance);
    }

    #[test]
    fn temporal_constraints_compose_with_filter_and_request_provenance() {
        let mut params = base(SearchMode::Sparse);
        params.text = Some("query".into());
        params.filter = Some(json!({"op":"eq", "path":["tenant"], "value":"a"}));
        params.after = Some(100);
        params.before = Some(200);
        params.search_profile = Some("agent@2".into());
        let (_, _, request) = params.into_request().unwrap();
        assert_eq!(request.search_profile.as_deref(), Some("agent@2"));
        assert!(request.include_metadata);
        assert!(request.include_provenance);
        assert_eq!(
            request.predicate,
            Some(json!({
                "op":"and",
                "predicates":[
                    {"op":"eq", "path":["tenant"], "value":"a"},
                    {"op":"gt", "path":["_ketebe_source","observed_at_unix_ms"], "value":100},
                    {"op":"lt", "path":["_ketebe_source","observed_at_unix_ms"], "value":200}
                ]
            }))
        );
    }

    #[test]
    fn prefer_recent_only_breaks_equal_relevance_scores() {
        let response = SearchResponse {
            hits: vec![
                response_hit("older-high", 2.0, Some(100), 1),
                response_hit("older-tie", 1.0, Some(100), 2),
                response_hit("newer-tie", 1.0, Some(200), 3),
            ],
            explain: None,
        };
        let output = response.project(SearchMode::Dense, &[], true);
        assert_eq!(
            output.hits[0].id,
            AgentRecordId::String("older-high".into())
        );
        assert_eq!(output.hits[1].id, AgentRecordId::String("newer-tie".into()));
        assert_eq!(output.hits[1].source_timestamp_unix_ms, Some(200));
        assert_eq!(output.hits[2].id, AgentRecordId::String("older-tie".into()));
    }

    #[test]
    fn metadata_projection_is_explicit_and_hides_reserved_provenance() {
        let response = SearchResponse {
            hits: vec![response_hit("a", 1.0, Some(42), 7)],
            explain: None,
        };
        let output = response.project(SearchMode::Dense, &["metadata.title".into()], false);
        assert_eq!(output.hits[0].metadata, Some(json!({"title":"a"})));
        assert_eq!(output.hits[0].source_timestamp_unix_ms, Some(42));
    }

    #[test]
    fn source_timestamp_accepts_integer_valued_json_float() {
        let metadata = json!({"_ketebe_source":{"observed_at_unix_ms":42.0}});
        assert_eq!(source_timestamp(Some(&metadata)), Some(42));
    }
}
