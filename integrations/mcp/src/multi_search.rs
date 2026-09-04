use rmcp::schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{
    error::McpError,
    search::{SearchHit, SearchMode, SearchParams},
};

const DEFAULT_LIMIT: usize = 10;

#[derive(Clone, Debug, Deserialize, JsonSchema)]
pub struct SearchManyTarget {
    pub collection: String,
    #[serde(default)]
    pub search_profile: Option<String>,
}

#[derive(Clone, Debug, Deserialize, JsonSchema)]
pub struct SearchManyParams {
    pub collections: Vec<SearchManyTarget>,
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
    pub timeout_ms: Option<u64>,
    #[serde(default)]
    pub explain: bool,
}

const fn default_limit() -> usize {
    DEFAULT_LIMIT
}

impl SearchManyParams {
    pub(crate) fn validate(&self) -> Result<(), String> {
        if self.collections.is_empty() {
            return Err("Ketebe multi-search request failed: collections_required".to_string());
        }
        if self
            .collections
            .iter()
            .any(|target| target.collection.trim().is_empty())
        {
            return Err("Ketebe multi-search request failed: invalid_collection".to_string());
        }
        if self
            .after
            .zip(self.before)
            .is_some_and(|(after, before)| after >= before)
        {
            return Err("Ketebe multi-search request failed: invalid_time_range".to_string());
        }
        Ok(())
    }

    pub(crate) fn search_params_for(&self, target: &SearchManyTarget) -> SearchParams {
        SearchParams {
            collection: target.collection.clone(),
            mode: self.mode,
            text: self.text.clone(),
            vector: self.vector.clone(),
            filter: self.filter.clone(),
            after: self.after,
            before: self.before,
            prefer_recent: self.prefer_recent,
            limit: self.limit,
            fields: self.fields.clone(),
            execution: self.execution.clone(),
            dense_candidates: self.dense_candidates,
            sparse_candidates: self.sparse_candidates,
            search_profile: target.search_profile.clone(),
            timeout_ms: self.timeout_ms,
            explain: self.explain,
        }
    }
}

#[derive(Clone, Debug, Serialize, JsonSchema, PartialEq)]
pub struct SearchManyOutput {
    pub results: Vec<CollectionSearchResult>,
    pub merge_input: Vec<ProvenancedSearchHit>,
}

#[derive(Clone, Debug, Serialize, JsonSchema, PartialEq)]
pub struct CollectionSearchResult {
    pub collection: String,
    pub status: CollectionSearchStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mode: Option<SearchMode>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub hits: Vec<SearchHit>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub explain: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<McpError>,
}

#[derive(Clone, Copy, Debug, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum CollectionSearchStatus {
    Ok,
    Error,
}

#[derive(Clone, Debug, Serialize, JsonSchema, PartialEq)]
pub struct ProvenancedSearchHit {
    pub source_collection: String,
    pub source_rank: usize,
    pub hit: SearchHit,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::McpErrorCategory;

    #[test]
    fn target_order_is_preserved_for_deterministic_merge_input() {
        let params = SearchManyParams {
            collections: vec![
                SearchManyTarget {
                    collection: "b".into(),
                    search_profile: Some("p@1".into()),
                },
                SearchManyTarget {
                    collection: "a".into(),
                    search_profile: None,
                },
            ],
            mode: SearchMode::Dense,
            text: None,
            vector: Some(vec![1.0, 0.0]),
            filter: None,
            after: Some(100),
            before: Some(200),
            prefer_recent: true,
            limit: 10,
            fields: Vec::new(),
            execution: None,
            dense_candidates: None,
            sparse_candidates: None,
            timeout_ms: None,
            explain: false,
        };
        params.validate().unwrap();
        let first = params.search_params_for(&params.collections[0]);
        let second = params.search_params_for(&params.collections[1]);
        assert_eq!(first.collection, "b");
        assert_eq!(first.search_profile.as_deref(), Some("p@1"));
        assert_eq!(first.after, Some(100));
        assert_eq!(first.before, Some(200));
        assert!(first.prefer_recent);
        assert_eq!(second.collection, "a");
        assert!(second.search_profile.is_none());
    }

    #[test]
    fn partial_failure_shape_is_machine_readable() {
        let result = CollectionSearchResult {
            collection: "private".into(),
            status: CollectionSearchStatus::Error,
            mode: None,
            hits: Vec::new(),
            explain: None,
            error: Some(McpError::from_http(403, "forbidden")),
        };
        let value = serde_json::to_value(result).unwrap();
        assert_eq!(value["error"]["code"], "forbidden");
        assert_eq!(value["error"]["category"], "authorization");
        assert_eq!(value["error"]["retryable"], false);
        assert_eq!(
            McpError::from_http(403, "forbidden").category,
            McpErrorCategory::Authorization
        );
    }
}
