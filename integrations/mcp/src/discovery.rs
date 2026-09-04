use std::collections::BTreeMap;

use rmcp::schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;

const SAFE_METADATA_FIELDS: &[&str] = &[
    "lexical_fields",
    "embedding_profile",
    "reranker_profile",
    "search_profiles",
    "record_count",
    "document_count",
    "created_at",
    "updated_at",
];

#[derive(Clone, Debug, Deserialize, JsonSchema)]
pub struct CollectionParams {
    pub collection: String,
}

#[derive(Clone, Debug, Serialize, JsonSchema, PartialEq)]
pub struct CollectionView {
    pub id: String,
    pub dimension: usize,
    pub metric: String,
    pub metadata: BTreeMap<String, Value>,
}

#[derive(Clone, Debug, Serialize, JsonSchema, PartialEq)]
pub struct ListCollectionsOutput {
    pub collections: Vec<CollectionView>,
}

#[derive(Clone, Debug, Serialize, JsonSchema, PartialEq)]
pub struct CollectionStatsOutput {
    pub id: String,
    pub dimension: usize,
    pub metric: String,
    pub record_count: Option<u64>,
    pub document_count: Option<u64>,
}

impl From<ketebe_sdk::Collection> for CollectionView {
    fn from(collection: ketebe_sdk::Collection) -> Self {
        let metadata = SAFE_METADATA_FIELDS
            .iter()
            .filter_map(|key| {
                collection
                    .extra
                    .get(*key)
                    .cloned()
                    .map(|value| ((*key).to_string(), value))
            })
            .collect();
        Self {
            id: collection.id,
            dimension: collection.dimension,
            metric: collection.metric,
            metadata,
        }
    }
}

impl From<ketebe_sdk::Collection> for CollectionStatsOutput {
    fn from(collection: ketebe_sdk::Collection) -> Self {
        Self {
            id: collection.id,
            dimension: collection.dimension,
            metric: collection.metric,
            record_count: collection.extra.get("record_count").and_then(Value::as_u64),
            document_count: collection
                .extra
                .get("document_count")
                .and_then(Value::as_u64),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn collection() -> ketebe_sdk::Collection {
        let mut extra = serde_json::Map::new();
        extra.insert("record_count".into(), Value::from(12_u64));
        extra.insert("embedding_profile".into(), Value::from("default"));
        extra.insert("shard_id".into(), Value::from("internal-shard"));
        extra.insert("node".into(), Value::from("internal-node"));
        ketebe_sdk::Collection {
            id: "docs".into(),
            dimension: 768,
            metric: "cosine".into(),
            extra,
        }
    }

    #[test]
    fn discovery_projection_does_not_expose_topology() {
        let view = CollectionView::from(collection());
        assert_eq!(view.id, "docs");
        assert_eq!(
            view.metadata.get("record_count"),
            Some(&Value::from(12_u64))
        );
        assert!(!view.metadata.contains_key("shard_id"));
        assert!(!view.metadata.contains_key("node"));
    }

    #[test]
    fn stats_projection_uses_public_collection_metadata_only() {
        let stats = CollectionStatsOutput::from(collection());
        assert_eq!(stats.record_count, Some(12));
        assert_eq!(stats.document_count, None);
    }
}
