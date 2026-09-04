use std::collections::BTreeSet;

use rmcp::schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{
    fusion::{FusedSearchOutput, FusedSearchParams, FusionContribution},
    retrieval::AgentRecordId,
};

const DEFAULT_MAX_TOKENS: usize = 4_000;
const DEFAULT_MAX_BYTES: usize = 16_000;
const DEFAULT_MAX_DOCUMENTS: usize = 10;

#[derive(Clone, Debug, Deserialize, JsonSchema)]
pub struct RetrieveContextParams {
    pub search: FusedSearchParams,
    #[serde(default = "default_content_field")]
    pub content_field: String,
    #[serde(default = "default_max_tokens")]
    pub max_tokens: usize,
    #[serde(default = "default_max_bytes")]
    pub max_bytes: usize,
    #[serde(default = "default_max_documents")]
    pub max_documents: usize,
}

fn default_content_field() -> String {
    "content".into()
}

const fn default_max_tokens() -> usize {
    DEFAULT_MAX_TOKENS
}

const fn default_max_bytes() -> usize {
    DEFAULT_MAX_BYTES
}

const fn default_max_documents() -> usize {
    DEFAULT_MAX_DOCUMENTS
}

impl RetrieveContextParams {
    pub(crate) fn validate(&self) -> Result<(), String> {
        self.search.validate()?;
        if self.content_field.trim().is_empty() {
            return Err("Ketebe context request failed: invalid_content_field".to_string());
        }
        if self.max_tokens == 0 {
            return Err("Ketebe context request failed: invalid_token_budget".to_string());
        }
        if self.max_bytes == 0 {
            return Err("Ketebe context request failed: invalid_byte_budget".to_string());
        }
        if self.max_documents == 0 {
            return Err("Ketebe context request failed: invalid_document_budget".to_string());
        }
        Ok(())
    }

    pub(crate) fn prepare_search(&self) -> FusedSearchParams {
        let mut search = self.search.clone();
        ensure_metadata_field(&mut search.fields, &self.content_field);
        for field in ["source_uri", "document_id", "chunk_id"] {
            ensure_metadata_field(&mut search.fields, field);
        }
        search
    }
}

fn ensure_metadata_field(fields: &mut Vec<String>, field: &str) {
    if fields.is_empty() {
        return;
    }
    let projection = format!("metadata.{field}");
    if !fields.iter().any(|value| value == &projection) {
        fields.push(projection);
    }
}

#[derive(Clone, Debug, Serialize, JsonSchema, PartialEq)]
pub struct RetrieveContextOutput {
    pub context_text: String,
    pub blocks: Vec<ContextBlock>,
    pub citations: Vec<ContextCitation>,
    pub budget: ContextBudgetUsage,
}

#[derive(Clone, Debug, Serialize, JsonSchema, PartialEq, Eq)]
pub struct ContextBlock {
    pub citation_id: String,
    pub text: String,
    pub token_count: usize,
    pub byte_count: usize,
    pub truncated: bool,
}

#[derive(Clone, Debug, Serialize, JsonSchema, PartialEq)]
pub struct ContextCitation {
    pub citation_id: String,
    pub collection: String,
    pub record_id: AgentRecordId,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_uri: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub document_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub chunk_id: Option<String>,
    pub fusion_rank: usize,
    pub fusion_score: f64,
    pub provenance: Vec<FusionContribution>,
}

#[derive(Clone, Debug, Serialize, JsonSchema, PartialEq, Eq)]
pub struct ContextBudgetUsage {
    pub tokenizer: String,
    pub max_tokens: usize,
    pub used_tokens: usize,
    pub max_bytes: usize,
    pub used_bytes: usize,
    pub max_documents: usize,
    pub used_documents: usize,
    pub omitted_hits: usize,
    pub truncated_blocks: usize,
}

#[must_use]
pub fn assemble_context(
    params: &RetrieveContextParams,
    result: FusedSearchOutput,
) -> RetrieveContextOutput {
    let mut blocks = Vec::new();
    let mut citations = Vec::new();
    let mut documents = BTreeSet::new();
    let mut used_tokens = 0usize;
    let mut used_bytes = 0usize;
    let mut omitted_hits = 0usize;
    let mut truncated_blocks = 0usize;

    for hit in &result.hits {
        let metadata = hit.representative.metadata.as_ref();
        let Some(content) = metadata
            .and_then(|value| metadata_string(value, &params.content_field))
            .filter(|value| !value.is_empty())
        else {
            omitted_hits = omitted_hits.saturating_add(1);
            continue;
        };

        let collection = hit
            .provenance
            .first()
            .map(|value| value.source_collection.clone())
            .unwrap_or_default();
        let source_uri = metadata.and_then(|value| metadata_string(value, "source_uri"));
        let document_id = metadata.and_then(|value| metadata_string(value, "document_id"));
        let chunk_id = metadata.and_then(|value| metadata_string(value, "chunk_id"));
        let document_key = document_id
            .clone()
            .unwrap_or_else(|| format!("{collection}:{}", record_key(&hit.id)));
        let is_new_document = !documents.contains(&document_key);
        if is_new_document && documents.len() >= params.max_documents {
            omitted_hits = omitted_hits.saturating_add(1);
            continue;
        }

        let remaining_tokens = params.max_tokens.saturating_sub(used_tokens);
        let remaining_bytes = params.max_bytes.saturating_sub(used_bytes);
        if remaining_tokens == 0 || remaining_bytes == 0 {
            omitted_hits = omitted_hits.saturating_add(1);
            continue;
        }

        let (text, token_count, truncated) =
            truncate_text(&content, remaining_tokens, remaining_bytes);
        if text.is_empty() {
            omitted_hits = omitted_hits.saturating_add(1);
            continue;
        }
        if is_new_document {
            documents.insert(document_key);
        }

        let byte_count = text.len();
        used_tokens = used_tokens.saturating_add(token_count);
        used_bytes = used_bytes.saturating_add(byte_count);
        if truncated {
            truncated_blocks = truncated_blocks.saturating_add(1);
        }

        let citation_id = format!("ctx-{}", blocks.len() + 1);
        blocks.push(ContextBlock {
            citation_id: citation_id.clone(),
            text,
            token_count,
            byte_count,
            truncated,
        });
        citations.push(ContextCitation {
            citation_id,
            collection,
            record_id: hit.id.clone(),
            source_uri,
            document_id,
            chunk_id,
            fusion_rank: hit.fusion_rank,
            fusion_score: hit.fusion_score,
            provenance: hit.provenance.clone(),
        });
    }

    let context_text = blocks
        .iter()
        .map(|block| format!("[{}]\n{}", block.citation_id, block.text))
        .collect::<Vec<_>>()
        .join("\n\n");

    RetrieveContextOutput {
        context_text,
        blocks,
        citations,
        budget: ContextBudgetUsage {
            tokenizer: "unicode_whitespace_v0".into(),
            max_tokens: params.max_tokens,
            used_tokens,
            max_bytes: params.max_bytes,
            used_bytes,
            max_documents: params.max_documents,
            used_documents: documents.len(),
            omitted_hits,
            truncated_blocks,
        },
    }
}

fn metadata_string(metadata: &Value, path: &str) -> Option<String> {
    let mut current = metadata;
    for segment in path.split('.') {
        current = current.get(segment)?;
    }
    match current {
        Value::String(value) => Some(value.clone()),
        Value::Number(value) => Some(value.to_string()),
        _ => None,
    }
}

fn truncate_text(text: &str, max_tokens: usize, max_bytes: usize) -> (String, usize, bool) {
    let mut end = 0usize;
    let mut tokens = 0usize;
    let mut in_token = false;

    for (index, character) in text.char_indices() {
        let character_end = index + character.len_utf8();
        if character_end > max_bytes {
            break;
        }
        let whitespace = character.is_whitespace();
        if !whitespace && !in_token {
            if tokens >= max_tokens {
                break;
            }
            tokens += 1;
        }
        in_token = !whitespace;
        end = character_end;
    }

    let truncated = end < text.len();
    (text[..end].trim_end().to_string(), tokens, truncated)
}

fn record_key(id: &AgentRecordId) -> String {
    match id {
        AgentRecordId::String(value) => format!("string:{value}"),
        AgentRecordId::U64(value) => format!("u64:{value}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        fusion::{DedupStrategy, FusedSearchHit, FusionStrategy},
        multi_search::SearchManyTarget,
        search::{SearchHit, SearchMode},
    };
    use serde_json::json;

    fn params() -> RetrieveContextParams {
        RetrieveContextParams {
            search: FusedSearchParams {
                collections: vec![SearchManyTarget {
                    collection: "docs".into(),
                    search_profile: None,
                }],
                mode: SearchMode::Dense,
                text: None,
                vector: Some(vec![1.0]),
                filter: None,
                after: None,
                before: None,
                prefer_recent: false,
                candidate_limit: 10,
                final_limit: 10,
                fields: Vec::new(),
                execution: None,
                dense_candidates: None,
                sparse_candidates: None,
                timeout_ms: None,
                fusion: FusionStrategy::Rrf,
                rrf_k: 60,
                dedup: DedupStrategy::RecordId,
                rerank: None,
                explain: false,
            },
            content_field: "content".into(),
            max_tokens: 3,
            max_bytes: 100,
            max_documents: 1,
        }
    }

    #[test]
    fn context_budgeting_is_deterministic_and_preserves_source_identity() {
        let hit = FusedSearchHit {
            fusion_rank: 1,
            fusion_score: 1.0,
            id: AgentRecordId::String("a".into()),
            representative: SearchHit {
                id: AgentRecordId::String("a".into()),
                score: 0.9,
                sequence_number: 1,
                source_timestamp_unix_ms: None,
                metadata: Some(json!({
                    "content":"one two three four",
                    "source_uri":"file:///docs/a.md",
                    "document_id":"doc-a",
                    "chunk_id":"chunk-1"
                })),
                dense_rank: Some(1),
                sparse_rank: None,
                dense_score: Some(0.9),
                sparse_score: None,
                rerank_score: None,
                original_rank: None,
            },
            provenance: vec![FusionContribution {
                source_collection: "docs".into(),
                source_rank: 1,
                retrieval_score: 0.9,
                rerank_score: None,
                original_rank: None,
                fusion_contribution: 1.0,
            }],
        };
        let output = assemble_context(
            &params(),
            FusedSearchOutput {
                fusion: FusionStrategy::Rrf,
                dedup: DedupStrategy::RecordId,
                results: Vec::new(),
                hits: vec![hit],
            },
        );
        assert_eq!(output.blocks[0].text, "one two three");
        assert!(output.blocks[0].truncated);
        assert_eq!(
            output.citations[0].source_uri.as_deref(),
            Some("file:///docs/a.md")
        );
        assert_eq!(output.citations[0].document_id.as_deref(), Some("doc-a"));
        assert_eq!(output.citations[0].chunk_id.as_deref(), Some("chunk-1"));
        assert_eq!(output.budget.used_tokens, 3);
    }
}
