use crate::token_chunking::{StructuredChunk, TokenCounter, UnicodeWordTokenCounter};
use ketebe_core::{ChunkingStructure, SemanticChunkingPolicy, TokenChunkingPolicy};
use std::collections::BTreeSet;

pub const SEMANTIC_CHUNKER_VERSION: &str = "semantic-embedding-v1";
pub const SEMANTIC_SCORER_ID: &str = "embedding-profile-cosine-v1";

#[derive(Debug, Clone, PartialEq)]
pub struct SemanticBoundaryCandidate {
    pub token_index: usize,
    pub byte_index: usize,
    pub left_context: String,
    pub right_context: String,
}

pub trait SemanticBoundaryScorer: Send + Sync {
    fn identity(&self) -> &'static str;
    fn score(&self, left: &str, right: &str) -> f32;
}

#[derive(Debug, Clone, Copy, Default)]
pub struct ReferenceLexicalBoundaryScorer;

impl SemanticBoundaryScorer for ReferenceLexicalBoundaryScorer {
    fn identity(&self) -> &'static str {
        "reference-token-jaccard-v1"
    }

    fn score(&self, left: &str, right: &str) -> f32 {
        let left = lexical_set(left);
        let right = lexical_set(right);
        if left.is_empty() && right.is_empty() {
            return 1.0;
        }
        let intersection = left.intersection(&right).count() as f32;
        let union = left.union(&right).count() as f32;
        if union == 0.0 {
            1.0
        } else {
            intersection / union
        }
    }
}

fn lexical_set(text: &str) -> BTreeSet<String> {
    text.split(|c: char| !c.is_alphanumeric() && c != '_')
        .filter(|v| !v.is_empty())
        .map(|v| v.to_lowercase())
        .collect()
}

#[must_use]
pub fn semantic_chunker_fingerprint(policy: SemanticChunkingPolicy, profile: &str) -> String {
    format!(
        "{SEMANTIC_CHUNKER_VERSION}:scorer={SEMANTIC_SCORER_ID}:profile={profile}:tokenizer=unicode_words_v1:max_tokens={}:overlap={}:min_tokens={}:threshold_milli={}",
        policy.max_tokens(),
        policy.token_overlap(),
        policy.min_tokens(),
        policy.breakpoint_threshold_milli()
    )
}

#[must_use]
pub fn semantic_boundary_candidates(
    text: &str,
    policy: SemanticChunkingPolicy,
) -> Vec<SemanticBoundaryCandidate> {
    let counter = UnicodeWordTokenCounter;
    let tokens = counter.spans(text);
    if tokens.len() <= policy.min_tokens() {
        return Vec::new();
    }
    let stride = policy.min_tokens();
    let context = policy.min_tokens().max(1);
    let mut out = Vec::new();
    let mut boundary = stride;
    while boundary < tokens.len() {
        let left_start = boundary.saturating_sub(context);
        let right_end = (boundary + context).min(tokens.len());
        let left_start_byte = tokens[left_start].start_byte;
        let left_end_byte = tokens[boundary - 1].end_byte;
        let right_start_byte = tokens[boundary].start_byte;
        let right_end_byte = tokens[right_end - 1].end_byte;
        out.push(SemanticBoundaryCandidate {
            token_index: boundary,
            byte_index: right_start_byte,
            left_context: text[left_start_byte..left_end_byte].to_string(),
            right_context: text[right_start_byte..right_end_byte].to_string(),
        });
        boundary = boundary.saturating_add(stride);
    }
    out
}

#[must_use]
pub fn chunks_from_similarity_scores(
    text: &str,
    policy: SemanticChunkingPolicy,
    scores: &[(usize, f32)],
) -> Vec<StructuredChunk> {
    let counter = UnicodeWordTokenCounter;
    let tokens = counter.spans(text);
    if tokens.is_empty() {
        return Vec::new();
    }
    let threshold = f32::from(policy.breakpoint_threshold_milli()) / 1000.0;
    let preferred: BTreeSet<usize> = scores
        .iter()
        .filter_map(|(index, score)| (*score < threshold).then_some(*index))
        .collect();
    let mut chunks = Vec::new();
    let mut start = 0usize;
    while start < tokens.len() {
        let hard_end = (start + policy.max_tokens()).min(tokens.len());
        let min_end = (start + policy.min_tokens()).min(hard_end);
        let end = if hard_end == tokens.len() {
            hard_end
        } else {
            preferred
                .range(min_end..=hard_end)
                .next_back()
                .copied()
                .unwrap_or(hard_end)
        };
        let start_byte = tokens[start].start_byte;
        let end_byte = tokens[end - 1].end_byte;
        chunks.push(StructuredChunk {
            ordinal: chunks.len(),
            start_byte,
            end_byte,
            token_count: end - start,
            text: text[start_byte..end_byte].to_string(),
        });
        if end == tokens.len() {
            break;
        }
        let candidate = end.saturating_sub(policy.token_overlap());
        start = if candidate <= start { end } else { candidate };
    }
    chunks
}

#[must_use]
pub fn token_fallback_policy(policy: SemanticChunkingPolicy) -> TokenChunkingPolicy {
    TokenChunkingPolicy::new(
        ChunkingStructure::Tokens,
        policy.max_tokens(),
        policy.token_overlap(),
        policy.tokenizer(),
    )
    .expect("semantic policy already validates token limits")
}

#[must_use]
pub fn cosine_similarity(left: &[f32], right: &[f32]) -> Option<f32> {
    if left.len() != right.len() || left.is_empty() {
        return None;
    }
    let mut dot = 0.0f32;
    let mut l2 = 0.0f32;
    let mut r2 = 0.0f32;
    for (&l, &r) in left.iter().zip(right) {
        if !l.is_finite() || !r.is_finite() {
            return None;
        }
        dot += l * r;
        l2 += l * l;
        r2 += r * r;
    }
    if l2 == 0.0 || r2 == 0.0 {
        return None;
    }
    Some((dot / (l2.sqrt() * r2.sqrt())).clamp(-1.0, 1.0))
}

#[cfg(test)]
mod tests {
    use super::*;
    use ketebe_core::TokenizerKind;

    fn policy(max: usize, min: usize, threshold: u16) -> SemanticChunkingPolicy {
        SemanticChunkingPolicy::new(max, 1, min, threshold, TokenizerKind::UnicodeWordsV1).unwrap()
    }

    #[test]
    fn reference_scorer_is_hermetic_and_detects_topic_change() {
        let scorer = ReferenceLexicalBoundaryScorer;
        let same = scorer.score("database vector search", "vector search database");
        let changed = scorer.score("database vector search", "banana recipe kitchen");
        assert!(same > changed);
        assert_eq!(scorer.identity(), "reference-token-jaccard-v1");
    }

    #[test]
    fn scores_prefer_semantic_breaks_but_never_exceed_hard_limit() {
        let text = "one two three four five six seven eight nine ten eleven twelve";
        let p = policy(6, 3, 500);
        let chunks = chunks_from_similarity_scores(text, p, &[(3, 0.9), (6, 0.1), (9, 0.9)]);
        assert!(chunks.iter().all(|c| c.token_count <= 6));
        assert!(chunks.len() >= 2);
    }

    #[test]
    fn embedding_similarity_is_bounded_and_validated() {
        assert_eq!(cosine_similarity(&[1.0, 0.0], &[1.0, 0.0]), Some(1.0));
        assert!(cosine_similarity(&[1.0], &[1.0, 2.0]).is_none());
    }
}
