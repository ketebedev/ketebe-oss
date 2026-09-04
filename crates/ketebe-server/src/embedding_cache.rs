use crate::embedding::{EmbeddingProvider, EmbeddingProviderError, embed_texts_batched};
use crate::provenance::canonical_content_hash;
use crate::resource_scheduler::{WorkKind, global_resource_scheduler};
use std::collections::{HashMap, VecDeque};
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

pub const DEFAULT_EMBEDDING_CACHE_CAPACITY: usize = 4_096;
const EMBEDDING_INPUT_NORMALIZATION: &str = "line_endings_v1";

#[derive(Debug, Clone, Eq)]
pub struct EmbeddingCacheKey {
    pub profile: String,
    pub provider: String,
    pub model: String,
    pub model_version: String,
    pub dimension: usize,
    pub normalization: String,
    pub content_sha256: String,
}

impl PartialEq for EmbeddingCacheKey {
    fn eq(&self, other: &Self) -> bool {
        self.profile == other.profile
            && self.provider == other.provider
            && self.model == other.model
            && self.model_version == other.model_version
            && self.dimension == other.dimension
            && self.normalization == other.normalization
            && self.content_sha256 == other.content_sha256
    }
}

impl Hash for EmbeddingCacheKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.profile.hash(state);
        self.provider.hash(state);
        self.model.hash(state);
        self.model_version.hash(state);
        self.dimension.hash(state);
        self.normalization.hash(state);
        self.content_sha256.hash(state);
    }
}

#[derive(Debug)]
struct EmbeddingCacheState {
    entries: HashMap<EmbeddingCacheKey, Vec<f32>>,
    order: VecDeque<EmbeddingCacheKey>,
}

#[derive(Debug)]
pub struct EmbeddingCache {
    capacity: usize,
    state: Mutex<EmbeddingCacheState>,
}

impl Default for EmbeddingCache {
    fn default() -> Self {
        Self::new(DEFAULT_EMBEDDING_CACHE_CAPACITY)
    }
}

impl EmbeddingCache {
    #[must_use]
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity,
            state: Mutex::new(EmbeddingCacheState {
                entries: HashMap::new(),
                order: VecDeque::new(),
            }),
        }
    }

    #[must_use]
    pub const fn capacity(&self) -> usize {
        self.capacity
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.state
            .lock()
            .map(|state| state.entries.len())
            .unwrap_or(0)
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn clear(&self) {
        if let Ok(mut state) = self.state.lock() {
            state.entries.clear();
            state.order.clear();
        }
    }

    fn get(&self, key: &EmbeddingCacheKey) -> Option<Vec<f32>> {
        let Ok(mut state) = self.state.lock() else {
            CACHE_FAILURES.fetch_add(1, Ordering::Relaxed);
            CACHE_MISSES.fetch_add(1, Ordering::Relaxed);
            return None;
        };
        let Some(vector) = state.entries.get(key).cloned() else {
            CACHE_MISSES.fetch_add(1, Ordering::Relaxed);
            return None;
        };
        if let Some(position) = state.order.iter().position(|candidate| candidate == key) {
            state.order.remove(position);
        }
        state.order.push_back(key.clone());
        CACHE_HITS.fetch_add(1, Ordering::Relaxed);
        Some(vector)
    }

    fn insert(&self, key: EmbeddingCacheKey, vector: Vec<f32>) {
        if self.capacity == 0 {
            return;
        }
        let Ok(mut state) = self.state.lock() else {
            CACHE_FAILURES.fetch_add(1, Ordering::Relaxed);
            return;
        };
        if state.entries.contains_key(&key) {
            state.entries.insert(key.clone(), vector);
            if let Some(position) = state.order.iter().position(|candidate| candidate == &key) {
                state.order.remove(position);
            }
            state.order.push_back(key);
            return;
        }
        while state.entries.len() >= self.capacity {
            let Some(evicted) = state.order.pop_front() else {
                break;
            };
            if state.entries.remove(&evicted).is_some() {
                CACHE_EVICTIONS.fetch_add(1, Ordering::Relaxed);
            }
        }
        state.entries.insert(key.clone(), vector);
        state.order.push_back(key);
    }
}

#[must_use]
pub fn embedding_cache_key(
    profile: &str,
    provider: &dyn EmbeddingProvider,
    dimension: usize,
    text: &str,
) -> EmbeddingCacheKey {
    let model = provider.model();
    EmbeddingCacheKey {
        profile: profile.to_string(),
        provider: provider.provider_name().to_string(),
        model: model.name,
        model_version: model.version,
        dimension,
        normalization: EMBEDDING_INPUT_NORMALIZATION.to_string(),
        content_sha256: canonical_content_hash(text),
    }
}

pub async fn embed_texts_cached(
    cache: Arc<EmbeddingCache>,
    profile: &str,
    provider: Arc<dyn EmbeddingProvider>,
    texts: &[String],
    expected_dimension: usize,
) -> Result<Vec<Vec<f32>>, EmbeddingProviderError> {
    if texts.is_empty() {
        return Ok(Vec::new());
    }

    let mut output: Vec<Option<Vec<f32>>> = vec![None; texts.len()];
    let mut unique_misses: Vec<(EmbeddingCacheKey, String)> = Vec::new();
    let mut pending: HashMap<EmbeddingCacheKey, usize> = HashMap::new();
    let mut output_to_miss: Vec<Option<usize>> = vec![None; texts.len()];

    for (index, text) in texts.iter().enumerate() {
        let key = embedding_cache_key(profile, provider.as_ref(), expected_dimension, text);
        if let Some(vector) = cache.get(&key) {
            output[index] = Some(vector);
            continue;
        }
        let miss_index = if let Some(existing) = pending.get(&key) {
            CACHE_DEDUPLICATED_INPUTS.fetch_add(1, Ordering::Relaxed);
            *existing
        } else {
            let miss_index = unique_misses.len();
            pending.insert(key.clone(), miss_index);
            unique_misses.push((key, text.clone()));
            miss_index
        };
        output_to_miss[index] = Some(miss_index);
    }

    let miss_texts = unique_misses
        .iter()
        .map(|(_, text)| text.clone())
        .collect::<Vec<_>>();
    let _resource_permit = if miss_texts.is_empty() {
        None
    } else {
        Some(
            global_resource_scheduler()
                .acquire(WorkKind::Embedding)
                .await
                .map_err(|error| {
                    EmbeddingProviderError::new(format!(
                        "embedding resource admission failed: {error}"
                    ))
                })?,
        )
    };
    let miss_vectors = embed_texts_batched(provider, &miss_texts, expected_dimension).await?;
    if miss_vectors.len() != unique_misses.len() {
        return Err(EmbeddingProviderError::new(
            "embedding cache executor received an unexpected provider result count",
        ));
    }

    for ((key, _), vector) in unique_misses.iter().zip(miss_vectors.iter()) {
        cache.insert(key.clone(), vector.clone());
    }
    for (index, miss_index) in output_to_miss.into_iter().enumerate() {
        if let Some(miss_index) = miss_index {
            output[index] = Some(miss_vectors[miss_index].clone());
        }
    }

    output
        .into_iter()
        .map(|vector| {
            vector.ok_or_else(|| {
                EmbeddingProviderError::new("embedding cache executor lost an output vector")
            })
        })
        .collect()
}

static CACHE_HITS: AtomicU64 = AtomicU64::new(0);
static CACHE_MISSES: AtomicU64 = AtomicU64::new(0);
static CACHE_EVICTIONS: AtomicU64 = AtomicU64::new(0);
static CACHE_FAILURES: AtomicU64 = AtomicU64::new(0);
static CACHE_DEDUPLICATED_INPUTS: AtomicU64 = AtomicU64::new(0);

#[must_use]
pub fn embedding_cache_prometheus_metrics() -> String {
    format!(
        concat!(
            "ketebe_embedding_cache_hits_total {}\n",
            "ketebe_embedding_cache_misses_total {}\n",
            "ketebe_embedding_cache_evictions_total {}\n",
            "ketebe_embedding_cache_failures_total {}\n",
            "ketebe_embedding_cache_deduplicated_inputs_total {}\n"
        ),
        CACHE_HITS.load(Ordering::Relaxed),
        CACHE_MISSES.load(Ordering::Relaxed),
        CACHE_EVICTIONS.load(Ordering::Relaxed),
        CACHE_FAILURES.load(Ordering::Relaxed),
        CACHE_DEDUPLICATED_INPUTS.load(Ordering::Relaxed),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::embedding::DeterministicEmbeddingProvider;

    #[test]
    fn key_changes_with_profile_model_version_dimension_and_content() {
        let provider_v1 = DeterministicEmbeddingProvider::new("model", "v1").unwrap();
        let provider_v2 = DeterministicEmbeddingProvider::new("model", "v2").unwrap();
        let base = embedding_cache_key("p1", &provider_v1, 4, "hello\r\nworld");
        assert_eq!(
            base,
            embedding_cache_key("p1", &provider_v1, 4, "hello\nworld")
        );
        assert_ne!(
            base,
            embedding_cache_key("p2", &provider_v1, 4, "hello\nworld")
        );
        assert_ne!(
            base,
            embedding_cache_key("p1", &provider_v2, 4, "hello\nworld")
        );
        assert_ne!(
            base,
            embedding_cache_key("p1", &provider_v1, 8, "hello\nworld")
        );
        assert_ne!(base, embedding_cache_key("p1", &provider_v1, 4, "changed"));
    }

    #[test]
    fn bounded_cache_evicts_least_recently_used_entry() {
        let provider = DeterministicEmbeddingProvider::new("model", "v1").unwrap();
        let cache = EmbeddingCache::new(2);
        let a = embedding_cache_key("p", &provider, 2, "a");
        let b = embedding_cache_key("p", &provider, 2, "b");
        let c = embedding_cache_key("p", &provider, 2, "c");
        cache.insert(a.clone(), vec![1.0, 0.0]);
        cache.insert(b.clone(), vec![0.0, 1.0]);
        assert!(cache.get(&a).is_some());
        cache.insert(c, vec![1.0, 1.0]);
        assert!(cache.get(&a).is_some());
        assert!(cache.get(&b).is_none());
        assert_eq!(cache.len(), 2);
    }
}
