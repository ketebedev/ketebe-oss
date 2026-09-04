# Retrieval

Ketebe provides a single retrieval platform for dense semantic search, sparse or lexical retrieval, and hybrid search.

## Collections and records

A collection is the primary retrieval namespace. Records carry identifiers, retrieval representations, and metadata used by filtering and result interpretation.

Ketebe also supports document-oriented ingestion. Documents can be transformed into retrieval records through chunking and embedding pipelines while preserving parent-document context and provenance.

## Dense retrieval

Dense vector retrieval supports exact search and HNSW-backed approximate nearest-neighbor search. Exact retrieval remains useful for validation and smaller workloads; HNSW provides an acceleration path for larger collections.

## Sparse and lexical retrieval

Sparse and lexical retrieval preserves exact-term and token-sensitive signals that dense embeddings can miss. This is useful for identifiers, product codes, names, technical terms, and other queries where literal matching matters.

## Hybrid retrieval

Hybrid retrieval combines dense and sparse candidates. Fusion and reranking can then produce a final ranking from multiple retrieval signals.

Use hybrid retrieval when both semantic similarity and exact lexical evidence matter. This mirrors a common pattern in current vector-search systems: Qdrant exposes dense+sparse hybrid queries, Weaviate documents hybrid search as a core query mode, and Milvus exposes hybrid retrieval across vector representations.

## Metadata filtering

Filters constrain the candidate set using structured metadata. Filtering should represent business constraints that embeddings alone cannot encode reliably, such as tenant, category, status, source, or time boundaries.

## Reranking

Reranking is a second-stage relevance operation over retrieved candidates. It is useful when the first-stage retriever is optimized for recall and a more expensive model or scoring strategy can improve final ordering.

## Explainability and provenance

Ketebe can expose retrieval explanations and provenance so applications and agents can understand why a result was selected and where its context originated.

## Choosing a retrieval strategy

Start simple:

1. Use dense retrieval for primarily semantic similarity.
2. Add filters for hard business constraints.
3. Use sparse or lexical retrieval when literal matching is important.
4. Use hybrid retrieval when both semantic and lexical evidence matter.
5. Add reranking when first-stage candidate quality is good but final ordering still needs improvement.

The public API contract is the source of truth for request and response fields; see [`api/openapi`](../../api/openapi/).