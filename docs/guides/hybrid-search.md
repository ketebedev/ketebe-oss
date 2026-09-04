# Hybrid search

Hybrid search combines semantic and lexical retrieval so one query can benefit from both meaning and exact-term evidence.

## When to use it

Hybrid retrieval is especially useful for datasets that mix natural language with identifiers, names, SKUs, error codes, technical vocabulary, or other terms whose literal form matters.

Dense-only retrieval can miss exact lexical intent. Sparse-only retrieval can miss semantically related wording. Hybrid retrieval lets both signals contribute candidates.

## Query flow

A typical Ketebe hybrid query follows this logical flow:

```text
query
  -> dense retrieval
  -> sparse / lexical retrieval
  -> candidate fusion
  -> optional reranking
  -> filtering / final result shaping
  -> explanation and provenance
```

The precise execution plan is an implementation detail and may be optimized while preserving public query semantics.

## Fusion and reranking

Fusion combines rankings or scores from multiple retrievers. Reranking is a later stage that can apply a more expensive relevance model to a smaller candidate set.

Tune these independently: first ensure each retriever contributes useful candidates, then evaluate fusion, then add reranking only when it measurably improves quality.

## Evaluate relevance

Do not optimize hybrid parameters against a handful of anecdotal queries. Maintain a representative evaluation set and track relevance metrics together with latency and resource cost.

Ketebe's benchmark tooling includes search-quality evaluation support; see [benchmark methodology](../benchmarks.md).