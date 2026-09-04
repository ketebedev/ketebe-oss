# Embeddings

Ketebe supports server-side embedding pipelines and embedding lifecycle management so applications do not have to treat vector generation as an unrelated external concern.

## Provider boundary

Embedding providers are integrations, not storage authorities. Provider credentials and provider-specific behavior remain separated from Ketebe's durable record state and authorization boundaries.

## Ingestion-time embeddings

Document and record ingestion can use configured embedding pipelines to create retrieval representations before data becomes queryable through the intended retrieval path.

## Re-embedding and migration

Embedding models change. Ketebe's lifecycle model accounts for re-embedding rather than assuming vectors are immutable forever.

Plan model migration around:

- model identity and dimension changes,
- coexistence of old and new representations during migration,
- validation of retrieval quality before cutover,
- restart and failure recovery,
- rollback strategy,
- provider rate limits and cost.

## Batch workloads

Large embedding or re-embedding operations should be treated as observable asynchronous work rather than an unbounded synchronous request. Monitor progress, failures, retries, and provider throttling.

## Secrets

Do not place provider API keys in documents, collection metadata, or source-controlled configuration. Use deployment secret mechanisms and scope credentials to the minimum permissions required.