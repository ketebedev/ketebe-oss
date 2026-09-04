# Ingestion

Ketebe supports direct record writes, document-oriented ingestion, and Kafka-native continuous ingestion.

## Direct records

Use direct record ingestion when your application already owns vector generation or when you need explicit control over record identifiers, vectors, and metadata.

## Documents

Document ingestion is intended for unstructured content that needs to be transformed into retrieval-ready records. A document pipeline can chunk source text, generate embeddings, retain metadata and provenance, and associate generated records with the parent document lifecycle.

## Continuous ingestion with Kafka

Kafka-native ingestion is designed for continuously changing datasets and event-driven pipelines. Treat Kafka as an integration boundary rather than bypassing Ketebe's normal validation, storage, and authorization semantics.

Operationally, plan for retry behavior, dead-letter handling, consumer recovery, and observable lag rather than assuming every external event succeeds on its first attempt.

## Idempotency

Stable identifiers are important for retryable ingestion. Callers should design writes so that retrying after an ambiguous network failure does not create unintended duplicate logical content.

## Before production

Validate at least:

- record and document identifiers,
- metadata/filter fields,
- embedding dimensions and model ownership,
- retry and backpressure behavior,
- restart/recovery behavior,
- authorization scope,
- representative ingestion throughput.

For exact API fields, use the checked-in OpenAPI contract and the first-party SDK corresponding to your application language.