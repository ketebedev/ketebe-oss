# Ketebe Roadmap

Ketebe is in **v0.9 release preparation**. The current public roadmap focuses on making the open-source product easy to install, operate, validate and contribute to.

## Current product baseline

Implemented and verified:

- durable storage, WAL/segment recovery and ANN retrieval,
- metadata filtering, sparse/lexical and hybrid retrieval,
- fusion, reranking and explainability,
- document ingestion and embedding/re-embedding lifecycle,
- Kafka-native continuous ingestion,
- REST and gRPC product APIs,
- Rust, Python, TypeScript, Java and Go SDKs,
- organizations/projects, authentication, API keys, RBAC, quotas and audit boundaries,
- TLS/mTLS and secret-management boundaries,
- first-party MCP integration with retrieval, context assembly, governance and compatibility tests.

## v0.9 — Public release readiness

The first public release focuses on a small, reliable distribution surface:

- production-oriented `ketebe-server` container image,
- persistent standalone Docker Compose quickstart,
- tag-driven release workflow,
- binary archive and SHA256 release assets,
- versioned container publishing,
- smoke/restart validation against packaged artifacts,
- concise release/version/upgrade notes.

## Public OSS repository

The public repository launch must provide:

- independently buildable and testable source,
- Apache-2.0 licensing,
- user-focused README and documentation,
- security reporting guidance,
- contribution guidance,
- CI that validates formatting, compilation, linting and tests,
- no private/internal development material in the public tree.

## Toward v1.0

The v1.0 path emphasizes evidence rather than feature accumulation:

- sustained soak testing on representative datasets,
- restart/recovery and upgrade drills using release artifacts,
- repeatable benchmark profiles,
- external-user feedback from v0.9 releases,
- release compatibility and migration verification,
- operational documentation based on real deployments.

## Engineering principles

- correctness before optimization,
- public APIs remain topology-independent,
- durability source of truth is explicit and recoverable,
- derived indexes remain rebuildable state,
- persistence changes require deterministic recovery evidence,
- performance claims require repeatable benchmarks,
- authentication and authorization remain separate boundaries,
- complexity must earn its place through a measurable product requirement.
