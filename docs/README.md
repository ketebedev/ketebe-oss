# Ketebe Documentation

Ketebe documentation is organized around what users are trying to accomplish rather than around internal implementation milestones.

Website: `ketebe.dev`

> Ketebe is preparing its first public v0.9 release. Source documentation describes the implemented product surface, but installation commands for unpublished release artifacts are intentionally not presented as generally available.

## Get started

- [Getting started](getting-started.md) — build from source, choose an API, and understand the v0.9 onboarding path.
- REST contract: [`api/openapi`](../api/openapi/)
- First-party SDKs: Rust, Python, TypeScript, Java, and Go.

## Concepts

- [Retrieval](concepts/retrieval.md) — dense, sparse, hybrid retrieval, filtering, reranking, and explainability.
- [Storage and consistency](concepts/storage-and-consistency.md) — durability, recovery, visibility, and derived indexes.

## Guides

- [Ingestion](guides/ingestion.md)
- [Hybrid search](guides/hybrid-search.md)
- [Embeddings](guides/embeddings.md)

## Agents and MCP

- [MCP quickstart](mcp/quickstart.md)
- [MCP operations](mcp/operations.md)

Ketebe MCP is a first-party adapter over Ketebe's public API. It does not bypass storage, authorization, or correctness boundaries.

## Operate

- [Security](operations/security.md)
- [Backup and recovery](operations/backup-recovery.md)
- [Benchmark methodology](benchmarks.md)


## Reference

- OpenAPI: [`api/openapi/v1.json`](../api/openapi/v1.json)
- API compatibility data: [`api/openapi/v1.compatibility.json`](../api/openapi/v1.compatibility.json)
- Roadmap: [`ROADMAP.md`](../ROADMAP.md)

## Contribute and report security issues

- [Contributing](../CONTRIBUTING.md)
- [Security reporting](../SECURITY.md)
- [Code of conduct](../CODE_OF_CONDUCT.md)
