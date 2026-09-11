# Ketebe Documentation

Ketebe documentation is organized around what users are trying to accomplish rather than around internal implementation milestones.

Website: `ketebe.dev`

> Ketebe is preparing its first public v0.9 release. Source documentation describes the implemented product surface, but installation commands for unpublished release artifacts are intentionally not presented as generally available.

## Get started

- [Getting started](getting-started.md) — start with Docker Compose, choose an API, and understand the v0.9 onboarding path.
- [Standalone Docker Compose quickstart](operations/compose-quickstart.md) — persistent server startup, health/readiness, write/query, restart and teardown.
- REST contract: [`api/openapi`](../api/openapi/)
- First-party SDKs: Rust, Python, TypeScript, Java, and Go.

## Concepts

- [Retrieval](concepts/retrieval.md) — dense, sparse, hybrid retrieval, filtering, reranking, and explainability.
- [Storage and consistency](concepts/storage-and-consistency.md) — durability, recovery, visibility, and derived indexes.
- [Control-plane persistence](concepts/control-plane-persistence.md) — durable Organization/Project ownership, atomic publication, and fail-closed recovery.
- [Organization and Project lifecycle](concepts/control-plane-lifecycle.md) — REST/gRPC lifecycle, safe deletion, membership management, and fail-closed discovery.
- [Organization membership and roles](concepts/organization-membership.md) — explicit account authority, role separation, durability, and last-owner safeguards.
- [Project membership and roles](concepts/project-membership.md) — durable multi-project human access, Reader/Editor/Owner semantics, and Collection override composition.
- [Service Accounts](concepts/service-accounts.md) — explicit workload identity, Project-role assignments, lifecycle, and API-key attachment.

## Guides

- [Ingestion](guides/ingestion.md)
- [Hybrid search](guides/hybrid-search.md)
- [Embeddings](guides/embeddings.md)

## Agents and MCP

- [MCP quickstart](mcp/quickstart.md)
- [MCP operations](mcp/operations.md)

Ketebe MCP is a first-party adapter over Ketebe's public API. It does not bypass storage, authorization, or correctness boundaries.

## Operate

- [Server container](operations/container.md) — image runtime, ports, health, non-root identity and persistent data path.
- [Security](operations/security.md)
- [Backup and recovery](operations/backup-recovery.md)
- [Benchmark methodology](benchmarks.md)
- [Server release workflow](operations/server-release.md) — tag/manual packaging, artifact naming, validation and publication behavior.
- [Upgrade and downgrade notes](operations/upgrade.md) — migration checks, rollback expectations and the minimal release-specific upgrade-note structure.

## Reference

- OpenAPI: [`api/openapi/v1.json`](../api/openapi/v1.json)
- API compatibility data: [`api/openapi/v1.compatibility.json`](../api/openapi/v1.compatibility.json)
- [Release versioning and compatibility](reference/release-versioning.md) — product tags, RC convention, artifact identity and independent API/storage/license/MCP version domains.
- [v0.9 Organization and Project public contract](reference/v0.9-domain-contract.md) — stabilized domain semantics, compatibility boundary, REST/SDK/MCP usage and migration behavior.
- Roadmap: [`ROADMAP.md`](../ROADMAP.md)

## Contribute and report security issues

- [Contributing](../CONTRIBUTING.md)
- [CI quality gates](ci-quality-gates.md)
- [Security reporting](../SECURITY.md)
- [Code of conduct](../CODE_OF_CONDUCT.md)

- [Default control-plane migration](operations/default-control-plane-migration.md) — deterministic default Organization/Project bootstrap and legacy-state validation.

- `concepts/organization-governance.md` — provider-neutral Organization policy resolution into Project-scoped quota, rate-limit, concurrency and throughput enforcement.

- `concepts/developer-project-context.md` — explicit SDK/MCP Project selection for human multi-project workflows and workload credential invariants.

- `security/saas-domain-verification.md` — ADR 0004 migration, cross-Organization isolation, authorization freshness and restart verification matrix.
