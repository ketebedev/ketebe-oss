# Ketebe

**Ketebe is an open-source retrieval and vector data platform for real-time ingestion, hybrid search, embedding lifecycle management, and AI-agent retrieval.**

Website: `ketebe.dev`

> Status: v0.9 release preparation. The core product surface is implemented and tested, but the first public release is still in progress. Ketebe is not yet a v1.0 GA product.

## What Ketebe provides

- dense vector search with exact and HNSW-backed retrieval,
- sparse/lexical and hybrid retrieval with fusion and reranking,
- metadata filtering and query explainability,
- durable WAL/segment storage and crash recovery,
- document ingestion and server-side embedding pipelines,
- embedding and re-embedding lifecycle management,
- Kafka-native continuous ingestion,
- organizations, projects, authentication, RBAC, quotas and audit boundaries,
- REST and gRPC APIs,
- first-party Rust, Python, TypeScript, Java and Go SDKs,
- first-party MCP integration for agent discovery, retrieval, context assembly and controlled ingestion.

## Get started

The supported v0.9 distribution path is being finalized. The target first-run experience is:

```bash
docker compose up -d
```

Until packaged release artifacts are published, developers can build with Rust 1.98:

```bash
cargo build --locked --workspace
cargo test --workspace --all-features
```

Run the server from source with:

```bash
cargo run -p ketebe-server
```

## Documentation

Start with [docs/README.md](docs/README.md).

- **Get started** — installation, quickstarts and SDK entry points.
- **Use Ketebe** — collections, records, documents, search and ingestion.
- **Deploy & operate** — configuration, security, TLS, backup/restore and operational guidance.
- **Concepts & architecture** — storage, consistency and retrieval contracts.
- **Integrations** — MCP, Kafka and embedding/reranking providers.
- **Reference** — OpenAPI, compatibility contracts and versioning policies.
- **Contribute** — development rules and security reporting.

## Public APIs and SDKs

Ketebe's public REST contract is checked in under [`api/openapi`](api/openapi). First-party SDKs live under:

```text
sdk/rust
sdk/python
sdk/typescript
sdk/java
sdk/go
```

## MCP and AI agents

`ketebe-mcp` is a first-party adapter over the stable Ketebe product API. It supports local stdio and remote Streamable HTTP deployment, project/RBAC scoping, read-only-by-default tool policy, dense/sparse/hybrid retrieval, multi-collection retrieval, reranking, explainability, provenance and context budgeting.

See [MCP documentation](docs/mcp/).

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md).

## Security

Do not disclose security vulnerabilities through normal public issues. See [SECURITY.md](SECURITY.md).

## Roadmap

See [ROADMAP.md](ROADMAP.md).

## License

Ketebe OSS is licensed under the [Apache License 2.0](LICENSE).
