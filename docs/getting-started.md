# Getting started

Ketebe is an open-source retrieval and vector data platform for hybrid search, ingestion, embedding lifecycle management, and AI-agent retrieval.

## Current release status

Ketebe is preparing its first public v0.9 release. The core product surface is implemented and tested, while packaged release artifacts and the final container-based onboarding flow are still being finalized.

Do not treat unreleased container, Helm, or binary commands as stable installation contracts yet.

## Build from source

The current source build uses Rust 1.98.

```bash
cargo build --locked --workspace
cargo test --workspace --all-features
```

Run the server from the repository checkout:

```bash
cargo run -p ketebe-server
```

## Choose an API

Ketebe exposes REST and gRPC APIs and ships first-party SDKs for:

- Rust
- Python
- TypeScript
- Java
- Go

The machine-readable REST contract is maintained under [`api/openapi`](../api/openapi/).

## What to learn next

- [Retrieval concepts](concepts/retrieval.md)
- [Storage and consistency](concepts/storage-and-consistency.md)
- [Ingestion](guides/ingestion.md)
- [Hybrid search](guides/hybrid-search.md)
- [Embeddings](guides/embeddings.md)
- [Security and operations](operations/security.md)
- [MCP quickstart](mcp/quickstart.md)

## Target v0.9 onboarding

The intended v0.9 first-run experience is a persistent standalone container deployment, followed by an end-to-end create, ingest, and query workflow. Exact release commands will be documented only after the corresponding artifacts are published and validated.