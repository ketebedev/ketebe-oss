# Getting started

Ketebe is an open-source retrieval and vector data platform for hybrid search, ingestion, embedding lifecycle management, and AI-agent retrieval.

## Standalone Docker Compose

The supported v0.9 standalone onboarding path is Docker Compose:

```bash
docker compose up -d
```

This starts the published Ketebe server image with a persistent named volume and the deterministic default Organization/Project bootstrap.

See [Standalone Docker Compose quickstart](operations/compose-quickstart.md) for health/readiness checks, an end-to-end write/query example, restart persistence and teardown.

## Build the server container

The main server image can be built directly from the repository:

```bash
docker build -t ketebe-server:local .
```

Run it with persistent storage:

```bash
docker volume create ketebe-data

docker run --rm \
  --name ketebe-server \
  -p 7610:7610 \
  -p 7611:7611 \
  -v ketebe-data:/var/lib/ketebe \
  ketebe-server:local
```

See [Server container](operations/container.md) for the runtime, health and persistence contract.

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
- [Server container](operations/container.md)
- [Security and operations](operations/security.md)
- [MCP quickstart](mcp/quickstart.md)

## v0.9 onboarding

Docker Compose is the standalone first-run path. Published release artifact smoke/restart validation remains separately gated before a public release candidate is declared.
