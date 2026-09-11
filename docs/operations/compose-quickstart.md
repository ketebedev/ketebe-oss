# Standalone Docker Compose Quickstart

This is the supported v0.9 standalone self-hosted baseline. It runs only the Ketebe server and persists all state in the named `ketebe-data` volume.

## Start

With Docker Compose v2 installed:

```bash
docker compose up -d
```

The compose file uses:

```text
ghcr.io/ketebedev/ketebe-server:v0.9.0
```

You can override the exact published image without changing the compose file:

```bash
KETEBE_IMAGE=ghcr.io/ketebedev/ketebe-server:v0.9.1 docker compose up -d
```

REST listens on `http://127.0.0.1:7610`; gRPC listens on `127.0.0.1:7611`.

## Verify health and readiness

```bash
curl --fail http://127.0.0.1:7610/healthz
curl --fail http://127.0.0.1:7610/readyz
```

A clean data volume bootstraps the real `default` Organization and `default` Project defined by ADR 0004. The quickstart does not bypass the Organization -> Project -> Collection model.

## Create, write and query in the default Project

Create a three-dimensional Collection:

```bash
curl --fail --silent --show-error \
  -X POST http://127.0.0.1:7610/v0/collections \
  -H 'content-type: application/json' \
  -H 'X-Ketebe-Project: default' \
  --data '{"id":"quickstart-docs","dimension":3,"metric":"cosine"}'
```

Write one Record:

```bash
curl --fail --silent --show-error \
  -X PUT http://127.0.0.1:7610/v0/collections/quickstart-docs/records/doc-1 \
  -H 'content-type: application/json' \
  -H 'X-Ketebe-Project: default' \
  --data '{"vector":[1.0,0.0,0.0],"metadata":{"title":"Ketebe quickstart"}}'
```

Query it:

```bash
curl --fail --silent --show-error \
  -X POST http://127.0.0.1:7610/v1/collections/quickstart-docs/query \
  -H 'content-type: application/json' \
  -H 'X-Ketebe-Project: default' \
  --data '{"vector":[1.0,0.0,0.0],"top_k":1}'
```

The response should contain `doc-1`.

## Verify restart persistence

Recreate the container while preserving the named volume:

```bash
docker compose down
docker compose up -d
```

After readiness returns, run the same query again. The `default` Organization/Project and `quickstart-docs` data remain available because `docker compose down` does not remove the named volume.

## Stop

Keep data:

```bash
docker compose down
```

Remove the standalone data volume as well:

```bash
docker compose down -v
```

The Compose quickstart is intentionally not a multi-node or production-HA orchestrator. For runtime details, see `docs/operations/container.md`; for the v0.9 domain contract, see `docs/reference/v0.9-domain-contract.md`.
