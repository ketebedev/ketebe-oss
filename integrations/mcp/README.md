# Ketebe MCP

First-party MCP adapter for Ketebe. It depends on `ketebe-sdk`, never storage/query/server internals.

## Configuration file

Set `KETEBE_MCP_CONFIG` to a TOML file. Plain HTTP is the default:

```toml
[ketebe]
url = "http://127.0.0.1:17610"

[transport]
type = "streamable_http"
protocol = "http" # optional; defaults to http
bind = "127.0.0.1:8000"
path = "/mcp"
request_timeout_ms = 30000
max_request_bytes = 1048576
```

No certificate configuration is required in HTTP mode. TLS may still terminate at a reverse proxy or load balancer.

For native TLS termination inside `ketebe-mcp`:

```toml
[ketebe]
url = "http://127.0.0.1:17610"

[transport]
type = "streamable_http"
protocol = "https"
bind = "0.0.0.0:8443"
path = "/mcp"

[tls]
certificate = "/etc/ketebe/tls/server.crt"
private_key = "/etc/ketebe/tls/server.key"
```

HTTPS fails at startup when the certificate or private key is missing, unreadable or malformed. HTTP and HTTPS use the same Streamable HTTP handler, request-size/timeout policy and graceful-shutdown path.

Local stdio remains supported through the legacy environment configuration:

```bash
KETEBE_MCP_KETEBE_URL=http://127.0.0.1:17610 KETEBE_MCP_TRANSPORT=stdio cargo run -p ketebe-mcp
```

## Authentication and project scoping

MCP authentication is an adapter boundary, not a separate Ketebe identity system. Set `KETEBE_MCP_AUTH_MODE=required` outside explicit development environments.

For `stdio`, required mode also requires `KETEBE_MCP_KETEBE_TOKEN`. The token is forwarded as a Bearer credential to the normal Ketebe public API and is never emitted through Debug/log output.

For Streamable HTTP, required mode accepts `Authorization: Bearer <credential>` on every MCP request. The adapter validates that credential against the configured Ketebe public API before the MCP request is admitted. The same credential is retained in request extensions for later tools to forward to Ketebe. Ketebe remains the authority for principal establishment, project scoping, collection filtering and RBAC; the MCP adapter does not duplicate those policies.

Missing or invalid credentials return HTTP 401 with `WWW-Authenticate: Bearer`. A Ketebe authorization denial is preserved as HTTP 403. Resource-level tools forward the request credential and preserve Ketebe's non-disclosure semantics rather than performing local authorization.

## Mutation policy

MCP remains read-only by default. `upsert_records`, `ingest_documents`, `start_reembedding`, and `cancel_job` are available only when the adapter is enabled and write tools are explicitly allowed:

```bash
KETEBE_MCP_ENABLED=true
KETEBE_MCP_ALLOW_WRITE=true
```

`KETEBE_MCP_ALLOW_TOOLS` and `KETEBE_MCP_DENY_TOOLS` can further restrict individual tools. Enabling MCP writes never bypasses Ketebe authorization: the authenticated credential is forwarded to the normal public write endpoint and Ketebe remains the RBAC authority.

`upsert_records` delegates to the idempotent public batch-upsert contract. `ingest_documents` requires stable parent document IDs and delegates to the idempotent document `PUT` contract; chunking and embedding run inside Ketebe so provider credentials are not exposed to the MCP process or client. Repeating either operation with the same identifiers replaces the corresponding logical records/documents rather than creating a second identity.

`start_reembedding` delegates to Ketebe's public embedding-migration contract and is separately gated as a write tool. Migration execution and catch-up remain server-side; MCP never receives provider credentials and does not perform embedding work locally.

Destructive delete/admin tools are intentionally not part of the v0 mutation surface.

## Embedding and reranker profiles

`list_embedding_profiles` and `describe_embedding_profile` expose only safe profile identity and capability metadata: profile name, provider type, model name/version, optional fixed dimension, and default status. Provider endpoints, secret references, API keys, and credentials are not part of the public response schema.

`list_reranker_profiles` and `describe_reranker_profile` similarly expose only profile name, provider type, and default status. Profile discovery is authorized through the Ketebe public API and requires project read permission; MCP does not inspect runtime registries directly.

`get_reembedding_status` exposes the current migration state and progress through the public API. `start_reembedding` requires explicit MCP write enablement plus normal Ketebe collection-write authorization.

## Asynchronous jobs

Long-running Ketebe work stays inside the server job runtime. MCP does not wait for re-embedding catch-up, backup, restore, or other background work to finish and never accesses the job store directly.

`list_jobs` returns only jobs belonging to the authenticated project, `get_job` returns one authorized lifecycle snapshot, and `cancel_job` forwards a cancellation request through the public HTTP API. Job state uses Ketebe's stable `queued`, `running`, `completed`, `failed`, and `cancelled` values together with progress, result, `cancel_requested`, and safe structured error fields.

Embedding migration uses the same server-side job runtime for asynchronous catch-up. Agents start or inspect migration through the embedding lifecycle tools, then use `list_jobs`/`get_job` for background job progress and `cancel_job` when cancellation is permitted. MCP never holds a request open for the long-running operation.

Job inspection is read-only. `cancel_job` is a write tool, is disabled by default, and requires both MCP write-policy enablement and normal Ketebe authorization. Cross-project job IDs are treated as not found to preserve resource non-disclosure.
