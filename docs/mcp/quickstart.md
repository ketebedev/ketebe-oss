# Ketebe MCP Quickstart

Ketebe MCP is the first-party Model Context Protocol adapter for Ketebe. It consumes Ketebe's public API and does not access storage, shard, WAL, index, catalog, or commercial internals directly.

## Build the binary

From a Ketebe source checkout:

```bash
cargo install --path integrations/mcp --locked
```

The installed executable is `ketebe-mcp`.

Packaged release artifacts are part of the v0.9 release-readiness work. Use source builds until release binaries and container images are published and validated.

## Required startup policy

MCP is disabled by default. Every deployment must explicitly enable it:

```bash
export KETEBE_MCP_ENABLED=true
export KETEBE_MCP_KETEBE_URL=http://127.0.0.1:7610
```

Write and admin tools remain disabled unless separately enabled.

## stdio quickstart

For a local agent process:

```bash
export KETEBE_MCP_ENABLED=true
export KETEBE_MCP_KETEBE_URL=http://127.0.0.1:7610
export KETEBE_MCP_TRANSPORT=stdio
export KETEBE_MCP_AUTH_MODE=development
ketebe-mcp
```

For required auth over stdio, set `KETEBE_MCP_AUTH_MODE=required` and provide `KETEBE_MCP_KETEBE_TOKEN`.

### Claude Desktop example

```json
{
  "mcpServers": {
    "ketebe": {
      "command": "/usr/local/bin/ketebe-mcp",
      "env": {
        "KETEBE_MCP_ENABLED": "true",
        "KETEBE_MCP_KETEBE_URL": "http://127.0.0.1:7610",
        "KETEBE_MCP_TRANSPORT": "stdio",
        "KETEBE_MCP_AUTH_MODE": "development"
      }
    }
  }
}
```

The same stdio command model is suitable for MCP clients such as Cursor that can launch a local MCP server process.

## Remote Streamable HTTP quickstart

```bash
export KETEBE_MCP_ENABLED=true
export KETEBE_MCP_KETEBE_URL=http://ketebe-api.internal:7610
export KETEBE_MCP_TRANSPORT=streamable-http
export KETEBE_MCP_PROTOCOL=http
export KETEBE_MCP_BIND_ADDR=0.0.0.0:8000
export KETEBE_MCP_PATH=/mcp
export KETEBE_MCP_AUTH_MODE=required
ketebe-mcp
```

Remote clients connect to `http://HOST:8000/mcp` and send their bearer credential. Ketebe MCP forwards authorization through Ketebe's normal API boundary; it does not implement a second RBAC system.

For native HTTPS, use `KETEBE_MCP_PROTOCOL=https` together with `KETEBE_MCP_TLS_CERTIFICATE` and `KETEBE_MCP_TLS_PRIVATE_KEY`. External reverse-proxy or load-balancer TLS termination remains supported.

## Configuration reference

| Setting | Default | Purpose |
| --- | --- | --- |
| `KETEBE_MCP_ENABLED` | `false` | Master MCP enable gate. |
| `KETEBE_MCP_KETEBE_URL` | required | Ketebe public HTTP API base URL. |
| `KETEBE_MCP_TRANSPORT` | required | `stdio`, `streamable-http`, or `streamable_http`. |
| `KETEBE_MCP_PROTOCOL` | `http` | Remote protocol: `http` or `https`. |
| `KETEBE_MCP_BIND_ADDR` | `127.0.0.1:8000` | Streamable HTTP listen address. |
| `KETEBE_MCP_PATH` | `/mcp` | Streamable HTTP MCP path. |
| `KETEBE_MCP_PROBE_INTERVAL_MS` | `5000` | Ketebe readiness probe interval. |
| `KETEBE_MCP_REQUEST_TIMEOUT_MS` | `30000` | Remote request timeout. |
| `KETEBE_MCP_MAX_REQUEST_BYTES` | `1048576` | Maximum remote request size. |
| `KETEBE_MCP_AUTH_MODE` | `development` | `development` or `required`. |
| `KETEBE_MCP_KETEBE_TOKEN` | unset | Static bearer token required for stdio when auth mode is `required`. |
| `KETEBE_MCP_TLS_CERTIFICATE` | unset | PEM certificate chain for native HTTPS. |
| `KETEBE_MCP_TLS_PRIVATE_KEY` | unset | PEM private key for native HTTPS. |
| `KETEBE_MCP_ALLOW_WRITE` | `false` | Enables write-class tools; normal Ketebe authorization still applies. |
| `KETEBE_MCP_ALLOW_ADMIN` | `false` | Enables admin-class tools; normal Ketebe authorization still applies. |
| `KETEBE_MCP_TOOL_ALLOW` | unset | Optional comma-separated tool allowlist. |
| `KETEBE_MCP_TOOL_DENY` | unset | Optional comma-separated tool denylist; deny wins. |
| `KETEBE_MCP_CONFIG` | unset | TOML configuration file path for Ketebe/transport/TLS settings. |

A TOML file may configure `[ketebe]`, `[transport]`, and `[tls]`; policy and credential gates remain environment-controlled so secrets do not need to be stored in the file.

## Production checks

Use `/healthz` for process liveness, `/readyz` for Ketebe dependency readiness, and `/metrics` for MCP Prometheus metrics. Keep write/admin tools disabled unless the deployment explicitly requires them.

## Releases and upgrades

Ketebe MCP follows the repository's versioned `v*` tag releases. Release packaging is not treated as generally available until the corresponding v0.9 assets are published and validated.

Before upgrading:

1. Read the release notes and the public MCP operational documentation.
2. Pin the target version in deployment manifests.
3. Run the compatibility workflow or equivalent staging smoke test against the target Ketebe API.
4. Verify auth mode, tool policy, TLS, request limits, and client configuration.
5. Roll back by restoring the previous validated binary/image tag; MCP does not own Ketebe storage state.

Do not assume new write/admin capabilities are enabled by an upgrade: mutation and admin gates remain explicit and default-deny.