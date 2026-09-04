# MCP operations

Ketebe MCP is a first-party adapter over Ketebe's public API. It must not bypass Ketebe storage, authorization, or correctness boundaries.

## Default policy

MCP is disabled unless explicitly enabled. Write-class and admin-class tools remain disabled unless the deployment explicitly enables them.

This default-deny posture is intentional: an agent-facing integration should receive only the capabilities required for its task.

## Transports

Ketebe MCP supports local stdio and remote Streamable HTTP deployments. Remote deployments should use authenticated transport and an explicit TLS boundary.

## Authorization

MCP forwards requests through Ketebe's normal API and authorization model rather than implementing an independent RBAC system. Project and tenant scope must therefore remain intact end to end.

## Tool policy

Use allow/deny policy to keep the exposed tool surface small. Deny rules should take precedence when allow and deny configuration overlap.

## Production checks

Before exposing MCP to agents:

- verify Ketebe dependency readiness,
- require production authentication,
- keep write/admin tools disabled unless necessary,
- configure request-size and timeout limits,
- configure TLS for remote access,
- validate project/RBAC isolation,
- test representative client compatibility,
- monitor health, readiness, and metrics,
- exercise rollback with the previously validated release.

## Context quality

Agent retrieval quality depends on more than protocol connectivity. Validate collection selection, hybrid retrieval, reranking, provenance, freshness, and context-budget behavior using representative agent tasks.

See the [MCP quickstart](quickstart.md) for configuration examples.