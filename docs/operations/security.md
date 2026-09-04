# Security

Ketebe's security model is built around explicit identity, project scope, authorization, transport security, and auditable administrative boundaries.

## Authentication and API keys

Applications authenticate through Ketebe's public API boundary. Project API keys and other credentials should be scoped, rotated, and revoked as operational credentials rather than embedded permanently in application source.

## Authorization

Authentication and authorization are separate concerns. RBAC decisions are evaluated within organization and project boundaries. Integrations such as MCP must not create an alternate path around Ketebe authorization.

## Tenant and project isolation

Requests must remain scoped to the organization/project context authorized for the caller. Cross-tenant access should be treated as a security invariant and included in automated verification.

## TLS and mTLS

Use TLS for network deployments. mTLS may be used where the deployment requires mutually authenticated transport. Termination can be native or provided by a trusted reverse proxy or load balancer, provided the resulting trust boundary is explicit.

## Provider secrets

Embedding and reranking provider credentials should come from deployment secret mechanisms. Avoid persisting raw provider secrets in user data, logs, repository configuration, or metadata.

## Audit events

Security-relevant actions should be observable through Ketebe's audit boundary. Production operators should retain audit events according to their security and compliance requirements.

## MCP

Ketebe MCP is read-only by default for mutation/admin classes and relies on Ketebe's normal authorization boundary. See [MCP operations](../mcp/operations.md).

## Vulnerabilities

Do not report suspected vulnerabilities through public issues. Follow the repository's [security reporting policy](../../SECURITY.md).