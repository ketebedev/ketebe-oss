# Security Policy

Ketebe is infrastructure software that may store sensitive application data and credentials. Security reports should be handled privately until a fix and disclosure plan are ready.

## Reporting a vulnerability

Do **not** open a normal public GitHub issue for suspected vulnerabilities.

After the public repository launches, use GitHub's private **Report a vulnerability / Security Advisory** flow on `github.com/ketebedev/ketebe` when available.

Before that public reporting channel is enabled, contact the Ketebe maintainers through an existing private project communication channel. Do not include secrets, production data or unnecessary customer information in the initial report.

A useful report includes:

- affected Ketebe version or commit,
- affected component and deployment mode,
- reproducible steps or a minimal proof of concept,
- expected and observed security impact,
- relevant logs with credentials and sensitive data removed,
- whether the issue appears exploitable across tenant/project boundaries.

## Scope

Security-sensitive areas include authentication/API-key handling, authorization/RBAC, cross-tenant isolation, TLS/mTLS, secret/provider credentials, audit-data leakage, unsafe file/network access, MCP authorization/data-exfiltration boundaries, resource-exhaustion vulnerabilities and persistence/recovery behavior that could expose another tenant's data.

Ordinary bugs, performance regressions and feature requests should use the normal issue flow unless they have a credible security impact.

## Supported versions

Ketebe is currently preparing its first v0.9 public release. There is not yet a long-term support matrix. Security fixes target the latest supported release line and, when necessary, the current development head.

A formal supported-version table will be introduced when multiple maintained release lines actually exist; it should not be invented in advance.

## Disclosure

Please allow maintainers reasonable time to validate, fix, test and publish a security release before public disclosure. Ketebe will credit reporters when appropriate and when the reporter wants attribution.

## Security design documentation

Public security and operational guidance is documented in [`docs/operations/security.md`](docs/operations/security.md) and the MCP operational documentation under [`docs/mcp/`](docs/mcp/). Internal security design records are not part of the public repository.
