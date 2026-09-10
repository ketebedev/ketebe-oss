# Security

Ketebe's security model is built around explicit identity, project scope, authorization, transport security, and auditable administrative boundaries.

## Authentication and API keys

Applications authenticate through Ketebe's public API boundary. Authentication establishes an opaque Principal identity; it does not assign a human caller to one Project.

Human or interactive Principals therefore carry subject/kind identity only and may later be authorized for zero, one, or many Projects through explicit membership state.

Project-scoped workload credentials are different: the credential itself may carry an explicit workload Project binding. Ketebe represents that binding separately from Principal identity so an API key can remain least-privilege without restoring the obsolete one-human-one-Project model.

Project API keys and other credentials should be scoped, rotated, and revoked as operational credentials rather than embedded permanently in application source.

API-key Project bindings are authoritative control-plane references, not caller-provided namespace strings. Key issuance validates that the referenced first-class Project exists and is `Active`. Authentication and rotation re-resolve that Project on every admission path, so a Project that becomes `Suspended` or `Deleting` immediately stops admitting the key. A deleted Project is represented by the absence of its durable Project record and therefore fails closed in the same way. Revocation remains available while a Project is unavailable so operators can always invalidate credentials.

Existing version-1 API-key metadata is migrated without rewriting key IDs, verifier material, timestamps, or Project IDs. Bootstrap validates every persisted Project reference against the authoritative control-plane store after the deterministic default Organization/Project resources are established. An unmapped Project reference fails bootstrap rather than silently creating or rebinding a Project.

API-key lifecycle audit events contain the resolved Project and owning Organization identities when that ownership is available. Raw API-key secrets remain one-way verifier material only and are never added to audit records.

## Authorization

Authentication and authorization are separate concerns. A human Principal's authenticated identity is never sufficient evidence of Project authority.

Required-mode authorization resolves authority through one trusted control-plane chain:

```text
Principal
  + requested Project/resource
        |
        v
authoritative Project lookup
        |
        v
Project -> Organization ownership
        |
        +-- human identity -> explicit ProjectMembership role
        |
        +-- workload credential -> exact bound Project only
        |
        v
Collection permission override, when present
```

Unknown Projects fail closed. An unrecognized human subject is never treated as Project Owner. Existing legacy project-role JSON may remain as migration state, but it is no longer authoritative for human Project access.

Organization account and billing actions are resolved separately through explicit OrganizationMembership roles. Organization membership never grants Project or Collection data-plane access by itself.

Collection permissions are evaluated only after Project authority. A Collection override cannot manufacture Project-level administration authority.

REST and gRPC adapters propagate the same Principal object and use the shared AuthorizationService boundary. Integrations such as MCP must not create an alternate path around Ketebe authorization.

Project-scoped workload credentials may continue to supply their bound Project for existing data-plane calls that do not carry a separate Project selector. Human identity is not mutated to restore an authentication-time Project field; explicit Project-selection surfaces are introduced by the control-plane API work rather than by weakening Principal semantics.

Service Accounts provide an explicit workload identity above raw credentials. A Service Account may be assigned Reader, Editor, or Owner independently per Project. An API key attached to a Service Account authenticates as the stable Service Account subject and carries the assigned Project role into the same central authorization boundary. Disabling or deleting the Service Account, removing its Project assignment, or making the Project unavailable causes subsequent attached-credential admission to fail closed. The API-key store remains the only secret/verifier store; Service Account metadata contains no credential material.

## Organization and Project isolation

`Tenant` is an internal isolation term, not a public Ketebe business-domain resource.

Requests remain scoped to the Organization/Project context authorized for the caller. Cross-Organization and cross-Project access are fail-closed security invariants and are covered by the SaaS domain verification suite.

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

## Control-plane audit and discovery isolation

Organization, Project and membership control-plane actions share one audit context across REST and gRPC.

- REST propagates `x-request-id` into audit `correlation_id`.
- gRPC propagates `x-request-id` metadata into the same field.
- actor, Organization, Project and resource dimensions are bounded before persistence to prevent unbounded cardinality.
- correlation IDs are bounded independently and are never treated as authorization input.
- denied control-plane discovery and membership authorization decisions are audited without changing the public non-enumeration response.
- cross-Organization access to an existing resource and lookup of a missing resource remain externally indistinguishable where the API contract uses not-found semantics.

Audit records contain identifiers and authorization outcomes only. Request payloads, raw API keys, credential secrets and provider secrets are not audit dimensions.

Service Account audit attribution uses the stable `service-account:<id>` actor and Service Account resource identity. Project assignment events include Project and owning Organization when resolvable. API-key lifecycle audit keeps the key identifier, authoritative Project and owning Organization, and uses Service Account actor attribution for attached workload credentials without recording the raw secret.
