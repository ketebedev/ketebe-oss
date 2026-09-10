# Service Accounts

Service Accounts are Ketebe workload identities. They are distinct from human Principals and from API credentials.

The canonical relationship is:

```text
Service Account
    |
    +-- explicit Project assignment -> Reader / Editor / Owner
    |
    +-- API Key credential attachment
```

A Service Account never stores raw credential material. API keys continue to own secret generation, one-way verifier persistence, rotation, expiration and revocation. Service Account metadata stores only stable identity, lifecycle and Project-role assignments.

## Identity and lifecycle

A Service Account has:

- a stable `ServiceAccountId`,
- a display name,
- `Active` or `Disabled` lifecycle,
- creation/update timestamps,
- zero or more explicit Project assignments.

Deleting a Service Account removes the durable identity. Disabling or deleting the identity causes credentials attached to it to fail subsequent authentication. Existing API keys remain independently revocable so operators can invalidate credential state even after the identity stops admitting traffic.

## Project assignments

Assignments are explicit and Project-scoped:

```text
ServiceAccount -> ProjectId -> ProjectRole
```

The initial role model reuses Ketebe's Project roles:

- `Reader`: collection discovery and read,
- `Editor`: collection discovery/read/create/write/delete,
- `Owner`: all Project actions including Project administration.

A Service Account with an assignment in Project A has no authority in Project B unless Project B is assigned separately. Organization-wide workload authority is not implicit and is not introduced by this model.

Assignments validate the authoritative first-class Project resource and require an active Project.

## Credential attachment

An API key may reference a Service Account while remaining scoped to exactly one Project.

At issuance, Ketebe validates all three facts:

1. the Project exists and is active,
2. the Service Account exists and is active,
3. the Service Account has an explicit assignment to that Project.

The API-key store remains the only place that persists verifier material. The Service Account store never contains raw secrets, verifiers or API-key secret material.

At authentication time Ketebe resolves the current Service Account lifecycle and assignment again. The authenticated workload Principal uses the Service Account identity as its subject and carries the assigned Project role into the central authorization boundary.

## Audit

Service Account create, assignment, unassignment, lifecycle and deletion events are auditable. Project assignment events include Project and owning Organization attribution when resolvable.

Audit records use the stable Service Account identity and never contain raw API-key secrets.

## Security invariants

- Service Account is an identity; API Key is a credential.
- Project assignments are explicit.
- Organization-wide workload authority is not the default.
- Disabled/deleted Service Accounts cannot admit attached credentials.
- Cross-Project use without assignment fails closed.
- Raw secrets never live in Service Account metadata.
