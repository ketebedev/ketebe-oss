# Default Organization and Project bootstrap migration

Ketebe bootstraps a first-class default Organization and default Project for simple self-hosted installations and for compatibility with pre-control-plane local state.

This migration implements ADR 0004 without changing existing `DataPlaneScope(default, collection)` identities.

## Bootstrap identities

The deterministic resources are:

```text
Organization ID: default
Organization slug: default

Project ID: default
Project slug: default
Project owner: Organization(default)
```

The IDs are stable compatibility identities. They are not generated per installation.

## Startup behavior

Standalone startup performs control-plane bootstrap before normal runtime recovery.

The sequence is:

1. open the durable control-plane store,
2. read existing authorization, API-key and collection catalog project references,
3. validate every non-default legacy project against an existing first-class Project,
4. fail without creating default resources when an unmapped or invalid legacy project is found,
5. atomically create Organization(default) and Project(default) together when neither exists,
6. accept an already-complete default pair without rewriting it,
7. fail closed when only one default resource exists or the Project does not belong to Organization(default),
8. continue with normal runtime recovery.

## Legacy state mapping

Existing state already stores project identity, so the migration does not rewrite project-scoped files merely to introduce Organization ownership.

Existing references such as:

```text
authorization -> project_id = default
API key       -> project_id = default
catalog       -> project_id = default
data plane    -> DataPlaneScope(default, collection)
```

remain unchanged. The migration creates the authoritative durable ownership relation:

```text
Project(default) -> Organization(default)
```

This avoids data movement and preserves collection, WAL, segment and index storage identity.

## Non-default legacy projects

A non-default project reference is accepted only when the durable control-plane store already contains a first-class Project with that exact stable ID.

Ketebe does not guess an Organization for legacy non-default projects. If a referenced Project has no durable control-plane record, startup fails explicitly with an unmapped legacy project error.

Operators must create or migrate the correct Organization and Project ownership before restarting. Arbitrary reassignment to the default Organization is intentionally prohibited.

## Idempotency

Repeated startup is safe:

- a complete default Organization + Project pair is reused,
- no duplicate default resources are created,
- existing legacy security and catalog files are not rewritten,
- existing data-plane scope remains unchanged.

## Failure handling

The migration fails closed for:

- corrupt or unsupported control-plane state,
- invalid project identifiers in legacy state,
- legacy non-default projects without a first-class Project,
- incomplete default bootstrap state,
- a default Project whose owner is not Organization(default).

Validation of legacy project references occurs before first-time default resource creation, so an invalid legacy mapping cannot leave a partially applied bootstrap behind.
