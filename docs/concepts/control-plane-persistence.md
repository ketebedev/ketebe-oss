# Control-plane persistence

Ketebe persists first-class Organization and Project resources independently from data-plane collection, WAL, segment and index state.

This storage boundary implements the Organization -> Project ownership rules defined by ADR 0004.

## Durable ownership model

The authoritative hierarchy is:

```text
Organization
  -> Project
      -> Collection
```

A Project record contains its immutable parent `OrganizationId`. Project ownership is resolved from this durable control-plane state; callers cannot establish or override ownership through request claims.

The control-plane store deliberately does not add Organization to `DataPlaneScope`. Data-plane storage continues to use stable Project + Collection identity.

## Persistence format

The local v0 control-plane store publishes one versioned snapshot named:

```text
control-plane.ktcp
```

The file contains:

- format magic and version,
- payload length,
- payload checksum,
- Organization records,
- Project records and their parent Organization IDs,
- lifecycle state,
- created/updated timestamps.

Unsupported versions, malformed payloads, checksum failures, duplicate durable IDs and orphan Project records fail closed when the store is opened.

## Atomicity and recovery

Mutations use copy-on-write state publication:

1. validate the complete proposed Organization/Project state,
2. encode a complete snapshot,
3. write a temporary file,
4. flush and sync the temporary file,
5. atomically rename it over the committed snapshot,
6. sync the containing directory,
7. publish the new in-memory state only after durable publication succeeds.

This ordering prevents a failed write from making an uncommitted in-memory mutation authoritative and prevents partial Organization/Project writes from creating orphan Projects.

A stale temporary file is never treated as committed state.

## Identity and uniqueness

Stable IDs are durable identity and are independent from names and slugs.

The v0 uniqueness rules are:

- Organization slug is unique across the control-plane store.
- Project slug is unique within its owning Organization.
- Display names are labels and are not unique durable routing keys.
- A Project cannot change Organization through an ordinary update.

Moving a Project between Organizations, if supported in the future, requires an explicit migration operation because it changes authorization, billing and data ownership boundaries.

## Lifecycle

Organization and Project lifecycle state is persisted as part of the same atomic snapshot. Restart therefore preserves both ownership and lifecycle state.

## Scope

This store is the durable domain foundation only. Membership persistence, API-key rebinding, authorization resolution, lifecycle REST/gRPC APIs, billing and bootstrap migration are handled by later Organization/Project control-plane issues.
