# Project Membership and Roles

Ketebe models Project access as an explicit durable relationship between an authenticated Principal subject and a first-class Project.

This follows ADR 0004:

```text
Principal
  + requested Project
        |
        v
ProjectMembership
        |
        v
Project role
        |
        v
Collection override
```

Project access is not inferred from authentication-time Principal scope.

## Roles

The v0 Project role vocabulary retains the established Ketebe roles:

- **Reader** — Collection discovery and read.
- **Editor** — Reader permissions plus Collection create and write.
- **Owner** — full Project and Collection administration.

A subject may hold different roles in different Projects at the same time.

For example:

```text
subject: alice

project-search  -> Reader
project-support -> Editor
project-platform -> Owner
```

The Principal remains one authenticated identity. Project authority is resolved from membership.

## Durable membership

Project memberships are persisted in:

`security/project-memberships.json`

Each membership contains:

- Project ID
- Principal subject
- Project role

The referenced Project must already exist in the authoritative control-plane store.

Mutations are persisted before new in-memory membership state is published.

## Bootstrap and owner safeguard

A new Project has no implicit human membership.

The first Project Owner can be bootstrapped only while the Project has no memberships. After bootstrap, membership creation, role changes, and removal require Project Owner authority.

Ketebe rejects:

- removal of the last Project Owner
- downgrade of the last Project Owner

This prevents a Project from becoming administratively orphaned through ordinary membership lifecycle operations.

## Multi-project identity

Project membership is intentionally many-to-many:

```text
Principal subject
    |
    +-- Project A / Reader
    +-- Project B / Editor
    +-- Project C / Owner
```

No separate authenticated identity is required for each Project.

This is the human identity model required by ADR 0004.

## Collection overrides

Collection permissions remain subordinate to Project membership.

Evaluation is:

1. resolve explicit Project membership,
2. obtain its Project role,
3. if a Collection-specific permission exists for the same subject, apply that override,
4. otherwise use the Project role.

An override can therefore narrow a broader Project role. For example, an Editor with a Collection-level `Read` permission cannot write that Collection.

Collection override state remains owned by the existing authorization policy surface. #193 wires ProjectMembership into the central REST/gRPC/MCP authorization resolver.

## Non-disclosure

Membership lookup and authorization return undiscoverable semantics when the caller has no authority in the requested Project.

A subject with access to Project A cannot use membership APIs to determine membership state in Project B merely because it knows or guesses a Project ID.

Unknown Projects also fail closed during authorization.

## Workload credentials

Project-scoped workload credentials remain separate from human ProjectMembership.

An API key may authenticate with an explicit workload Project binding. Human access instead comes from ProjectMembership.

The central resolver introduced by #193 will combine these identity modes without treating authentication itself as authorization.

## Audit

Successful Project membership bootstrap, creation, role update, and removal emit authorization audit events with:

- actor subject
- Project ID
- membership resource identifier
- lifecycle action
- allowed result

Credentials and secrets are not stored in ProjectMembership state or membership audit events.

## Implementation boundary

#192 establishes the durable ProjectMembership model and role semantics.

It does not replace the current request-path authorization resolver. That is intentionally handled by #193, which will resolve first-class Project ownership, ProjectMembership, OrganizationMembership, workload credentials, and Collection overrides through one central authorization boundary.
