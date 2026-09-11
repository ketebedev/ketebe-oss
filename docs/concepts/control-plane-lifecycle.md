# Organization and Project lifecycle

Ketebe exposes Organization and Project as first-class control-plane resources while keeping data-plane operations scoped by Project + Collection.

## Public hierarchy

```text
Organization
  -> Project
      -> Collection
```

Project IDs are globally stable. A data-plane request does not need to repeat Organization ID because Project ownership resolves authoritatively through the control plane.

## Organization lifecycle

REST:

```text
POST   /v0/organizations
GET    /v0/organizations
GET    /v0/organizations/{organization_id}
PATCH  /v0/organizations/{organization_id}
DELETE /v0/organizations/{organization_id}
```

Creating an Organization bootstraps the authenticated human Principal as Organization Owner. Workload credentials cannot self-create Organizations.

Organization lifecycle states are:

- `active`
- `suspended`
- `deleting`

Once an Organization enters `deleting`, it cannot return to `active` or `suspended`.

Physical deletion requires:

1. lifecycle is already `deleting`;
2. the Organization owns no Projects.

Deleting an Organization with owned Projects returns a conflict/failure-precondition result.

## Project lifecycle

REST:

```text
POST   /v0/organizations/{organization_id}/projects
GET    /v0/organizations/{organization_id}/projects

GET    /v0/projects/{project_id}
PATCH  /v0/projects/{project_id}
DELETE /v0/projects/{project_id}
```

Project creation requires authorized Organization context and an active parent Organization. The creator is bootstrapped as Project Owner.

Project lifecycle states use the same `active`, `suspended`, and `deleting` state machine.

Physical deletion requires:

1. lifecycle is already `deleting`;
2. the Project owns no Collections.

Ketebe checks the durable collection namespace catalog before deleting a Project. A Project with any owned Collection cannot be deleted and therefore cannot silently orphan data-plane state.

## Membership management

Organization membership:

```text
GET    /v0/organizations/{organization_id}/memberships
PUT    /v0/organizations/{organization_id}/memberships/{subject}
DELETE /v0/organizations/{organization_id}/memberships/{subject}
```

Project membership:

```text
GET    /v0/projects/{project_id}/memberships
PUT    /v0/projects/{project_id}/memberships/{subject}
DELETE /v0/projects/{project_id}/memberships/{subject}
```

These adapters use the existing OrganizationMembershipService and ProjectMembershipService. Last-owner safeguards remain authoritative.

## Authorization and discovery

Protocol adapters do not implement independent policy.

Both REST and gRPC call the shared `ControlPlaneApiService`, which resolves:

- Organization membership for account-level lifecycle operations;
- Organization authority or explicit Project authority for Project lifecycle operations;
- cross-Organization discovery as fail-closed.

Listing Organizations returns only Organizations visible to the Principal. Project lookup does not reveal a Project across an unauthorized Organization boundary.

Organization membership does not automatically grant Collection data-plane access. The existing Project membership / workload credential authorization model remains separate.

## gRPC alignment

The `ketebe.v0.ControlPlane` gRPC service exposes the same Organization, Project, membership, lifecycle and deletion semantics as REST.

Protocol mapping follows stable status classes:

- invalid input -> `INVALID_ARGUMENT`
- unauthorized discovery -> `NOT_FOUND`
- explicit authorization denial -> `PERMISSION_DENIED`
- invalid lifecycle/delete precondition -> `FAILED_PRECONDITION`
- resource/slug conflict -> `ALREADY_EXISTS` or REST `409`
- unexpected storage failures -> `INTERNAL`

## API contract

The versioned REST contract lives in `api/openapi/v1.json`. Lifecycle and membership errors use the standard Ketebe error envelope.

Billing/payment APIs, cloud provisioning and Organization IDs on every data-plane route are intentionally outside this lifecycle surface.
