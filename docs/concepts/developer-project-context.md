# Developer Project Context

Ketebe follows ADR 0004's public hierarchy:

```text
Organization
  -> Project
      -> Collection
```

Human identities may belong to multiple Projects. Data-plane and project-scoped management calls therefore support an explicit request context:

```http
X-Ketebe-Project: project-a
```

This header is a selection request, not an authorization grant.

Ketebe resolves the selected Project through the durable control plane and evaluates the authenticated principal through the central Project authorization service before the request proceeds.

## Human identities

For a human principal:

1. the client selects a Project with `X-Ketebe-Project`,
2. Ketebe validates that Project exists,
3. Ketebe evaluates the principal's Project membership and role,
4. the validated Project becomes request-scoped context,
5. collection, query, ingestion, job and profile authorization continue through their existing shared boundaries.

A missing or unauthorized Project is non-discoverable according to the existing Project authorization contract.

## Workload credentials

Project-scoped API keys and Service Account credentials keep their bound Project semantics.

If a workload credential sends `X-Ketebe-Project`:

- the value may match the credential's bound Project,
- a different Project value is rejected,
- the header never rebinds a workload credential.

This preserves least-privilege Project scoping from ADR 0004 and issues #194 and #195.

## Control-plane APIs

Organization and Project lifecycle APIs do not require a selected Project context.

The public control-plane surface remains:

- `/v0/organizations`
- `/v0/organizations/{organization_id}`
- `/v0/organizations/{organization_id}/projects`
- `/v0/projects/{project_id}`
- Organization and Project membership endpoints

Their visibility and mutation rules are decided by the shared control-plane authorization service.

## SDK behavior

Supported first-party SDKs expose:

- Organization lifecycle operations,
- Project lifecycle operations,
- an explicit Project client/request context.

SDK Project context only emits `X-Ketebe-Project`; it does not implement a second authorization model.

## MCP behavior

Ketebe MCP exposes authorization-backed Organization/Project discovery:

- `list_organizations`
- `list_projects`
- `describe_project`

For project-scoped MCP tools, clients select a Project with the same `X-Ketebe-Project` transport header. MCP forwards this request context downstream. Ketebe remains authoritative for Project existence, membership, role and workload credential binding.

MCP does not maintain a separate tenant, workspace or session-level authorization store.

## Compatibility

The header is optional and additive within the current public API major version.

Existing project-scoped workload clients continue to work without the header because their credential already supplies the Project scope.

Existing single-project development/self-hosted behavior remains unchanged.

Organization is not added to Collection identity or low-level `DataPlaneScope`; the durable data-plane key remains Project + Collection.
