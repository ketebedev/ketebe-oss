# Organization to Project governance policy resolution

Ketebe resolves SaaS account-level governance through the canonical domain hierarchy defined by ADR 0004:

```text
Organization policy
        |
        v
Project override
        |
        v
Effective Project policy
        |
        +--> GovernanceService
        |
        +--> InMemoryResourceGovernor
```

The Organization policy is provider-neutral. Billing, subscription and payment-provider objects are not part of the governance model and are not passed to either enforcement engine.

## Policy model

`OrganizationGovernancePolicy` contains:

- request admission and quota policy through `GovernancePolicy`,
- concurrent-work and ingestion-throughput budgets through `ProjectResourceBudget`,
- whether Project overrides are permitted.

`ProjectGovernanceOverride` may independently override request/quota policy and resource budgets.

Policies are persisted in:

```text
<data-dir>/control-plane/governance-policies.json
```

The store is versioned and validated when opened. Writes use a temporary file followed by rename, and the temporary file is mode `0600` on Unix.

## Authoritative ownership

A Project never supplies its Organization policy directly.

Resolution always performs:

```text
ProjectId
   |
   v
ControlPlaneStore
   |
   v
authoritative Project -> Organization ownership
   |
   v
Organization policy
```

This prevents a Project from selecting policy from another Organization and preserves cross-Organization isolation.

## Deterministic precedence

If an Organization policy exists, it is the maximum permissiveness boundary.

For numeric quotas and concurrency:

```text
effective = min(organization, project)
```

For rate and throughput windows:

```text
effective units/requests = min(organization, project)
effective window         = max(organization, project)
```

This deliberately chooses the stricter effective value.

If a Project omits an override field, the Organization value is inherited.

If no Organization policy exists, existing Project/default behavior is preserved:

- Project override values are used when present.
- Existing default governance/resource budgets are used otherwise.

If `project_overrides_allowed` is false, attempts to persist a Project override fail.

`EffectiveProjectGovernancePolicy.constrained_by_organization` makes it inspectable whether a requested Project override was tightened by the Organization boundary.

## Enforcement boundary

Resolution does not introduce a new admission engine.

`apply_project_policy` configures the existing Project-scoped enforcement points:

- `GovernanceService::set_project_policy` for read/write/admin rate limits and collection/record quotas.
- `InMemoryResourceGovernor::set_project_budget` for concurrent query/write/ingestion/background budgets and ingestion throughput.

Admission remains Project-scoped. Organization identity is not added to data-plane scope or low-level storage keys.

## Correctness

Organization policy may reject or throttle a Project, but it does not alter mutation ordering or durability semantics.

The existing enforcement boundaries remain responsible for ensuring rejected work does not weaken:

- WAL ordering,
- Kafka offset correctness,
- query consistency,
- Project/Collection isolation,
- retry semantics.

## Accounting

The resolver exports only bounded global counters:

- `ketebe_organization_governance_resolutions_total`
- `ketebe_organization_governance_constrained_resolutions_total`
- `ketebe_organization_governance_applications_total`

Organization IDs and Project IDs are intentionally not Prometheus labels.

## Non-goals

This layer does not:

- integrate Paddle or another payment provider,
- calculate invoices,
- meter billable usage externally,
- change the public Organization/Project hierarchy,
- replace the existing Project-scoped governance engines.
