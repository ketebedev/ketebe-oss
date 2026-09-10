# Organization Membership and Roles

Ketebe treats Organization membership as an explicit control-plane relationship between an authenticated Principal subject and a first-class Organization.

This model follows ADR 0004 and keeps Organization account authority separate from Project data-plane authority.

## Roles

The v0 role vocabulary is:

- **Owner** — full Organization control-plane authority, including membership and billing actions.
- **Admin** — Organization administration and membership management, but no billing-management authority.
- **Member** — Organization read access only.
- **BillingAdmin** — Organization read access plus billing-management authority, without membership administration.

Role evaluation is explicit through Organization actions. No Organization role grants Collection read/write access by itself.

## First owner bootstrap

A newly created Organization has no implicit members. The first membership must be bootstrapped explicitly as an Owner.

Bootstrap is allowed only while the Organization has no memberships. After the first Owner exists, all membership creation, role changes, and removals require an authenticated member with membership-write authority.

## Last-owner safeguard

An Organization must retain at least one Owner.

Ketebe rejects both:

- removal of the last Owner
- downgrade of the last Owner to another role

The safeguard is deterministic and applied before durable state is published.

## Durability

Membership state is stored in:

`security/organization-memberships.json`

The store is versioned. Mutations are written to a temporary file and renamed into place before the in-memory state is published.

On restart, memberships and roles are restored from the durable file.

## Non-disclosure

Organization membership lookups and authorization checks fail as undiscoverable when the authenticated subject has no authority in the requested Organization.

This prevents callers from using authorization error differences to enumerate memberships in other Organizations.

## Project access remains separate

Organization membership never creates a Project binding on a Principal and never creates a Project role.

A human Principal can therefore be:

- an Organization Owner with no Project access
- a Project member in one or more Projects through explicit ProjectMembership state
- both, with each authorization path evaluated independently

Project membership is introduced separately by #192 and central Organization/Project authorization resolution by #193.

## Audit

Successful membership bootstrap, creation, role update, and removal emit authorization audit events with:

- actor subject
- Organization membership resource identifier
- lifecycle action
- allowed result

Raw credentials and secrets are never written into membership state or membership audit events.
