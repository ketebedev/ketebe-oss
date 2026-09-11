# Upgrade and Downgrade Notes

Ketebe upgrade notes are release-specific and describe only migrations that actually exist in the target release.

## Before upgrading

For a self-hosted deployment:

1. Record the currently running Ketebe product version.
2. Read the target release notes and compatibility notes.
3. Verify that the target architecture matches the published artifact.
4. Back up persistent Ketebe data before any release that changes storage or control-plane formats.
5. Validate configuration changes before replacing the running server.
6. Confirm that API clients and MCP integrations are compatible with the target public contract.

## Default Organization and Project migration

The v0.9 control-plane model uses deliberate durable default resources for simple self-hosted deployments:

```text
Organization ID: default
Project ID: default
```

Startup migration validates existing persisted Project references before publishing bootstrap state.

Existing `DataPlaneScope(default, collection)` identities are preserved. Ketebe does not silently remap unknown non-default Projects into the default Organization.

See `docs/operations/default-control-plane-migration.md` for the complete migration contract.

## Downgrade expectations

Downgrade is supported only when the target older release can read every persisted format and control-plane state written by the newer release.

Do not infer downgrade safety from product version numbers alone.

When release notes do not explicitly state that downgrade is supported after a migration, treat downgrade as unsupported once the newer release has written persistent state.

The safe rollback path is then:

1. stop the newer server,
2. restore a backup created before the incompatible migration,
3. start the previously validated older release against that restored state.

## Minimal upgrade-note template

Each release that changes upgrade behavior uses the following fixed sections. The section text must state the concrete behavior for that release; empty or placeholder sections are not acceptable.

### Compatibility

State whether the public REST/domain compatibility floor changes. Breaking changes require a contract-major transition.

### Storage and control-plane migration

State which persisted formats change and how they migrate. If none change, explicitly state that no storage/control-plane migration is required.

### Configuration

List only configuration keys whose meaning, default, or required status changes. If there are no configuration changes, state that explicitly.

### Upgrade procedure

Describe the exact order required to move from the previous supported release to the target release.

### Downgrade

State whether downgrade is supported after the target release has written persistent state. If unsupported, identify backup/restore as the rollback path.

### Artifact identity

Reference the exact server binary archive, checksum, and container tag for the release.

### MCP compatibility

State any required MCP adapter version or confirm that the existing compatibility boundary remains unchanged.

This structure is intentionally lightweight; Ketebe does not introduce a separate release-management framework for v0.9.
