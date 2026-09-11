# Server release workflow

Ketebe server releases are produced by `.github/workflows/server-release.yml`.

## Release trigger

Product releases use `vMAJOR.MINOR.PATCH`. The v0.9 release-candidate convention is `v0.9.0-rc.N`.

A Git tag matching `v*` triggers publication.

The workflow refuses to publish a tag unless the tagged commit belongs to `main` history. This prevents publication from a divergent release-only commit while allowing `main` to advance after a valid release commit is tagged.

Manual `workflow_dispatch` runs execute the same source validation, binary packaging and container build/smoke path without publishing a GitHub Release or pushing an image.

## Published artifacts

For a tag such as `v0.9.0-rc.1`, the workflow produces:

```text
ketebe-server-v0.9.0-rc.1-linux-arm64.tar.gz
ketebe-server-v0.9.0-rc.1-linux-arm64.tar.gz.sha256
ghcr.io/ketebedev/ketebe-server:v0.9.0-rc.1
```

The archive contains:

- `ketebe-server`
- `LICENSE`

The checksum file is verified before the artifact is uploaded.

## Validation before publication

The release job runs:

```bash
cargo fmt --all -- --check
cargo check --workspace --all-targets --all-features
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
cargo build --locked --release -p ketebe-server
```

The release source also passes canonical OSS dependency/projection boundary checks. The packaged binary archive and the versioned container are both validated as release artifacts before publication.

For each artifact path, release validation:
- starts from clean persistent state,
- waits for both health and readiness,
- verifies the real default Organization and default Project bootstrap,
- creates a Project-scoped collection,
- writes a record through the public REST API,
- queries that record through Query v1,
- restarts the exact artifact while reusing the same persistent state,
- verifies the control-plane and query state after restart.

The binary smoke extracts and executes the packaged `tar.gz`; it does not use `cargo run` or the source-tree build path. The container smoke uses the exact locally built release image that will be pushed for a tag release. Any smoke/restart failure blocks publication.

## Platform

The current release runner is Linux ARM64, so the v0.9 binary archive is explicitly named `linux-arm64`. Additional platforms require explicit build and validation jobs rather than relabeling one architecture as another.

## Version ownership

Release tags identify Ketebe product releases. They do not identify storage-format versions.

Product version, OpenAPI contract version, persisted storage-format versions, commercial license/entitlement contract versions, and MCP adapter versions are independent domains.

MCP keeps its independent release workflow and artifact lifecycle.

See:

- `docs/reference/release-versioning.md` for product/tag, artifact, API/storage/license, and MCP version rules.
- `docs/operations/upgrade.md` for upgrade, migration, and downgrade expectations.
- `docs/reference/v0.9-domain-contract.md` for the ADR 0004 Organization/Project public compatibility boundary.

## Artifact smoke implementation

The shared public-API smoke flow lives in `scripts/release/smoke_server_artifact.sh`.

It uses the stabilized ADR 0004 contract and explicitly selects the default Project with:

```text
X-Ketebe-Project: default
```

The helper validates artifact behavior only; it does not duplicate the workspace test suite already executed earlier in the release job.
