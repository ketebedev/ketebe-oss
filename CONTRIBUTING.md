# Contributing to Ketebe

Thank you for contributing to Ketebe.

Project website: `ketebe.dev`

## Before opening a change

Keep changes focused and avoid introducing infrastructure, dependencies or abstractions without a concrete product requirement. For architectural changes, explain the design rationale, public API impact, compatibility considerations, and validation plan in the pull request.

Security vulnerabilities must follow [SECURITY.md](SECURITY.md), not the normal public issue flow.

## Development prerequisites

- Rust 1.98.0 via `rustup`
- Git
- additional SDK toolchains only when changing the corresponding SDK

The repository includes `rust-toolchain.toml`.

## Local Rust quality gates

For Rust or cross-product changes, run:

```bash
cargo fmt --all -- --check
cargo check --workspace --all-targets --all-features
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
```

Documentation-only changes do not require rebuilding the full Rust/SDK matrix locally.

## Public API changes

Public behavior is versioned independently from internal storage and cluster topology.

When changing a public API or SDK contract:

- preserve the OpenAPI compatibility contract under `api/openapi`,
- keep string and unsigned numeric record IDs lossless and distinct,
- avoid exposing internal node/topology details to normal clients,
- make retry semantics safe for non-idempotent operations,
- update relevant SDK and real-server contract tests.

## Persistence and correctness changes

Changes to WAL, segments, recovery or indexing must preserve the documented durability model. Derived indexes are rebuildable state and must not become the durability source of truth.

Persistence changes require deterministic restart/recovery coverage. Performance changes should include repeatable benchmark evidence when they make performance claims.

## Pull requests

A useful PR description includes:

- the problem being solved,
- the chosen approach,
- architecture/API compatibility impact,
- tests performed,
- benchmark impact when relevant.

Small, reviewable changes are preferred.

## Documentation

User-facing documentation should be organized by intent rather than implementation history:

- get started,
- use Ketebe,
- deploy and operate,
- concepts and architecture,
- integrations,
- reference,
- contribute and security.

Avoid describing completed capabilities as future plans. Version-specific architecture documents may retain `v0` in their names when that is part of the contract.

## Code of conduct

Participation in Ketebe community spaces is governed by [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md).
