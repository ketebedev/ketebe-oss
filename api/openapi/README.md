# Ketebe OpenAPI contract

`v1.json` is the checked-in source of truth for the public REST contract consumed by first-party and generated clients.

## Ownership

The server implementation and this contract change together. Public REST changes must update `v1.json`, the compatibility baseline when appropriate, and the contract tests in the same change. SDK generation must consume this checked-in file rather than scrape runtime routes or maintain a separate schema.

The contract version is independent from individual path prefixes. Existing `/v0/...` resources remain in the OpenAPI v1 contract while they are supported; `/v1/...` endpoints represent newer resource contracts such as unified retrieval and search profiles.

## Compatibility policy

Within contract major version 1, changes are additive by default. Existing operations, operation IDs, required error-envelope fields, and compatibility components cannot be removed or renamed. A breaking change requires an explicit contract-major transition and migration notes instead of silently editing the v1 contract.

`v1.compatibility.json` is the machine-readable compatibility floor. CI verifies that every baseline operation still exists with the same method, path and operation ID, required components remain present, operation IDs are unique, and the stable error envelope is intact. New operations may be added without changing the baseline immediately; once an operation is considered public and stable it must be added to the baseline in the same change.

## Error envelope

All JSON error responses use the stable shape:

```json
{
  "error": {
    "code": "machine_readable_code",
    "message": "human readable message"
  }
}
```

Clients may branch on `error.code`; `error.message` is descriptive and is not a compatibility key.

## Generated-client boundary

The generated-client input boundary is exactly `api/openapi/v1.json`. Generated SDK code is an output artifact and must not become a second source of truth. The Rust, Python and TypeScript SDK work in issues #65, #66 and #67 should pin the contract revision used for generation and run generated-client compatibility tests against a real Ketebe server.

## gRPC parity

REST and gRPC do not need identical wire representations, but public operations that exist on both transports must preserve semantic behavior. Query v1 parity is covered by the server integration suite; new cross-transport features must extend those tests.
