# Ketebe Go SDK

First-party synchronous Go client for Ketebe's versioned public REST API.

```go
client, err := ketebe.NewClient(ketebe.ClientOptions{BaseURL: "http://127.0.0.1:17610"})
collections, err := client.ListCollections(context.Background())
```

The SDK requires a `context.Context` on every network operation, preserves typed string/u64 RecordIds and full `uint64` sequence numbers, retries only operations explicitly classified as idempotent, and exposes `APIError` and `TransportError` separately.

The implementation is handwritten against `api/openapi/v1.json`; the OpenAPI document remains the public contract authority. Server topology and embedding-provider credentials are intentionally absent from the client surface.

Run:

```bash
cd sdk/go
go test ./...
```
