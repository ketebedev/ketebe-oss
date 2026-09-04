# Ketebe TypeScript SDK

First-party Node.js/TypeScript client for Ketebe's public HTTP API.

```bash
npm install @ketebe/client
```

```ts
import { Client } from "@ketebe/client";

const ketebe = new Client({ baseUrl: "http://127.0.0.1:7610" });

await ketebe.createCollection({ id: "docs", dimension: 3, metric: "cosine" });
await ketebe.upsertRecord(
  "docs",
  { type: "string", value: "doc-1" },
  { vector: [0.1, 0.2, 0.3], metadata: { title: "Ketebe" } },
);

const result = await ketebe.query("docs", {
  vector: [0.1, 0.2, 0.3],
  text: "vector database",
  dense_candidates: 100,
  lexical_candidates: 100,
  top_k: 10,
  explain: true,
});

console.log(result.hits);
```

Unsigned 64-bit record IDs use `bigint`, never JavaScript `number`:

```ts
const id = { type: "u64" as const, value: 18_446_744_073_709_551_615n };
```

The SDK serializes and parses those numeric JSON values losslessly. Retry defaults are idempotency-aware; non-idempotent operations are not retried automatically.
