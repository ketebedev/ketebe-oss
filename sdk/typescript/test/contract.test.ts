import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import { dirname, resolve } from "node:path";
import test from "node:test";
import JSONBigFactory from "json-bigint";
import type { RecordId } from "../src/models.js";

const JSONBig = JSONBigFactory({ useNativeBigInt: true });
const here = dirname(fileURLToPath(import.meta.url));
const openApiPath = resolve(here, "../../../../api/openapi/v1.json");

test("u64 RecordId round-trips without JavaScript number coercion", () => {
  const id: RecordId = { type: "u64", value: 18_446_744_073_709_551_615n };
  const encoded = JSONBig.stringify(id);
  assert.match(encoded, /18446744073709551615/);
  const decoded = JSONBig.parse(encoded) as RecordId;
  assert.equal(decoded.type, "u64");
  assert.equal(decoded.value, 18_446_744_073_709_551_615n);
});

test("OpenAPI contract contains TypeScript SDK operations", async () => {
  const spec = JSON.parse(await readFile(openApiPath, "utf8")) as { paths: Record<string, Record<string, unknown>> };
  const operations: Array<[string, string]> = [
    ["get", "/v0/collections"],
    ["post", "/v0/collections"],
    ["put", "/v0/collections/{collection_id}/records/{record_id}"],
    ["post", "/v0/collections/{collection_id}/records:batchUpsert"],
    ["put", "/v0/collections/{collection_id}/documents/{record_id}"],
    ["post", "/v1/collections/{collection_id}/query"],
    ["get", "/v0/jobs/{job_id}"],
    ["get", "/v0/collections/{collection_id}/embedding-migration"],
    ["post", "/v0/collections/{collection_id}/embedding-migration"],
    ["post", "/v0/collections/{collection_id}/embedding-migration/catch-up"],
    ["post", "/v0/collections/{collection_id}/embedding-migration/catch-up-job"],
    ["post", "/v0/collections/{collection_id}/embedding-migration/activate"],
  ];
  for (const [method, path] of operations) {
    assert.ok(spec.paths[path]?.[method], `missing ${method.toUpperCase()} ${path}`);
  }
});
