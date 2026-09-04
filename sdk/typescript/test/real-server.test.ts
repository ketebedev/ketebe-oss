import assert from "node:assert/strict";
import test from "node:test";
import { ApiError, Client } from "../src/index.js";

const baseUrl = process.env.KETEBE_BASE_URL;

test("TypeScript SDK round-trips against a real Ketebe server", { skip: !baseUrl }, async () => {
  const client = new Client({ baseUrl: baseUrl! });
  const collection = await client.createCollection({ id: "tsdocs", dimension: 2, metric: "l2" });
  assert.equal(collection.id, "tsdocs");

  const mutation = await client.upsertRecord(
    "tsdocs",
    { type: "string", value: "one" },
    { vector: [1, 0], metadata: { title: "one" } },
  );
  assert.ok(mutation.sequence_number > 0n);

  await client.batchUpsertRecords("tsdocs", {
    records: [
      { id: { type: "string", value: "two" }, vector: [0, 1], metadata: { title: "two" } },
      { id: { type: "string", value: "three" }, vector: [0.5, 0.5], metadata: { title: "three" } },
    ],
  });

  const result = await client.query("tsdocs", {
    vector: [1, 0],
    top_k: 3,
    execution: "exact",
    explain: true,
  });
  assert.equal(result.api_version, "v1");
  assert.equal(result.hits.length, 3);
  assert.equal(result.hits[0]?.id.type, "string");
  assert.equal(result.hits[0]?.id.value, "one");

  await assert.rejects(
    () => client.getCollection("missing"),
    (error: unknown) => error instanceof ApiError && error.status === 404 && error.code.length > 0,
  );
});
