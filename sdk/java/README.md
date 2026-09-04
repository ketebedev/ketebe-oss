# Ketebe Java SDK

First-party synchronous Java 17+ client for Ketebe's versioned public REST API.

```xml
<dependency>
  <groupId>io.ketebe</groupId>
  <artifactId>ketebe-client</artifactId>
  <version>0.1.0</version>
</dependency>
```

```java
KetebeClient client = new KetebeClient("http://127.0.0.1:8080");
RecordId id = RecordId.unsigned(new BigInteger("18446744073709551615"));
client.upsertRecord("docs", id, new RecordUpsert(List.of(1.0, 0.0), null));
QueryResponse result = client.query("docs", new QueryRequest().vector(List.of(1.0, 0.0)).topK(10).execution("exact"));
```

`RecordId.StringId("42")` and unsigned `42` are distinct. Unsigned IDs and sequence numbers use `BigInteger` and are validated against the full `u64` range.

Retries are enabled only for operations the SDK marks idempotent. 429/5xx and transport failures may be retried for those operations; unsafe lifecycle POST operations are not retried automatically.

The SDK is handwritten against `api/openapi/v1.json`. The OpenAPI file is the compatibility authority; Java models must not import or mirror Ketebe's internal Rust storage types.
