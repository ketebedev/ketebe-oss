# Ketebe Python SDK

First-party synchronous Python client for Ketebe's public REST contract.

```bash
pip install -e sdk/python
```

## RAG-style quickstart

```python
from ketebe import Client, CreateCollection, DocumentUpsert, QueryRequest, RecordId

with Client("http://127.0.0.1:7610") as client:
    client.create_collection(CreateCollection("docs", 384, "cosine"))

    client.upsert_document(
        "docs",
        RecordId.string("intro"),
        DocumentUpsert(
            text="Ketebe is an AI-native retrieval platform.",
            metadata={"source": "guide"},
        ),
    )

    result = client.query(
        "docs",
        QueryRequest(
            text="What is Ketebe?",
            top_k=5,
            search_profile="balanced",
            explain=True,
        ),
    )

    for hit in result.hits:
        print(hit.id, hit.score, hit.metadata)
```

The SDK is topology-independent and does not accept server-side embedding provider credentials. Automatic retries are limited to requests explicitly classified as idempotent by the client.
