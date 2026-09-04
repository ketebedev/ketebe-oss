import json
from pathlib import Path

from ketebe import QueryRequest, RecordId

ROOT = Path(__file__).resolve().parents[3]


def test_typed_record_ids_are_lossless() -> None:
    string = RecordId.string("42").to_wire()
    numeric = RecordId.u64(42).to_wire()
    assert string != numeric
    assert RecordId.from_wire(string) == RecordId.string("42")
    assert RecordId.from_wire(numeric) == RecordId.u64(42)


def test_query_request_maps_to_v1_contract() -> None:
    body = QueryRequest(
        vector=[1.0, 0.0],
        text="vector database",
        top_k=5,
        dense_candidates=20,
        lexical_candidates=30,
        search_profile="balanced",
        explain=True,
    ).to_wire()
    assert body["vector"] == [1.0, 0.0]
    assert body["text"] == "vector database"
    assert body["search_profile"] == "balanced"


def test_openapi_contains_python_sdk_surface() -> None:
    spec = json.loads((ROOT / "api/openapi/v1.json").read_text())
    operations = [
        ("get", "/v0/collections"),
        ("post", "/v0/collections"),
        ("put", "/v0/collections/{collection_id}/records/{record_id}"),
        ("post", "/v0/collections/{collection_id}/records:batchUpsert"),
        ("put", "/v0/collections/{collection_id}/documents/{record_id}"),
        ("post", "/v1/collections/{collection_id}/query"),
        ("get", "/v0/jobs/{job_id}"),
        ("post", "/v0/jobs/{job_id}/cancel"),
        ("get", "/v0/collections/{collection_id}/embedding-migration"),
        ("post", "/v0/collections/{collection_id}/embedding-migration"),
        ("post", "/v0/collections/{collection_id}/embedding-migration/catch-up"),
        ("post", "/v0/collections/{collection_id}/embedding-migration/catch-up-job"),
        ("post", "/v0/collections/{collection_id}/embedding-migration/activate"),
    ]
    for method, path in operations:
        assert isinstance(spec["paths"][path][method], dict), f"missing {method} {path}"
