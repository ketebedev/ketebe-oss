import os

import pytest

from ketebe import (
    BatchRecordUpsert,
    Client,
    CreateCollection,
    QueryRequest,
    RecordId,
    RecordUpsert,
)

BASE_URL = os.environ.get("KETEBE_TEST_BASE_URL")


@pytest.mark.skipif(BASE_URL is None, reason="KETEBE_TEST_BASE_URL is not configured")
def test_python_sdk_round_trip_against_real_server() -> None:
    assert BASE_URL is not None
    with Client(BASE_URL) as client:
        client.create_collection(CreateCollection("python_docs", 2, "l2"))
        mutation = client.upsert_record(
            "python_docs",
            RecordId.string("one"),
            RecordUpsert([1.0, 0.0], {"title": "one"}),
        )
        assert int(mutation["sequence_number"]) > 0

        batch = client.batch_upsert_records(
            "python_docs",
            [
                BatchRecordUpsert(RecordId.string("two"), [0.0, 1.0], {"title": "two"}),
                BatchRecordUpsert(RecordId.string("three"), [0.5, 0.5], {"title": "three"}),
            ],
        )
        assert isinstance(batch, dict)

        result = client.query(
            "python_docs",
            QueryRequest(vector=[1.0, 0.0], top_k=3, execution="exact", explain=True),
        )
        assert result.api_version == "v1"
        assert len(result.hits) == 3
        assert result.hits[0].id == RecordId.string("one")
