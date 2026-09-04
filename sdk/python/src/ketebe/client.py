from __future__ import annotations

import time
from typing import Any

import httpx

from .errors import ApiError, TransportError
from .models import (
    BatchRecordUpsert,
    CreateCollection,
    DocumentUpsert,
    Json,
    QueryRequest,
    QueryResponse,
    RecordId,
    RecordUpsert,
)


class Client:
    def __init__(
        self,
        base_url: str,
        *,
        timeout: float = 10.0,
        max_retries: int = 2,
        retry_backoff: float = 0.05,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.max_retries = max_retries
        self.retry_backoff = retry_backoff
        self._http = httpx.Client(timeout=timeout, transport=transport)

    def close(self) -> None:
        self._http.close()

    def __enter__(self) -> "Client":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def list_collections(self) -> list[Json]:
        return list(self._request("GET", "/v0/collections", idempotent=True)["collections"])

    def create_collection(self, request: CreateCollection) -> Json:
        return self._request("POST", "/v0/collections", json=request.to_wire())

    def get_collection(self, collection: str) -> Json:
        return self._request("GET", f"/v0/collections/{collection}", idempotent=True)

    def delete_collection(self, collection: str) -> None:
        self._request("DELETE", f"/v0/collections/{collection}", idempotent=True, empty_ok=True)

    def upsert_record(self, collection: str, record_id: RecordId, request: RecordUpsert) -> Json:
        return self._request(
            "PUT",
            f"/v0/collections/{collection}/records/{record_id.path_component()}",
            json=request.to_wire(),
            idempotent=True,
        )

    def batch_upsert_records(self, collection: str, records: list[BatchRecordUpsert]) -> Json:
        return self._request(
            "POST",
            f"/v0/collections/{collection}/records:batchUpsert",
            json={"records": [record.to_wire() for record in records]},
            idempotent=True,
        )

    def delete_record(self, collection: str, record_id: RecordId) -> Json:
        return self._request(
            "DELETE",
            f"/v0/collections/{collection}/records/{record_id.path_component()}",
            idempotent=True,
        )

    def upsert_document(self, collection: str, record_id: RecordId, request: DocumentUpsert) -> Json:
        return self._request(
            "PUT",
            f"/v0/collections/{collection}/documents/{record_id.path_component()}",
            json=request.to_wire(),
            idempotent=True,
        )

    def query(self, collection: str, request: QueryRequest) -> QueryResponse:
        body = self._request(
            "POST",
            f"/v1/collections/{collection}/query",
            json=request.to_wire(),
            idempotent=True,
        )
        return QueryResponse.from_wire(body)

    def get_job(self, job_id: str) -> Json:
        return self._request("GET", f"/v0/jobs/{job_id}", idempotent=True)

    def cancel_job(self, job_id: str) -> Json:
        return self._request("POST", f"/v0/jobs/{job_id}/cancel")

    def get_embedding_migration(self, collection: str) -> Json:
        return self._request(
            "GET", f"/v0/collections/{collection}/embedding-migration", idempotent=True
        )

    def start_embedding_migration(self, collection: str, target_profile: str) -> Json:
        return self._request(
            "POST",
            f"/v0/collections/{collection}/embedding-migration",
            json={"target_profile": target_profile},
        )

    def catch_up_embedding_migration(self, collection: str) -> Json:
        return self._request(
            "POST", f"/v0/collections/{collection}/embedding-migration/catch-up"
        )

    def start_embedding_migration_catch_up_job(self, collection: str) -> Json:
        return self._request(
            "POST", f"/v0/collections/{collection}/embedding-migration/catch-up-job"
        )

    def activate_embedding_migration(self, collection: str) -> Json:
        return self._request(
            "POST", f"/v0/collections/{collection}/embedding-migration/activate"
        )

    def _request(
        self,
        method: str,
        path: str,
        *,
        json: Json | None = None,
        idempotent: bool = False,
        empty_ok: bool = False,
    ) -> Any:
        attempts = self.max_retries + 1 if idempotent else 1
        for attempt in range(attempts):
            try:
                response = self._http.request(method, f"{self.base_url}{path}", json=json)
            except (httpx.ConnectError, httpx.TimeoutException) as error:
                if idempotent and attempt + 1 < attempts:
                    time.sleep(self.retry_backoff)
                    continue
                raise TransportError(str(error)) from error
            except httpx.HTTPError as error:
                raise TransportError(str(error)) from error

            if idempotent and attempt + 1 < attempts and (
                response.status_code == 429 or response.status_code >= 500
            ):
                time.sleep(self.retry_backoff)
                continue

            if response.is_error:
                try:
                    error = response.json()["error"]
                    raise ApiError(response.status_code, str(error["code"]), str(error["message"]))
                except (ValueError, KeyError, TypeError) as parse_error:
                    raise ApiError(response.status_code, "http_error", response.reason_phrase) from parse_error

            if empty_ok and not response.content:
                return None
            try:
                return response.json()
            except ValueError as error:
                if empty_ok:
                    return None
                raise TransportError("error decoding response body") from error

        raise AssertionError("at least one HTTP attempt is always executed")
