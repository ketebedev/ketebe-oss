from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal

Json = dict[str, Any]


@dataclass(frozen=True, slots=True)
class RecordId:
    type: Literal["string", "u64"]
    value: str | int

    @classmethod
    def string(cls, value: str) -> "RecordId":
        if not value:
            raise ValueError("record id must not be empty")
        return cls("string", value)

    @classmethod
    def u64(cls, value: int) -> "RecordId":
        if value < 0 or value > 2**64 - 1:
            raise ValueError("u64 record id is out of range")
        return cls("u64", value)

    def to_wire(self) -> Json:
        return {"type": self.type, "value": self.value}

    @classmethod
    def from_wire(cls, value: Json) -> "RecordId":
        kind = value.get("type")
        raw = value.get("value")
        if kind == "string" and isinstance(raw, str):
            return cls.string(raw)
        if kind == "u64" and isinstance(raw, int) and not isinstance(raw, bool):
            return cls.u64(raw)
        raise ValueError(f"invalid Ketebe RecordId: {value!r}")

    def path_component(self) -> str:
        return str(self.value)


@dataclass(slots=True)
class CreateCollection:
    id: str
    dimension: int
    metric: str
    lexical_fields: list[list[str]] | None = None

    def to_wire(self) -> Json:
        body: Json = {"id": self.id, "dimension": self.dimension, "metric": self.metric}
        if self.lexical_fields is not None:
            body["lexical_fields"] = self.lexical_fields
        return body


@dataclass(slots=True)
class RecordUpsert:
    vector: list[float]
    metadata: Json | None = None

    def to_wire(self) -> Json:
        body: Json = {"vector": self.vector}
        if self.metadata is not None:
            body["metadata"] = self.metadata
        return body


@dataclass(slots=True)
class BatchRecordUpsert:
    id: RecordId
    vector: list[float]
    metadata: Json | None = None

    def to_wire(self) -> Json:
        body: Json = {"id": self.id.to_wire(), "vector": self.vector}
        if self.metadata is not None:
            body["metadata"] = self.metadata
        return body


@dataclass(slots=True)
class DocumentUpsert:
    text: str
    metadata: Json | None = None
    source: Json | None = None
    chunking: Json | None = None

    def to_wire(self) -> Json:
        body: Json = {"text": self.text}
        for key in ("metadata", "source", "chunking"):
            value = getattr(self, key)
            if value is not None:
                body[key] = value
        return body


@dataclass(slots=True)
class QueryRequest:
    vector: list[float] | None = None
    text: str | None = None
    top_k: int | None = None
    predicate: Json | None = None
    execution: str | None = None
    dense_candidates: int | None = None
    lexical_candidates: int | None = None
    search_profile: str | None = None
    timeout_ms: int | None = None
    explain: bool = False

    def to_wire(self) -> Json:
        body: Json = {"explain": self.explain}
        for key in (
            "vector",
            "text",
            "top_k",
            "predicate",
            "execution",
            "dense_candidates",
            "lexical_candidates",
            "search_profile",
            "timeout_ms",
        ):
            value = getattr(self, key)
            if value is not None:
                body[key] = value
        return body


@dataclass(slots=True)
class QueryHit:
    id: RecordId
    score: float
    sequence_number: int
    metadata: Json | None = None
    extra: Json = field(default_factory=dict)

    @classmethod
    def from_wire(cls, body: Json) -> "QueryHit":
        known = {"id", "score", "sequence_number", "metadata"}
        return cls(
            id=RecordId.from_wire(body["id"]),
            score=float(body["score"]),
            sequence_number=int(body["sequence_number"]),
            metadata=body.get("metadata"),
            extra={key: value for key, value in body.items() if key not in known},
        )


@dataclass(slots=True)
class QueryResponse:
    api_version: str
    hits: list[QueryHit]
    explain: Json | None = None

    @classmethod
    def from_wire(cls, body: Json) -> "QueryResponse":
        return cls(
            api_version=str(body["api_version"]),
            hits=[QueryHit.from_wire(hit) for hit in body.get("hits", [])],
            explain=body.get("explain"),
        )
