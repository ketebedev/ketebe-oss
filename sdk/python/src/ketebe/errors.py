from __future__ import annotations


class KetebeError(Exception):
    """Base exception for the Ketebe Python SDK."""


class TransportError(KetebeError):
    """The request could not be completed at the HTTP transport layer."""


class ApiError(KetebeError):
    def __init__(self, status_code: int, code: str, message: str) -> None:
        self.status_code = status_code
        self.code = code
        self.message = message
        super().__init__(f"Ketebe API error {status_code} {code}: {message}")
