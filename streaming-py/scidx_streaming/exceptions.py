"""Lightweight exception types used across SciDX streaming services."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class HTTPException(Exception):
    """Minimal replacement for `fastapi.HTTPException`.

    Only the attributes accessed by the streaming code (`detail` and `status_code`)
    are implemented so that we avoid the heavy FastAPI dependency while keeping
    backwards-compatible semantics.
    """

    status_code: int
    detail: str | None = None

    def __post_init__(self) -> None:
        super().__init__(self.detail or "")
