"""Shared definitions for supported SciDX streaming method types."""

from __future__ import annotations

from enum import Enum
from typing import FrozenSet


class MethodType(str, Enum):
    """Legal resource/consumption method types exposed by the streaming client."""

    CSV = "csv"
    KAFKA = "kafka"
    RSS = "rss"
    STREAM = "stream"
    TXT = "txt"
    NETCDF = "netcdf"
    JSON = "json"
    WEBSOCKET = "websocket"


SUPPORTED_METHOD_TYPES: FrozenSet[str] = frozenset(t.value for t in MethodType)


URL_BACKED_METHOD_TYPES: FrozenSet[str] = frozenset(
    {
        MethodType.CSV.value,
        MethodType.TXT.value,
        MethodType.JSON.value,
        MethodType.STREAM.value,
        MethodType.NETCDF.value,
        MethodType.RSS.value,
        MethodType.WEBSOCKET.value,
    }
)

