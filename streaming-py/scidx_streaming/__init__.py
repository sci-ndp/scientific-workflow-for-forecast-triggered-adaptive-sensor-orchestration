"""Top-level package exports for scidx_streaming."""

from __future__ import annotations

import warnings
from importlib import import_module
from typing import Any

from .utils import ensure_env_loaded

# Suppress noisy third-party warnings commonly triggered in notebook workflows.
warnings.filterwarnings(
    "ignore",
    message=r"Could not determine API version from status endpoint.*",
)

ensure_env_loaded()

__all__ = ["StreamingClient"]


def __getattr__(name: str) -> Any:  # pragma: no cover - thin delegator
    if name == "StreamingClient":
        module = import_module("scidx_streaming.client.__init__")
        return module.StreamingClient
    raise AttributeError(f"module 'scidx_streaming' has no attribute '{name}'")
