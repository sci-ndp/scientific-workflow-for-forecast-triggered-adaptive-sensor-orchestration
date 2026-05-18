"""Lightweight .env loader for SciDX Streaming."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Iterable, Optional

_ENV_LOADED = False


def _candidate_paths(explicit: Optional[Path]) -> Iterable[Path]:
    if explicit:
        yield explicit
    cwd = Path.cwd()
    yield cwd / ".env"
    module_root = Path(__file__).resolve().parents[2]
    yield module_root / ".env"
    project_root = module_root.parent
    yield project_root / ".env"


def load_env(path: str | Path | None = None, *, overwrite: bool = False) -> None:
    """Populate ``os.environ`` with variables from a .env file.

    Parameters
    ----------
    path:
        Explicit path to a .env file. When omitted, common project locations are
        checked (current working directory, package root, repository root).
    overwrite:
        When ``True``, existing environment variables are overwritten by values
        from the file. Defaults to ``False`` to preserve externally provided
        credentials/configuration.
    """

    global _ENV_LOADED

    explicit_path = Path(path).expanduser() if path else None

    candidates = [candidate for candidate in _candidate_paths(explicit_path) if candidate.exists()]
    if not candidates:
        _ENV_LOADED = True
        return

    for candidate in candidates:
        _load_file(candidate, overwrite=overwrite)
    _ENV_LOADED = True


def ensure_env_loaded() -> None:
    """Idempotent helper to load environment variables once."""

    if not _ENV_LOADED:
        load_env()


def _load_file(path: Path, *, overwrite: bool) -> None:
    try:
        with path.open("r", encoding="utf-8") as handle:
            for raw_line in handle:
                line = raw_line.strip()
                if not line or line.startswith("#"):
                    continue
                if "=" not in line:
                    continue
                key, value = line.split("=", 1)
                key = key.strip()
                value = value.strip().strip("'").strip('"')
                if not key:
                    continue
                if key in os.environ and not overwrite:
                    continue
                os.environ[key] = value
    except OSError:
        # Silently ignore unreadable files; caller can decide to warn elsewhere.
        return
