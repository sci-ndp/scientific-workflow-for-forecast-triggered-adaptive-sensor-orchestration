"""Utilities for normalising MCP module selections provided to `StreamingClient`."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
from urllib.parse import urlparse
from collections.abc import Iterable as IterableABC

ModuleInput = Optional[Iterable[str] | Sequence[str] | str]


@dataclass(frozen=True)
class ModuleConfiguration:
    """Normalised module directives extracted from user configuration."""

    enabled: Optional[List[str]]
    disabled: Optional[List[str]]
    remote: Optional[List[Dict[str, Any]]]


def derive_module_configuration(
    modules: ModuleInput,
    disable_modules: ModuleInput,
) -> ModuleConfiguration:
    """Return cleaned module directives split into local and remote selections."""

    module_entries = _normalise_entries(modules)
    disabled_entries = _normalise_strings(disable_modules)
    local_enabled, remote_specs = _partition_remote(module_entries)

    return ModuleConfiguration(
        enabled=local_enabled,
        disabled=disabled_entries,
        remote=remote_specs,
    )


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _normalise_entries(value: ModuleInput) -> Optional[List[Any]]:
    if value is None:
        return None

    def _coerce(entry: Any) -> List[Any]:
        if entry is None:
            return []
        if isinstance(entry, dict):
            return [dict(entry)]
        if isinstance(entry, str):
            return [item.strip() for item in entry.split(",") if item and item.strip()]
        if isinstance(entry, IterableABC) and not isinstance(entry, (str, bytes)):
            result: List[Any] = []
            for nested in entry:
                result.extend(_coerce(nested))
            return result
        coerced = str(entry).strip()
        return [coerced] if coerced else []

    if isinstance(value, str):
        entries = _coerce(value)
    elif isinstance(value, IterableABC):
        entries = []
        for item in value:
            entries.extend(_coerce(item))
    else:
        entries = _coerce(value)

    return entries or None


def _normalise_strings(value: ModuleInput) -> Optional[List[str]]:
    entries = _normalise_entries(value)
    if entries is None:
        return None
    normalised: List[str] = []
    for entry in entries:
        text = str(entry).strip()
        if text:
            normalised.append(text)
    return normalised or None


def _partition_remote(
    entries: Optional[List[Any]],
) -> Tuple[Optional[List[str]], Optional[List[Dict[str, Any]]]]:
    if not entries:
        return None, None

    local: List[str] = []
    remote: List[Dict[str, Any]] = []

    for entry in entries:
        if isinstance(entry, dict):
            spec = dict(entry)
            transport = spec.get("transport") or spec.get("type")
            if transport:
                spec["transport"] = str(transport).strip().lower()
            elif spec.get("url"):
                spec["transport"] = "http"
            elif spec.get("command") or spec.get("args"):
                spec["transport"] = "stdio"
            spec["name"] = spec.get("name") or _derive_dict_name(spec)
            remote.append(spec)
            continue

        alias, candidate = _split_alias(str(entry))
        if _looks_like_url(candidate):
            remote.append(
                {
                    "name": alias or _derive_remote_name(candidate),
                    "url": candidate,
                    "transport": "http",
                }
            )
        else:
            local.append(candidate)

    return (local or None), (remote or None)


def _split_alias(entry: str) -> Tuple[Optional[str], str]:
    for separator in ("=", "|", "@"):
        if separator in entry:
            left, right = entry.split(separator, 1)
            if right and "://" in right:
                alias = left.strip()
                return (alias or None), right.strip()
    return None, entry.strip()


def _looks_like_url(candidate: str) -> bool:
    parsed = urlparse(candidate)
    return bool(parsed.scheme and parsed.netloc)


def _derive_remote_name(url: str) -> str:
    parsed = urlparse(url)
    base = parsed.netloc or parsed.path or "remote"
    if parsed.path and parsed.path not in ("/", ""):
        suffix = parsed.path.strip("/").replace("/", "-")
        if suffix:
            base = f"{base}-{suffix}"
    sanitised = re.sub(r"[^a-zA-Z0-9_-]+", "-", base).strip("-").lower()
    if not sanitised:
        sanitised = "remote"
    if not sanitised.startswith("remote"):
        sanitised = f"remote-{sanitised}"
    return sanitised


def _derive_dict_name(spec: Dict[str, Any]) -> str:
    explicit = str(spec.get("name") or "").strip()
    if explicit:
        return explicit
    url = spec.get("url")
    if isinstance(url, str) and url:
        return _derive_remote_name(url)
    command = spec.get("command")
    if isinstance(command, str) and command:
        return _derive_remote_name(command)
    args = spec.get("args")
    if isinstance(args, (list, tuple)) and args:
        return _derive_remote_name(str(args[0]))
    return "remote"


__all__ = ["ModuleConfiguration", "derive_module_configuration"]

