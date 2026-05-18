"""Utility helpers for SciDX MCP modules."""

from __future__ import annotations

import json
from typing import Any, Dict, List, Optional


def pretty_json(payload: Dict[str, Any]) -> str:
    """Serialize dictionaries in a stable, human-readable form for MCP responses."""

    return json.dumps(payload, indent=2, sort_keys=True, default=_json_default)


def format_tool_output(summary_lines: List[str], payload: Dict[str, Any]) -> "ToolOutput":
    """Construct a readable summary section followed by indented bullet details."""

    summary_lines = summary_lines or ["(none)"]
    summary_text = "Summary:\n" + "\n".join(f"- {line}" for line in summary_lines)
    detail_lines = _format_structure(payload)
    if detail_lines:
        details_text = "Details:\n" + "\n".join(detail_lines)
    else:
        details_text = "Details:\n- (empty)"
    return ToolOutput(f"{summary_text}\n\n{details_text}", payload=payload)


def _format_structure(data: Any, indent: int = 0) -> List[str]:
    """Render dictionaries/lists into nested bullet-point lines."""

    pad = "  " * indent
    lines: List[str] = []

    if isinstance(data, dict):
        if not data:
            return [f"{pad}- (empty dict)"]
        for key, value in data.items():
            label = f"{pad}- {key}:"
            if isinstance(value, (dict, list)):
                lines.append(label)
                lines.extend(_format_structure(value, indent + 1))
            else:
                lines.append(f"{label} {_stringify(value)}")
        return lines

    if isinstance(data, list):
        if not data:
            return [f"{pad}- (empty list)"]
        for index, item in enumerate(data, start=1):
            prefix = f"{pad}- [{index}]"
            if isinstance(item, (dict, list)):
                lines.append(f"{prefix}:")
                lines.extend(_format_structure(item, indent + 1))
            else:
                lines.append(f"{prefix}: {_stringify(item)}")
        return lines

    return [f"{pad}- {_stringify(data)}"]


def _stringify(value: Any) -> str:
    """Convert primitives to readable text."""

    if value is None:
        return "null"
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


class ToolOutput(str):
    """String subclass whose repr omits quotes for notebook readability."""

    def __new__(cls, value: str, *, payload: Optional[Dict[str, Any]] = None):
        sanitised = _sanitize_text(value)
        instance = super().__new__(cls, sanitised)
        instance.payload = payload
        return instance

    def __repr__(self) -> str:  # pragma: no cover - presentation helper
        return str(self)

    def _repr_pretty_(self, printer, cycle):  # pragma: no cover - IPython pretty print hook
        printer.text(str(self))


def _json_default(value: Any) -> Any:
    """Fallback serializer for objects returned by streaming actions."""

    if isinstance(value, set):
        return sorted(value)
    if isinstance(value, (list, dict, str, int, float, type(None))):
        return value
    if hasattr(value, "__dict__"):
        return value.__dict__
    return repr(value)


__all__ = ["pretty_json", "format_tool_output", "ToolOutput"]


def _sanitize_text(value: str) -> str:
    """Ensure text is safe to encode for terminal output."""

    if not isinstance(value, str):
        value = str(value)
    if not value:
        return ""
    try:
        # Fast-path for already safe strings.
        value.encode("utf-8")
        return value
    except UnicodeEncodeError:
        # Replace problematic surrogate characters so printing never fails.
        return value.encode("utf-8", "replace").decode("utf-8", "replace")
