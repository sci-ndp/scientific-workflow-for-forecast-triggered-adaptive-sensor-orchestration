"""Profile loader for configured LLM/MCP backends."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

try:  # pragma: no cover - optional dependency
    import yaml
except Exception:  # pragma: no cover
    yaml = None  # type: ignore[assignment]


_PROFILES_DIR = Path(__file__).resolve().parent / "profiles"


@dataclass
class LLMProfile:
    """Describes an MCP backend configuration stored on disk."""

    name: str
    provider: str
    model: Optional[str] = None
    system_prompt: Optional[str] = None
    options: Dict[str, Any] = field(default_factory=dict)
    env: Dict[str, str] = field(default_factory=dict)
    python_executable: Optional[str] = None
    description: Optional[str] = None


def profiles_dir() -> Path:
    """Return the directory where profile YAML files live."""

    return _PROFILES_DIR


def profile_path(name: str) -> Path:
    """Return the resolved path for a given profile name."""

    filename = f"{name}.yaml" if not name.endswith(".yaml") else name
    return profiles_dir() / filename


def list_profiles() -> List[str]:
    """List available profile names (without extensions)."""

    if not profiles_dir().exists():
        return []
    entries: Iterable[Path] = profiles_dir().glob("*.yaml")
    return sorted(path.stem for path in entries if path.is_file())


def profile_exists(name: str) -> bool:
    """Return True if a profile YAML file exists for ``name``."""

    return profile_path(name).is_file()


def load_profile(name: str) -> LLMProfile:
    """Load the profile configuration from disk."""

    if yaml is None:  # pragma: no cover - defensive fallback
        raise RuntimeError("PyYAML is required to load MCP LLM profiles. Install `pyyaml`.")

    path = profile_path(name)
    if not path.is_file():
        raise FileNotFoundError(f"Unknown LLM profile '{name}'. Expected YAML at {path}.")

    with path.open("r", encoding="utf-8") as handle:
        payload = yaml.safe_load(handle) or {}

    if not isinstance(payload, dict):
        raise ValueError(f"Profile {path} must contain a mapping.")

    provider = payload.get("provider")
    if not provider:
        raise ValueError(f"Profile {path} is missing required key 'provider'.")

    options = dict(payload.get("options") or {})
    # Allow backwards compatible top-level overrides.
    for key in ("max_history", "temperature", "max_output_tokens", "request_timeout", "api_base"):
        if key in payload and key not in options:
            options[key] = payload[key]

    return LLMProfile(
        name=path.stem,
        provider=str(provider),
        model=payload.get("model"),
        system_prompt=payload.get("system_prompt") or payload.get("prompt"),
        options=options,
        env=dict(payload.get("env") or {}),
        python_executable=payload.get("python_executable"),
        description=payload.get("description"),
    )


__all__ = [
    "LLMProfile",
    "load_profile",
    "list_profiles",
    "profile_exists",
    "profile_path",
    "profiles_dir",
]
