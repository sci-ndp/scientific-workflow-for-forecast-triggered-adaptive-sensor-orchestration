"""Shared abstractions for BeeAI MCP modules built on the official BeeAI framework."""

from __future__ import annotations

from dataclasses import dataclass, field
from importlib import import_module
from pathlib import Path
from types import ModuleType
from typing import Any, Dict, List, Optional, Sequence, Type, TypeVar

try:  # pragma: no cover - optional dependency
    import yaml
except Exception:  # pragma: no cover
    yaml = None  # type: ignore[assignment]

from beeai_framework.tools import AnyTool
from mcp.server.fastmcp.resources import Resource

from scidx_streaming.agentic_mcp.core.state import MCPStreamState
from scidx_streaming.agentic_mcp.core.resources import build_default_resources

_T = TypeVar("_T", bound="BaseMCPModule")


def _load_yaml(path: Path) -> Dict[str, Any]:
    """Load a YAML file into a dictionary."""

    if yaml is None:  # pragma: no cover - defensive fallback
        raise RuntimeError(
            "PyYAML is required to load MCP module configurations. Install `pyyaml`."
        )
    with path.open("r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle) or {}
    if not isinstance(data, dict):
        raise ValueError(f"Expected mapping in {path}, received {type(data)!r}.")
    return data


@dataclass
class MCPModuleConfig:
    """Lightweight configuration container for MCP modules."""

    name: str
    description: str
    server_name: Optional[str] = None
    version: Optional[str] = None
    tags: List[str] = field(default_factory=list)
    dependencies: List[str] = field(default_factory=list)
    extras: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_path(cls, path: Path) -> "MCPModuleConfig":
        payload = _load_yaml(path)
        extras = {k: v for k, v in payload.items() if k not in {"name", "description", "server_name", "version", "tags", "dependencies"}}
        return cls(
            name=str(payload.get("name") or path.stem),
            description=str(payload.get("description") or ""),
            server_name=payload.get("server_name"),
            version=payload.get("version"),
            tags=list(payload.get("tags") or []),
            dependencies=list(payload.get("dependencies") or []),
            extras=extras,
        )

    def metadata(self) -> Dict[str, Any]:
        """Return a dict representation suitable for logging or introspection."""

        payload: Dict[str, Any] = {
            "name": self.name,
            "description": self.description,
        }
        if self.version:
            payload["version"] = self.version
        if self.server_name:
            payload["server_name"] = self.server_name
        if self.tags:
            payload["tags"] = list(self.tags)
        if self.dependencies:
            payload["dependencies"] = list(self.dependencies)
        if self.extras:
            payload["extras"] = dict(self.extras)
        return payload


class BaseMCPModule:
    """Base implementation for BeeAI tool modules used by the streaming orchestrator."""

    config: Optional[MCPModuleConfig]
    client: Any
    state: MCPStreamState
    tool_names: List[str]

    def __init__(
        self,
        client: Any,
        *,
        state: Optional[MCPStreamState] = None,
        config: Optional[MCPModuleConfig] = None,
        register_default_resources: bool = True,
    ) -> None:
        self.client = client
        self.state = state or MCPStreamState()
        self.config = config
        self.tool_names = []

        if register_default_resources:
            self._resources = build_default_resources(self.client, self.state)
        else:
            self._resources = []

        self._resources.extend(self.build_additional_resources())
        self._tools = list(self.build_tools())
        self.tool_names = [tool.name for tool in self._tools]

    # ------------------------------------------------------------------
    # Hooks for subclasses
    # ------------------------------------------------------------------

    def build_tools(self) -> Sequence[AnyTool]:
        """Create the BeeAI tools exposed by this module."""

        raise NotImplementedError("Subclasses must implement build_tools().")

    def build_additional_resources(self) -> Sequence[Resource]:
        """Optional hook for subclasses that expose extra MCP resources."""

        return []

    # ------------------------------------------------------------------
    # Accessors
    # ------------------------------------------------------------------

    @property
    def tools(self) -> List[AnyTool]:
        """Return the instantiated BeeAI tools for this module."""

        return list(self._tools)

    @property
    def resources(self) -> List[Resource]:
        """Return the MCP resources contributed by this module."""

        return list(self._resources)

    @classmethod
    def load_config(cls, config_path: Optional[Path]) -> Optional[MCPModuleConfig]:
        """Load a config from disk if provided."""

        if config_path is None:
            return None
        return MCPModuleConfig.from_path(config_path)

    @classmethod
    def from_config(
        cls: Type[_T],
        client: Any,
        *,
        state: Optional[MCPStreamState] = None,
        config_path: Optional[Path] = None,
        register_default_resources: bool = True,
    ) -> _T:
        """Instantiate the module loading configuration data when available."""

        config = cls.load_config(config_path)
        return cls(
            client,
            state=state,
            config=config,
            register_default_resources=register_default_resources,
        )


def import_from_string(path: str) -> Type[BaseMCPModule]:
    """Resolve a ``module:attribute`` string into a class object."""

    if ":" not in path:
        raise ValueError(f"Invalid import path '{path}'. Expected format 'module:attribute'.")
    module_name, attribute = path.split(":", 1)
    module: ModuleType = import_module(module_name)
    target = getattr(module, attribute, None)
    if target is None:
        raise AttributeError(f"Module '{module_name}' does not define '{attribute}'.")
    if not issubclass(target, BaseMCPModule):
        raise TypeError(f"{path} does not resolve to a BaseMCPModule subclass.")
    return target


__all__ = [
    "BaseMCPModule",
    "MCPModuleConfig",
    "import_from_string",
]
