"""BeeAI orchestrator for coordinating the streaming MCP tool modules."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set

try:  # pragma: no cover - optional dependency
    import yaml
except Exception:  # pragma: no cover
    yaml = None  # type: ignore[assignment]

from beeai_framework.agents.requirement import RequirementAgent
from beeai_framework.backend import ChatModel
from beeai_framework.tools import AnyTool
from mcp.server.fastmcp.resources import Resource

from scidx_streaming.agentic_mcp.core.mcp_base import (
    BaseMCPModule,
    MCPModuleConfig,
    import_from_string,
)
from scidx_streaming.agentic_mcp.core.state import MCPStreamState


logger = logging.getLogger(__name__)


def _load_yaml(path: Path) -> Dict[str, Any]:
    if yaml is None:  # pragma: no cover - defensive fallback
        raise RuntimeError("PyYAML is required to load agent configuration files.")
    with path.open("r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle) or {}
    if not isinstance(data, dict):
        raise ValueError(f"Agent config at {path} must be a mapping.")
    return data


@dataclass
class ModuleSpec:
    """Configuration details for a module entry in the agent configuration."""

    name: str
    class_path: str
    config_path: Optional[Path]
    shared_state: bool = True


@dataclass
class ModuleEntry:
    """Runtime metadata for a loaded module."""

    name: str
    tools: List[AnyTool]
    resources: List[Resource]
    config: Optional[MCPModuleConfig]
    shared_state: bool
    module_class: str


class BeeAIAgentOrchestrator:
    """High-level builder that wires BeeAI tools, resources, and agents together."""

    def __init__(
        self,
        client: Any,
        *,
        config_path: Path,
        shared_state: Optional[MCPStreamState] = None,
        enabled_modules: Optional[Iterable[str]] = None,
        disabled_modules: Optional[Iterable[str]] = None,
    ) -> None:
        self._client = client
        self._config_path = config_path
        self._config_dir = config_path.parent
        self._config_payload = _load_yaml(config_path)
        self._shared_state = shared_state or MCPStreamState()
        config_enabled = self._normalize_module_filter(self._config_payload.get("enabled_modules"))
        config_disabled = self._normalize_module_filter(self._config_payload.get("disabled_modules"))
        passed_enabled = self._normalize_module_filter(enabled_modules)
        passed_disabled = self._normalize_module_filter(disabled_modules)

        self._enabled_modules = passed_enabled if passed_enabled is not None else config_enabled
        self._disabled_modules = self._merge_module_filters(config_disabled, passed_disabled)

        self._module_specs = self._parse_module_specs(
            self._config_payload.get("modules") or []
        )
        self._modules: Dict[str, ModuleEntry] = {}
        self._tools: List[AnyTool] = []
        self._resources: List[Resource] = []
        self._tool_registry: Dict[str, Dict[str, Any]] = {}
        self._agent_settings = self._normalize_agent_settings(self._config_payload)

    # ------------------------------------------------------------------
    # Configuration parsing helpers
    # ------------------------------------------------------------------

    def _parse_module_specs(self, raw: Iterable[Dict[str, Any]]) -> List[ModuleSpec]:
        specs: List[ModuleSpec] = []
        for idx, item in enumerate(raw):
            if not isinstance(item, dict):
                raise ValueError(f"Module entry #{idx} must be a mapping.")
            name = str(item.get("name") or f"module_{idx}")
            normalized_name = name.lower()
            if self._enabled_modules and normalized_name not in self._enabled_modules:
                logger.info("Skipping module '%s' (not in enabled_modules).", name)
                continue
            if self._disabled_modules and normalized_name in self._disabled_modules:
                logger.info("Skipping module '%s' (disabled).", name)
                continue
            class_path = item.get("class") or item.get("entrypoint")
            if not class_path:
                raise ValueError(f"Module '{name}' is missing a 'class' attribute.")
            config_path = item.get("config")
            resolved_config: Optional[Path] = None
            if config_path:
                candidate = Path(config_path)
                if not candidate.is_absolute():
                    candidate = (self._config_dir / candidate).resolve()
                resolved_config = candidate
            specs.append(
                ModuleSpec(
                    name=name,
                    class_path=str(class_path),
                    config_path=resolved_config,
                    shared_state=bool(item.get("shared_state", True)),
                )
            )
        return specs

    @staticmethod
    def _normalize_agent_settings(config: Dict[str, Any]) -> Dict[str, Any]:
        """Merge top-level agent keys with optional nested ``agent`` section."""

        agent_cfg = config.get("agent")
        if isinstance(agent_cfg, dict):
            settings = dict(agent_cfg)
        else:
            settings = {}

        # Allow backward compatible top-level entries.
        for key in (
            "name",
            "description",
            "tags",
            "role",
            "instructions",
            "notes",
            "default_llm",
            "llm",
            "requirements",
            "tool_call_checker",
            "final_answer_as_tool",
            "save_intermediate_steps",
            "templates",
        ):
            if key in config and key not in settings:
                settings[key] = config[key]
        return settings

    @staticmethod
    def _normalize_module_filter(raw: Optional[Iterable[str] | str]) -> Optional[Set[str]]:
        if raw is None:
            return None
        if isinstance(raw, str):
            candidates = [item.strip() for item in raw.split(",")]
        else:
            candidates = []
            for item in raw:
                if item is None:
                    continue
                candidates.append(str(item).strip())
        normalized = {item.lower() for item in candidates if item}
        return normalized or None

    @staticmethod
    def _merge_module_filters(
        primary: Optional[Set[str]],
        secondary: Optional[Set[str]],
    ) -> Optional[Set[str]]:
        if primary and secondary:
            return primary.union(secondary)
        return primary or secondary

    # ------------------------------------------------------------------
    # Module loading / aggregation
    # ------------------------------------------------------------------

    def load_modules(
        self,
        *,
        register_default_resources: bool = True,
    ) -> Dict[str, BaseMCPModule]:
        """Instantiate and cache tool modules declared in the agent config."""

        if self._modules:
            return self.modules

        include_defaults = register_default_resources
        for spec in self._module_specs:
            module_cls = import_from_string(spec.class_path)
            state = self._shared_state if spec.shared_state else MCPStreamState()
            module = module_cls.from_config(
                self._client,
                state=state,
                config_path=spec.config_path,
                register_default_resources=include_defaults,
            )
            include_defaults = False

            module_class_name = f"{module.__class__.__module__}:{module.__class__.__qualname__}"
            entry = ModuleEntry(
                name=spec.name,
                tools=list(module.tools),
                resources=list(module.resources),
                config=getattr(module, "config", None),
                shared_state=spec.shared_state,
                module_class=module_class_name,
            )
            self._modules[spec.name] = entry
            logger.debug(
                "Loaded module name=%s class=%s tools=%s resources=%s shared_state=%s",
                spec.name,
                module.__class__.__qualname__,
                len(module.tools),
                len(module.resources),
                spec.shared_state,
            )
            for tool in entry.tools:
                self._tools.append(tool)
                self._tool_registry[tool.name] = {
                    "module": spec.name,
                    "module_class": module.__class__.__qualname__,
                    "module_path": module.__class__.__module__,
                    "description": getattr(tool, "description", None),
                }
            self._resources.extend(entry.resources)

            # Allow the module instance to be garbage-collected; tools retain the required state.
            del module

        return self.modules

    # ------------------------------------------------------------------
    # Public accessors
    # ------------------------------------------------------------------

    @property
    def modules(self) -> Dict[str, BaseMCPModule]:
        """Return loaded modules keyed by their configuration names."""

        return dict(self._modules)

    @property
    def tools(self) -> List[AnyTool]:
        """Return the aggregated BeeAI tools exposed by all modules."""

        if not self._modules:
            self.load_modules()
        return list(self._tools)

    @property
    def resources(self) -> List[Resource]:
        """Return the MCP resources contributed by the modules."""

        if not self._modules:
            self.load_modules()
        return list(self._resources)

    @property
    def tool_metadata(self) -> Dict[str, Dict[str, Any]]:
        """Return metadata describing where each tool originated."""

        if not self._modules:
            self.load_modules()
        return dict(self._tool_registry)

    # ------------------------------------------------------------------
    # Agent construction
    # ------------------------------------------------------------------

    def _resolve_llm(
        self,
        llm: ChatModel | str | None,
        *,
        llm_options: Optional[Dict[str, Any]] = None,
    ) -> ChatModel:
        settings = self._agent_settings
        llm_cfg = settings.get("llm")
        llm_name: Optional[str] = None

        if isinstance(llm, ChatModel):
            return llm

        if isinstance(llm, str):
            llm_name = llm
            options: Dict[str, Any] = {}
        else:
            options = {}
            if isinstance(llm_cfg, str):
                llm_name = llm_cfg
            elif isinstance(llm_cfg, dict):
                llm_name = llm_cfg.get("model") or llm_cfg.get("name") or settings.get("default_llm")
                options = llm_cfg.get("options") or {}
            else:
                llm_name = settings.get("default_llm")

            if llm_name is None:
                llm_name = self._config_payload.get("default_llm")

            if llm is not None and not isinstance(llm, str):
                raise TypeError("Unsupported llm override type. Provide a string model name or ChatModel instance.")

        if llm_options:
            options = {**options, **llm_options}

        if not isinstance(llm, ChatModel):
            if not llm_name:
                raise ValueError(
                    "Unable to resolve an LLM model. Provide llm=... or configure 'default_llm'/'agent.llm' in agent_config.yaml."
                )
            resolved_model = ChatModel.from_name(llm_name, options=options or None)
            logger.debug("Resolved BeeAI LLM model=%s options=%s", llm_name, options)
            return resolved_model
        return llm

    def build_agent(
        self,
        *,
        llm: ChatModel | str | None = None,
        llm_options: Optional[Dict[str, Any]] = None,
        extra_tools: Optional[Sequence[AnyTool]] = None,
        **overrides: Any,
    ) -> RequirementAgent:
        """Create a BeeAI RequirementAgent wired with the configured tools."""

        self.load_modules()
        model = self._resolve_llm(llm, llm_options=llm_options)

        agent_kwargs: Dict[str, Any] = {
            "llm": model,
            "tools": list(self.tools) + list(extra_tools or []),
        }

        settings = dict(self._agent_settings)
        name = overrides.pop("name", None) or settings.pop("name", None) or self._config_payload.get("name")
        description = (
            overrides.pop("description", None)
            or settings.pop("description", None)
            or self._config_payload.get("description")
        )

        if name:
            agent_kwargs["name"] = name
        if description:
            agent_kwargs["description"] = description

        for key in ("role", "instructions", "notes", "requirements", "tool_call_checker", "final_answer_as_tool", "save_intermediate_steps", "templates"):
            if key in overrides:
                agent_kwargs[key] = overrides.pop(key)
            elif key in settings:
                agent_kwargs[key] = settings.pop(key)

        if overrides:
            agent_kwargs.update(overrides)

        logger.info(
            "Creating BeeAI RequirementAgent name=%s llm=%s tools=%s",
            agent_kwargs.get("name"),
            getattr(model, "model", getattr(model, "id", None)),
            len(agent_kwargs["tools"]),
        )
        return RequirementAgent(**agent_kwargs)

    # ------------------------------------------------------------------
    # Introspection helpers
    # ------------------------------------------------------------------

    def summary(self) -> Dict[str, Any]:
        """Return a consumable structure describing the orchestrator setup."""

        modules = self.modules or self.load_modules()
        tool_metadata = self.tool_metadata
        payload: Dict[str, Any] = {
            "agent": {
                "name": (
                    self._agent_settings.get("name")
                    or self._config_payload.get("name")
                    or "BeeAI Streaming Agent"
                ),
                "description": self._agent_settings.get("description") or self._config_payload.get("description"),
                "role": self._agent_settings.get("role"),
                "instructions": self._agent_settings.get("instructions"),
                "notes": self._agent_settings.get("notes"),
                "tags": self._agent_settings.get("tags") or self._config_payload.get("tags"),
                "default_llm": (
                    self._agent_settings.get("llm", {}).get("model")
                    if isinstance(self._agent_settings.get("llm"), dict)
                    else self._agent_settings.get("llm")
                )
                or self._agent_settings.get("default_llm")
                or self._config_payload.get("default_llm"),
            },
            "modules": [],
            "tools": [],
            "resources": [str(resource.uri) for resource in self.resources],
            "services": [],
        }

        for name, entry in modules.items():
            config: Optional[MCPModuleConfig] = entry.config
            tool_names = [tool.name for tool in entry.tools]
            module_entry = {
                "name": name,
                "class": entry.module_class,
                "tools": tool_names,
                "config": config.metadata() if config else None,
                "shared_state": entry.shared_state,
            }
            payload["modules"].append(module_entry)

            service_name = (config.name if config else None) or name.title()
            service_description = config.description if config else None
            service_tags = config.tags if config else None
            capabilities: List[Dict[str, Any]] = []
            for tool_name in tool_names:
                meta = tool_metadata.get(tool_name, {})
                capabilities.append(
                    {
                        "tool": tool_name,
                        "description": meta.get("description"),
                        "module": meta.get("module"),
                    }
                )
            payload["services"].append(
                {
                    "service": service_name,
                    "module": name,
                    "description": service_description,
                    "tags": service_tags,
                    "capabilities": capabilities,
                }
            )

        for tool_name, meta in tool_metadata.items():
            payload["tools"].append({"name": tool_name, **meta})

        return payload


__all__ = ["BeeAIAgentOrchestrator"]
