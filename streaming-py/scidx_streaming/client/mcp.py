"""Helpers that connect a `StreamingClient` to the BeeAI MCP tooling layer."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

try:  # pragma: no cover - optional dependency already bundled elsewhere
    import yaml
except Exception:  # pragma: no cover
    yaml = None  # type: ignore[assignment]

from beeai_framework.agents.requirement import RequirementAgent

from scidx_streaming.agentic_mcp.llms import (
    build_client as build_llm_client,
    list_profiles as list_llm_profiles,
    load_profile as load_llm_profile,
    profile_exists,
)
from scidx_streaming.agentic_mcp.toolkit import StreamingMCPToolkit


class StreamingClientMCPAdapter:
    """High-level helper that wires a `StreamingClient` to the BeeAI MCP toolkit."""

    def __init__(self, streaming_client: "StreamingClient") -> None:
        self._client = streaming_client
        self._toolkit: Optional[StreamingMCPToolkit] = None
        self._enabled_modules: Optional[List[str]] = None
        self._disabled_modules: Optional[List[str]] = None
        self._remote_modules: Optional[List[Dict[str, str]]] = None

    # ------------------------------------------------------------------
    # Toolkit/Server accessors
    # ------------------------------------------------------------------

    def ensure_toolkit(self) -> StreamingMCPToolkit:
        if self._toolkit is None:
            self._toolkit = StreamingMCPToolkit(
                self._client,
                enabled_modules=self._enabled_modules,
                disabled_modules=self._disabled_modules,
                remote_modules=self._remote_modules,
            )
        return self._toolkit

    def get_adapter(self) -> "StreamingClientMCPAdapter":
        """Return self. Provided for ergonomic parity with README examples."""

        return self

    @property
    def toolkit(self) -> StreamingMCPToolkit:
        return self.ensure_toolkit()

    @property
    def server(self):
        return self.toolkit.server

    @property
    def orchestrator(self):
        """Expose the BeeAI orchestrator coordinating the MCP modules."""

        return self.toolkit.orchestrator

    def summary(self) -> Dict[str, object]:
        """Return the orchestrator summary describing loaded modules."""

        toolkit = self.ensure_toolkit()
        return toolkit.summary()

    def tool_names(self) -> List[str]:
        return self.toolkit.tool_names()

    def create_agent(self, **kwargs) -> RequirementAgent:
        """Instantiate a BeeAI agent bound to the registered tools."""

        return self.toolkit.create_agent(**kwargs)

    # ------------------------------------------------------------------
    # Convenience helpers
    # ------------------------------------------------------------------

    def server_script_path(self) -> str:
        """Return the absolute path to the packaged BeeAI MCP server script."""

        return str(Path(__file__).resolve().parents[1] / "agentic_mcp" / "server.py")

    async def shutdown(self) -> Dict[str, List[str]]:
        """Stop tracked producers/consumers if the toolkit was used."""

        if self._toolkit is None:
            return {"consumers": [], "producers": []}
        toolkit = self._toolkit
        self._toolkit = None
        return await toolkit.shutdown()

    def describe_services(self) -> List[Dict[str, Any]]:
        """Return the high-level MCP services and their capabilities."""

        toolkit = self.ensure_toolkit()
        return toolkit.services

    def list_profiles(self) -> List[str]:
        """Return the names of bundled LLM profiles."""

        return list_llm_profiles()

    def get_profile(self, name: Optional[str]):
        if not name or not profile_exists(name):
            return None
        return load_llm_profile(name)

    def build_llm_client(
        self,
        profile_name: Optional[str],
        provider_override: Optional[str],
        config: Dict[str, object],
    ) -> Tuple[object, str]:
        profile = self.get_profile(profile_name)
        toolkit = self.ensure_toolkit()
        client, python_executable = build_llm_client(
            profile=profile,
            provider_override=provider_override,
            config=config,
            toolkit=toolkit,
            tool_metadata=toolkit.tool_metadata,
        )
        return client, python_executable

    # ------------------------------------------------------------------
    # Module filter helpers
    # ------------------------------------------------------------------

    def configure_modules(
        self,
        enabled: Optional[Sequence[str]],
        disabled: Optional[Sequence[str]],
    ) -> None:
        self._enabled_modules = list(enabled) if enabled else None
        # When only enabled modules are provided, treat it as an explicit whitelist.
        if enabled and not disabled:
            self._disabled_modules = None
        else:
            self._disabled_modules = list(disabled) if disabled else None
        # Force toolkit re-initialisation with the new filters on next use.
        self._toolkit = None

    def configure_remote_modules(
        self,
        remotes: Optional[Sequence[Dict[str, Any]]],
    ) -> None:
        self._remote_modules = [dict(entry) for entry in remotes] if remotes else None
        self._toolkit = None

    def available_modules(self) -> List[str]:
        """Return the module names declared in the default agent configuration."""

        if yaml is None:  # pragma: no cover - defensive fallback
            raise RuntimeError("PyYAML is required to inspect available modules.")

        config_path = StreamingMCPToolkit._resolve_agent_config(None)
        with config_path.open("r", encoding="utf-8") as handle:
            payload = yaml.safe_load(handle) or {}

        modules = []
        for idx, entry in enumerate(payload.get("modules") or []):
            if isinstance(entry, dict):
                name = entry.get("name") or f"module_{idx}"
                modules.append(str(name))
        return modules

    def module_filters(self) -> Tuple[Optional[List[str]], Optional[List[str]]]:
        """Return the currently configured module allow/deny lists."""

        enabled = list(self._enabled_modules) if self._enabled_modules else None
        disabled = list(self._disabled_modules) if self._disabled_modules else None
        return enabled, disabled

    async def stop_all(self) -> None:
        """Backward compatible alias for :meth:`shutdown`."""

        await self.shutdown()


# Deferred import to avoid circular dependency in type checking
from typing import TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover
    from scidx_streaming.client.init_client import StreamingClient
