"""Assembly of BeeAI MCP tools for the SciDX streaming client."""

from __future__ import annotations

import copy
import logging
import os
import re
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from beeai_framework.adapters.mcp.serve.server import MCPServer, MCPServerConfig
from beeai_framework.agents.requirement import RequirementAgent
from beeai_framework.tools import AnyTool
from mcp.server.fastmcp.resources import Resource

from scidx_streaming.agentic_mcp.agent import BeeAIAgentOrchestrator
from scidx_streaming.agentic_mcp.agent.orchestrator import ModuleEntry
from scidx_streaming.agentic_mcp.core import MCPStreamState
from scidx_streaming.agentic_mcp.mcps.remote.server import (
    RemoteMCPModule,
    RemoteModuleLoadError,
    RemoteModuleSpec,
)


logger = logging.getLogger(__name__)


def _unique(sequence: Iterable[str]) -> List[str]:
    seen = set()
    ordered: List[str] = []
    for item in sequence:
        if item in seen:
            continue
        seen.add(item)
        ordered.append(item)
    return ordered


def _derive_remote_name(url: str) -> str:
    """Generate a deterministic module name from a remote URL."""

    parsed = re.sub(r"[^a-zA-Z0-9/_-]+", "-", url).strip("-")
    if not parsed:
        return "remote"
    if not parsed.lower().startswith("remote"):
        parsed = f"remote-{parsed}"
    return parsed.replace("/", "-").replace("--", "-").lower()


class StreamingMCPToolkit:
    """Wires SciDX streaming capabilities into a BeeAI MCP server."""

    def __init__(
        self,
        streaming_client,
        *,
        server_name: str = "SciDX Streaming MCP",
        agent_config_path: Optional[str] = None,
        enabled_modules: Optional[Iterable[str]] = None,
        disabled_modules: Optional[Iterable[str]] = None,
        remote_modules: Optional[Iterable[Dict[str, Any]]] = None,
    ) -> None:
        self._client = streaming_client
        self._state = MCPStreamState()
        self._mcp = MCPServer(config=MCPServerConfig(name=server_name))

        self._agent_config_path = self._resolve_agent_config(agent_config_path)
        env_enabled, env_disabled = self._resolve_module_filters()
        if enabled_modules is not None:
            effective_enabled = [str(item).strip() for item in enabled_modules if str(item).strip()]
        else:
            effective_enabled = env_enabled
        if disabled_modules is not None:
            effective_disabled = [str(item).strip() for item in disabled_modules if str(item).strip()]
        else:
            effective_disabled = env_disabled
        self._orchestrator = BeeAIAgentOrchestrator(
            streaming_client,
            config_path=self._agent_config_path,
            shared_state=self._state,
            enabled_modules=effective_enabled,
            disabled_modules=effective_disabled,
        )
        self._modules = dict(self._orchestrator.load_modules(register_default_resources=True))
        self._resources = list(self._orchestrator.resources)
        self._tools = list(self._orchestrator.tools)

        base_summary = copy.deepcopy(self._orchestrator.summary())
        self._summary_payload: Dict[str, Any] = base_summary
        self._summary_modules: List[Dict[str, Any]] = list(base_summary.get("modules", []))
        self._summary_tools: List[Dict[str, Any]] = list(base_summary.get("tools", []))
        self._services: List[Dict[str, Any]] = list(base_summary.get("services", []))

        self._tool_metadata: Dict[str, Dict[str, Any]] = dict(self._orchestrator.tool_metadata)
        self._remote_specs: List[RemoteModuleSpec] = self._normalize_remote_specs(remote_modules)
        self._remote_modules: Dict[str, RemoteMCPModule] = {}

        if self._remote_specs:
            self._load_remote_modules()

        # Register resources and tools with the BeeAI MCP server.
        if self._resources:
            self._mcp.register_many(self._resources)
        if self._tools:
            self._mcp.register_many(self._tools)

        self._registered_tool_names = _unique(tool.name for tool in self._tools)
        logger.info(
            "StreamingMCPToolkit initialised server_name=%s modules=%s tools=%s resources=%s",
            server_name,
            len(self._modules),
            len(self._tools),
            len(self._resources),
        )

    @staticmethod
    def _resolve_agent_config(value: Optional[str]) -> Path:
        if value:
            path = Path(value)
            if not path.is_absolute():
                path = (Path(__file__).resolve().parent / value).resolve()
            return path
        return (Path(__file__).resolve().parent / "agent" / "agent_config.yaml").resolve()

    @staticmethod
    def _resolve_module_filters() -> tuple[Optional[List[str]], Optional[List[str]]]:
        def _split(value: Optional[str]) -> Optional[List[str]]:
            if not value:
                return None
            entries = [item.strip() for item in value.split(",")]
            entries = [item for item in entries if item]
            return entries or None

        enabled_env = os.getenv("SCIDX_MCP_MODULES")
        disabled_env = os.getenv("SCIDX_MCP_DISABLE_MODULES")
        return _split(enabled_env), _split(disabled_env)

    @staticmethod
    def _normalize_remote_specs(entries: Optional[Iterable[Dict[str, Any]]]) -> List[RemoteModuleSpec]:
        specs: List[RemoteModuleSpec] = []
        if not entries:
            return specs
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            raw = dict(entry)
            transport = str(raw.get("transport") or raw.get("type") or "").strip().lower()
            if not transport:
                if raw.get("url"):
                    transport = "http"
                elif raw.get("command") or raw.get("args"):
                    transport = "stdio"
            if transport not in {"http", "stdio"}:
                logger.warning("Ignoring remote MCP module with unsupported transport: %s", raw)
                continue

            name = str(raw.get("name") or "").strip()
            if not name:
                if transport == "http" and isinstance(raw.get("url"), str):
                    name = _derive_remote_name(str(raw["url"]))
                elif transport == "stdio":
                    command_candidate = raw.get("command")
                    if isinstance(command_candidate, str) and command_candidate.strip():
                        name = _derive_remote_name(command_candidate)
                    else:
                        args_candidate = raw.get("args")
                        if isinstance(args_candidate, (list, tuple)) and args_candidate:
                            name = _derive_remote_name(str(args_candidate[0]))
                if not name:
                    name = "remote"

            if transport == "http":
                url = str(raw.get("url") or "").strip()
                if not url:
                    logger.warning("Remote HTTP module '%s' missing url; skipping.", name)
                    continue

                headers_payload = raw.get("headers")
                headers: Optional[Dict[str, str]] = None
                if isinstance(headers_payload, dict):
                    headers = {
                        str(key): str(value)
                        for key, value in headers_payload.items()
                        if value is not None
                    } or None

                token = raw.get("token")
                if token:
                    token_value = str(token).strip()
                    if token_value:
                        if headers is None:
                            headers = {}
                        if token_value.lower().startswith("bearer "):
                            headers.setdefault("Authorization", token_value)
                        else:
                            headers.setdefault("Authorization", f"Bearer {token_value}")

                timeout_value: Optional[float] = None
                try:
                    if raw.get("timeout") is not None:
                        timeout_value = float(raw["timeout"])
                except (TypeError, ValueError):
                    timeout_value = None

                specs.append(
                    RemoteModuleSpec(
                        name=name,
                        transport="http",
                        url=url,
                        headers=headers,
                        timeout=timeout_value,
                    )
                )
                continue

            # stdio transport
            command_value = raw.get("command")
            args_value = raw.get("args")
            command: Optional[str] = None
            args: List[str] = []

            if isinstance(command_value, (list, tuple)):
                command_list = [str(item).strip() for item in command_value if str(item).strip()]
                if command_list:
                    command = command_list[0]
                    args.extend(command_list[1:])
            elif isinstance(command_value, str):
                command = command_value.strip()

            if args_value:
                if isinstance(args_value, (list, tuple)):
                    args.extend(str(arg).strip() for arg in args_value if str(arg).strip())
                elif isinstance(args_value, str):
                    args.extend(part for part in args_value.split(" ") if part)

            if command is None:
                if args:
                    command, args = args[0], args[1:]
                else:
                    logger.warning("Remote stdio module '%s' missing command; skipping.", name)
                    continue

            env_payload = raw.get("env")
            env: Dict[str, str] = {}
            if isinstance(env_payload, dict):
                env = {
                    str(key): str(value)
                    for key, value in env_payload.items()
                    if value is not None
                }

            cwd = str(raw.get("cwd") or "").strip() or None
            encoding = str(raw.get("encoding") or "").strip() or "utf-8"
            encoding_errors = str(
                raw.get("encoding_errors")
                or raw.get("encoding_error_handler")
                or "strict"
            ).strip()

            timeout_value: Optional[float] = None
            try:
                if raw.get("timeout") is not None:
                    timeout_value = float(raw["timeout"])
            except (TypeError, ValueError):
                timeout_value = None

            specs.append(
                RemoteModuleSpec(
                    name=name,
                    transport="stdio",
                    command=command,
                    args=args,
                    cwd=cwd,
                    env=env,
                    encoding=encoding,
                    encoding_errors=encoding_errors,
                    timeout=timeout_value,
                )
            )
        return specs

    def _load_remote_modules(self) -> None:
        for spec in self._remote_specs:
            if spec.name in self._modules:
                logger.warning(
                    "Remote MCP module name '%s' conflicts with an existing module; skipping.",
                    spec.name,
                )
                continue

            try:
                module = RemoteMCPModule(
                    self._client,
                    state=self._state,
                    spec=spec,
                )
            except RemoteModuleLoadError as exc:
                logger.warning(
                    "Skipping remote MCP module %s (%s): %s",
                    spec.name,
                    spec.display_target,
                    exc,
                )
                continue
            except Exception as exc:  # pragma: no cover - defensive logging
                logger.exception(
                    "Failed to initialise remote MCP module %s (%s)",
                    spec.name,
                    spec.display_target,
                )
                continue

            self._remote_modules[spec.name] = module
            module_entry = ModuleEntry(
                name=spec.name,
                tools=list(module.tools),
                resources=list(module.resources),
                config=module.config,
                shared_state=False,
                module_class=f"{module.__class__.__module__}:{module.__class__.__qualname__}",
            )
            self._modules[spec.name] = module_entry
            self._resources.extend(module.resources)
            self._tools.extend(module.tools)

            summary_module = {
                "name": spec.name,
                "class": module_entry.module_class,
                "tools": [tool.name for tool in module.tools],
                "config": module.config.metadata() if module.config else None,
                "shared_state": False,
            }
            self._summary_modules.append(summary_module)

            service_entry = module.service_entry
            if service_entry is not None:
                self._services.append(service_entry)

            for tool_name, meta in module.tool_metadata.items():
                if tool_name in self._tool_metadata:
                    logger.warning(
                        "Tool '%s' from remote module '%s' already registered; overwriting metadata.",
                        tool_name,
                        spec.name,
                    )
                self._tool_metadata[tool_name] = meta
                self._summary_tools.append({"name": tool_name, **meta})

    @property
    def server(self) -> MCPServer:
        """Expose the underlying BeeAI MCP server instance."""

        return self._mcp

    @property
    def state(self) -> MCPStreamState:
        """Return the shared MCP state object."""

        return self._state

    @property
    def orchestrator(self) -> BeeAIAgentOrchestrator:
        """Expose the BeeAI orchestrator coordinating the MCP modules."""

        return self._orchestrator

    @property
    def modules(self) -> Dict[str, object]:
        """Return the loaded module instances."""

        return dict(self._modules)

    @property
    def tools(self) -> List[AnyTool]:
        """Return the BeeAI tools currently registered with the server."""

        return list(self._tools)

    @property
    def resources(self) -> List[Resource]:
        """Return the MCP resources registered with the server."""

        return list(self._resources)

    @property
    def services(self) -> List[Dict[str, Any]]:
        """Return the high-level service catalog derived from module metadata."""

        return list(self._services)

    @property
    def tool_metadata(self) -> Dict[str, Dict[str, Any]]:
        """Mapping of tool names to orchestrator metadata for logging."""

        return dict(self._tool_metadata)

    def tool_names(self) -> List[str]:
        """Return the list of registered tool names."""

        return list(self._registered_tool_names)

    def summary(self) -> Dict[str, Any]:
        """Return a combined summary including remote modules."""

        payload = copy.deepcopy(self._summary_payload)
        payload["modules"] = list(self._summary_modules)
        payload["tools"] = list(self._summary_tools)
        payload["services"] = list(self._services)
        payload["resources"] = [str(resource.uri) for resource in self._resources]
        return payload

    def create_agent(self, **kwargs: Any) -> RequirementAgent:
        """Convenience wrapper around :meth:`BeeAIAgentOrchestrator.build_agent`."""

        return self._orchestrator.build_agent(**kwargs)

    async def shutdown(self) -> dict:
        """Stop tracked producers/consumers and return the stopped topics."""

        consumer_topics = self._state.stop_all_consumers()
        producer_topics = await self._state.stop_all_producers()
        stopped_modules: List[str] = []
        for name, module in self._remote_modules.items():
            try:
                await module.shutdown()
                stopped_modules.append(name)
            except Exception:  # pragma: no cover - defensive
                logger.debug("Shutdown for remote module %s raised.", name, exc_info=True)
        return {
            "consumers": consumer_topics,
            "producers": producer_topics,
            "remote_modules": stopped_modules,
        }


__all__ = ["StreamingMCPToolkit"]
