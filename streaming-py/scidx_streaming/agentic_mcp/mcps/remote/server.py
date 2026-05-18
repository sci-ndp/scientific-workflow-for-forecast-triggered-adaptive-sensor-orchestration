"""Proxy module that bridges external MCP servers into the SciDX toolkit."""

from __future__ import annotations

import asyncio
import json
import logging
import threading
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from beeai_framework.context import RunContext
from beeai_framework.emitter.emitter import Emitter
from beeai_framework.tools.tool import Tool
from beeai_framework.tools.types import StringToolOutput, ToolRunOptions
from beeai_framework.utils.strings import to_safe_word
from mcp import types as mcp_types
from mcp.client.session import ClientSession
from mcp.client.sse import sse_client
from mcp.client.stdio import StdioServerParameters, stdio_client
from pydantic import BaseModel, ConfigDict

from scidx_streaming.agentic_mcp.core.mcp_base import BaseMCPModule, MCPModuleConfig


logger = logging.getLogger(__name__)


class RemoteModuleLoadError(RuntimeError):
    """Raised when a remote MCP endpoint cannot be initialised."""


@dataclass
class RemoteModuleSpec:
    """Normalised configuration for a remote MCP module."""

    name: str
    transport: str
    url: Optional[str] = None
    headers: Optional[Dict[str, str]] = None
    timeout: Optional[float] = None
    command: Optional[str] = None
    args: List[str] = field(default_factory=list)
    cwd: Optional[str] = None
    env: Dict[str, str] = field(default_factory=dict)
    encoding: str = "utf-8"
    encoding_errors: str = "strict"

    @property
    def display_target(self) -> str:
        if self.transport == "http" and self.url:
            return self.url
        if self.transport == "stdio" and self.command:
            return " ".join([self.command, *self.args]) if self.args else self.command
        return self.name


class RemoteToolInput(BaseModel):
    """Permissive schema that forwards arbitrary arguments."""

    model_config = ConfigDict(extra="allow")


class RemoteMCPBridge:
    """Manages a persistent MCP session against a remote server."""

    def __init__(self, spec: RemoteModuleSpec) -> None:
        self._spec = spec
        self._session: Optional[ClientSession] = None
        self._context: Optional[AbstractAsyncContextManager] = None
        self._initialise_result: Optional[mcp_types.InitializeResult] = None
        self._connect_lock = asyncio.Lock()
        self._session_lock = asyncio.Lock()

    @property
    def initialise_result(self) -> Optional[mcp_types.InitializeResult]:
        return self._initialise_result

    async def ensure_connected(self) -> None:
        if self._session is not None:
            return
        async with self._connect_lock:
            if self._session is not None:
                return
            self._context = self._build_context()
            try:
                read_stream, write_stream = await self._context.__aenter__()
            except FileNotFoundError as exc:  # pragma: no cover - depends on host setup
                self._context = None
                raise RemoteModuleLoadError(
                    f"Command not found for remote MCP module '{self._spec.name}': {exc}"
                ) from exc
            except Exception:
                self._context = None
                raise
            session = ClientSession(read_stream, write_stream)
            await session.__aenter__()
            initialise_result = await session.initialize()
            self._session = session
            self._initialise_result = initialise_result

    def _build_context(self) -> AbstractAsyncContextManager:
        if self._spec.transport == "http":
            if not self._spec.url:
                raise RemoteModuleLoadError("HTTP transport requires a 'url' field.")
            headers = dict(self._spec.headers or {})
            headers.setdefault("Accept", "text/event-stream")
            timeout = self._spec.timeout or 10.0
            return sse_client(
                self._spec.url,
                headers=headers,
                timeout=timeout,
            )

        if self._spec.transport == "stdio":
            if not self._spec.command:
                raise RemoteModuleLoadError("stdio transport requires a 'command' field.")
            params = StdioServerParameters(
                command=self._spec.command,
                args=list(self._spec.args),
                env=self._spec.env or None,
                cwd=self._spec.cwd,
                encoding=self._spec.encoding,
                encoding_error_handler=self._spec.encoding_errors,
            )
            return stdio_client(params)

        raise RemoteModuleLoadError(f"Unsupported transport '{self._spec.transport}'.")

    async def describe(self) -> Dict[str, Any]:
        await self.ensure_connected()
        async with self._session_lock:
            assert self._session is not None
            tools_result = await self._session.list_tools()
            try:
                resources_result = await self._session.list_resources()
                resources = list(resources_result.resources)
            except Exception:
                resources = []
            try:
                templates_result = await self._session.list_resource_templates()
                resource_templates = list(templates_result.resourceTemplates)
            except Exception:
                resource_templates = []
            return {
                "initialise": self._initialise_result,
                "tools": list(tools_result.tools),
                "resources": resources,
                "resource_templates": resource_templates,
            }

    async def call_tool(self, tool_name: str, arguments: Dict[str, Any]) -> mcp_types.CallToolResult:
        await self.ensure_connected()
        async with self._session_lock:
            assert self._session is not None
            try:
                return await self._session.call_tool(tool_name, arguments)
            except Exception:
                await self._reset()
                raise

    async def close(self) -> None:
        async with self._connect_lock:
            await self._reset()

    async def _reset(self) -> None:
        async with self._session_lock:
            if self._session is not None:
                try:
                    await self._session.__aexit__(None, None, None)
                except Exception:
                    logger.debug("Error while closing remote MCP session", exc_info=True)
                self._session = None
            if self._context is not None:
                try:
                    await self._context.__aexit__(None, None, None)
                except Exception:
                    logger.debug("Error while closing remote MCP transport", exc_info=True)
                self._context = None
            self._initialise_result = None


class RemoteProxyTool(Tool[RemoteToolInput, ToolRunOptions, StringToolOutput]):
    """BeeAI tool wrapper that proxies calls to a remote MCP endpoint."""

    input_schema = RemoteToolInput

    def __init__(
        self,
        *,
        descriptor: mcp_types.Tool,
        bridge: RemoteMCPBridge,
        spec: RemoteModuleSpec,
    ) -> None:
        super().__init__()
        self._descriptor = descriptor
        self._bridge = bridge
        self._spec = spec
        self._name = descriptor.name
        self._description = descriptor.description or f"Remote MCP tool '{descriptor.name}'"

    @property
    def name(self) -> str:
        return self._name

    @property
    def description(self) -> str:
        return self._description

    def _create_emitter(self) -> Emitter:
        return Emitter.root().child(
            namespace=["tool", "remote", to_safe_word(self.name)],
            creator=self,
        )

    async def _run(
        self,
        input: RemoteToolInput,
        options: ToolRunOptions | None,
        context: RunContext,
    ) -> StringToolOutput:
        arguments = input.model_dump()
        result = await self._bridge.call_tool(self._descriptor.name, arguments)
        return self._format_result(result)

    @staticmethod
    def _format_result(result: mcp_types.CallToolResult) -> StringToolOutput:
        text_lines: List[str] = []
        for block in result.content:
            block_type = getattr(block, "type", None)
            if block_type == "text":
                text_lines.append(getattr(block, "text", ""))
            elif block_type == "image":
                text_lines.append("[image]")
            elif block_type:
                text_lines.append(f"[{block_type}]")

        if not text_lines:
            text_lines.append("(remote tool returned no text output)")

        if result.structuredContent is not None:
            try:
                structured = json.dumps(result.structuredContent, indent=2, sort_keys=True, default=str)
            except Exception:  # pragma: no cover - defensive
                structured = str(result.structuredContent)
            text_lines.append("")
            text_lines.append("Structured content:")
            text_lines.append(structured)

        summary = "\n".join(line for line in text_lines if line is not None)
        if result.isError:
            summary = f"[remote error]\n{summary}"

        return StringToolOutput(summary)


def _run_blocking(coro):
    """Execute an async coroutine without assuming an active loop."""

    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coro)

    result: Dict[str, Any] = {}
    error: List[BaseException] = []
    done = threading.Event()

    def _runner():
        try:
            result["value"] = asyncio.run(coro)
        except BaseException as exc:  # noqa: BLE001
            error.append(exc)
        finally:
            done.set()

    thread = threading.Thread(target=_runner, daemon=True)
    thread.start()
    done.wait()
    if error:
        raise error[0]
    return result.get("value")


class RemoteMCPModule(BaseMCPModule):
    """Module that exposes tools from a remote MCP endpoint."""

    def __init__(
        self,
        client: Any,
        *,
        state=None,
        spec: RemoteModuleSpec,
        register_default_resources: bool = False,
    ) -> None:
        self._spec = spec
        self._bridge = RemoteMCPBridge(spec)

        try:
            descriptor = _run_blocking(self._bridge.describe())
        except RemoteModuleLoadError:
            raise
        except Exception as exc:  # noqa: BLE001
            raise RemoteModuleLoadError(
                f"Failed to contact remote MCP server at {spec.display_target}: {exc}"
            ) from exc

        self._initialise_result: Optional[mcp_types.InitializeResult] = descriptor.get("initialise")
        self._remote_tools: List[mcp_types.Tool] = descriptor.get("tools", [])

        module_config = self._build_config()

        super().__init__(
            client,
            state=state,
            config=module_config,
            register_default_resources=register_default_resources,
        )

        self._tool_metadata_entries = self._build_tool_metadata()
        self._service_entry = self._build_service_entry()

    @property
    def spec(self) -> RemoteModuleSpec:
        return self._spec

    @property
    def service_entry(self) -> Dict[str, Any]:
        return dict(self._service_entry)

    @property
    def tool_metadata(self) -> Dict[str, Dict[str, Any]]:
        return dict(self._tool_metadata_entries)

    def build_tools(self) -> List[Tool]:
        tools: List[Tool] = []
        for descriptor in self._remote_tools:
            tools.append(
                RemoteProxyTool(
                    descriptor=descriptor,
                    bridge=self._bridge,
                    spec=self._spec,
                )
            )
        return tools

    def build_additional_resources(self) -> List[Any]:
        # Remote MCP endpoints are responsible for exposing their own resources.
        return []

    async def shutdown(self) -> None:
        await self._bridge.close()

    def _build_config(self) -> MCPModuleConfig:
        server_info = getattr(self._initialise_result, "serverInfo", None)
        server_name = getattr(server_info, "name", None)
        server_version = getattr(server_info, "version", None)
        server_description = getattr(server_info, "description", None)

        extras: Dict[str, Any] = {
            "remote_transport": self._spec.transport,
            "remote_target": self._spec.display_target,
        }
        if server_info is not None:
            try:
                extras["server_info"] = server_info.model_dump(mode="json", exclude_none=True)
            except Exception:  # pragma: no cover - defensive
                extras["server_info"] = {
                    "name": server_name,
                    "version": server_version,
                }

        description = server_description or f"Remote MCP endpoint ({self._spec.display_target})"

        return MCPModuleConfig(
            name=server_name or self._spec.name.replace("-", " ").title(),
            description=description,
            server_name=server_name,
            version=server_version,
            tags=["remote", f"transport:{self._spec.transport}"],
            extras=extras,
        )

    def _build_tool_metadata(self) -> Dict[str, Dict[str, Any]]:
        metadata: Dict[str, Dict[str, Any]] = {}
        for tool in self.tools:
            metadata[tool.name] = {
                "module": self._spec.name,
                "module_class": self.__class__.__qualname__,
                "module_path": self.__class__.__module__,
                "description": getattr(tool, "description", None),
                "remote_transport": self._spec.transport,
                "remote_target": self._spec.display_target,
            }
        return metadata

    def _build_service_entry(self) -> Dict[str, Any]:
        capabilities = [
            {
                "tool": tool.name,
                "description": getattr(tool, "description", None),
                "module": self._spec.name,
            }
            for tool in self.tools
        ]

        remote_info: Dict[str, Any] = {
            "transport": self._spec.transport,
            "target": self._spec.display_target,
        }
        if self._spec.transport == "stdio":
            remote_info["command"] = self._spec.command
            if self._spec.args:
                remote_info["args"] = list(self._spec.args)
            if self._spec.cwd:
                remote_info["cwd"] = self._spec.cwd

        return {
            "service": self.config.name if self.config else self._spec.name.title(),
            "module": self._spec.name,
            "description": self.config.description if self.config else None,
            "tags": (self.config.tags if self.config else None),
            "capabilities": capabilities,
            "remote": remote_info,
        }


__all__ = ["RemoteMCPModule", "RemoteModuleLoadError", "RemoteModuleSpec"]
