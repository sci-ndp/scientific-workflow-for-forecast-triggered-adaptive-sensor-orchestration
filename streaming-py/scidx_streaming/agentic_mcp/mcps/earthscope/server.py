"""EarthScope demo MCP module."""

from __future__ import annotations

from typing import List

from beeai_framework.tools import AnyTool

from scidx_streaming.agentic_mcp.core.mcp_base import BaseMCPModule

from .tools.earthscope_tools import register_earthscope_tools


class EarthScopeMCPModule(BaseMCPModule):
    """Pre-configured set of tools for the EarthScope demonstration."""

    def build_tools(self) -> List[AnyTool]:
        tools: List[AnyTool] = []

        def _tool_supplier() -> List[str]:
            return [tool.name for tool in tools]

        tools.extend(register_earthscope_tools(self, _tool_supplier))
        return tools


__all__ = ["EarthScopeMCPModule"]
