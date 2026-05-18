"""BeeAI tool module providing service catalog and diagnostics."""

from __future__ import annotations

from typing import List

from beeai_framework.tools import AnyTool

from scidx_streaming.agentic_mcp.core.mcp_base import BaseMCPModule

from .tools.insights import register_insight_tools


class InsightsMCPModule(BaseMCPModule):
    """Expose service discovery and diagnostic helpers for the MCP stack."""

    def build_tools(self) -> List[AnyTool]:
        tools: List[AnyTool] = []

        def _tool_names() -> List[str]:
            return list(self.tool_names)

        tools.extend(register_insight_tools(self, _tool_names))
        return tools


__all__ = ["InsightsMCPModule"]
