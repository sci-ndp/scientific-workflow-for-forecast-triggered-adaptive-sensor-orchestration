"""BeeAI tool module implementation for stream filtering utilities."""

from __future__ import annotations

from typing import List

from beeai_framework.tools import AnyTool
from scidx_streaming.agentic_mcp.core.mcp_base import BaseMCPModule

from .tools.streaming import register_streaming_tools


class FilteringMCPModule(BaseMCPModule):
    """BeeAI tool module focused on stream consumption and filtering workflows."""

    def build_tools(self) -> List[AnyTool]:
        tools: List[AnyTool] = []

        def _tool_supplier() -> List[str]:
            return [tool.name for tool in tools]

        tools.extend(register_streaming_tools(self, _tool_supplier))
        return tools


__all__ = ["FilteringMCPModule"]
