"""BeeAI tool module implementation for the catalog module."""

from __future__ import annotations

from typing import List

from beeai_framework.tools import AnyTool
from scidx_streaming.agentic_mcp.core.mcp_base import BaseMCPModule

from .tools.management import register_management_tools
from .tools.registration import register_dataset_tools


class CatalogMCPModule(BaseMCPModule):
    """BeeAI tool module exposing dataset catalog management utilities."""

    def build_tools(self) -> List[AnyTool]:
        tools: List[AnyTool] = []

        def _tool_supplier() -> List[str]:
            return [tool.name for tool in tools]

        tools.extend(register_management_tools(self, _tool_supplier))
        tools.extend(register_dataset_tools(self, _tool_supplier))
        return tools


__all__ = ["CatalogMCPModule"]
