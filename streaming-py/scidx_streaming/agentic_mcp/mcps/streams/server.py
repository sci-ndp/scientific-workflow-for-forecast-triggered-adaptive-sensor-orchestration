"""BeeAI tool module implementation for stream producer operations."""

from __future__ import annotations

from typing import List

from beeai_framework.tools import AnyTool
from scidx_streaming.agentic_mcp.core.mcp_base import BaseMCPModule

from .tools.producers import register_producer_tools


class StreamsMCPModule(BaseMCPModule):
    """BeeAI tool module exposing Kafka stream creation and producer tooling."""

    def build_tools(self) -> List[AnyTool]:
        return list(register_producer_tools(self))


__all__ = ["StreamsMCPModule"]
