"""Registered MCP module packages."""

from .catalog.server import CatalogMCPModule
from .filtering.server import FilteringMCPModule
from .insights.server import InsightsMCPModule
from .streams.server import StreamsMCPModule
from .earthscope.server import EarthScopeMCPModule

__all__ = [
    "CatalogMCPModule",
    "FilteringMCPModule",
    "StreamsMCPModule",
    "InsightsMCPModule",
    "EarthScopeMCPModule",
]
