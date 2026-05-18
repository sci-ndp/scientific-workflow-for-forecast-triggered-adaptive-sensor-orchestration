"""Insight and diagnostic tools surfaced by the Insights MCP module."""

from __future__ import annotations

import logging
import os
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from beeai_framework.tools import AnyTool, tool as bee_tool

from scidx_streaming.agentic_mcp.utils import format_tool_output

if TYPE_CHECKING:  # pragma: no cover
    from scidx_streaming.agentic_mcp.mcps.insights.server import InsightsMCPModule


logger = logging.getLogger(__name__)


def register_insight_tools(
    module: "InsightsMCPModule",
    tool_supplier: Optional[callable] = None,
) -> List[AnyTool]:
    """Attach service discovery and diagnostic tools to the insights module."""

    client = module.client
    tools: List[AnyTool] = []

    @bee_tool()
    def describe_services() -> str:
        """Return a catalogue of MCP services and their capabilities."""

        describe = getattr(client, "describe_mcp_services", None)
        if not callable(describe):
            logger.warning("describe_mcp_services missing on client")
            return format_tool_output(
                ["Status: warning", "Service catalogue not available on this client."],
                {"status": "warning", "message": "Service catalogue not available."},
            )

        services = describe() or []
        summary = [
            "Status: success",
            f"Services discovered: {len(services)}",
        ]
        payload = {
            "status": "success",
            "services": services,
        }
        return format_tool_output(summary, payload)

    tools.append(describe_services)

    @bee_tool()
    def llm_diagnostics(include_history: bool = False) -> str:
        """Surface runtime information about the active LLM backend."""

        adapter = client.mcp  # type: ignore[attr-defined]
        toolkit = adapter.ensure_toolkit()
        agent_summary = toolkit.orchestrator.summary().get("agent", {})

        llm_info: Dict[str, Any] = {
            "configured_model": client._mcp_config.get("model") if hasattr(client, "_mcp_config") else None,  # type: ignore[attr-defined]
            "profile": client._mcp_config.get("profile") if hasattr(client, "_mcp_config") else None,  # type: ignore[attr-defined]
            "agent_default_llm": agent_summary.get("default_llm"),
            "service_endpoint": os.getenv("OLLAMA_HOST") or os.getenv("OLLAMA_BASE_URL"),
        }

        profile_details = None
        if llm_info["profile"]:
            try:
                profile_details = adapter.get_profile(llm_info["profile"])
            except Exception:  # pragma: no cover - defensive
                profile_details = None
        if profile_details is not None:
            llm_info["profile_details"] = profile_details.__dict__

        resolved_options = getattr(adapter.ensure_toolkit().orchestrator, "_agent_settings", {})
        llm_info["agent_settings"] = resolved_options

        if include_history:
            llm_info["connection_history"] = getattr(client, "_mcp_connection_history", [])

        summary = [
            "Status: success",
            f"Default LLM: {llm_info.get('agent_default_llm')}",
        ]
        payload = {
            "status": "success",
            "llm": llm_info,
        }
        return format_tool_output(summary, payload)

    tools.append(llm_diagnostics)

    @bee_tool()
    def list_llm_profiles() -> str:
        """List LLM profiles bundled with the streaming library."""

        profiles = client.mcp.list_profiles()  # type: ignore[attr-defined]
        summary = [
            "Status: success",
            f"Profiles available: {len(profiles)}",
        ]
        payload = {
            "status": "success",
            "profiles": profiles,
        }
        return format_tool_output(summary, payload)

    tools.append(list_llm_profiles)

    return tools


__all__ = ["register_insight_tools"]
