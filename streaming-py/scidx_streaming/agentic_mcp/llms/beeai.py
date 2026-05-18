"""BeeAI-native client that delegates prompts to a RequirementAgent."""

from __future__ import annotations

import logging
import os
from typing import Any, Dict, List, Optional

from beeai_framework.agents.requirement import RequirementAgent

from scidx_streaming.agentic_mcp.toolkit import StreamingMCPToolkit
from scidx_streaming.agentic_mcp.utils import ToolOutput
from scidx_streaming.agentic_mcp.llms.base import validate_llm_payload


logger = logging.getLogger(__name__)


class BeeAIAgentMCPClient:
    """Thin wrapper that reuses the BeeAI RequirementAgent instead of an external LLM."""

    def __init__(
        self,
        toolkit: StreamingMCPToolkit,
        *,
        llm: Optional[str],
        llm_options: Optional[Dict[str, Any]] = None,
        system_prompt: Optional[str] = None,
    ) -> None:
        self._toolkit = toolkit
        self._llm = llm
        self._llm_options = dict(llm_options or {})
        self._system_prompt = system_prompt
        self._agent: Optional[RequirementAgent] = None
        logger.info("BeeAI MCP client configured llm=%s", self._llm)

    @staticmethod
    def _sanitize_endpoint(value: str) -> str:
        """Normalise Ollama endpoints, trimming trailing '/api' segments if present."""

        trimmed = value.strip()
        if not trimmed:
            return trimmed
        # Remove trailing API paths (users sometimes pass /api/tags).
        for suffix in ("/api/tags", "/api"):
            if trimmed.lower().endswith(suffix):
                trimmed = trimmed[: -len(suffix)]
        return trimmed.rstrip("/")

    def _resolved_llm_options(self) -> Dict[str, Any]:
        """Merge profile options with environment fallbacks for local Ollama setups."""

        resolved = dict(self._llm_options)

        # Look for explicit environment overrides when not supplied in the profile.
        env_candidates = [
            os.getenv("OLLAMA_BASE_URL"),
            os.getenv("OLLAMA_HOST"),
            os.getenv("OLLAMA_API_BASE"),
        ]
        fallback: Optional[str] = None
        for candidate in env_candidates:
            if candidate and candidate.strip():
                fallback = candidate.strip()
                break

        if fallback:
            fallback = self._sanitize_endpoint(fallback)

        if fallback:
            resolved.setdefault("base_url", fallback)
            resolved.setdefault("api_base", fallback)

        # Sanitise any provided URLs to avoid trailing /api/tags mistakes.
        for key in ("base_url", "api_base"):
            if key in resolved and isinstance(resolved[key], str):
                resolved[key] = self._sanitize_endpoint(resolved[key])

        endpoint_for_env = resolved.get("api_base") or resolved.get("base_url") or fallback
        if endpoint_for_env:
            os.environ["OLLAMA_HOST"] = endpoint_for_env
            os.environ.setdefault("OLLAMA_BASE_URL", endpoint_for_env)
            os.environ.setdefault("OLLAMA_API_BASE", endpoint_for_env)
            logger.info("BeeAI MCP using Ollama endpoint %s", endpoint_for_env)

        logger.debug("Resolved BeeAI llm options: %s", resolved)
        return resolved

    async def connect(self, *_, **__) -> None:
        """Instantiate the RequirementAgent using the configured toolkit."""

        agent_kwargs: Dict[str, Any] = {}
        if self._llm is not None:
            agent_kwargs["llm"] = self._llm
        resolved_options = self._resolved_llm_options()
        if resolved_options:
            agent_kwargs["llm_options"] = resolved_options
        if self._system_prompt:
            agent_kwargs["system_prompt"] = self._system_prompt

        self._agent = self._toolkit.create_agent(**agent_kwargs)
        active_model = agent_kwargs.get("llm")
        logger.info("BeeAI MCP using model %s", active_model)
        logger.debug(
            "Initialised BeeAI RequirementAgent llm=%s tools=%s",
            agent_kwargs.get("llm") or "default",
            len(self._toolkit.tools),
        )

    async def disconnect(self) -> None:
        """Release the agent reference."""

        self._agent = None

    async def chat(self, user_message: str) -> str:
        """Forward the prompt to the RequirementAgent and return its final answer."""

        if self._agent is None:
            raise RuntimeError("BeeAI agent is not connected. Call `connect` first.")

        result = await self._agent.run(user_message)
        output = getattr(result, "output", None)

        if isinstance(output, list) and output:
            message = str(output[0])
        elif isinstance(output, str):
            message = output
        elif hasattr(result, "final_answer"):
            message = str(result.final_answer)
        else:
            message = str(result)

        payload = {"message": message}
        validation_error = validate_llm_payload(payload)
        if validation_error:
            logger.error("BeeAI agent returned an invalid response: %s", validation_error)
            failure = ToolOutput(
                f"BeeAI agent produced an unsupported response: {validation_error}",
                payload={"status": "error", "reason": validation_error},
            )
            return failure

        return ToolOutput(message, payload=payload)

    async def call_tool(self, tool_name: str, arguments: Dict[str, Any]) -> str:
        raise RuntimeError(
            "Direct tool execution is not supported for the BeeAI provider. "
            "Use `StreamingMCPToolkit` or the RequirementAgent directly."
        )

    async def list_available_resources(self) -> List[str]:
        return [str(resource.uri) for resource in self._toolkit.resources]

    async def read_resource(self, uri: str) -> str:
        raise RuntimeError(
            "Resource access is not handled via the BeeAI provider. "
            "Call `toolkit.resources` directly if needed."
        )


__all__ = ["BeeAIAgentMCPClient"]
