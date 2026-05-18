"""Factory helpers to instantiate MCP chat clients from profile settings."""

from __future__ import annotations

import os
from typing import Any, Callable, Dict, Optional, Tuple

from scidx_streaming.agentic_mcp.toolkit import StreamingMCPToolkit

from .beeai import BeeAIAgentMCPClient
from .config import LLMProfile
from .gemini import GeminiMCPClient
from .groq import GroqMCPClient


def build_client(
    *,
    profile: Optional[LLMProfile],
    provider_override: Optional[str],
    config: Dict[str, Any],
    toolkit: StreamingMCPToolkit,
    tool_metadata: Dict[str, Dict[str, Any]],
) -> Tuple[object, str]:
    """Create an MCP chat client and return it along with the python executable to use."""

    provider = _resolve_provider(profile, provider_override)
    python_executable = _resolve_python_executable(profile, config)
    context_provider = _build_context_provider(toolkit)

    if provider == "gemini":
        api_key = _resolve_value("api_key", profile, config)
        if not api_key:
            raise RuntimeError(
                "Gemini provider selected but no API key supplied. "
                "Set GEMINI_API_KEY or pass api_key=... to configure_mcp()."
            )
        model_name = config.get("model") or (profile.model if profile else None) or "models/gemini-2.5-flash"
        system_prompt = config.get("system_prompt") or (profile.system_prompt if profile else None)
        max_history = int(
            config.get("max_history")
            or (profile.options.get("max_history") if profile else None)
            or 8
        )
        max_output_tokens = config.get("max_output_tokens")
        if max_output_tokens is None and profile and "max_output_tokens" in profile.options:
            max_output_tokens = profile.options["max_output_tokens"]
        client = GeminiMCPClient(
            api_key,
            model_name=model_name,
            system_prompt=system_prompt,
            max_history=max_history,
            tool_metadata=tool_metadata,
            max_output_tokens=None if max_output_tokens is None else int(max_output_tokens),
            context_provider=context_provider,
        )
        return client, python_executable

    if provider == "groq":
        api_key = _resolve_value("api_key", profile, config)
        if not api_key:
            raise RuntimeError(
                "Groq provider selected but no API key supplied. "
                "Set GROQ_API_KEY or pass api_key=... to configure_mcp()."
            )
        model_name = config.get("model") or (profile.model if profile else None) or "llama3-8b-8192"
        system_prompt = config.get("system_prompt") or (profile.system_prompt if profile else None)
        max_history = int(
            config.get("max_history")
            or (profile.options.get("max_history") if profile else None)
            or 8
        )
        api_base = config.get("api_base") or (profile.options.get("api_base") if profile else None) or "https://api.groq.com/openai/v1"
        temperature = float(
            config.get("temperature")
            or (profile.options.get("temperature") if profile else None)
            or 0.0
        )
        max_output_tokens = config.get("max_output_tokens")
        if max_output_tokens is None and profile and "max_output_tokens" in profile.options:
            max_output_tokens = profile.options["max_output_tokens"]
        request_timeout = float(
            config.get("request_timeout")
            or (profile.options.get("request_timeout") if profile else None)
            or 60.0
        )
        client = GroqMCPClient(
            api_key,
            model_name=model_name,
            system_prompt=system_prompt,
            max_history=max_history,
            api_base=api_base,
            temperature=temperature,
            max_output_tokens=None if max_output_tokens is None else int(max_output_tokens),
            request_timeout=request_timeout,
            tool_metadata=tool_metadata,
            context_provider=context_provider,
        )
        return client, python_executable

    if provider in {"beeai", "bee-ai"}:
        llm_name = config.get("model") or (profile.model if profile else None)
        llm_options: Dict[str, Any] = {}
        if profile:
            llm_options.update(profile.options)
        # Allow snake_case overrides for options.
        for key in ("temperature", "max_history", "api_base", "request_timeout", "max_output_tokens"):
            if key in config and config[key] is not None:
                llm_options[key] = config[key]
        system_prompt = config.get("system_prompt") or (profile.system_prompt if profile else None)
        client = BeeAIAgentMCPClient(
            toolkit,
            llm=llm_name,
            llm_options=llm_options,
            system_prompt=system_prompt,
        )
        return client, python_executable

    raise ValueError(f"Unsupported MCP provider '{provider}'.")


def _resolve_provider(profile: Optional[LLMProfile], override: Optional[str]) -> str:
    value = override or (profile.provider if profile else None)
    if not value:
        raise ValueError("No MCP provider configured. Supply provider=... or select a named profile.")
    return str(value).strip().lower()


def _resolve_python_executable(profile: Optional[LLMProfile], config: Dict[str, Any]) -> str:
    return (
        config.get("python_executable")
        or (profile.python_executable if profile and profile.python_executable else None)
        or "python"
    )


def _resolve_value(key: str, profile: Optional[LLMProfile], config: Dict[str, Any]) -> Optional[str]:
    if key in config and config[key]:
        return str(config[key])
    if not profile:
        return None
    env_mapping = profile.env.get(key)
    if not env_mapping:
        return None
    if isinstance(env_mapping, str):
        return os.getenv(env_mapping)
    # Allow dict entries like {"env": "MY_VAR", "default": "value"}
    if isinstance(env_mapping, dict):
        env_name = env_mapping.get("env")
        default = env_mapping.get("default")
        if env_name:
            return os.getenv(env_name, default)
        return default
    return None


def _build_context_provider(toolkit: StreamingMCPToolkit) -> Callable[[], Dict[str, Any]]:
    def _provider() -> Dict[str, Any]:
        context: Dict[str, Any] = {
            "services": toolkit.services,
            "state": toolkit.state.snapshot(),
        }
        return context

    return _provider


__all__ = ["build_client"]
