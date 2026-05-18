"""Gemini-powered MCP client utilities for SciDX streaming."""

from __future__ import annotations

import logging
import warnings
from typing import Any, Callable, Dict, Optional

with warnings.catch_warnings():
    warnings.filterwarnings(
        "ignore",
        message="All support for the `google.generativeai` package has ended.*",
        category=FutureWarning,
    )
    import google.generativeai as genai

from .base import BaseStdioMCPClient


logger = logging.getLogger(__name__)


class GeminiMCPClient(BaseStdioMCPClient):
    """Routes Gemini responses through the MCP tool invocation layer."""

    DEFAULT_MAX_OUTPUT_TOKENS = 1024

    def __init__(
        self,
        api_key: str,
        *,
        model_name: str = "models/gemini-2.5-flash",
        system_prompt: Optional[str] = None,
        max_history: int = 8,
        tool_metadata: Optional[Dict[str, Dict[str, object]]] = None,
        max_output_tokens: Optional[int] = None,
        context_provider: Optional[Callable[[], Dict[str, Any]]] = None,
    ) -> None:
        if not api_key:
            raise ValueError("Gemini API key is required to initialize the MCP client.")

        super().__init__(
            system_prompt=system_prompt,
            max_history=max_history,
            tool_metadata=tool_metadata,
            context_provider=context_provider,
        )

        genai.configure(api_key=api_key)

        self._max_output_tokens = (
            int(max_output_tokens)
            if max_output_tokens is not None
            else self.DEFAULT_MAX_OUTPUT_TOKENS
        )
        generation_config = {
            "response_mime_type": "application/json",
            "max_output_tokens": self._max_output_tokens,
        }
        try:
            self._model = genai.GenerativeModel(model_name, generation_config=generation_config)
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError(f"Failed to initialise Gemini model '{model_name}': {exc}") from exc
        self._chat = self._model.start_chat(history=[])

    async def _invoke_model(self, prompt: str) -> str:
        try:
            response = self._chat.send_message(
                prompt,
                generation_config={"max_output_tokens": self._max_output_tokens},
                stream=False,
            )
        except Exception as exc:  # noqa: BLE001
            logger.error("Gemini model failed to respond: %s", exc)
            raise
        return response.text or ""


__all__ = ["GeminiMCPClient"]
