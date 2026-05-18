"""Groq-hosted LLM client for MCP interactions."""

from __future__ import annotations

import json
import logging
from typing import Any, Callable, Dict, Optional

import requests

from .base import BaseStdioMCPClient


logger = logging.getLogger(__name__)


class GroqMCPClient(BaseStdioMCPClient):
    """Routes Groq-hosted completions into MCP tool invocations."""

    DEFAULT_MAX_OUTPUT_TOKENS = 900

    def __init__(
        self,
        api_key: str,
        *,
        model_name: str = "llama3-8b-8192",
        system_prompt: Optional[str] = None,
        max_history: int = 8,
        api_base: str = "https://api.groq.com/openai/v1",
        temperature: float = 0.0,
        max_output_tokens: Optional[int] = None,
        request_timeout: float = 60.0,
        tool_metadata: Optional[Dict[str, Dict[str, object]]] = None,
        context_provider: Optional[Callable[[], Dict[str, Any]]] = None,
    ) -> None:
        if not api_key:
            raise ValueError("Groq API key is required to initialize the MCP client.")

        super().__init__(
            system_prompt=system_prompt,
            max_history=max_history,
            tool_metadata=tool_metadata,
            context_provider=context_provider,
        )

        self._api_key = api_key
        self._model_name = model_name
        self._temperature = temperature
        self._max_output_tokens = (
            int(max_output_tokens)
            if max_output_tokens is not None
            else self.DEFAULT_MAX_OUTPUT_TOKENS
        )
        self._request_timeout = request_timeout
        self._api_base = api_base.rstrip("/")
        self._http = requests.Session()

    async def disconnect(self) -> None:
        await super().disconnect()
        self._http.close()

    async def _invoke_model(self, prompt: str) -> str:
        payload: Dict[str, object] = {
            "model": self._model_name,
            "messages": self._build_messages(prompt),
            "temperature": self._temperature,
            "response_format": {"type": "json_object"},
            "stream": False,
        }
        payload["max_tokens"] = self._max_output_tokens

        headers = {
            "Authorization": f"Bearer {self._api_key}",
            "Content-Type": "application/json",
        }
        url = f"{self._api_base}/chat/completions"
        resp = self._http.post(url, headers=headers, json=payload, timeout=self._request_timeout)
        resp.raise_for_status()
        data = resp.json()
        try:
            return data["choices"][0]["message"]["content"]
        except (KeyError, IndexError) as exc:
            raise RuntimeError(f"Unexpected Groq response: {data}") from exc

    def _build_messages(self, prompt: str) -> list[Dict[str, str]]:
        messages: list[Dict[str, str]] = [{"role": "system", "content": self._system_prompt}]
        history_slice = self._history[-self._max_history * 2 :]
        for entry in history_slice:
            role = entry.get("role", "user")
            content = entry.get("content", "")
            messages.append({"role": role, "content": content})
        messages.append({"role": "user", "content": prompt})
        return messages

    def _extract_json(self, response_text: str):
        candidate = response_text
        if "</think>" in candidate:
            candidate = candidate.split("</think>", 1)[1]
        candidate = candidate.strip()
        if candidate.startswith("```"):
            candidate = candidate.strip("`").strip()
        try:
            return json.loads(candidate)
        except json.JSONDecodeError:
            return None


__all__ = ["GroqMCPClient"]
