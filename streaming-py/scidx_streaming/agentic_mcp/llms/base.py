"""Shared scaffolding for MCP chat clients that drive the BeeAI tooling."""

from __future__ import annotations

import json
import logging
import os
import time
from typing import Any, Callable, Dict, List, Optional

from mcp import ClientSession
from mcp.client.stdio import StdioServerParameters, stdio_client

from scidx_streaming.agentic_mcp.utils import ToolOutput


logger = logging.getLogger(__name__)


def validate_llm_payload(payload: Dict[str, Any]) -> Optional[str]:
    """Ensure a payload conforms to the expected MCP JSON schema."""

    if not isinstance(payload, dict):
        return "Response root must be a JSON object."

    allowed_keys = {"message", "tool_calls", "explanation"}
    unexpected = set(payload.keys()) - allowed_keys
    if unexpected:
        return f"Unexpected keys present: {sorted(unexpected)}."

    tool_calls = payload.get("tool_calls")
    message = payload.get("message")

    if tool_calls is None:
        if message is None:
            return "Response must include either 'message' or 'tool_calls'."
        if not isinstance(message, str):
            return "'message' must be a string when provided."
        explanation = payload.get("explanation")
        if explanation is not None and not isinstance(explanation, str):
            return "'explanation' must be a string when provided."
        return None

    if not isinstance(tool_calls, list):
        return "'tool_calls' must be an array."
    if not tool_calls:
        return "'tool_calls' cannot be empty."

    for index, entry in enumerate(tool_calls):
        if not isinstance(entry, dict):
            return f"tool_calls[{index}] must be an object."
        name = entry.get("name")
        arguments = entry.get("arguments")
        if not isinstance(name, str) or not name.strip():
            return f"tool_calls[{index}].name must be a non-empty string."
        if arguments is None:
            return f"tool_calls[{index}].arguments is required."
        if not isinstance(arguments, dict):
            return f"tool_calls[{index}].arguments must be an object."

    if message is not None and not isinstance(message, str):
        return "'message' must be a string when 'tool_calls' are present."

    explanation = payload.get("explanation")
    if explanation is not None and not isinstance(explanation, str):
        return "'explanation' must be a string when provided."

    return None


_EXPECTED_SCHEMA_DESCRIPTION = """
{
  "message": "optional string when no tool call is needed",
  "explanation": "optional string explaining tool usage",
  "tool_calls": [
    {
      "name": "required tool name string",
      "arguments": { "key": "value" }  // required object of arguments
    }
  ]
}
If no tool is required, omit tool_calls entirely and provide only {"message": "..."}.
""".strip()


class BaseStdioMCPClient:
    """Base implementation that handles stdio server plumbing and tool execution."""

    def __init__(
        self,
        *,
        system_prompt: Optional[str],
        max_history: int,
        tool_metadata: Optional[Dict[str, Dict[str, Any]]] = None,
        context_provider: Optional[Callable[[], Dict[str, Any]]] = None,
    ) -> None:
        self._system_prompt = system_prompt or _DEFAULT_SYSTEM_PROMPT
        self._max_history = max(1, int(max_history))
        self._tool_metadata = tool_metadata or {}
        self._context_provider = context_provider

        self._session: Optional[ClientSession] = None
        self._stdio_context = None
        self._tools: Any = None
        self._history: List[Dict[str, str]] = []
        self._max_schema_repairs: int = 1
        self._max_context_chars: int = 1200
        self._max_context_items: int = 5
        self._max_context_services: int = 4
        self._tool_call_counts: Dict[str, int] = {}
        self._tool_call_timestamps: Dict[str, float] = {}
        self._tool_guard_cooldown: float = 6.0
        self._max_tool_repeats: int = 1

    # ------------------------------------------------------------------
    # Connection lifecycle
    # ------------------------------------------------------------------

    async def connect(
        self,
        server_script_path: str,
        *,
        extra_env: Optional[Dict[str, str]] = None,
        python_executable: str = "python",
    ) -> None:
        """Launch the MCP server script and establish a stdio session."""

        if not os.path.exists(server_script_path):
            raise FileNotFoundError(f"MCP server script not found at '{server_script_path}'.")

        env = {**os.environ}
        if extra_env:
            env.update(extra_env)

        params = StdioServerParameters(
            command=python_executable,
            args=[server_script_path],
            env={**env, "PYTHONUNBUFFERED": "1"},
        )

        self._stdio_context = stdio_client(params)
        stdio, write = await self._stdio_context.__aenter__()
        self._session = ClientSession(stdio, write)
        await self._session.__aenter__()
        await self._session.initialize()
        self._tools = await self._session.list_tools()
        logger.debug("Connected to MCP server at %s", server_script_path)

    async def disconnect(self) -> None:
        """Terminate the MCP session and associated server process."""

        try:
            if self._session is not None:
                await self._session.__aexit__(None, None, None)
        finally:
            self._session = None

        try:
            if self._stdio_context is not None:
                await self._stdio_context.__aexit__(None, None, None)
        finally:
            self._stdio_context = None

    # ------------------------------------------------------------------
    # Chat / tool execution helpers
    # ------------------------------------------------------------------

    async def chat(self, user_message: str) -> str:
        """Send a prompt to the model and execute any requested tool calls."""

        if self._session is None:
            raise RuntimeError("MCP session is not connected. Call `connect` first.")

        self._tool_call_counts.clear()
        self._tool_call_timestamps.clear()
        self._append_history("user", user_message)
        last_error: Optional[InvalidLLMResponseError] = None

        for attempt in range(self._max_schema_repairs + 1):
            prompt = self._build_prompt()
            if attempt > 0 and last_error is not None:
                prompt = self._augment_prompt_for_retry(prompt, last_error)

            response_text = await self._invoke_model(prompt)
            try:
                final_message = await self._process_model_response(response_text)
            except InvalidLLMResponseError as exc:
                last_error = exc
                logger.warning(
                    "LLM response failed schema validation (attempt %s/%s): %s",
                    attempt + 1,
                    self._max_schema_repairs + 1,
                    exc,
                )
                if attempt >= self._max_schema_repairs:
                    if self._history and self._history[-1]["role"] == "user":
                        self._history.pop()
                    raise
                continue

            self._append_history("assistant", final_message)
            return final_message

        failure_reason = (
            f"Unable to produce a valid MCP JSON response after {self._max_schema_repairs + 1} attempts."
        )
        if last_error:
            failure_reason += f" Last error: {last_error}."
        logger.error(failure_reason)
        failure_output = ToolOutput(
            failure_reason,
            payload={
                "status": "error",
                "reason": str(last_error) if last_error else "unknown",
            },
        )
        self._append_history("assistant", failure_output)
        return failure_output

    async def call_tool(self, tool_name: str, arguments: Dict[str, Any]) -> str:
        """Invoke a specific MCP tool directly by name."""

        if self._session is None:
            raise RuntimeError("MCP session is not connected.")
        result = await self._session.call_tool(tool_name, arguments)
        if result.content:
            return ToolOutput(result.content[0].text)
        return ToolOutput("")

    async def list_available_resources(self) -> List[str]:
        """Return the URIs of resources exposed by the MCP server."""

        if self._session is None:
            raise RuntimeError("MCP session is not connected.")
        resources = await self._session.list_resources()
        return [str(entry.uri) for entry in resources.resources]

    async def read_resource(self, uri: str) -> str:
        """Fetch the text contents of a resource from the MCP server."""

        if self._session is None:
            raise RuntimeError("MCP session is not connected.")
        result = await self._session.read_resource(uri)
        if not result.contents:
            return ""
        return result.contents[0].text

    # ------------------------------------------------------------------
    # Model invocation hooks
    # ------------------------------------------------------------------

    async def _invoke_model(self, prompt: str) -> str:
        """Subclasses must implement the transport-specific inference call."""

        raise NotImplementedError

    async def _process_model_response(self, response_text: str) -> str:
        payload = self._extract_json(response_text)
        if payload is None:
            raise InvalidLLMResponseError("Response was not valid JSON.", None, response_text)

        validation_error = validate_llm_payload(payload)
        if validation_error:
            raise InvalidLLMResponseError(validation_error, payload, response_text)

        if "message" in payload:
            return ToolOutput(str(payload["message"]))

        explanation = payload.get("explanation")
        tool_calls = payload.get("tool_calls") or []
        if not tool_calls:
            return ToolOutput(payload.get("message", response_text))

        outputs: List[str] = []
        if explanation:
            outputs.append(str(explanation))

        for call in tool_calls:
            name = call.get("name")
            arguments = call.get("arguments", {})
            if not name:
                continue

            meta = self._tool_metadata.get(name) if isinstance(self._tool_metadata, dict) else None
            module_name = meta.get("module") if isinstance(meta, dict) else None
            if module_name:
                logger.info("Agent invoking MCP module '%s' tool '%s'", module_name, name)
            else:
                logger.info("Agent invoking MCP tool '%s'", name)
            logger.debug("Tool '%s' arguments: %s", name, arguments)

            guard_message = self._check_tool_guard(name, arguments)
            if guard_message:
                outputs.append(ToolOutput(guard_message))
                continue

            try:
                tool_result = await self.call_tool(name, arguments)
                outputs.append(tool_result)
                if module_name:
                    logger.info("Tool '%s' from module '%s' completed.", name, module_name)
                else:
                    logger.info("Tool '%s' completed.", name)
            except Exception as exc:  # noqa: BLE001
                logger.exception("Tool '%s' failed: %s", name, exc)
                outputs.append(f"Error calling tool '{name}': {exc}")

        return ToolOutput("\n\n".join(str(item) for item in outputs))

    def _extract_json(self, response_text: str) -> Optional[Dict[str, Any]]:
        """Parse the first JSON object from the model response."""

        candidate = response_text.strip()
        if candidate.startswith("```"):
            candidate = candidate.strip("`").strip()
        try:
            return json.loads(candidate)
        except json.JSONDecodeError:
            return None

    # ------------------------------------------------------------------
    # Prompt + history helpers
    # ------------------------------------------------------------------

    def _build_prompt(self) -> str:
        base_prompt = self._system_prompt
        tools_description = self._format_tools_for_prompt()
        context_block = self._format_context_for_prompt()

        conversation_lines: List[str] = []
        prior_messages = self._history[:-1]
        if prior_messages:
            prior_messages = prior_messages[-self._max_history * 2 :]
        for entry in prior_messages:
            role = entry.get("role", "user").upper()
            content = entry.get("content", "")
            conversation_lines.append(f"{role}: {content}")

        conversation_block = "\n".join(conversation_lines)
        if conversation_block:
            conversation_block = f"Conversation so far:\n{conversation_block}\n\n"

        sections = [base_prompt, tools_description]
        if context_block:
            sections.append(context_block)

        sections_text = "\n\n".join(sections)
        return f"{sections_text}\n\n{conversation_block}USER: {self._history[-1]['content']}"

    def _format_tools_for_prompt(self) -> str:
        if not getattr(self._tools, "tools", None):
            return "No MCP tools discovered."

        lines = ["Available MCP tools:"]
        for tool in getattr(self._tools, "tools", []):
            lines.append(f"- {tool.name}: {tool.description}")
            meta = self._tool_metadata.get(tool.name, {}) if isinstance(self._tool_metadata, dict) else {}
            module_name = meta.get("module")
            module_path = meta.get("module_path")
            if module_name:
                lines.append(f"  • Module: {module_name}")
            if module_path:
                lines.append(f"  • Implementation: {module_path}")
            schema = getattr(tool, "inputSchema", None)
            if schema and isinstance(schema, dict):
                properties = schema.get("properties", {})
                required = set(schema.get("required", []) or [])
                param_lines: List[str] = []
                for name, details in properties.items():
                    description = details.get("description") or ""
                    param_type = details.get("type")
                    default = details.get("default")
                    parts = [f"  • {name}"]
                    if param_type:
                        parts.append(f"<{param_type}>")
                    if name in required:
                        parts.append("[required]")
                    if default is not None:
                        parts.append(f"default={default}")
                    if description:
                        parts.append(f"- {description}")
                    param_lines.append(" ".join(parts))
                if param_lines:
                    lines.extend(param_lines)
            if tool.name == "register_dataset":
                lines.append("  Registration tips:")
                lines.append(
                    "  • dataset_metadata must include name, title, notes, and owner_org."
                )
                lines.append(
                    "  • methods is a list of method dicts; each entry needs type, name,"
                    " description, and a nested config dictionary."
                )
                lines.append(
                    "  • Kafka config example: {\"host\": \"broker.example\", \"port\": 9092, \"topic\": \"demo\","
                    " \"security_protocol\": \"SASL_SSL\", \"sasl_mechanism\": \"SCRAM-SHA-512\","
                    " \"auto_offset_reset\": \"latest\", \"batch_interval\": 0.2}."
                )
                lines.append(
                    "  • mapping and processing must be JSON objects (e.g., \"x\": \"coor[0]\")."
                )
        return "\n".join(lines)

    def _format_context_for_prompt(self) -> str:
        if self._context_provider is None:
            return ""

        try:
            context_payload = self._context_provider() or {}
        except Exception as exc:  # noqa: BLE001
            logger.debug("Context provider failed: %s", exc)
            return ""

        if not isinstance(context_payload, dict) or not context_payload:
            return ""

        pruned = self._prune_context(context_payload)

        try:
            serialized = json.dumps(pruned, indent=2, sort_keys=True, default=str)
        except Exception:  # noqa: BLE001
            serialized = str(pruned)

        if len(serialized) > self._max_context_chars:
            serialized = serialized[: self._max_context_chars] + " ... (truncated)"

        return f"Latest MCP context:\n{serialized}"

    def _prune_context(self, context: Dict[str, Any]) -> Dict[str, Any]:
        pruned: Dict[str, Any] = {}

        if "services" in context and isinstance(context["services"], list):
            services: List[Any] = []
            for entry in context["services"][: self._max_context_services]:
                if not isinstance(entry, dict):
                    continue
                services.append(
                    {
                        "service": entry.get("service"),
                        "module": entry.get("module"),
                        "description": entry.get("description"),
                        "capabilities": [
                            capability.get("tool")
                            for capability in (entry.get("capabilities") or [])[: self._max_context_items]
                            if isinstance(capability, dict)
                        ],
                    }
                )
            pruned["services"] = services

        state = context.get("state")
        if isinstance(state, dict):
            state_summary: Dict[str, Any] = {}
            for key in (
                "last_search_terms",
                "last_registered_dataset",
                "active_streams",
                "active_consumers",
                "producer_metadata",
                "registered_dataset_names",
            ):
                value = state.get(key)
                if isinstance(value, list):
                    state_summary[key] = value[: self._max_context_items]
                elif isinstance(value, dict) and key == "producer_metadata":
                    state_summary[key] = {
                        name: {
                            "resource_ids": details.get("resource_ids", [])[: self._max_context_items]
                        }
                        for name, details in list(value.items())[: self._max_context_items]
                        if isinstance(details, dict)
                    }
                elif value is not None:
                    state_summary[key] = value
            pruned["state"] = state_summary

        for key, value in context.items():
            if key in {"services", "state"}:
                continue
            pruned[key] = value

        return pruned

    def _append_history(self, role: str, message: str) -> None:
        self._history.append({"role": role, "content": message})

    def _augment_prompt_for_retry(self, prompt: str, error: "InvalidLLMResponseError") -> str:
        serialized_payload = ""
        if error.payload is not None:
            try:
                serialized_payload = json.dumps(error.payload, indent=2, sort_keys=True)
            except Exception:  # noqa: BLE001
                serialized_payload = str(error.payload)

        retry_lines = [
            "SYSTEM: The previous response did not match the required JSON schema.",
            f"Reason: {error}",
            "Respond again with a single JSON object matching this schema:",
            _EXPECTED_SCHEMA_DESCRIPTION,
        ]
        if serialized_payload:
            retry_lines.append("Last invalid payload:")
            retry_lines.append(serialized_payload)
        return f"{prompt}\n\n" + "\n".join(retry_lines)

    def _check_tool_guard(self, name: str, arguments: Dict[str, Any]) -> Optional[str]:
        key = self._tool_call_signature(name, arguments)
        now = time.time()
        last_time = self._tool_call_timestamps.get(key)
        count = self._tool_call_counts.get(key, 0)

        if last_time is not None and now - last_time > self._tool_guard_cooldown:
            self._tool_call_counts.pop(key, None)
            count = 0

        if count >= self._max_tool_repeats and last_time is not None and now - last_time <= self._tool_guard_cooldown:
            logger.warning(
                "Skipping tool '%s' with duplicate arguments to prevent execution loops.",
                name,
            )
            return (
                f"Tool '{name}' was recently executed with the same arguments. Skipping to avoid loops."
            )

        self._tool_call_counts[key] = count + 1
        self._tool_call_timestamps[key] = now
        return None

    @staticmethod
    def _tool_call_signature(name: str, arguments: Dict[str, Any]) -> str:
        try:
            serialized_args = json.dumps(arguments, sort_keys=True, default=str)
        except Exception:  # noqa: BLE001
            serialized_args = repr(arguments)
        return f"{name}:{serialized_args}"

_DEFAULT_SYSTEM_PROMPT = """
You are an AI operator for the SciDX streaming platform. Your job is to decide which MCP tool
should be invoked to satisfy the user's request and to provide the exact arguments. Always respond
with a plain JSON object (no code fences) using one of the following patterns:

Tool call response:
{
  "tool_calls": [
    {
      "name": "tool_name",
      "arguments": { "param": "value" }
    }
  ],
  "explanation": "Very brief explanation of the planned action"
}

Message-only response:
{
  "message": "Short message when no tool invocation is required"
}

Do not include additional keys. Keep explanations concise. If information is missing, ask the user
for clarification via the message-only format. Never output markdown or natural language outside of
the JSON envelope.

Important SciDX streaming rules when registering datasets or methods:
- `register_dataset` requires `dataset_metadata` with `name`, `title`, `notes`, and `owner_org`.
- The `methods` array must contain dictionaries. Each entry must include `type`, `name`,
  `description`, and a nested `config` dictionary. Never collapse `config` into a string.
- Kafka `config` dictionaries require `host` (string), `port` (integer), and `topic` (string).
  When the user supplies values like security protocol, SASL mechanism, offsets, or batch timing,
  add them as keys inside the same `config` dictionary. Use numeric types for numeric inputs
  (for example, `batch_interval: 0.2`).
- Any provided `mapping` or `processing` values must be JSON objects. A mapping such as
  "x=coor[0]" becomes `"x": "coor[0]"` inside the mapping dictionary.
- Leave omitted fields out of the payload instead of using nulls. Ask a clarification question if
  a required value is missing.
""".strip()


class InvalidLLMResponseError(RuntimeError):
    """Raised when an LLM response fails schema validation."""

    def __init__(self, message: str, payload: Optional[Dict[str, Any]], raw_text: str) -> None:
        super().__init__(message)
        self.payload = payload
        self.raw_text = raw_text


__all__ = ["BaseStdioMCPClient", "_DEFAULT_SYSTEM_PROMPT", "InvalidLLMResponseError", "validate_llm_payload"]
