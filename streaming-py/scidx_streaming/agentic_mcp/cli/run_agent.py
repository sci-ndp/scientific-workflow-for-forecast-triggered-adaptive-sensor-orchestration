"""CLI entry point to inspect the BeeAI orchestrator configuration."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Dict, Optional

from ndp_ep import APIClient

from scidx_streaming.agentic_mcp.agent import BeeAIAgentOrchestrator
from scidx_streaming.client.init_client import StreamingClient


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Inspect the BeeAI orchestrator and optionally override the default LLM.",
    )
    parser.add_argument(
        "--config",
        default=Path(__file__).resolve().parents[1] / "agent" / "agent_config.yaml",
        help="Path to the agent configuration YAML file.",
    )
    parser.add_argument("--api-url", required=True, help="Streaming API base URL.")
    parser.add_argument("--token", required=True, help="Authentication token for the API.")
    parser.add_argument(
        "--llm",
        default=None,
        help="Override the agent's default LLM identifier (e.g., 'ollama:granite3.3:8b', 'openai:gpt-4o-mini').",
    )
    parser.add_argument(
        "--print-summary",
        action="store_true",
        help="Print a JSON summary of the orchestrator setup instead of the human-readable banner.",
    )
    return parser.parse_args()


def _build_client(args: argparse.Namespace) -> StreamingClient:
    pop_client = APIClient(base_url=args.api_url, token=args.token)
    return StreamingClient(pop_client)


def _print_banner(summary: Dict[str, Any], llm_override: Optional[str]) -> None:
    agent = summary.get("agent") or {}
    modules = summary.get("modules") or []
    print("=== BeeAI Streaming Agent ===")
    print(f"Agent name: {agent.get('name')}")
    description = agent.get("description")
    if description:
        print(f"Description: {description}")
    role = agent.get("role")
    if role:
        print(f"Role: {role}")

    default_llm = llm_override or agent.get("default_llm")
    if default_llm:
        print(f"LLM: {default_llm}")

    print(f"Modules loaded: {len(modules)}")
    for entry in modules:
        name = entry.get("name")
        tools = entry.get("tools") or []
        print(f" - {name} ({len(tools)} tools)")
    print("Use --print-summary to view the full JSON payload.")


def main() -> None:
    args = _parse_args()
    config_path = Path(args.config).resolve()

    client = _build_client(args)
    orchestrator = BeeAIAgentOrchestrator(client, config_path=config_path)
    summary = orchestrator.summary()

    # Instantiate the agent to validate configuration (no prompt execution).
    orchestrator.build_agent(llm=args.llm)

    if args.print_summary:
        print(json.dumps(summary, indent=2, sort_keys=True))
    else:
        _print_banner(summary, args.llm)


if __name__ == "__main__":
    main()
