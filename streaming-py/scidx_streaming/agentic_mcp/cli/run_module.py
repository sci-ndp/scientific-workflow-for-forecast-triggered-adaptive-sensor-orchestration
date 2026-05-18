"""CLI helper to instantiate a single MCP module for development."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Dict, Optional

from ndp_ep import APIClient

from scidx_streaming.agentic_mcp.core import BaseMCPModule, MCPModuleConfig, import_from_string
from scidx_streaming.client.init_client import StreamingClient


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Instantiate a single MCP module and print its configuration details."
    )
    parser.add_argument(
        "module",
        help="Module class path in the form 'package.module:ClassName'.",
    )
    parser.add_argument(
        "--config",
        default=None,
        help="Optional path to the module configuration YAML file.",
    )
    parser.add_argument("--api-url", required=True, help="Streaming API base URL.")
    parser.add_argument("--token", required=True, help="Authentication token for the API.")
    parser.add_argument(
        "--print-config",
        action="store_true",
        help="Emit the resolved module configuration as JSON.",
    )
    parser.add_argument(
        "--list-tools",
        action="store_true",
        help="Print the tool names registered on the module.",
    )
    return parser.parse_args()


def _build_client(args: argparse.Namespace) -> StreamingClient:
    pop_client = APIClient(base_url=args.api_url, token=args.token)
    return StreamingClient(pop_client)


def _summarize(module: BaseMCPModule) -> Dict[str, Any]:
    config: Optional[MCPModuleConfig] = getattr(module, "config", None)
    payload: Dict[str, Any] = {
        "class": module.__class__.__module__ + ":" + module.__class__.__qualname__,
        "tool_count": len(module.tool_names),
        "tools": list(module.tool_names),
        "resource_count": len(module.resources),
    }
    if config:
        payload["config"] = config.metadata()
    return payload


def main() -> None:
    args = _parse_args()
    module_cls = import_from_string(args.module)
    config_path = Path(args.config).resolve() if args.config else None

    client = _build_client(args)
    module = module_cls.from_config(client, config_path=config_path)

    summary = _summarize(module)
    print(f"Loaded module {summary['class']}")
    print(f"Tools registered: {summary['tool_count']}")
    print(f"Resources registered: {summary['resource_count']}")

    if args.list_tools:
        print("Tool names:")
        for name in summary["tools"]:
            print(f" - {name}")

    if args.print_config and summary.get("config"):
        print("Configuration:")
        print(json.dumps(summary["config"], indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
