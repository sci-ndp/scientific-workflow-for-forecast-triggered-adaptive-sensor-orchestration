"""CLI entrypoint to expose SciDX streaming capabilities via the BeeAI MCP server."""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys
from typing import List, Optional

from ndp_ep import APIClient

from scidx_streaming.agentic_mcp.toolkit import StreamingMCPToolkit
from scidx_streaming.client.init_client import StreamingClient

logger = logging.getLogger(__name__)


def _configure_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(level=level, format="[%(levelname)s] %(name)s: %(message)s")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Start the SciDX agentic BeeAI MCP server",
    )
    parser.add_argument(
        "--api-url",
        dest="api_url",
        default=os.getenv("SCIDX_API_URL"),
        help="Base URL for the SciDX API (env: SCIDX_API_URL)",
    )
    parser.add_argument(
        "--token",
        dest="token",
        default=os.getenv("SCIDX_API_TOKEN"),
        help="Authentication token for the SciDX API (env: SCIDX_API_TOKEN)",
    )
    parser.add_argument(
        "--server-name",
        dest="server_name",
        default=os.getenv("SCIDX_MCP_SERVER_NAME", "SciDX Streaming MCP"),
        help="Display name for the MCP server (env: SCIDX_MCP_SERVER_NAME)",
    )
    parser.add_argument(
        "--agent-config",
        dest="agent_config",
        default=os.getenv("SCIDX_MCP_AGENT_CONFIG"),
        help="Optional path to an alternate agent_config.yaml",
    )
    parser.add_argument(
        "--modules",
        dest="modules",
        default=None,
        help="Comma-separated list of MCP modules to load (overrides SCIDX_MCP_MODULES).",
    )
    parser.add_argument(
        "--disable-modules",
        dest="disable_modules",
        default=None,
        help="Comma-separated list of MCP modules to disable (overrides SCIDX_MCP_DISABLE_MODULES).",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose debug logging",
    )
    return parser


def _create_streaming_client(api_url: Optional[str], token: Optional[str]) -> StreamingClient:
    if not token:
        raise SystemExit("Missing SciDX API token. Provide --token or set SCIDX_API_TOKEN.")
    if not api_url:
        raise SystemExit("Missing SciDX API URL. Provide --api-url or set SCIDX_API_URL.")

    api_client = APIClient(base_url=api_url, token=token)
    return StreamingClient(api_client)


def main(argv: Optional[list[str]] = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    _configure_logging(args.verbose)

    def _split_modules(value: Optional[str]) -> Optional[List[str]]:
        if not value:
            return None
        entries = [item.strip() for item in value.split(",")]
        entries = [item for item in entries if item]
        return entries or None

    try:
        streaming_client = _create_streaming_client(args.api_url, args.token)
    except Exception as exc:  # pragma: no cover
        logger.error("Failed to initialise streaming client: %s", exc)
        return 1

    enabled_modules = _split_modules(args.modules)
    disabled_modules = _split_modules(args.disable_modules)

    toolkit = StreamingMCPToolkit(
        streaming_client,
        server_name=args.server_name,
        agent_config_path=args.agent_config,
        enabled_modules=enabled_modules,
        disabled_modules=disabled_modules,
    )
    server = toolkit.server

    logger.info(
        "Starting BeeAI MCP server '%s' (base_url=%s, user_id=%s)",
        args.server_name,
        streaming_client.base_url,
        getattr(streaming_client, "user_id", "unknown"),
    )

    try:
        server.serve()
    finally:
        logger.info("Shutting down tracked producers/consumers")
        try:
            asyncio.run(toolkit.shutdown())
        except Exception as exc:  # pragma: no cover
            logger.debug("Cleanup raised: %s", exc)

    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
