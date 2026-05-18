"""Resource endpoints exposed via BeeAI MCP modules."""

from __future__ import annotations

import json
from typing import Any, Callable, List

from mcp.server.fastmcp.resources import FunctionResource, Resource

from .state import MCPStreamState

REGISTRATION_HELP = (
    "Dataset metadata must include 'name', 'title', 'notes', and 'owner_org'. "
    "Each consumption method is a dict with 'type', 'name', 'description', and a 'config' section (mandatory per type) plus optional 'mapping' and 'processing' dictionaries. "
    "See resource scidx://registration-guide for the full table."
)


def _function_resource(
    uri: str,
    fn: Callable[[], str],
    *,
    title: str | None = None,
    description: str | None = None,
) -> FunctionResource:
    return FunctionResource.from_function(fn, uri=uri, title=title, description=description)


def build_default_resources(client: object, state: MCPStreamState) -> List[Resource]:
    """Create the shared status/documentation resources for the toolkit."""

    def get_status() -> str:
        status = {
            "user_id": getattr(client, "user_id", None),
            "kafka_host": getattr(client, "KAFKA_HOST", None),
            "kafka_port": getattr(client, "KAFKA_PORT", None),
            "state": state.snapshot(),
        }
        return json.dumps(status, indent=2, sort_keys=True, default=str)

    def get_registration_guide() -> str:
        guide = {
            "summary": REGISTRATION_HELP,
            "dataset_metadata": {
                "required": ["name", "title", "notes", "owner_org"],
                "optional": "Standard CKAN dataset fields (tags, groups, extras, etc.)",
            },
            "consumption_method": {
                "required": ["type", "name", "description", "config"],
                "optional": ["mapping", "processing"],
                "types": {
                    "csv/txt": {
                        "config": {"url": "HTTP(S) location"},
                        "processing": {
                            "delimiter": "Field separator (default tab/autodetect)",
                            "header_line": "Zero-based header row index",
                            "start_line": "First data row index",
                        },
                    },
                    "json": {
                        "config": {"url": "HTTP(S) endpoint returning JSON"},
                        "processing": {
                            "data_key": "Path to records",
                            "info_key": "Path merged into stream_info",
                            "additional_key": "Path appended to stream_info.additional_info",
                        },
                    },
                    "stream": {
                        "config": {
                            "url": "Streaming endpoint",
                            "batch_mode": "'True'|'False' to treat payloads columnar",
                            "time_window": "Idle flush timeout (s)",
                            "batch_interval": "Minimum flush interval (s)",
                        },
                        "processing": {"data_key": "Path to records"},
                    },
                    "kafka": {
                        "config": {
                            "host": "Bootstrap host",
                            "port": "Bootstrap port",
                            "topic": "Source topic",
                            "security_protocol": "Optional protocol",
                            "sasl_mechanism": "Optional SASL mechanism",
                            "auto_offset_reset": "earliest (default) or latest",
                            "time_window": "Idle timeout",
                            "batch_interval": "Flush interval",
                        },
                        "processing": {"data_key": "Nested message path"},
                    },
                    "netcdf": {
                        "config": {"url": "HTTP(S) URL to .nc/.nc4", "group": "Optional HDF5 group"},
                    },
                    "rss": {
                        "config": {"url": "RSS/Atom feed URL"},
                        "processing": {
                            "fetch_mode": "continuous or once",
                            "poll_interval": "Seconds between polls",
                            "duration": "Total seconds to poll",
                        },
                    },
                },
                "notes": [
                    "mapping applies to every type; omit to keep original keys",
                    "processing must be a dict when provided; unknown keys are ignored",
                    "Re-registering a dataset slug is idempotent; new methods are merged",
                    "Use search_consumption_methods to verify resources before streaming",
                ],
            },
        }
        return json.dumps(guide, indent=2, sort_keys=True)

    return [
        _function_resource(
            "scidx://status",
            get_status,
            title="SciDX Streaming Status",
            description="Snapshot of the current SciDX streaming client, Kafka configuration, and MCP state cache.",
        ),
        _function_resource(
            "scidx://registration-guide",
            get_registration_guide,
            title="SciDX Dataset Registration Guide",
            description="Reference documentation describing required fields for registering datasets and consumption methods.",
        ),
    ]


__all__ = ["build_default_resources", "REGISTRATION_HELP"]
