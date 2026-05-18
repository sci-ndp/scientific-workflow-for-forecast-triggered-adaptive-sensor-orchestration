import logging
import asyncio
from confluent_kafka.admin import AdminClient
from scidx_streaming.exceptions import HTTPException
from .producer import Producer
import json as _json

logger = logging.getLogger(__name__)

async def create_stream(self, payload):
    """
    Create a new stream for the user from specific consumption method IDs.

    payload:
      - consumption_method_ids: List[str] (required)
      - filter_semantics: List[str]
      - username: Optional[str]
      - password: Optional[str]
      - server: str

    Returns
    -------
    (producer, wrappers_for_patch)
        producer : Producer (receives a FLAT list of parsed methods)
        wrappers_for_patch : list of dataset dicts, each with exactly one resource (the matched method),
                             preserved for callers that want to register derived resources back to CKAN.
    """
    id_list = payload.get("consumption_method_ids") or []
    if not id_list:
        logger.error("No consumption_method_ids provided.")
        raise HTTPException(status_code=400, detail="consumption_method_ids is required")

    server = payload.get("server", "local")
    logger.info("Resolving resources by IDs=%s on server=%s", id_list, server)

    # Resolve to:
    #   - resources_for_producer: flat list of parsed method JSONs
    #   - wrappers_for_patch: dataset wrappers (each with a single matched resource)
    resources_for_producer, wrappers_for_patch = await _resolve_resources_flat_and_wrappers(
        self, id_list, server=server
    )
    if not resources_for_producer:
        logger.error("No methods resolved from the provided IDs.")
        raise HTTPException(status_code=404, detail="No methods found for provided IDs")

    # Get the next available stream ID
    stream_id = get_next_stream_id(self)
    logger.info("Assigned stream ID %s for new Kafka stream.", stream_id)

    username = payload.get("username")
    password = payload.get("password")

    # IMPORTANT: Producer receives the FLAT parsed methods
    producer = Producer(
        streaming_client=self,
        filter_semantics=payload.get("filter_semantics", []),
        data_streams=resources_for_producer,  # <-- flat list of method dicts
        stream_id=stream_id,
        username=username,
        password=password,
    )

    asyncio.create_task(safe_producer_run(producer))
    logger.info("Stream created with Kafka topic: %s", producer.data_stream_id)

    logger.debug("Resolved method IDs sent to Producer: %s", [m.get("id") for m in resources_for_producer])

    # Return producer + wrappers (wrappers are NOT used by Producer; only for caller's patching needs)
    return producer, wrappers_for_patch


async def _resolve_resources_flat_and_wrappers(self, id_list, server="local"):
    """
    Resolve given resource IDs into:
      - resources_for_producer: flat list of parsed resource JSONs (exact shape you want to send to Producer).
      - wrappers_for_patch: minimal dataset entries (id/name/notes/extras) each containing the matched resource
                            (used only if caller wants to write derived entries back).

    Strategy:
      - Enumerate datasets via advanced_search({"server": ...}).
      - Short-circuit once all desired IDs are found.
      - Parse resource.description JSON exactly like search_consumption_methods.
    """
    try:
        datasets = self.advanced_search({"server": server}) or []
    except Exception as e:
        logger.error("advanced_search(server=%s) failed: %s", server, e)
        raise HTTPException(status_code=500, detail="Failed to enumerate datasets for resolution")

    wanted = set(id_list)
    found = set()

    resources_for_producer = []  # flat list of dicts: {id, type, name, description, config, mapping, processing}
    wrappers_for_patch = []      # dataset wrappers (for caller patching only)

    for ds in datasets:
        ds_resources = ds.get("resources") or []
        if not ds_resources:
            continue

        for res in ds_resources:
            rid = res.get("id")
            if rid not in wanted:
                continue

            # Parse JSON from description (same semantics as search_consumption_methods)
            desc_text = res.get("description") or ""
            parsed = {}
            if desc_text:
                try:
                    parsed = _json.loads(desc_text)
                    if not isinstance(parsed, dict):
                        parsed = {"value": parsed}
                except Exception as ex:
                    logger.debug("Failed to parse description JSON for resource %s: %s", rid, ex)
                    parsed = {"raw_description": desc_text}

            # Ensure the expected fields exist
            # type/name/description/config/mapping/processing come from parsed JSON
            # Guarantee type & name at least
            fmt = (res.get("format") or "").lower()
            parsed_type = (parsed.get("type") or fmt)
            parsed["type"] = str(parsed_type).lower()
            parsed["name"] = parsed.get("name") or res.get("name") or ""


            # Build the FLAT resource for Producer (no synthetic 'url')
            flat_entry = {"id": rid}
            flat_entry.update(parsed)
            flat_entry["type"] = (flat_entry.get("type") or fmt).lower()
            resources_for_producer.append(flat_entry)


            # Build a minimal wrapper for optional patching (keep it, but Producer won't use it)
            # We also add a normalized resource with a 'format' key only for potential external use.
            normalized_type = (parsed.get("type") or fmt or "").lower()
            normalized_res = {
                "id": rid,
                "name": res.get("name", parsed.get("name", "")),
                "format": normalized_type,                 # keep format lowercase
                "type": normalized_type,                   # explicit lowercase type
                **({"description": parsed.get("description")} if parsed.get("description") else {}),
                **({"config": parsed.get("config")} if parsed.get("config") else {}),
                **({"mapping": parsed.get("mapping")} if parsed.get("mapping") else {}),
                **({"processing": parsed.get("processing")} if parsed.get("processing") else {}),
            }


            extras = ds.get("extras", {}) or {}
            extras = {
                **extras,
                "source_resource_id": rid,
                "source_dataset": ds.get("name"),
                "source_type": normalized_res["format"],
            }

            wrapper = {
                "id": ds.get("id"),
                "name": ds.get("name"),
                "title": ds.get("title"),
                "notes": ds.get("notes"),
                "extras": extras,
                "resources": [normalized_res],
            }
            wrappers_for_patch.append(wrapper)

            found.add(rid)
            logger.info("Resolved resource %s in dataset %s (%s)", rid, ds.get("name"), ds.get("id"))

            if found == wanted:
                break
        if found == wanted:
            break

    missing = wanted - found
    if missing:
        logger.error("Consumption method IDs not found: %s", ", ".join(sorted(missing)))
        raise HTTPException(status_code=404, detail=f"Consumption method IDs not found: {', '.join(sorted(missing))}")

    logger.info("Resolved %d/%d resources.", len(resources_for_producer), len(id_list))
    return resources_for_producer, wrappers_for_patch


def get_kafka_topics_with_prefix(self):
    """Retrieve all Kafka topics starting with the specified prefix."""
    try:
        admin_client = AdminClient({'bootstrap.servers': f"{self.KAFKA_HOST}:{self.KAFKA_PORT}"})
        topics = admin_client.list_topics(timeout=10).topics.keys()
        return [topic for topic in topics if topic.startswith(self.KAFKA_PREFIX)]
    except Exception as e:
        logger.error("Error fetching Kafka topics: %s", e)
        return []


def get_available_user_stream_ids(self):
    """Get a list of available stream IDs for a given user based on Kafka topics."""
    topics = get_kafka_topics_with_prefix(self)
    user_topic_ids = set()
    for topic in topics:
        if topic.startswith(f"{self.KAFKA_PREFIX}{self.user_id}_"):
            try:
                stream_id = int(topic.split(f"{self.KAFKA_PREFIX}{self.user_id}_")[1])
                user_topic_ids.add(stream_id)
            except (IndexError, ValueError):
                continue

    all_possible_ids = set(range(1, self.MAX_STREAMS + 1))
    return sorted(all_possible_ids - user_topic_ids)


def get_next_stream_id(self):
    """Get the next available stream ID for a given user."""
    available_ids = get_available_user_stream_ids(self)
    if not available_ids:
        raise Exception(f"No available stream IDs for user {self.user_id}. Maximum number of streams reached.")
    return available_ids[0]


async def safe_producer_run(producer):
    """Ensure the producer runs safely and handles errors gracefully."""
    try:
        await producer.run()
    except Exception as e:
        logger.error("Producer encountered an error: %s", e)
    finally:
        logger.info("Producer %s has stopped.", producer.data_stream_id)
