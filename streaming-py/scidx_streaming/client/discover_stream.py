from typing import List, Dict, Any, Iterable, Hashable
import logging

logger = logging.getLogger(__name__) 

def _dedupe_resources(resources: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Deduplicate CKAN resources by a stable key. Prefer 'id'; fall back to (name, url).
    """
    seen: set[Hashable] = set()
    out: List[Dict[str, Any]] = []
    for r in resources:
        key: Hashable
        rid = r.get("id")
        if rid:
            key = rid
        else:
            # Fall back to a tuple that’s hashable and stable enough
            key = (r.get("name"), r.get("url"))
        if key in seen:
            continue
        seen.add(key)
        out.append(r)
    return out

def my_streams(self) -> List[Dict[str, Any]]:
    """
    Retrieve user-specific Kafka stream resources.

    Constructs a topic name based on the user's ID, searches for datasets
    matching that topic, and filters out relevant stream resources.

    Parameters
    ----------
    self : StreamingClient
        The StreamingClient instance.

    Returns
    -------
    List[Dict[str, Any]]
        A list of stream resources associated with the user.
    """

    # Construct topic and search name
    topic = f"data_stream_{self.user_id}"
    name = f"derived {topic}"

    # Determine server context
    server = "local"

    # Search for datasets matching the derived topic name
    dataset_list = self.search_datasets(name, server=server)
    user_streams = []

    # Filter resources that match the topic name
    for dataset in dataset_list:
        for resource in dataset.get("resources", []):
            if resource.get("name", "").startswith(name) and resource.get("format", "") == "stream":
                user_streams.append(resource)

    return _dedupe_resources(user_streams)


def public_streams(self) -> List[Dict[str, Any]]:
    """
    Retrieve publicly available Kafka stream resources.

    Searches for datasets with names starting with "derived data_stream_"
    and filters out matching stream resources.

    Parameters
    ----------
    self : StreamingClient
        The StreamingClient instance.

    Returns
    -------
    List[Dict[str, Any]]
        A list of public stream resources.
    """
    # Construct search prefix
    name_prefix = "derived data_stream_"

    # Determine server context
    server = "local"

    # Search for datasets matching the prefix
    dataset_list = self.search_datasets(name_prefix, server=server)
    public_streams = []

    # Filter resources that match the prefix
    for dataset in dataset_list:
        for resource in dataset.get("resources", []):
            if resource.get("name", "").startswith(name_prefix) and resource.get("format", "") == "stream":
                public_streams.append(resource)

    return _dedupe_resources(public_streams)
