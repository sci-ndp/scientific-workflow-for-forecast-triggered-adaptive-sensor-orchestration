import logging
from typing import List, Dict, Optional
from scidx_streaming.services.stream_manager import create_stream

logger = logging.getLogger(__name__)

async def create_kafka_stream(
    self,
    consumption_method_ids: List[str],
    filter_semantics: Optional[List[str] | str] = None,
    username: Optional[str] = None,
    password: Optional[str] = None,
    server: Optional[str] = "local",
) -> Dict:
    """
    Create a Kafka stream from the specified consumption method resource IDs.
    Then register the derived Kafka stream as a proper Kafka resource on each source dataset.

    Parameters
    ----------
    consumption_method_ids : list[str]
        Resource IDs (consumption methods) to consume from.
    filter_semantics : list[str], optional
        Filtering semantics for the producer pipeline.
    username : str, optional
        Optional Kafka SASL/SCRAM username.
    password : str, optional
        Optional Kafka SASL/SCRAM password.
    server : str, optional
        Server to search on ("local" by default).

    Returns
    -------
    Producer
        The Producer instance (or an error dict if something went wrong).
    """
    try:
        if not self.user_id:
            raise ValueError("User ID not available in the StreamingClient instance.")
        if not consumption_method_ids:
            raise ValueError("You must provide at least one consumption method ID.")

        if isinstance(filter_semantics, str):
            filter_semantics = [filter_semantics]
        filter_semantics = filter_semantics or []
        logger.debug(
            "Creating Kafka stream using resource IDs=%s, server=%s, semantics=%s",
            consumption_method_ids, server, filter_semantics
        )

        payload = {
            "consumption_method_ids": consumption_method_ids,
            "filter_semantics": filter_semantics,
            "server": server,
        }
        if username:
            payload["username"] = username
        if password:
            payload["password"] = password

        # Create the stream (resolves resources, builds Producer, starts it)
        producer, dataset_list = await create_stream(self, payload)

        topic = producer.data_stream_id if producer else None
        if not topic:
            raise RuntimeError("Producer failed to provide a topic.")
        logger.debug("Created stream topic: %s", topic)

        self.server = server if server and server != "global" else "local"

        # Register the derived Kafka stream as a proper Kafka consumption method on EACH source dataset
        # (type=kafka, description text, config with {host, port, topic})
        created_count = 0
        for dataset in dataset_list or []:
            ds_name = dataset.get("name") or dataset.get("id")
            if not ds_name:
                logger.warning("Skipping dataset without name/id: %s", dataset)
                continue

            method_payload = {
                "type": "kafka",
                "name": f"derived stream: {topic}",
                "description": (
                    "Derived Kafka stream generated from resource IDs: "
                    f"{', '.join(consumption_method_ids)}. "
                    f"Filter semantics: {', '.join(filter_semantics) if filter_semantics else 'none'}."
                ),
                "config": {
                    "host": self.KAFKA_HOST,
                    "port": self.KAFKA_PORT,
                    "topic": topic,
                },
            }

            try:
                logger.debug("Registering derived Kafka method on dataset '%s'", ds_name)
                self.add_consumption_method(dataset_id=ds_name, method_payload=method_payload, server=self.server)
                created_count += 1
            except Exception as e:
                msg = str(e).lower()
                if "already exists" in msg or "with name" in msg:
                    logger.debug("Derived Kafka method already exists on dataset '%s' (idempotent).", ds_name)
                else:
                    logger.error("Failed to register derived Kafka method on dataset '%s': %s", ds_name, e)
                    # keep going for other datasets

        logger.debug("Registered derived Kafka method on %d dataset(s).", created_count)
        return producer

    except Exception as e:
        logger.error("Error while creating Kafka stream: %s", e)
        return {"error": str(e)}
