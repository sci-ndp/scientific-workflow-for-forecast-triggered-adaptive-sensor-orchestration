import logging
from confluent_kafka.admin import AdminClient, KafkaException
from typing import Union
from ..services.producer import Producer

logger = logging.getLogger(__name__)

async def delete_stream(self, stream_id_or_name: Union[str, Producer]):
    """
    Delete a Kafka topic by its stream ID, full topic name, or all topics for the user.

    Parameters
    ----------
    stream_id_or_name : Union[str, Producer]
        The stream ID (int or str), full topic name, 'all'/'ALL' to delete all user topics, or a Producer instance.
    """
    if not self.user_id:
        raise ValueError("User ID not available in the StreamingClient instance.")

    admin_client = AdminClient({'bootstrap.servers': f"{self.KAFKA_HOST}:{self.KAFKA_PORT}"})
    logger.info(f"Admin client created for Kafka at {self.KAFKA_HOST}:{self.KAFKA_PORT}")

    # If 'all' or 'ALL' is passed, delete all streams for the user
    if isinstance(stream_id_or_name, str) and stream_id_or_name.lower() == 'all':

        try:
            logger.info("Fetching all user topics for deletion.")
            topics = admin_client.list_topics(timeout=10).topics
            user_topics = [
                topic for topic in topics
                if topic.startswith(f"{self.KAFKA_PREFIX}{self.user_id}_")
            ]

            if not user_topics:
                logger.info("No user topics found for deletion.")
                return {"message": "No streams found to delete."}

            logger.info(f"Deleting all user topics: {user_topics}")
            fs = admin_client.delete_topics(user_topics, operation_timeout=30)

            for t, f in fs.items():
                try:
                    f.result()
                    logger.info(f"Deleted Kafka topic: {t}")
                except Exception as e:
                    logger.error(f"Failed to delete topic '{t}': {e}")
                    raise Exception(f"Failed to delete Kafka topic '{t}': {e}")
            return {"message": f"Deleted {len(user_topics)} stream(s) successfully."}

        except KafkaException as e:
            logger.error(f"Kafka error while listing or deleting topics: {e}")
            raise Exception(f"Kafka error: {e}")

    # If a producer instance is passed, stop it and delete its topic
    if isinstance(stream_id_or_name, Producer):

        topic = stream_id_or_name.data_stream_id

        name = f"derived {topic}"

        url = f"http://{self.KAFKA_HOST}:{self.KAFKA_PORT}"

        server = self.server if self.server and self.server != "global" else "local"

        dataset_list =  self.search_datasets(name, server=server)

        for dataset in dataset_list:
            resources = dataset["resources"]

            for resource in resources:
                if resource["name"] == name and resource["url"] == url:
                    resource["status"] = "inactive"
                    
                    resource["description"] = resource["description"].replace("The stream status is active.", "The stream status is inactive.")

                    logger.info(f"Updating dataset {dataset['id']} to remove resource {name}.")
                    
                    patch_response = self.patch_general_dataset(
                        dataset_id=dataset['id'],
                        server=self.server,
                        data={"resources": [resource]}
                    )
                    logger.info(f"Patch response: {patch_response}")
        
        logger.info(f"Deleting topic using producer: {topic}")

        # Stop the producer if it’s active
        try:
            await stream_id_or_name.stop()
            logger.info(f"Producer for topic '{topic}' stopped.")
        except Exception as e:
            logger.warning(f"Error stopping producer for topic '{topic}': {e}")

        # Delete the topic
        try:
            fs = admin_client.delete_topics([topic], operation_timeout=30)
            for t, f in fs.items():
                f.result()  # Wait for deletion result
            logger.info(f"Deleted Kafka topic: {topic}")
            return {"message": f"Stream '{topic}' deleted successfully"}
        except KafkaException as e:
            logger.error(f"Kafka error while deleting topic '{topic}': {e}")
            raise Exception(f"Kafka error: {e}")

    # Handle single topic deletion by ID or name
    if isinstance(stream_id_or_name, str) and stream_id_or_name.isdigit():
        stream_id = int(stream_id_or_name)
        topic = f"{self.KAFKA_PREFIX}{self.user_id}_{stream_id}"
    else:
        topic = stream_id_or_name

    logger.info(f"Checking if topic '{topic}' exists before deletion.")

    try:
        topics = admin_client.list_topics(timeout=10).topics
        if topic not in topics:
            logger.warning(f"Topic '{topic}' does not exist. Skipping deletion.")
            return {"message": f"Stream '{topic}' already deleted or does not exist."}

        fs = admin_client.delete_topics([topic], operation_timeout=30)
        for t, f in fs.items():
            try:
                f.result()  # Wait for deletion result
                logger.info(f"Deleted Kafka topic: {topic}")
                return {"message": f"Stream {topic} deleted successfully"}
            except Exception as e:
                logger.error(f"Failed to delete topic '{topic}': {e}")
                raise Exception(f"Failed to delete Kafka topic '{topic}': {e}")

    except KafkaException as e:
        logger.error(f"Kafka error while deleting topic '{topic}': {e}")
        raise Exception(f"Kafka error: {e}")
