import asyncio
import json
import logging
import pandas as pd
import threading
from typing import Optional
from aiokafka import AIOKafkaConsumer
from aiokafka.errors import UnsupportedCodecError
from confluent_kafka.admin import AdminClient, KafkaException
import blosc
import msgpack

logger = logging.getLogger(__name__)


class KafkaMessageConsumer:
    """
    Kafka Message Consumer that handles direct consumption from a Kafka topic.
    Dynamically updates a DataFrame and supports stopping the consumer.
    """

    def __init__(self, topic: str, host: Optional[str] = None, port: Optional[int] = None, retry_interval: Optional[int] = 2):
        """
        Initialize the KafkaMessageConsumer.

        Parameters
        ----------
        topic : str
            The Kafka topic to consume from.
        host : str, optional
            The Kafka host. Defaults to the hardcoded KAFKA_HOST.
        port : int, optional
            The Kafka port. Defaults to the hardcoded KAFKA_PORT.
        retry_interval : int, optional
            The interval in seconds to retry connecting to Kafka.
        """
        self.kafka_server = f"{host}:{port}"
        self.topic = topic
        self.use_compression = not (host and port)  # Decompress if only topic is provided
        self.retry_interval = retry_interval
        self._stop_event = threading.Event()
        self.data_list = []
        self._df = pd.DataFrame()
        self.records: list[dict] = []
        self.error: Optional[str] = None
        self._thread = threading.Thread(target=self._consume_messages)
        self._thread.daemon = True  # Ensure thread exits with the main program
        self._thread.start()

    def _consume_messages(self):
        asyncio.run(self._consume_loop())

    async def _consume_loop(self):
        while not await self._does_topic_exist():
            if self._stop_event.is_set():
                logger.info(f"Consumer stopping while waiting for topic '{self.topic}'...")
                return
            logger.info(f"Topic '{self.topic}' not found. Retrying in {self.retry_interval} seconds...")
            await asyncio.sleep(self.retry_interval)

        consumer = AIOKafkaConsumer(
            self.topic,
            bootstrap_servers=self.kafka_server,
            auto_offset_reset="earliest"
        )
        consumer_started = False

        try:
            await consumer.start()
            consumer_started = True
            logger.info(f"Consumer started for topic: {self.topic} on {self.kafka_server}")

            while not self._stop_event.is_set():
                try:
                    message = await asyncio.wait_for(consumer.getone(), timeout=1.0)
                    await self._process_message(message.value)
                except asyncio.TimeoutError:
                    continue  # Allows checking _stop_event periodically
        except UnsupportedCodecError as codec_err:
            guidance = (
                "The Kafka topic is compressed with the Snappy codec, but the local "
                "python-snappy/native libsnappy libraries are missing. Install them "
                "and restart the consumer."
            )
            self.error = guidance
            logger.error(f"{guidance} (topic={self.topic})")
            logger.debug("Kafka codec error details", exc_info=codec_err)
            self._stop_event.set()
        finally:
            if consumer_started:
                await consumer.stop()
                logger.info(f"Consumer stopped for topic: {self.topic}")

    async def _process_message(self, message: bytes):
        """
        Process a Kafka message, handling decompression and decoding.

        Parameters
        ----------
        message : bytes
            The Kafka message.
        """
        try:
            # Attempt decompression first
            data = self._decompress_data(message)
        except Exception as decompress_error:
            logger.warning(f"Decompression failed: {decompress_error}. Attempting UTF-8 decode.")
            try:
                # Fallback to UTF-8 decoding for non-compressed messages
                data = json.loads(message.decode("utf-8"))
            except Exception as decode_error:
                logger.error(f"Failed to decode message: {decode_error}")
                return  # Skip this message

        # Successfully processed data
        self.data_list.append(data)
        records = self._extract_records(data)
        if records:
            self.records.extend(records)
            self.records = self.records[-1000:]
        self._df = self._append_data_to_df(data)

    async def _does_topic_exist(self) -> bool:
        """
        Check if the Kafka topic exists.

        Returns
        -------
        bool
            True if the topic exists, False otherwise.
        """
        try:
            admin_client = AdminClient({'bootstrap.servers': self.kafka_server})
            metadata = admin_client.list_topics(timeout=10)
            return self.topic in metadata.topics
        except KafkaException as e:
            logger.error(f"Error checking Kafka topic existence: {e}")
            return False

    def _decompress_data(self, data: bytes) -> dict:
        """
        Decompress the Kafka message data using Blosc with Zstd.

        Parameters
        ----------
        data : bytes
            The compressed Kafka message.

        Returns
        -------
        dict
            The decompressed message as a dictionary.
        """
        try:
            decompressed_data = blosc.decompress(data)
            unpacked_data = msgpack.unpackb(decompressed_data, raw=False)
            return unpacked_data
        except Exception as e:
            logger.error(f"Error decompressing message: {e}")
            raise e

    def _append_data_to_df(self, data: dict) -> pd.DataFrame:
        """
        Normalize nested 'values' dictionary and dynamically add new columns to DataFrame.

        Parameters
        ----------
        data : dict
            The Kafka message data.

        Returns
        -------
        pd.DataFrame
            Updated DataFrame with the new data.
        """
        new_df = pd.json_normalize(data.get('values', {}))
        if self._df.empty:
            return new_df.fillna("N/A")
        return pd.concat([self._df, new_df], ignore_index=True).fillna("N/A")

    def _extract_records(self, data: dict) -> list[dict]:
        # Choose "values" block when available, otherwise use original payload
        payload = data.get("values") if isinstance(data.get("values"), dict) else data
        if not isinstance(payload, dict) or not payload:
            return []

        list_lengths = [len(item) for item in payload.values() if isinstance(item, list)]
        if not list_lengths:
            return [payload]

        length = max(list_lengths)
        rows: list[dict] = []
        for idx in range(length):
            entry = {}
            for key, item in payload.items():
                if isinstance(item, list):
                    entry[key] = item[idx] if idx < len(item) else None
                else:
                    entry[key] = item
            rows.append(entry)
        return rows

    def stop(self):
        """Stop the Kafka consumer."""
        logger.info(f"Stopping Kafka consumer for topic '{self.topic}'...")
        self._stop_event.set()
        self._thread.join(timeout=5)  # Ensure the thread stops gracefully
        self._clear_data()  # Clear data on stop
        logger.info("Kafka consumer thread successfully stopped.")
        

    def _clear_data(self):
        """Clear the stored data in the consumer."""
        self.data_list = []
        self._df = pd.DataFrame()

    @property
    def dataframe(self) -> pd.DataFrame:
        """Return the current DataFrame with messages."""
        return self._df.copy()  # Return a copy to avoid side effects


def consume_kafka_messages(self, topic: str, host: Optional[str] = None, port: Optional[int] = None) -> KafkaMessageConsumer:
    """
    Create and start a KafkaMessageConsumer.

    Parameters
    ----------
    self : StreamingClient
        The StreamingClient instance.
    topic : str
        The Kafka topic to consume.
    host : str, optional
        The Kafka host.
    port : int, optional
        The Kafka port.

    Returns
    -------
    KafkaMessageConsumer
        A KafkaMessageConsumer instance.
    """
    return KafkaMessageConsumer(topic, host or self.KAFKA_HOST, port or self.KAFKA_PORT)
