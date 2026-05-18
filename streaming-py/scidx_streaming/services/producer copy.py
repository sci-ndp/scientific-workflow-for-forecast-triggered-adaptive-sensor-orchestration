import logging
import asyncio
from concurrent.futures import ThreadPoolExecutor
from aiokafka import AIOKafkaProducer
from .compressor import compress_data
from .stream_processing.kafka_manager import process_kafka_stream
from .stream_processing.txt_manager import process_url_stream
from confluent_kafka.admin import AdminClient, KafkaException
import json
def _pretty(obj):
    try:
        return json.dumps(obj, indent=2, ensure_ascii=False, default=str)
    except Exception:
        return repr(obj)

logger = logging.getLogger(__name__)


class Producer:
    def __init__(self, streaming_client, filter_semantics, data_streams, stream_id, username=None, password=None):
        """
        Initialize the Producer with StreamingClient attributes.

        Parameters
        ----------
        streaming_client : StreamingClient
            The StreamingClient instance to derive configuration from.
        filter_semantics : list
            Filtering semantics for the streams.
        data_streams : list
            List of data streams.
        stream_id : int
            The unique stream ID.
        username : str, optional
            Optional Kafka username for authentication.
        password : str, optional
            Optional Kafka password for authentication.
        """
        self.streaming_client = streaming_client
        self.user_id = streaming_client.user_id
        self.data_stream_id = f"{streaming_client.KAFKA_PREFIX}{self.user_id}_{stream_id}"
        self.methods = data_streams
        self.filter_semantics = filter_semantics
        self.kafka_server = f"{streaming_client.KAFKA_HOST}:{streaming_client.KAFKA_PORT}"
        self.stop_event = asyncio.Event()
        self.executor = ThreadPoolExecutor(max_workers=10)
        self.loop = asyncio.get_running_loop()
        self.producer = AIOKafkaProducer(bootstrap_servers=self.kafka_server)
        self.buffer_lock = asyncio.Lock()
        self.tasks = []
        self.retry_limit = 5
        self.retry_attempts = {}
        self.username = username
        self.password = password

    async def run(self):
        print(f"[Producer.run] bootstrap_servers={self.kafka_server}, topic={self.data_stream_id}")
        await self.producer.start()
        try:
            # Keep the full dump; helps diagnose edge cases
            print("\n[Producer.run] Full methods dump going to tasks:")
            print(_pretty(self.methods))
            self.tasks = [asyncio.create_task(self.process_stream(stream)) for stream in self.methods]
            logger.info(f"Created tasks for data streams: {self.methods}")
            await asyncio.gather(*self.tasks)
        except Exception as e:
            logger.info(f"Exception in Producer: {e}")
        finally:
            await self.stop()  # Stop tasks without deletion.

    async def process_stream(self, stream):
        """Process a single stream based on its type (Kafka or URL)."""
        resource = stream["resources"][0]
        try:
            if resource["format"] == 'kafka':
                logger.info("CREATION: KAFKA")
                await process_kafka_stream(
                    stream,
                    self.filter_semantics,
                    self.buffer_lock,
                    self.send_data,
                    self.loop,
                    self.stop_event,
                    username=self.username,
                    password=self.password
                )
            elif resource["format"] == 'url':
                logger.info("CREATION: URL")
                await process_url_stream(
                    stream,
                    self.filter_semantics,
                    self.buffer_lock,
                    self.send_data,
                    self.loop,
                    self.stop_event
                )
            else:
                logger.warning(f"Unsupported stream format: {resource['format']}")
        except Exception as e:
            await self.handle_stream_error(stream, e)

    async def handle_stream_error(self, stream, error):
        """Handle errors during stream processing with retry logic."""
        resource = stream["resources"][0]
        logger.error(f"Error processing stream {resource['format']}: {error}")
        retries = self.retry_attempts.get(stream['id'], 0)
        if retries < self.retry_limit:
            self.retry_attempts[stream['id']] = retries + 1
            backoff_time = 2 ** retries
            logger.info(f"Retrying stream {resource['format']} in {backoff_time} seconds...")
            await asyncio.sleep(backoff_time)
            await self.process_stream(stream)
        else:
            logger.error(f"Retry limit reached for {resource['format']}, skipping further retries.")

    async def send_data(self, df, stream, loop):
        """Send data to Kafka in compressed format."""
        logger.info(f"GOT DATA: {len(df)} that we will send at TOPIC: {self.data_stream_id}")
        data_structure = {"values": {}, "stream_info": stream["extras"]}
        for col in df.columns:
            data_structure["values"][col] = df[col].tolist()

        compressed_data = compress_data(data_structure)
        await self.producer.send_and_wait(self.data_stream_id, compressed_data)
        logger.info(f"Data successfully sent to topic {self.data_stream_id}")

    async def stop(self):
        """Stop all tasks and shut down the producer."""
        logger.info("Stopping producer...")
        self.stop_event.set()

        # Cancel tasks
        for task in self.tasks:
            if not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    logger.info("Task cancelled successfully.")
                except Exception as e:
                    logger.error(f"Error during task cancellation: {e}")

        # Shutdown producer
        try:
            await self.producer.flush()
            await self.producer.stop()
            logger.info("Kafka producer stopped.")
        except Exception as e:
            logger.error(f"Failed to stop Kafka producer: {e}")

        # Shutdown executor
        self.executor.shutdown(wait=False)
        logger.info("Producer shutdown completed.")


    async def delete(self):
        """Delete the Kafka topic and related resources explicitly."""
        logger.info(f"Deleting Kafka stream: {self.data_stream_id}")
        try:
            admin_client = AdminClient({'bootstrap.servers': self.kafka_server})
            admin_client.delete_topics([self.data_stream_id])
            logger.info(f"Kafka topic {self.data_stream_id} deleted.")
        except KafkaException as e:
            logger.error(f"Failed to delete Kafka topic {self.data_stream_id}: {e}")

    async def shutdown_producer(self):
        try:
            await self.producer.stop()
        except Exception as e:
            logger.error(f"Failed to stop Kafka producer: {e}")
        finally:
            self.executor.shutdown(wait=False)
            logger.info(f"Producer shutdown completed for data stream ID: {self.data_stream_id}")
