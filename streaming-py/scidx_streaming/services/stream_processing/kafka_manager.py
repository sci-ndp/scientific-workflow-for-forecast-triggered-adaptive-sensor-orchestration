import json
from aiokafka import AIOKafkaConsumer
import asyncio
import logging
import time
import pandas as pd
from aiokafka.helpers import create_ssl_context
from ..data_cleaning import process_and_send_data
from ..compressor import decompress_data

# Create and configure logger for Kafka consumer
logger = logging.getLogger("aiokafka")
logger.setLevel(logging.ERROR)

class KafkaLogFilter(logging.Filter):
    def filter(self, record):
        message = record.getMessage()
        suppressed_messages = [
            "Topic ... is not available during auto-create initialization",
            "OffsetCommit failed for group",
            "Heartbeat session expired",
            "UnknownMemberIdError"
        ]
        return not any(msg in message for msg in suppressed_messages)

logger.addFilter(KafkaLogFilter())

CHUNK_SIZE = 10000  # max messages to batch before sending

async def process_kafka_stream(
    stream, filter_semantics, buffer_lock, send_data, loop, stop_event, username=None, password=None
):
    # Retrieve configuration details from stream
    conf = stream.get("config", {})
    kafka_host = conf.get("host")
    kafka_port = conf.get("port")
    kafka_topic = conf.get("topic")
    security_protocol = conf.get("security_protocol")
    sasl_mechanism = conf.get("sasl_mechanism")
    mapping = stream.get("mapping", None)
    # Configurable parameters for consumer
    auto_offset_reset = conf.get("auto_offset_reset", "earliest")
    time_window = float(conf.get("time_window", 10.0))
    batch_interval = float(conf.get("batch_interval", 1.0))
    # Read processing configurations
    processing = stream.get("processing", {})
    data_key = processing.get("data_key", None)
    batch_mode = str(conf.get("batch_mode", "False")).lower() == "true"

    # Initialize Kafka consumer with or without SASL as needed
    try:
        if security_protocol and sasl_mechanism:
            if not username or not password:
                logger.error(f"Stream {kafka_topic} requires SASL authentication but no credentials provided. Skipping.")
                return
            ssl_context = create_ssl_context()
            consumer = AIOKafkaConsumer(
                kafka_topic,
                bootstrap_servers=f"{kafka_host}:{kafka_port}",
                security_protocol=security_protocol,
                sasl_mechanism=sasl_mechanism,
                sasl_plain_username=username,
                sasl_plain_password=password,
                ssl_context=ssl_context,
                auto_offset_reset=auto_offset_reset,
                group_id=f"group_{kafka_topic}_{int(time.time())}",
                enable_auto_commit=False,
            )
        else:
            consumer = AIOKafkaConsumer(
                kafka_topic,
                bootstrap_servers=f"{kafka_host}:{kafka_port}",
                auto_offset_reset=auto_offset_reset,
                group_id=f"group_{kafka_topic}_{int(time.time())}",
                enable_auto_commit=False,
            )
        await consumer.start()
    except Exception as e:
        logger.error(f"Error initializing Kafka consumer for topic {kafka_topic}: {e}")
        return

    try:
        logger.info(f"Starting Kafka consumption for topic: {kafka_topic}")
        messages = []
        last_send_time = time.time()
        timeout_counter = 0

        while not stop_event.is_set():
            try:
                # Wait for next message up to time_window seconds
                message = await asyncio.wait_for(consumer.getone(), timeout=time_window)
                try:
                    data = decode_message(message.value)
                except ValueError as decode_error:
                    logger.error(f"Decoding failed: {decode_error}. Skipping message.")
                    continue
                # Extract nested data if data_key is specified
                if data_key and isinstance(data, dict) and data_key in data:
                    data = data[data_key]
                messages.append(data)
                timeout_counter = 0  # reset timeout counter on message
            except asyncio.TimeoutError:
                timeout_counter += 1
                logger.info(f"No messages received for {timeout_counter * time_window} seconds.")

            # If no messages for a long period, break out (inactivity)
            if timeout_counter >= 30:
                logger.info(f"Stopping Kafka stream {kafka_topic} due to inactivity.")
                break

            # Flush messages if batch size or interval reached
            if len(messages) >= CHUNK_SIZE or time.time() - last_send_time >= batch_interval:
                if messages:
                    processed_messages = preprocess_messages(messages, batch_mode)
                    await process_and_send_data(
                        processed_messages, mapping, stream,
                        send_data, buffer_lock, loop, filter_semantics, None
                    )
                    messages.clear()
                    last_send_time = time.time()

        # Final flush of any remaining messages
        if messages:
            processed_messages = preprocess_messages(messages, batch_mode)
            await process_and_send_data(
                processed_messages, mapping, stream,
                send_data, buffer_lock, loop, filter_semantics, None
            )
    except Exception as e:
        logger.error(f"Error in Kafka stream processing: {e}")
    finally:
        logger.info(f"Shutting down Kafka consumer for topic: {kafka_topic}")
        try:
            await consumer.stop()
        except Exception as shutdown_error:
            logger.error(f"Error shutting down Kafka consumer: {shutdown_error}")

def preprocess_messages(messages, batch_mode):
    """Convert batch messages from columnar format to row-based format if needed."""
    if not messages:
        return []
    if batch_mode:
        try:
            df = pd.DataFrame(messages)
            if not df.empty and all(isinstance(v, list) for v in df.iloc[0].values):
                df = df.apply(pd.Series.explode).reset_index(drop=True)
            return df.to_dict(orient="records")
        except Exception as e:
            logger.error(f"Error expanding batch messages: {e}")
            return messages
    return messages

def decode_message(message_value: bytes):
    """Decode a Kafka message from bytes into a Python object (JSON or decompressed data)."""
    try:
        return json.loads(message_value.decode("utf-8"))
    except UnicodeDecodeError:
        pass
    try:
        return json.loads(message_value)
    except UnicodeDecodeError:
        pass
    try:
        return decompress_data(message_value)
    except Exception as decompression_error:
        logger.error(f"Decompression failed: {decompression_error}")
    raise ValueError("Failed to decode the message.")
