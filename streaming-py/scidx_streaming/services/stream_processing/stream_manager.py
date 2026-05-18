import logging
import asyncio
import pandas as pd
import aiohttp
import json
import time
from ..data_cleaning import process_and_send_data

logger = logging.getLogger(__name__)

TIME_WINDOW = 10
RETRY_LIMIT = 10
BACKOFF_TIME = 5

def flatten_json(nested_json, parent_key='', sep='.'):
    """
    Flatten a nested JSON object for streaming data events.
    """
    flat_dict = {}
    for k, v in nested_json.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            flat_dict.update(flatten_json(v, new_key, sep=sep))
        elif isinstance(v, list):
            for idx, item in enumerate(v):
                flat_dict[f"{new_key}[{idx}]"] = item
        else:
            flat_dict[new_key] = v
    return flat_dict

def get_nested_json_value(data, keys):
    """
    Retrieve a nested value from JSON data by keys.
    """
    if isinstance(data, list):
        return data
    for key in keys:
        if not isinstance(data, dict):
            return None
        data = data.get(key, {})
    return data

def detect_stream_format(headers):
    """
    Detect the streaming data format (SSE or JSON or generic) based on Content-Type headers.
    """
    content_type = headers.get('Content-Type', '').lower()
    if "event-stream" in content_type:
        return "sse"
    elif "application/json" in content_type:
        return "json"
    else:
        return "generic"

async def process_streaming_data(response, processing, mapping, stream, send_data, buffer_lock, loop, filter_semantics, stop_event):
    """
    Consume an HTTP streaming response (SSE or newline-delimited JSON) and send data in batches.
    Returns True if any data was sent, False if no data was sent.
    """
    data_key = processing.get('data_key')
    batch_mode = str(stream.get("config", {}).get("batch_mode", "False")).lower() == "true"
    stream_type = detect_stream_format(response.headers)
    buffer = ""
    accumulated_data = []
    last_send_time = time.time()
    data_sent_flag = False
    logger.info(f"Detected stream type: {stream_type}")
    async for line in response.content:
        if stop_event.is_set():
            logger.info("Stop event set. Exiting streaming loop.")
            break
        try:
            line_str = line.decode('utf-8').strip()
            if not line_str:
                continue
            if stream_type == "sse":
                if line_str.startswith("data: "):
                    json_str = line_str[6:].strip()
                    buffer += json_str
                    try:
                        json_obj = json.loads(buffer)
                        buffer = ""
                        data = get_nested_json_value(json_obj, data_key.split('.')) if data_key else flatten_json(json_obj)
                        # Accumulate data
                        if batch_mode and isinstance(data, list):
                            for item in data:
                                accumulated_data.append(item)
                        else:
                            accumulated_data.append(data)
                        data_sent_flag = True
                        if time.time() - last_send_time >= TIME_WINDOW:
                            # Prepare batch to send
                            data_to_send = accumulated_data
                            if batch_mode:
                                try:
                                    df = pd.DataFrame(accumulated_data)
                                    if not df.empty and all(isinstance(v, list) for v in df.iloc[0].values):
                                        df = df.apply(pd.Series.explode).reset_index(drop=True)
                                    data_to_send = df.to_dict(orient="records")
                                except Exception as e:
                                    logger.error(f"Error expanding batch messages: {e}")
                                    data_to_send = accumulated_data
                            await process_and_send_data(
                                data_to_send,
                                mapping,
                                stream,
                                send_data,
                                buffer_lock,
                                loop,
                                filter_semantics
                            )
                            logger.info(f"Batch of {len(accumulated_data)} messages sent.")
                            accumulated_data = []
                            last_send_time = time.time()
                    except json.JSONDecodeError:
                        logger.warning("Incomplete JSON fragment received, waiting for more data.")
                        continue
            else:
                # JSON lines or generic text stream
                buffer += line_str
                try:
                    json_obj = json.loads(buffer)
                    buffer = ""
                    data = get_nested_json_value(json_obj, data_key.split('.')) if data_key else flatten_json(json_obj)
                    if batch_mode and isinstance(data, list):
                        for item in data:
                            accumulated_data.append(item)
                    else:
                        accumulated_data.append(data)
                    data_sent_flag = True
                    if time.time() - last_send_time >= TIME_WINDOW:
                        data_to_send = accumulated_data
                        if batch_mode:
                            try:
                                df = pd.DataFrame(accumulated_data)
                                if not df.empty and all(isinstance(v, list) for v in df.iloc[0].values):
                                    df = df.apply(pd.Series.explode).reset_index(drop=True)
                                data_to_send = df.to_dict(orient="records")
                            except Exception as e:
                                logger.error(f"Error expanding batch messages: {e}")
                                data_to_send = accumulated_data
                        await process_and_send_data(
                            data_to_send,
                            mapping,
                            stream,
                            send_data,
                            buffer_lock,
                            loop,
                            filter_semantics
                        )
                        logger.info(f"Batch of {len(accumulated_data)} messages sent.")
                        accumulated_data = []
                        last_send_time = time.time()
                except json.JSONDecodeError:
                    logger.warning("Incomplete JSON fragment received, waiting for more data.")
                    continue
        except Exception as e:
            logger.error(f"Error during streaming data processing: {e}")
            return data_sent_flag
    # End of stream: send remaining accumulated data
    if accumulated_data:
        data_to_send = accumulated_data
        if batch_mode:
            try:
                df = pd.DataFrame(accumulated_data)
                if not df.empty and all(isinstance(v, list) for v in df.iloc[0].values):
                    df = df.apply(pd.Series.explode).reset_index(drop=True)
                data_to_send = df.to_dict(orient="records")
            except Exception as e:
                logger.error(f"Error expanding batch messages: {e}")
                data_to_send = accumulated_data
        await process_and_send_data(
            data_to_send,
            mapping,
            stream,
            send_data,
            buffer_lock,
            loop,
            filter_semantics
        )
        logger.info(f"Final batch of {len(accumulated_data)} messages sent.")
    return data_sent_flag

async def process_stream_stream(stream, filter_semantics, buffer_lock, send_data, loop, stop_event):
    """
    Connect to a streaming URL and continuously process incoming data.
    """
    url = stream["config"].get("url")
    mapping = stream.get("mapping", None)
    processing = stream.get("processing", {})
    timeout_counter = 0
    while not stop_event.is_set():
        try:
            logger.info(f"Connecting to streaming source: {url}")
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as response:
                    response.raise_for_status()
                    logger.info(f"Successfully connected to stream: {url}")
                    data_sent = await process_streaming_data(response, processing, mapping, stream, send_data, buffer_lock, loop, filter_semantics, stop_event)
                    if data_sent:
                        timeout_counter = 0
                    else:
                        timeout_counter += 1
                    if timeout_counter >= 6:
                        logger.info(f"No new data received for {6 * TIME_WINDOW} seconds. Stopping the stream.")
                        break
        except Exception as e:
            logger.error(f"Error fetching streaming data from {url}: {e}. Retrying in {BACKOFF_TIME} seconds...")
            await asyncio.sleep(BACKOFF_TIME)
            continue
    logger.info(f"Streaming processing completed for {url}")
