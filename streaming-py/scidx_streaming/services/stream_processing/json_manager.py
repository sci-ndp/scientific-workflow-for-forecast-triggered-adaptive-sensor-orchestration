import logging
import requests
import asyncio
import pandas as pd
import json
import time
from ..data_cleaning import process_and_send_data

logger = logging.getLogger(__name__)

CHUNK_SIZE = 10000
TIME_WINDOW = 10
RETRY_LIMIT = 10
BACKOFF_TIME = 5

def flatten_json(nested_json: dict, parent_key: str = '', sep: str = '.') -> dict:
    """
    Flatten a nested JSON object. Nested keys are concatenated with parent keys by `sep`.
    Lists are expanded by index notation [i].
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
    Retrieve a nested value from a JSON structure given a list of keys.
    If data is a list, returns the list itself.
    """
    if isinstance(data, list):
        return data
    for key in keys:
        if not isinstance(data, dict):
            return None
        data = data.get(key, {})
    return data

def detect_json_data_key(json_data):
    """
    Heuristically detect the main data key in a JSON object by finding the largest list or dict.
    Returns the key as a string, or None if the root is already the data.
    """
    if isinstance(json_data, list):
        return None
    candidates = [(key, value) for key, value in json_data.items() if isinstance(value, (list, dict))]
    if not candidates:
        return None
    selected_key = max(candidates, key=lambda item: len(item[1]) if hasattr(item[1], '__len__') else 0)[0]
    logger.info(f"Auto-detected data key: {selected_key}")
    return selected_key

async def process_json_stream(stream, filter_semantics, buffer_lock, send_data, loop, stop_event):
    """
    Fetch and process a JSON file (batch JSON) stream.
    """
    url = stream["config"].get("url")
    mapping = stream.get("mapping", None)
    processing = stream.get("processing", {})
    retries = 0
    while retries < RETRY_LIMIT:
        try:
            logger.info(f"Fetching JSON data from URL: {url}")
            response = requests.get(url)
            response.raise_for_status()
            break
        except requests.RequestException as e:
            logger.error(f"Error fetching JSON from {url}: {e}. Retrying in {BACKOFF_TIME} seconds...")
            retries += 1
            await asyncio.sleep(BACKOFF_TIME)
        except Exception as e:
            logger.error(f"Unhandled error fetching JSON from {url}: {e}")
            retries += 1
            await asyncio.sleep(BACKOFF_TIME)
    else:
        logger.error(f"Retry limit reached ({RETRY_LIMIT}) for JSON stream, giving up on {url}")
        return
    try:
        json_data = response.json()
        data_key = processing.get('data_key')
        info_key = processing.get('info_key')
        additional_key = processing.get('additional_key')
        if data_key is None:
            data_key = detect_json_data_key(json_data)
        # Extract main data
        data = get_nested_json_value(json_data, data_key.split('.')) if data_key else json_data
        # Prepare records list
        if isinstance(data, dict):
            records = [flatten_json(data)]
        else:
            records = data if isinstance(data, list) else []
            # Flatten each record in list if needed
            records = [flatten_json(item) if isinstance(item, dict) else item for item in records]
        # Handle info_key and additional_key to enrich stream_info
        stream_info_update = {}
        if info_key:
            info_value = get_nested_json_value(json_data, info_key.split('.'))
            if isinstance(info_value, dict):
                stream_info_update.update(info_value)
            elif info_value is not None:
                stream_info_update[info_key] = info_value
        if additional_key:
            additional_value = get_nested_json_value(json_data, additional_key.split('.'))
            if additional_value is not None:
                if isinstance(additional_value, list):
                    stream_info_update.setdefault('additional_info', []).extend(additional_value)
                else:
                    stream_info_update.setdefault('additional_info', []).append(additional_value)
        if stream_info_update:
            stream.setdefault("extras", {}).update(stream_info_update)
        # Send data in chunks
        df = pd.DataFrame(records)
        start_time = loop.time()
        last_send_time = time.time()
        while not df.empty:
            chunk_df = df.iloc[:CHUNK_SIZE]
            df = df.iloc[CHUNK_SIZE:]
            await process_and_send_data(
                chunk_df.to_dict(orient='records'),
                mapping,
                stream,
                send_data,
                buffer_lock,
                loop,
                filter_semantics
            )
            elapsed_time = loop.time() - start_time
            time_since_last_send = time.time() - last_send_time
            if time_since_last_send >= TIME_WINDOW or elapsed_time >= TIME_WINDOW:
                break
            last_send_time = time.time()
        logger.info(f"Finished processing JSON stream from {url}")
    except Exception as e:
        logger.error(f"Error processing JSON stream: {e}")
