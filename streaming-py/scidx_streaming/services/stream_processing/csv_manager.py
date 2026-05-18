import logging
import requests
import asyncio
import pandas as pd
from io import StringIO
import time
from ..data_cleaning import process_and_send_data
from ._delimited import infer_layout

logger = logging.getLogger(__name__)

# Constants for processing
CHUNK_SIZE = 10000
TIME_WINDOW = 10  # seconds
RETRY_LIMIT = 10
BACKOFF_TIME = 5  # seconds

async def process_csv_stream(stream, filter_semantics, buffer_lock, send_data, loop, stop_event):
    """
    Fetch and process a CSV file stream.
    """
    url = stream["config"].get("url")
    mapping = stream.get("mapping", None)
    processing = stream.get("processing", {})
    retries = 0
    while retries < RETRY_LIMIT:
        try:
            logger.info(f"Fetching data for CSV file from URL: {url}")
            response = requests.get(url)
            response.raise_for_status()
            # Determine CSV parameters (delimiter, header, start line)
            delimiter = processing.get("delimiter")
            header_line = processing.get("header_line")
            start_line = processing.get("start_line")
            if delimiter is None or header_line is None or start_line is None:
                sample_data = response.text[:4096]
                detected_delimiter, detected_header_line, detected_start_line = infer_layout(
                    sample_data,
                    fallback_delimiter=",",
                    logger=logger,
                )
                delimiter = delimiter or detected_delimiter
                header_line = header_line if header_line is not None else detected_header_line
                start_line = start_line if start_line is not None else detected_start_line
            csv_data = StringIO(response.text)
            csv_data.seek(0)
            # Read first chunk (to get headers if present)
            first_chunk = pd.read_csv(
                csv_data,
                delimiter=delimiter,
                header=header_line,
                nrows=CHUNK_SIZE,
                skiprows=range(1, start_line) if (start_line and start_line > 1) else None
            )
            if first_chunk.empty:
                logger.error("No data found in the CSV stream.")
                return
            column_names = first_chunk.columns.tolist()
            # Process first chunk
            await process_and_send_data(
                first_chunk.to_dict(orient='records'),
                mapping,
                stream,
                send_data,
                buffer_lock,
                loop,
                filter_semantics
            )
            start_time = loop.time()
            last_send_time = time.time()
            # Read subsequent chunks
            while True:
                chunk = pd.read_csv(
                    csv_data,
                    delimiter=delimiter,
                    header=None,
                    nrows=CHUNK_SIZE,
                    names=column_names
                )
                if chunk.empty:
                    break
                await process_and_send_data(
                    chunk.to_dict(orient='records'),
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
            logger.info(f"Finished processing CSV file from {url}")
            return
        except requests.RequestException as e:
            logger.error(f"Error fetching data from {url}: {e}. Retrying in {BACKOFF_TIME} seconds...")
            retries += 1
            await asyncio.sleep(BACKOFF_TIME)
        except Exception as e:
            logger.error(f"Unhandled error in CSV stream processing: {e}")
            retries += 1
            await asyncio.sleep(BACKOFF_TIME)
    logger.error(f"Retry limit reached ({RETRY_LIMIT}) for CSV stream, giving up on {url}")
