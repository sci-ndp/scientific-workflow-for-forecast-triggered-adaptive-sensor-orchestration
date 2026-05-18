import logging
import requests
import asyncio
import pandas as pd
from io import StringIO
from ..data_cleaning import process_and_send_data
from ._delimited import infer_layout

logger = logging.getLogger(__name__)

CHUNK_SIZE = 10000
RETRY_LIMIT = 10
BACKOFF_TIME = 5

async def process_txt_stream(stream, filter_semantics, buffer_lock, send_data, loop, stop_event):
    """
    Fetch and process a TXT (tabular text) file stream.
    """
    url = stream["config"].get("url")
    mapping = stream.get("mapping", None)
    processing = stream.get("processing", {})
    retries = 0
    while retries < RETRY_LIMIT:
        try:
            logger.info(f"Fetching data for TXT file from URL: {url}")
            response = requests.get(url)
            response.raise_for_status()
            # Determine delimiter and header settings
            delimiter = processing.get('delimiter', '\t')
            header_line = processing.get('header_line')
            start_line = processing.get('start_line')
            if delimiter is None or header_line is None or start_line is None:
                sample_data = response.text[:4096]
                detected_delimiter, detected_header_line, detected_start_line = infer_layout(
                    sample_data,
                    fallback_delimiter=delimiter or '\t',
                    logger=logger,
                )
                delimiter = delimiter or detected_delimiter
                header_line = header_line if header_line is not None else detected_header_line
                start_line = start_line if start_line is not None else detected_start_line
            txt_data = StringIO(response.text)
            # Read first chunk (with header if present)
            first_chunk = pd.read_csv(
                txt_data,
                delimiter=delimiter,
                header=header_line,
                nrows=CHUNK_SIZE,
                skiprows=range(1, start_line) if (start_line and start_line > 1) else None
            )
            if first_chunk.empty:
                logger.error("No data found in the TXT stream.")
                return
            column_names = first_chunk.columns.tolist()
            # Process all data in chunks
            data_chunk = first_chunk
            while not data_chunk.empty:
                await process_and_send_data(
                    data_chunk.to_dict(orient='records'),
                    mapping,
                    stream,
                    send_data,
                    buffer_lock,
                    loop,
                    filter_semantics
                )
                data_chunk = pd.read_csv(
                    txt_data,
                    delimiter=delimiter,
                    header=None,
                    nrows=CHUNK_SIZE,
                    names=column_names,
                    skiprows=range(1, start_line) if (start_line and start_line > 1) else None
                )
            logger.info(f"Finished processing TXT file from {url}")
            return
        except requests.RequestException as e:
            logger.error(f"Error fetching data from {url}: {e}. Retrying in {BACKOFF_TIME} seconds...")
            retries += 1
            await asyncio.sleep(BACKOFF_TIME)
        except Exception as e:
            logger.error(f"Unhandled error in TXT stream processing: {e}")
            retries += 1
            await asyncio.sleep(BACKOFF_TIME)
    logger.error(f"Retry limit reached ({RETRY_LIMIT}) for TXT stream, giving up on {url}")
