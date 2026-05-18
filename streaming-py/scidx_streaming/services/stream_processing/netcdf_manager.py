import logging
import requests
import asyncio
import pandas as pd
import xarray as xr
from io import BytesIO
import time
from ..data_cleaning import process_and_send_data

logger = logging.getLogger(__name__)

CHUNK_SIZE = 10000
TIME_WINDOW = 10
RETRY_LIMIT = 10
BACKOFF_TIME = 5

async def process_netcdf_stream(stream, filter_semantics, buffer_lock, send_data, loop, stop_event):
    """
    Fetch and process a NetCDF file stream.
    """
    url = stream["config"].get("url")
    mapping = stream.get("mapping", None)
    processing = stream.get("processing", {})
    retries = 0
    while retries < RETRY_LIMIT:
        try:
            logger.info(f"Fetching NetCDF data from URL: {url}")
            response = requests.get(url)
            response.raise_for_status()
            break
        except requests.RequestException as e:
            logger.error(f"Error fetching NetCDF from {url}: {e}. Retrying in {BACKOFF_TIME} seconds...")
            retries += 1
            await asyncio.sleep(BACKOFF_TIME)
        except Exception as e:
            logger.error(f"Unhandled error fetching NetCDF from {url}: {e}")
            retries += 1
            await asyncio.sleep(BACKOFF_TIME)
    else:
        logger.error(f"Retry limit reached ({RETRY_LIMIT}) for NetCDF stream, giving up on {url}")
        return
    try:
        content = response.content
        file_obj = BytesIO(content)
        group = processing.get('group')
        ds = xr.open_dataset(file_obj, engine='h5netcdf', group=group) if group else xr.open_dataset(file_obj, engine='h5netcdf')
        if not mapping:
            mapping = {var: var for var in ds.variables}
        missing_vars = [var for var in mapping.values() if var not in ds.variables]
        if missing_vars:
            raise ValueError(f"Missing variables in dataset: {missing_vars}")
        selected_data = ds[list(mapping.values())]
        full_df = selected_data.to_dataframe().reset_index()
        full_df = full_df.dropna(subset=list(mapping.values()))
        # Convert datetime columns to string
        for col in full_df.columns:
            if pd.api.types.is_datetime64_any_dtype(full_df[col]):
                full_df[col] = full_df[col].astype(str)
        start_time = loop.time()
        last_send_time = time.time()
        while not full_df.empty:
            chunk_df = full_df.iloc[:CHUNK_SIZE]
            full_df = full_df.iloc[CHUNK_SIZE:]
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
        ds.close()
        logger.info(f"Finished processing NetCDF stream from {url}")
    except Exception as e:
        logger.error(f"Error processing NetCDF stream: {e}")
