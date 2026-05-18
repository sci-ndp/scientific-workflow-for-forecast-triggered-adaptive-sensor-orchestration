import logging
import asyncio
import pandas as pd
import feedparser
from datetime import datetime, timedelta
from ..data_cleaning import process_and_send_data

logger = logging.getLogger(__name__)

async def process_rss_stream(stream, filter_semantics, buffer_lock, send_data, loop, stop_event):
    """
    Poll an RSS/Atom feed and stream new entries.
    """
    url = stream["config"].get("url")
    mapping = stream.get("mapping", None)
    processing = stream.get("processing", {})
    fetch_mode = processing.get("fetch_mode", "continuous").lower()
    poll_interval = processing.get("poll_interval", 30)
    duration = processing.get("duration", 86400)
    end_time = datetime.utcnow() + timedelta(seconds=duration)
    seen_ids = set()
    while not stop_event.is_set():
        try:
            feed = feedparser.parse(url)
            if feed.bozo:
                logger.warning(f"Malformed RSS feed at {url}: {feed.bozo_exception}")
                await asyncio.sleep(poll_interval)
                continue
            entries = feed.entries
            new_entries = [entry for entry in entries if entry.get("id") not in seen_ids]
            if new_entries:
                seen_ids.update(entry.get("id") for entry in new_entries)
                df = pd.DataFrame(new_entries)
                await process_and_send_data(
                    df.to_dict(orient='records'),
                    mapping,
                    stream,
                    send_data,
                    buffer_lock,
                    loop,
                    filter_semantics
                )
                logger.info(f"Processed {len(new_entries)} new RSS entries from {url}.")
            if fetch_mode == "once" or datetime.utcnow() >= end_time:
                break
        except Exception as e:
            logger.error(f"Error in RSS processing loop: {e}")
        await asyncio.sleep(poll_interval)
    logger.info(f"RSS processing completed for {url}")
