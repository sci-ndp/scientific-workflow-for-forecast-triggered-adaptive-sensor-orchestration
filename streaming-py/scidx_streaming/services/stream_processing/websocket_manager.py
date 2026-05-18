# websocket_manager.py
import asyncio
import json
import logging
import queue
from typing import Any, Dict, Optional

import pandas as pd
from websocket import create_connection


from ..data_cleaning import process_and_send_data

logger = logging.getLogger(__name__)


def _to_utc_timestamp(s: Optional[str]) -> pd.Timestamp:
    """
    Synoptic example: "2025-12-14 01:45:00"
    Return UTC-aware pandas Timestamp, or NaT.
    """
    if not s:
        return pd.NaT
    try:
        return pd.to_datetime(s, utc=True, errors="coerce")
    except Exception:
        return pd.NaT


def _safe_json_load(raw: Any) -> Optional[Dict[str, Any]]:
    """
    Synoptic Push sometimes sends keepalives / non-JSON.
    Be defensive and never crash the reader thread.
    """
    try:
        if isinstance(raw, (bytes, bytearray)):
            raw = raw.decode("utf-8", errors="ignore")
        if not isinstance(raw, str):
            return None
        raw = raw.strip()
        if not raw:
            return None
        return json.loads(raw)
    except Exception:
        return None


def _ws_read_loop(
    ws,
    stop_event: asyncio.Event,
    q: queue.Queue,
):
    """
    Blocking websocket receive loop. Runs in a thread via run_in_executor.
    Pushes normalized records into a thread-safe Queue.
    """
    while not stop_event.is_set():
        try:
            raw = ws.recv()  # blocking
        except Exception as e:
            logger.exception("[websocket] ws.recv() failed: %s", e)
            break

        msg = _safe_json_load(raw)
        if not msg:
            continue

        mtype = msg.get("type")

        if mtype in {"auth", "metadata"}:
            q.put(
                {
                    "timestamp": pd.Timestamp.utcnow(),
                    "msg_type": mtype,
                    "payload": msg,
                }
            )
            continue

        if mtype != "data":
            continue

        rows = msg.get("data", [])
        if not isinstance(rows, list):
            continue

        for r in rows:
            if not isinstance(r, dict):
                continue
            q.put(
                {
                    "timestamp": _to_utc_timestamp(r.get("date")),
                    "msg_type": mtype,
                    "payload": msg,
                }
            )


async def process_websocket_stream(
    stream: Dict[str, Any],
    filter_semantics,
    buffer_lock: asyncio.Lock,
    send_data,
    loop: asyncio.AbstractEventLoop,
    stop_event: asyncio.Event,
):
    """
    Consume Synoptic Push (or any websocket JSON feed) and send to Kafka
    using the SAME pipeline as other managers via process_and_send_data().

    Requires:
      - stream["extras"]["url"] or stream["config"]["url"]
      - stream["mapping"] (optional, but typically present like other managers)
    """
    url = (stream.get("extras", {}) or {}).get("url") or (stream.get("config", {}) or {}).get("url")
    if not url:
        raise ValueError("websocket stream missing config.url (or extras.url)")

    mapping = stream.get("mapping", None)

    logger.info("[websocket] Connecting to %s", url)

  
    ws = create_connection(url)
    logger.warning("[websocket] CONNECTED url=%s", url)

    q: queue.Queue = queue.Queue()

    # Start background reader thread
    reader_future = loop.run_in_executor(None, _ws_read_loop, ws, stop_event, q)

    try:
        while not stop_event.is_set():
            await asyncio.sleep(5)

            # Drain queue into a batch
            batch = []
            while True:
                try:
                    batch.append(q.get_nowait())
                except queue.Empty:
                    break

            if not batch:
                continue

            df = pd.DataFrame(batch)
            if df.empty:
                continue

            # Normalize timestamps + ordering
            df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
            df = df.dropna(subset=["timestamp"]).sort_values("timestamp")
            df["timestamp"] = df["timestamp"].dt.strftime("%Y-%m-%dT%H:%M:%SZ")

            records = df.to_dict(orient="records")

            logger.warning(
                "[websocket] batch -> records=%d msg_types=%s",
                len(records),
                df["msg_type"].value_counts(dropna=False).to_dict() if "msg_type" in df.columns else {},
            )


            await process_and_send_data(
                records,           
                mapping,           
                stream,            
                send_data,         
                buffer_lock,       
                loop,              
                filter_semantics,  
                None,
            )

    finally:
        try:
            ws.close()
        except Exception:
            pass

        stop_event.set()

        try:
            await asyncio.wrap_future(reader_future)
        except Exception:
            pass

        logger.info("[websocket] Closed %s", url)
