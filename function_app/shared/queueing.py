# shared/queueing.py

from typing import Optional, Union
from datetime import timedelta
import json
from azure.storage.queue import QueueClient
from azure.storage.queue._message_encoding import TextBase64EncodePolicy  # v12

from .config import get
def queue_client(queue_name: str) -> QueueClient:
    conn = get("AzureWebJobsStorage")
    if not conn: raise RuntimeError("AzureWebJobsStorage not configured")
    qc = QueueClient.from_connection_string(
        conn_str=conn, 
        queue_name=queue_name,
        message_encode_policy=TextBase64EncodePolicy(), # <- Enforce Base64    
    )
    try: qc.create_queue()
    except Exception: pass
    return qc

def _as_seconds(v: Optional[Union[int, float, timedelta]]) -> Optional[int]:
    if v is None:
        return None
    if isinstance(v, timedelta):
        return int(v.total_seconds())
    return int(v)

def enqueue(
    queue_name: str,
    message_text: str,
    *,
    visibility_timeout: Optional[Union[int, float, timedelta]] = None,
    time_to_live: Optional[Union[int, float, timedelta]] = None,
):
    """
    Send a message to the queue.
    - visibility_timeout: delay (seconds) before the message becomes visible.
    - time_to_live: TTL (seconds) for the message (None = service default).
    Backwards compatible: if both are None, behaves like before.
    Convenience: if message_text is JSON and contains "visibility_delay_sec",
    we’ll use it (unless an explicit visibility_timeout arg was provided).
    """
    vt = _as_seconds(visibility_timeout)
    ttl = _as_seconds(time_to_live)

    # Optional convenience: auto-read from JSON payload if present
    if vt is None:
        try:
            obj = json.loads(message_text)
            if isinstance(obj, dict) and "visibility_delay_sec" in obj:
                vt = int(obj["visibility_delay_sec"])
                # Keep the field in the payload so workers can still read it if they want
        except Exception:
            pass

    qc = queue_client(queue_name)

    # azure-storage-queue v12 allows visibility_timeout & time_to_live (seconds or timedelta)
    if vt is not None or ttl is not None:
        qc.send_message(message_text, visibility_timeout=vt, time_to_live=ttl)
    else:
        qc.send_message(message_text)