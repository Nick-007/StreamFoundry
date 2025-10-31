import os, json, time, random
from typing import Callable, Dict, Any, Optional
from azure.storage.queue import QueueClient
from azure.core.exceptions import ResourceNotFoundError
from azure.storage.queue import QueueClient
import azure.functions as func
from .config import get

def queue_client(queue_name: str) -> QueueClient:
    conn = get("AzureWebJobsStorage")
    if not conn: raise RuntimeError("AzureWebJobsStorage not configured")
    qc = QueueClient.from_connection_string(conn_str=conn, queue_name=queue_name)
    try: qc.create_queue()
    except Exception: pass
    return qc

def enqueue(queue_name: str, message_text: str, visibility_timeout: int = 0) -> None:
    qc = queue_client(queue_name); qc.send_message(message_text, visibility_timeout=visibility_timeout)

# ---------- Helpers for re-enqueueing with self-caps ----------
# These are used by the Ingestor and Packager to handle self-capping and re-enqueueing logic.
# They are designed to be robust against host maxDequeueCount and to handle self-capping
# based on the number of polls and the age of the message.    
def _now() -> int: return int(time.time())

def _jittered(val: int, jitter: float = 0.2) -> int:
    if val <= 1: return 1
    delta = int(val * jitter)
    return max(1, val + random.randint(-delta, delta))

def _host_max_dequeue() -> int:
    # Host reads maxDequeueCount from host.json; we mirror it for decisions (default 3)
    try: return int(os.getenv("HOST_MAX_DEQUEUE", "3"))
    except Exception: return 3

def _host_retries_exhausted(msg) -> bool:
    dc = getattr(msg, "dequeue_count", 1) or 1
    return dc >= _host_max_dequeue()

# ---------- Ingestor: small, flat delay; self caps ----------
def _maybe_reenqueue_ingest(
    *,
    msg,                               # Azure func.QueueMessage
    queue_name: str,                   # e.g. "transcode-jobs"
    payload: Dict[str, Any],           # original ingest payload (will be augmented)
    log: Callable[[str], None],
    delay_sec: int = 3,                # tiny fixed delay
    max_polls: Optional[int] = None,   # override or env ING_MAX_POLLS
    max_age_s: Optional[int] = None,   # override or env ING_MAX_AGE_S
) -> None:
    """Return cleanly after re-enqueue (do NOT raise). Poisons when self-caps exceeded."""
    # Read caps
    if max_polls is None:
        max_polls = int(os.getenv("ING_MAX_POLLS", "200"))    # ~10 min @ 3s if steady
    if max_age_s is None:
        max_age_s = int(os.getenv("ING_MAX_AGE_S", "1800"))   # 30 min total wait

    # Seed or update counters
    polls = int(payload.get("ing_polls", 0))
    first_seen = int(payload.get("first_seen_ts", _now()))
    payload["ing_polls"] = polls + 1
    payload["first_seen_ts"] = first_seen

    # Self caps
    if payload["ing_polls"] > max_polls or (_now() - first_seen) > max_age_s:
        # Hard stop — move current msg to poison using your existing helper
        try:
            send_to_poison(msg, queue_name=queue_name, reason="ingestor-timeout",
                           details={"polls": payload["ing_polls"], "age_s": _now() - first_seen})
        except Exception:
            log("[ingestor] failed to send to poison; swallowing to prevent retry loop")
        return

    # Re-enqueue a fresh message (independent from host maxDequeueCount)
    enqueue(queue_name,json.dumps(payload, separators=(",", ":")),
                                           visibility_timeout=max(1, delay_sec))
    log(f"[ingestor] re-enqueued in {queue_name} (delay={delay_sec}s, polls={payload['ing_polls']})")


# ---------- Packager: backoff + jitter; self caps ----------
PKG_Q = get("PACKAGING_QUEUE", "packaging-jobs")  # queue this function listens to
def _enqueue_packaging_if_ready(*, stem: str, dist_dir: str, log):
    body = json.dumps({
        "stem": stem,
        "dist_dir": dist_dir,
        "prefix": f"{stem}/cmaf",
        # if you have duration in scope, include it here:
        # "duration_sec": <float_value>,
        "ts": int(time.time())
    }, separators=(",", ":"))
    try:
        enqueue(PKG_Q, body)
        log(f"[queue] packaging enqueued for {stem} → {PKG_Q}")
    except ResourceNotFoundError:
        raise RuntimeError(f"Queue '{PKG_Q}' not found. Run seed script.")
    
def _requeue_packager(
    *,
    msg,                                 # Azure func.QueueMessage
    queue_name: str,                     # e.g. "packaging-jobs"
    stem: str,
    container: str,
    prefix: str,                         # typically "<stem>/cmaf"
    log: Callable[[str], None],
    base_delay: int = 12,                # seconds
    cap_delay: int = 60,                 # seconds
    max_polls: Optional[int] = None,     # override or env PKG_MAX_POLLS
    max_age_s: Optional[int] = None,     # override or env PKG_MAX_WAIT_S
    extra: Optional[Dict[str, Any]] = None,  # carry any extra fields (duration, labels, etc.)
) -> None:
    """
    Return cleanly after re-enqueue (do NOT raise). Poisons when self-caps exceeded.
    Uses self 'polls' to back off (host retry count is irrelevant because we return success).
    """
    if max_polls is None:
        max_polls = int(os.getenv("PKG_MAX_POLLS", "30"))
    if max_age_s is None:
        max_age_s = int(os.getenv("PKG_MAX_WAIT_S", "3600"))  # 1 hour

    # Extract & advance counters from the **current message body** (caller should pass the parsed dict)
    # If the caller doesn’t have it, we seed anew; this keeps us robust.
    payload: Dict[str, Any] = {
        "stem": stem, "container": container, "prefix": prefix,
        **(extra or {})
    }
    polls = int(extra.get("polls", 0) if extra else 0)
    first_seen = int(extra.get("first_seen_ts", _now()) if extra else _now())
    polls += 1

    # Self caps
    if polls > max_polls or (_now() - first_seen) > max_age_s:
        try:
            send_to_poison(msg, queue_name=queue_name, reason="timeout-waiting-for-cmaf",
                           details={"polls": polls, "age_s": _now() - first_seen})
        except Exception:
            log("[packager] failed to send to poison; swallowing to prevent retry loop")
        return

    # Compute exponential backoff from *polls* (not host retries), add jitter
    delay = min(cap_delay, int(base_delay * (1.5 ** max(polls - 1, 0))))
    delay = _jittered(delay)

    payload["polls"] = polls
    payload["first_seen_ts"] = first_seen

    enqueue(queue_name,json.dumps(payload, separators=(",", ":")),
                                           visibility_timeout=max(1, delay))
    log(f"[packager] not_ready → re-enqueued {stem} ({queue_name}) "
        f"delay={delay}s polls={polls}")
    
def send_to_poison(msg: func.QueueMessage, *, queue_name: str, reason: str, details: dict | None = None):
    poison = f"{queue_name}-poison"
    try:
        body = msg.get_body().decode("utf-8", errors="ignore")
    except Exception:
        body = "<unreadable>"
    payload = {
        "reason": reason,
        "details": details or {},
        "original": {
            "id": getattr(msg, "id", None),
            "dequeue_count": getattr(msg, "dequeue_count", None),
            "insertion_time": getattr(msg, "insertion_time", None).isoformat() if getattr(msg, "insertion_time", None) else None,
            "body": body,
        },
        "moved_at": int(time.time())
    }
    # No auto-create; fail if poison queue missing so infra issues are visible
    try:
        enqueue(poison, json.dumps(payload))
    except ResourceNotFoundError:
        raise RuntimeError(f"Poison queue '{poison}' not found. Seed infra first.")
    
    