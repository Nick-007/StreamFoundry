# function_app/PackageQueue/__init__.py
from __future__ import annotations
import logging
import json
from typing import Any, Dict

import azure.functions as func
from pydantic import ValidationError

# Reuse the single FunctionApp instance defined at the project root
# (If your app instance lives elsewhere, adjust this import)
from .. import app

# Shared ingestion helpers that wrap your shared/schema.IngestPayload
from ..shared.ingest import validate_ingest
from ..shared.logger import StreamLogger, bridge_logger, make_slogger
from ..shared.workspace import job_paths
from ..shared.config import AppSettings

settings = AppSettings()
LOGGER = logging.getLogger("package.queue")
LOGS_CONTAINER = settings.LOGS_CONTAINER

# Your packaging entrypoint; adjust import/name if different
# Expecting: def _handle_packaging(payload: IngestPayload, *, log) -> None
from ..packager import _handle_packaging  # noqa: F401
from ..shared.status import set_raw_status


# Bindings
QNAME = settings.PACKAGING_QUEUE
CONNECTION = "AzureWebJobsStorage"  # per your requirement: only AzureWebJobsStorage

def handle_packaging_queue(msg: func.QueueMessage, context: func.Context) -> None:
    """
    Packaging worker (Queue Trigger)
    - Reads JSON message
    - Validates & normalizes with shared/schema (via shared.ingest)
    - Invokes the packager core
    - Any exception is re-raised to allow Azure Queues retry/poison handling
    """
    # 1) Decode body
    try:
        body_raw = msg.get_body().decode("utf-8")
    except Exception:
        # Rare, but surface clearly so retries are meaningful
        LOGGER.exception("Failed to decode queue body as UTF-8")
        raise

    # 2) Parse JSON
    if not body_raw:
        LOGGER.warning("Empty message body on %s; releasing", QNAME)
        return
    try:
        body: Dict[str, Any] = json.loads(body_raw)
    except json.JSONDecodeError:
        LOGGER.error("Malformed JSON in queue message (len=%s): %r", len(body_raw), body_raw[:512])
        # Re-raise so this message moves toward poison after retries
        raise

    # 3) Validate & normalize into IngestPayload (Pydantic v2)
    try:
        payload = validate_ingest(body)  # uses shared/schema.IngestPayload
    except ValidationError as ve:
        # Keep errors concise to avoid gRPC log bloat; still actionable
        LOGGER.error("ValidationError for packaging job: %s", ve.json()[:4000])
        raise

    job_id = getattr(payload, "id", None) or "<unknown>"
    paths = job_paths(job_id)

    sl = StreamLogger(job_id=job_id, dist_dir=str(paths.dist_dir), container=LOGS_CONTAINER, job_type="package")
    sl.start(interval_sec=20)
    log_fn = bridge_logger(LOGGER, sl)
    slog, slog_exc = make_slogger(text_log=log_fn, job_log=sl.job, ctx={"job_id": job_id, "stage": "package"})
    slog("accepted")

    # 4) Call the packager core
    raw_container = payload.in_.container or settings.RAW_CONTAINER
    raw_key = payload.in_.key
    extra_meta = payload.extra or {}
    pipeline_id = extra_meta.get("pipeline", "transcode")

    try:
        # NOTE: Your _handle_packaging should encapsulate:
        #  - locating mezz/assets
        #  - shaka packaging (HLS/DASH/CMAF)
        #  - manifest hygiene
        #  - verification/integrity checks
        #  - uploads (routed) and cleanups
        slog("start")
        try:
            set_raw_status(
                raw_container,
                raw_key,
                status="processing",
                pipeline=pipeline_id,
                reason="packaging_start",
            )
        except Exception:
            pass
        _handle_packaging(payload=payload, log=log_fn)
        slog("completed")
    except Exception as exc:
        slog_exc("failed", exc)
        try:
            set_raw_status(raw_container, raw_key, status="failed", pipeline=pipeline_id, reason="packaging_failed")
        except Exception:
            pass
        raise
    finally:
        sl.stop(flush=True)


packaging_queue = app.queue_trigger(
    arg_name="msg",
    queue_name=QNAME,
    connection=CONNECTION,
)(handle_packaging_queue)
