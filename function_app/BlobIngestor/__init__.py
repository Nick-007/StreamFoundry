import json
import logging
from pathlib import Path
from urllib.parse import unquote

import azure.functions as func

from .. import app
from ..shared.config import get
from ..shared.logger import log_exception, log_job
from ..shared.pipelines import select_pipeline_for_blob
from ..shared.queueing import enqueue
from ..shared.status import set_raw_status
from ..shared.storage import ensure_containers, blob_exists

LOGGER = logging.getLogger("blob_ingestor")

RAW = get("RAW_CONTAINER", "raw")
MEZZ = get("MEZZ_CONTAINER", "mezzanine")
HLS = get("HLS_CONTAINER", "hls")
DASH = get("DASH_CONTAINER", "dash")
LOGS = get("LOGS_CONTAINER", "logs")
PROCESSED = get("PROCESSED_CONTAINER", "processed")
TRANSCODE_QUEUE = get("TRANSCODE_QUEUE", "transcode-jobs")


def _extract_blob_ref(event: func.EventGridEvent) -> tuple[str | None, str | None]:
    """
    Parse the Event Grid subject to recover (container, blob_path).
    Example subject: /blobServices/default/containers/raw/blobs/path/to/file.mp4
    """
    subject = event.subject or ""
    if "/containers/" not in subject or "/blobs/" not in subject:
        return None, None

    try:
        after_containers = subject.split("/containers/", 1)[1]
        container, blob_part = after_containers.split("/blobs/", 1)
    except ValueError:
        return None, None

    blob_path = unquote(blob_part.lstrip("/"))
    return container, blob_path


def _handle_event_grid_notification(event: func.EventGridEvent) -> bool:
    """
    Returns True when work is enqueued, False when the event is ignored.
    """
    if event.event_type != "Microsoft.Storage.BlobCreated":
        return False

    container, blob_path = _extract_blob_ref(event)
    if not container or not blob_path:
        LOGGER.warning("Event Grid subject unsupported: %s", event.subject)
        log_exception("blob-enqueue", f"Unsupported subject {event.subject}")
        return False

    if container.lower() != RAW.lower():
        return False

    stem = Path(blob_path).stem
    LOGGER.info("Event Grid received %s (stem=%s)", blob_path, stem)

    ensure_containers([RAW, MEZZ, HLS, DASH, LOGS, PROCESSED])

    if blob_exists(PROCESSED, f"{stem}/manifest.json"):
        LOGGER.info("Skipping %s – processed manifest exists", stem)
        log_job(stem, "blob", "skip_manifest_exists")
        return False

    route = select_pipeline_for_blob(blob_path)
    if not route:
        LOGGER.info("Skipping %s – unsupported file type for %s", stem, blob_path)
        log_job(stem, "blob", "unsupported_file_type", blob=blob_path)
        return False

    pipeline_id = str(route.get("id") or "transcode")
    target_queue = str(route.get("queue") or TRANSCODE_QUEUE)
    LOGGER.info("Enqueueing %s → %s (pipeline=%s)", stem, target_queue, pipeline_id)

    payload = {
        "id": stem,
        "in": {"container": RAW, "key": blob_path},
        "captions": [],
        "extra": {"pipeline": pipeline_id},
    }

    try:
        set_raw_status(
            RAW,
            blob_path,
            status="queued",
            pipeline=pipeline_id,
            reason="blob_enqueued",
        )
    except Exception as exc:
        LOGGER.warning("set_raw_status failed for %s: %s", blob_path, exc)

    enqueue(target_queue, json.dumps(payload))
    LOGGER.info("Enqueued %s", stem)
    log_job(stem, "blob", "enqueued", queue=target_queue, payload=json.dumps(payload))
    return True


@app.event_grid_trigger(arg_name="event")
def blob_enqueuer(event: func.EventGridEvent):
    """Event Grid entrypoint that wraps the core handler."""
    try:
        _handle_event_grid_notification(event)
    except Exception as exc:
        LOGGER.exception("Blob trigger failed for subject %s", event.subject)
        log_exception("blob-enqueue", exc)
        raise
