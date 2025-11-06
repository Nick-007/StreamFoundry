import json
from pathlib import Path

import azure.functions as func
import logging

from .. import app
from ..shared.config import AppSettings
from ..shared.logger import log_exception, log_job
from ..shared.queueing import enqueue
from ..shared.storage import ensure_containers, blob_exists
from ..shared.pipelines import select_pipeline_for_blob
from ..shared.status import set_raw_status

settings = AppSettings()
LOGGER = logging.getLogger("blob_ingestor")

RAW = settings.RAW_CONTAINER
MEZZ = settings.MEZZ_CONTAINER
HLS = settings.HLS_CONTAINER
DASH = settings.DASH_CONTAINER
LOGS = settings.LOGS_CONTAINER
PROCESSED = settings.PROCESSED_CONTAINER
TRANSCODE_QUEUE = settings.TRANSCODE_QUEUE
SEED_BLOB_NAME = "__seed__.keep"
DEFAULT_PIPELINE_ID = "transcode"


@app.blob_trigger(arg_name="input_blob", path=f"{RAW}/{{name}}", connection="AzureWebJobsStorage")
def blob_enqueuer(input_blob: func.InputStream):
    try:
        full = input_blob.name
        inside = full.split("/", 1)[-1] if "/" in full else full
        stem = Path(inside).stem

        if inside == SEED_BLOB_NAME or stem == "__seed__":
            log_job(stem, f"BLOB SKIP: Ignoring seed blob {inside}")
            LOGGER.info("BLOB SKIP: Ignoring seed blob %s", inside)
            return

        route = select_pipeline_for_blob(inside)
        if not route:
            log_job(stem, f"BLOB SKIP: No pipeline route for {inside}")
            LOGGER.info("BLOB SKIP: No pipeline route for %s", inside)
            return

        target_queue = route.get("queue") or TRANSCODE_QUEUE
        pipeline_id = route.get("id") or DEFAULT_PIPELINE_ID

        ensure_containers([RAW, MEZZ, HLS, DASH, LOGS, PROCESSED])
        if blob_exists(PROCESSED, f"{stem}/manifest.json"):
            log_job(stem, "BLOB SKIP: Already processed (manifest exists).")
            LOGGER.info("BLOB SKIP: Already processed manifest exists for %s", stem)
            return

        payload = {
            "id": stem,
            "in": {"container": RAW, "key": inside},
            "captions": [],
            "extra": {"pipeline": pipeline_id},
        }

        enqueue(target_queue, json.dumps(payload))
        log_job(stem, f"BLOB ENQUEUED: {json.dumps(payload)}", queue=target_queue)
        LOGGER.info("BLOB ENQUEUED: stem=%s queue=%s payload=%s", stem, target_queue, payload)
        try:
            set_raw_status(RAW, inside, status="queued", pipeline=pipeline_id, reason="blob_trigger")
        except Exception:
            pass
    except Exception as exc:
        log_exception("blob-enqueue", exc)
        LOGGER.exception("blob_enqueuer failed for blob %s", getattr(input_blob, "name", "<unknown>"))
        raise
