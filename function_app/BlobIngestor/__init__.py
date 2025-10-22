import json
from pathlib import Path
import azure.functions as func
from ..shared.config import get
from ..shared.storage import ensure_containers, blob_exists
from ..shared.queueing import enqueue
from ..shared.logger import log_job, log_exception
from .. import app
RAW = get("RAW_CONTAINER", "raw-videos")
MEZZ = get("MEZZ_CONTAINER", "mezzanine")
HLS  = get("HLS_CONTAINER", "hls")
DASH = get("DASH_CONTAINER", "dash")
LOGS = get("LOGS_CONTAINER", "logs")
PROCESSED = get("PROCESSED_CONTAINER", "processed")
JOB_QUEUE = get("JOB_QUEUE", "transcode-jobs")
# Temporarily disabled to test gRPC error
# @app.blob_trigger(arg_name="input_blob", path=f"{RAW}/{{name}}", connection="AzureWebJobsStorage")
def blob_enqueuer(input_blob: func.InputStream):
    try:
        full = input_blob.name
        inside = full.split("/", 1)[-1] if "/" in full else full
        stem = Path(inside).stem
        ensure_containers([RAW, MEZZ, HLS, DASH, LOGS, PROCESSED])
        if blob_exists(PROCESSED, f"{stem}/manifest.json"):
            log_job(stem, "BLOB SKIP: Already processed (manifest exists)."); return
        payload = {"raw_key": inside, "job_id": stem}
        enqueue(JOB_QUEUE, json.dumps(payload))
        log_job(stem, f"BLOB ENQUEUED: {json.dumps(payload)}")
    except Exception as e:
        log_exception("blob-enqueue", e); raise
