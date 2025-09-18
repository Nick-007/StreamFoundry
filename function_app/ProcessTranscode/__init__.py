import azure.functions as func
import json, os, logging

class ConfigError(Exception): ...
class BadMessageError(Exception): ...

def _to_obj(raw: str):
    # Accept either a JSON object or a JSON-string-of-JSON
    obj = json.loads(raw)
    if isinstance(obj, str):
        obj = json.loads(obj)
    return obj

def _normalize(payload: dict):
    job_id = payload.get("job_id") or payload.get("jobId") or payload.get("id")
    # Accept multiple aliases for the input blob name
    raw_key = (payload.get("raw_key")
               or payload.get("blobName")
               or payload.get("key")
               or payload.get("blob"))
    # Container can come from the message or env (default raw-videos)
    container = payload.get("container") or os.getenv("RAW_CONTAINER", "raw-videos")
    captions  = payload.get("captions") or []

    missing = []
    if not raw_key: missing.append("raw_key/blobName")
    if missing:
        raise BadMessageError(f"Missing required field(s): {', '.join(missing)}")

    return {
        "job_id": job_id,
        "raw_key": raw_key,
        "container": container,
        "captions": captions,
    }

def _require_env(keys):
    for k in keys:
        if not os.getenv(k):
            raise ConfigError(f"Missing env var: {k}")

def main(msg: func.QueueMessage) -> None:
    raw = msg.get_body().decode("utf-8", errors="replace")
    logging.info("Dequeued message len=%d", len(raw))

    try:
        payload = _to_obj(raw)
        payload = _normalize(payload)
    except Exception as e:
        logging.exception("Bad queue message")
        raise

    # Fail fast on required envs (adjust for your app)
    _require_env(["AzureWebJobsStorage"])  # storage binding
    # Optional: FFMPEG_PATH, OUTPUT_CONTAINER, TMP_DIR, etc.
    for k in ["FFMPEG_PATH", "OUTPUT_CONTAINER", "TMP_DIR"]:
        logging.info("ENV %s=%s", k, "<set>" if os.getenv(k) else "<missing>")

    job_id   = payload["job_id"]
    raw_key  = payload["raw_key"]
    container= payload["container"]
    captions = payload["captions"]

    logging.info("Job %s -> %s/%s (captions=%d)",
                 job_id, container, raw_key, len(captions))

    # TODO: your actual work here (probe, transcode, package).
    # Make sure not to log giant blobs of data; chunk logs if needed.
    # If output already exists, short-circuit to success to be idempotent.
