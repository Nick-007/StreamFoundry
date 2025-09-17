import os, uuid, json, tempfile
from pathlib import Path
import azure.functions as func, requests
from ..shared.config import get
from ..shared.storage import upload_file, copy_blob, ensure_containers, blob_exists
from ..shared.logger import log_job, log_exception
from ..shared.queueing import enqueue
from .. import app
RAW = get("RAW_CONTAINER", "raw-videos")
MEZZ = get("MEZZ_CONTAINER", "mezzanine")
HLS = get("HLS_CONTAINER", "hls")
DASH = get("DASH_CONTAINER", "dash")
LOGS = get("LOGS_CONTAINER", "logs")
PROCESSED = get("PROCESSED_CONTAINER", "processed")
JOB_QUEUE = get("JOB_QUEUE", "transcode-jobs")
def _receipt(job_id: str, stem: str, raw_key: str):
    return {
        "jobId": job_id,
        "inputBlob": f"{RAW}/{raw_key}",
        "expectedOutputs": {
            "mezzanine": {
                "audio": f"{MEZZ}/v_<fingerprint>/{stem}/audio.mp4",
                "videos": [
                    f"{MEZZ}/v_<fingerprint>/{stem}/video_240.mp4",
                    f"{MEZZ}/v_<fingerprint>/{stem}/video_360.mp4",
                    f"{MEZZ}/v_<fingerprint>/{stem}/video_480.mp4",
                    f"{MEZZ}/v_<fingerprint>/{stem}/video_720.mp4",
                    f"{MEZZ}/v_<fingerprint>/{stem}/video_1080.mp4"
                ]
            },
            "dash": f"{DASH}/v_<fingerprint>/{stem}/stream.mpd",
            "hls":  f"{HLS}/v_<fingerprint>/{stem}/master.m3u8"
        }
    }
@app.route(route="submit", methods=["POST"])
def submit_job(req: func.HttpRequest) -> func.HttpResponse:
    try:
        ensure_containers([RAW, MEZZ, HLS, DASH, LOGS, PROCESSED])
        body = req.get_json()
        source = body.get("source"); captions = body.get("captions") or []
        if not source:
            return func.HttpResponse(json.dumps({"error":"Missing 'source'"}), status_code=400, mimetype="application/json")
        job_id = body.get("jobId") or str(uuid.uuid4())
        desired_name = body.get("name")
        if desired_name:
            stem = Path(desired_name).stem; raw_key = f"{stem}.mp4"
        else:
            stem = job_id; raw_key = f"{job_id}.mp4"
        if blob_exists(PROCESSED, f"{stem}/manifest.json"):
            rec = _receipt(job_id, stem, raw_key)
            log_job(stem, "SUBMIT SKIP: Already processed (manifest exists).")
            return func.HttpResponse(json.dumps(rec), status_code=200, mimetype="application/json")
        if source.startswith("http://") or source.startswith("https://"):
            with tempfile.NamedTemporaryFile(delete=False) as tmp:
                r = requests.get(source, timeout=60); r.raise_for_status()
                tmp.write(r.content); tmp_path = tmp.name
            upload_file(RAW, raw_key, tmp_path, "video/mp4")
            try: os.unlink(tmp_path)
            except Exception: pass
        else:
            if "/" not in source:
                return func.HttpResponse(json.dumps({"error":"For non-URL sources, use 'container/blob' format"}), status_code=400, mimetype="application/json")
            src_container, src_blob = source.split("/", 1)
            if src_container == RAW and src_blob == raw_key: pass
            else: copy_blob(src_container, src_blob, RAW, raw_key)
        payload = {"raw_key": raw_key, "job_id": job_id, "captions": captions}
        enqueue(JOB_QUEUE, json.dumps(payload))
        rec = _receipt(job_id, stem, raw_key)
        log_job(stem, f"SUBMIT ACCEPTED: {json.dumps(rec)}")
        return func.HttpResponse(json.dumps(rec), status_code=202, mimetype="application/json")
    except Exception as e:
        try: log_exception("submit", e)
        except Exception: pass
        return func.HttpResponse(json.dumps({"error": str(e)}), status_code=500, mimetype="application/json")
