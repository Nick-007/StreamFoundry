# function_app/SubmitJob/__init__.py
from __future__ import annotations
import os, uuid, json, tempfile, logging, time
from pathlib import Path
from typing import Optional, Union, List, Dict, Iterable
from urllib.parse import urlparse

import requests
import azure.functions as func

from .. import app
from ..shared.config import get
from ..shared.storage import (
    ensure_containers, upload_file, copy_blob, blob_exists
)
from ..shared.queueing import enqueue
from ..shared.logger import (
    StreamLogger, bridge_logger, log_job, log_exception
)
from ..shared.normalize import normalize_only_rung
from ..shared.rungs import receipt_payload
from ..shared.workspace import job_paths

# ---------------------------
# Config (env-driven)
# ---------------------------
RAW       = get("RAW_CONTAINER", "raw-videos")
MEZZ      = get("MEZZ_CONTAINER", "mezzanine")
HLS       = get("HLS_CONTAINER", "hls")
DASH      = get("DASH_CONTAINER", "dash")
LOGS      = get("LOGS_CONTAINER", "logs")
PROCESSED = get("PROCESSED_CONTAINER", "processed")
JOB_QUEUE = get("TRANSCODE_QUEUE", "transcode-jobs")
TMP_DIR   = get("TMP_DIR", "/tmp/ingestor")

# URL safety
ALLOWED_URL_SCHEMES = set((get("SUBMIT_URL_SCHEMES", "http,https")).lower().split(","))
# comma-separated host allow-list; empty = allow any
ALLOWED_URL_HOSTS = [h.strip().lower() for h in get("SUBMIT_URL_HOSTS", "").split(",") if h.strip()]

# Streaming download limits
MAX_DOWNLOAD_MB = int(get("SUBMIT_MAX_DOWNLOAD_MB", "2048"))  # 2 GB default
CHUNK_BYTES     = int(get("SUBMIT_CHUNK_BYTES", "4194304"))   # 4 MiB default
HTTP_TIMEOUT    = int(get("SUBMIT_HTTP_TIMEOUT_SEC", "60"))

# Idempotency on RAW (skip if exists unless overwrite=true)
DEFAULT_OVERWRITE = get("SUBMIT_OVERWRITE", "false").lower() in ("1","true","yes","on")

# CORS
CORS_ALLOW_ORIGIN  = get("CORS_ALLOW_ORIGIN", "*")
CORS_ALLOW_HEADERS = get("CORS_ALLOW_HEADERS", "content-type,authorization")
CORS_ALLOW_METHODS = get("CORS_ALLOW_METHODS", "POST,OPTIONS")

# Queue back-pressure (simple in-proc rate limit)
RATE_LIMIT_RPS = float(get("SUBMIT_RATE_LIMIT_RPS", "0"))  # 0 = disabled
# Optional delayed visibility for the consumer (seconds)
VISIBILITY_DELAY_SEC = int(get("SUBMIT_VISIBILITY_DELAY_SEC", "0"))

LOGGER = logging.getLogger("submit")

_last_request_ts = [0.0]  # tiny token bucket (instance local)


def _cors_headers() -> Dict[str, str]:
    return {
        "Access-Control-Allow-Origin": CORS_ALLOW_ORIGIN,
        "Access-Control-Allow-Headers": CORS_ALLOW_HEADERS,
        "Access-Control-Allow-Methods": CORS_ALLOW_METHODS,
    }


def _json_resp(body: Dict, status: int = 200) -> func.HttpResponse:
    return func.HttpResponse(
        json.dumps(body),
        status_code=status,
        mimetype="application/json",
        headers=_cors_headers(),
    )


def _rate_limit_ok() -> bool:
    if RATE_LIMIT_RPS <= 0:
        return True
    now = time.time()
    min_gap = 1.0 / RATE_LIMIT_RPS
    if now - _last_request_ts[0] < min_gap:
        return False
    _last_request_ts[0] = now
    return True


def _safe_url_ok(url: str) -> bool:
    try:
        u = urlparse(url)
        if (u.scheme or "").lower() not in ALLOWED_URL_SCHEMES:
            return False
        if ALLOWED_URL_HOSTS:
            host = (u.hostname or "").lower()
            return host in ALLOWED_URL_HOSTS
        return True
    except Exception:
        return False


def _build_raw_key_from_name(name: str) -> str:
    """
    Preserve subdirectories (virtual folders) in 'name'.
    Append .mp4 if missing.
    """
    name = name.strip().lstrip("/")  # keep inner slashes
    return name if name.endswith(".mp4") else f"{name}.mp4"


def _download_url_to_temp(url: str, *, max_mb: int, chunk_bytes: int, timeout: int, log) -> str:
    """
    Stream a URL to a temp file, enforcing size caps (from header and cumulative).
    Returns the temp file path.
    """
    with requests.get(url, stream=True, timeout=timeout) as r:
        r.raise_for_status()
        length = r.headers.get("Content-Length")
        if length:
            try:
                size_bytes = int(length)
                if size_bytes > max_mb * 1024 * 1024:
                    raise ValueError(f"Content-Length {size_bytes} exceeds {max_mb} MB limit")
            except Exception:
                # if header is malformed, fall back to cumulative check
                pass

        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            total = 0
            for chunk in r.iter_content(chunk_size=chunk_bytes):
                if not chunk:
                    continue
                total += len(chunk)
                if total > max_mb * 1024 * 1024:
                    tmp.close()
                    try:
                        os.unlink(tmp.name)
                    except Exception:
                        pass
                    raise ValueError(f"Download exceeded {max_mb} MB limit")
                tmp.write(chunk)
            tmp_path = tmp.name
    log(f"[submit] downloaded {total} bytes to {tmp_path}")
    return tmp_path


@app.route(route="submit", methods=["POST", "OPTIONS"])
def submit_job(req: func.HttpRequest) -> func.HttpResponse:
    # CORS preflight
    if req.method.upper() == "OPTIONS":
        return _json_resp({"ok": True}, status=204)

    # Back-pressure gate
    if not _rate_limit_ok():
        return _json_resp({"error": "rate_limited"}, status=429)

    try:
        ensure_containers([RAW, MEZZ, HLS, DASH, LOGS, PROCESSED])

        body = req.get_json()
        if not isinstance(body, dict):
            return _json_resp({"error": "invalid JSON"}, status=400)

        source = (body.get("source") or "").strip()
        if not source:
            return _json_resp({"error": "missing 'source' (URL or 'container/blob')"}, status=400)

        desired_name: Optional[str] = (body.get("name") or "").strip() or None
        only_rung_in: Optional[Union[str, int, List[Union[str, int, str]]]] = body.get("only_rung")
        captions: List[str] = body.get("captions") or []
        extra: Dict = body.get("extra") or {}
        overwrite: bool = bool(body.get("overwrite", DEFAULT_OVERWRITE))

        job_id = body.get("jobId") or str(uuid.uuid4())
        if desired_name:
            raw_key = _build_raw_key_from_name(desired_name)  # keep subdirs
            stem = Path(desired_name).stem
        else:
            stem = job_id
            raw_key = f"{job_id}.mp4"

        # StreamLogger → logs/submit/<id>-<ts>.log (blob), + local under TMP_DIR/<id>/dist/logs/
        paths = job_paths(stem)
        dist_dir = paths.dist_dir
        try:
            sl = StreamLogger(job_id=stem, dist_dir=str(dist_dir), container=LOGS, job_type="submit")
        except TypeError:
            sl = StreamLogger(job_id=stem, dist_dir=str(dist_dir), container=LOGS)
        sl.start(interval_sec=15)
        log = bridge_logger(LOGGER, sl)

        log(f"[submit] job_id={job_id} stem={stem} raw_key={raw_key} overwrite={overwrite}")

        # Idempotency: if *final* manifest exists, short-circuit
        if blob_exists(PROCESSED, f"{stem}/manifest.json"):
            rec = receipt_payload(
                job_id=job_id,
                stem=stem,
                raw_container=RAW,
                raw_key=raw_key,
                mezz_container=MEZZ,
                dash_container=DASH,
                hls_container=HLS,
            )
            log_job(stem, "submit", "already_processed_manifest_exists")
            sl.stop(flush=True)
            return _json_resp(rec, status=200)

        # Idempotency on RAW: if the target RAW/<raw_key> already exists and overwrite=False → skip ingest
        raw_exists = blob_exists(RAW, raw_key)
        if raw_exists and not overwrite:
            log(f"[submit] RAW exists; skip ingest (overwrite=false)")
        else:
            # Ingest the source into RAW/<raw_key>
            if "://" in source:
                # URL mode
                if not _safe_url_ok(source):
                    sl.stop(flush=True)
                    return _json_resp({"error": "URL not allowed by policy"}, status=400)
                tmp_path = _download_url_to_temp(
                    source,
                    max_mb=MAX_DOWNLOAD_MB,
                    chunk_bytes=CHUNK_BYTES,
                    timeout=HTTP_TIMEOUT,
                    log=log,
                )
                try:
                    upload_file(RAW, raw_key, tmp_path, content_type="video/mp4")
                    log(f"[submit] uploaded {RAW}/{raw_key} from URL")
                finally:
                    try:
                        os.unlink(tmp_path)
                    except Exception:
                        pass
            else:
                # Blob copy mode: expect container/blob
                if "/" not in source:
                    sl.stop(flush=True)
                    return _json_resp({"error": "For non-URL sources, use 'container/blob' format"}, status=400)
                src_container, src_blob = source.split("/", 1)
                if src_container == RAW and src_blob == raw_key:
                    log(f"[submit] RAW already has {raw_key}; no copy needed")
                else:
                    copy_blob(src_container, src_blob, RAW, raw_key)
                    log(f"[submit] copied {src_container}/{src_blob} → {RAW}/{raw_key}")

        # Normalize only_rung → list[str] like ["360p","720p"] or None
        try:
            only_rung_norm = normalize_only_rung(
                value=only_rung_in, values=None, as_set=False, suffix_p=True
            )
        except Exception as e:
            sl.stop(flush=True)
            return _json_resp({"error": f"only_rung normalization failed: {e}"}, status=400)

        # Build the queue payload (the v2 schema your QueueIngestor expects)
        payload: Dict[str, object] = {
            "id": stem,
            "in": {"container": RAW, "key": raw_key},
            "only_rung": only_rung_norm,     # None | "720p" | ["360p","720p"]
            "captions": captions or [],
            "extra": extra or {},
        }

        # Optional: delayed visibility to soften queue spikes
        if VISIBILITY_DELAY_SEC > 0:
            payload["visibility_delay_sec"] = VISIBILITY_DELAY_SEC

        # Enqueue
        enqueue(JOB_QUEUE, json.dumps(payload))
        log_job(stem, "submit", "accepted", queue=JOB_QUEUE, visibility_delay=VISIBILITY_DELAY_SEC)
        log(f"[submit] enqueued → {JOB_QUEUE} payload={payload}")

        rec = receipt_payload(
            job_id=job_id,
            stem=stem,
            raw_container=RAW,
            raw_key=raw_key,
            mezz_container=MEZZ,
            dash_container=DASH,
            hls_container=HLS,
            only_rung=only_rung_norm,
        )
        out = {
            "accepted": True,
            "receipt": rec,
            "payload": payload,
        }
        sl.stop(flush=True)
        return _json_resp(out, status=202)

    except Exception as e:
        try:
            log_exception("submit", e)
        except Exception:
            pass
        try:
            LOGGER.exception("submit failed")
        except Exception:
            pass
        return _json_resp({"error": str(e)}, status=500)
