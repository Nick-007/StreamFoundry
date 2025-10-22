# function_app/TranscodeQueue/__init__.py
from __future__ import annotations
import json, time, hashlib, base64
import logging
from pathlib import Path
from typing import List, Dict, Optional, Any

import azure.functions as func
from azure.functions import QueueMessage

# --- shared modules from your repo ---
from ..shared.config import AppSettings
from ..shared.logger import StreamLogger, bridge_logger, make_slogger
from ..shared.schema import IngestPayload

# transcode_queue_normalize.py (example co-located helper)
from ..shared.normalize import normalize_only_rung

from ..shared.storage import (
    blob_exists, blob_client, upload_bytes, upload_tree_routed,
    acquire_lock_with_break, renew_lock, download_blob_streaming,  # your existing lease helpers
)
from ..shared.mezz import (
    ensure_intermediates_from_mezz, upload_mezz_and_manifest,
)
from ..shared.qc import ffprobe_validate, CmdError
from ..shared.transcode import transcode_to_cmaf_ladder  # we do NOT import packaging here

settings = AppSettings()
LOGGER = logging.getLogger("transcode")

#LOGGER.info(f"RAW_CONTAINER: {settings.RAW_CONTAINER}")

# ---------- Config ----------
RAW        = settings.RAW_CONTAINER
MEZZ       = settings.MEZZ_CONTAINER
DASH       = settings.DASH_CONTAINER
HLS        = settings.HLS_CONTAINER
PROCESSED  = settings.PROCESSED_CONTAINER
LOGS       = settings.LOGS_CONTAINER

TMP_DIR      = settings.TMP_DIR
LOCK_TTL     = int(settings.LOCK_TTL_SECONDS)

# File size limits (similar to SubmitJob)
MAX_DOWNLOAD_MB = 2048  # 2 GB default
CHUNK_BYTES     = 4194304   # 4 MiB default

# queues
TRANSCODE_Q   = settings.TRANSCODE_QUEUE
POISON_Q      = settings.TRANSCODE_POISON_QUEUE
PACKAGING_Q   = settings.PACKAGING_QUEUE

STORAGE_CONN  = settings.AzureWebJobsStorage

from .. import app

# ---------- Helpers (restored) ----------

def queue_client(queue_name: str):
    """Create a QueueClient once; no repeated imports at call-sites."""
    from azure.storage.queue import QueueClient
    return QueueClient.from_connection_string(STORAGE_CONN, queue_name)

def send_to_poison(payload: dict, reason: str, log):
    """Send failed job to poison queue with safe payload."""
    try:
        qc = queue_client(POISON_Q)
        qc.send_message(json.dumps({
            "reason": reason,
            "at": int(time.time()),
            "payload": _safe_json(payload),
        }))
        log(f"[poison] sent to {POISON_Q}: {reason}")
    except Exception as e:
        log(f"[poison] failed to send: {e}")

def enqueue_packaging(job: dict, log):
    """Enqueue a packaging job for a separate worker."""
    qc = queue_client(PACKAGING_Q)
    # Serialize and encode the job
    raw_json = json.dumps(job)
    encoded_msg = base64.b64encode(raw_json.encode("utf-8")).decode("utf-8")
    # Send the encoded message
    qc.send_message(encoded_msg)
    log(f"[queue] enqueued packaging → {PACKAGING_Q} (id={job.get('id')}, rungs={job.get('only_rung') or 'all'})")

def _safe_json(obj: Any) -> Any:
    try:
        json.dumps(obj); return obj
    except Exception:
        if isinstance(obj, dict):
            return {str(k): _safe_json(v) for k, v in obj.items()}
        if isinstance(obj, (list, tuple, set)):
            return [_safe_json(x) for x in obj]
        try:
            return str(obj)
        except Exception:
            return f"<unserializable:{type(obj).__name__}>"

def _normalize(payload: dict) -> dict:
    data = IngestPayload(**payload)  # schema validation happens here

    # Merge singular/plural selectors without assuming either exists in the schema.
    sel = []
    if getattr(data, "only_rung", None) not in (None, "", []):
        sel.append(data.only_rung)
    if getattr(data, "only_rungs", None) not in (None, "", []):
        sel.append(getattr(data, "only_rungs"))

    # Normalize to a set like {"240p","360p",...}; suffix_p=True matches your ladder.
    only_rung = normalize_only_rung(
        sel or None,
        as_set=True,
        suffix_p=True,
    )

    return {
        "in_cont": data.in_.container,
        "raw_key": data.in_.key,
        "stem": data.id,
        "only_rung": only_rung,          # a set of "###p" labels, or empty set if none provided
        "captions": data.captions or [],
        "extra": data.extra or {},
    }


def _pull_caption(item: dict, work_dir: str, log) -> Optional[dict]:
    """
    { "container": "...", "key": "...", "lang": "en", "name": "subs_en.vtt" }
    → saves to work_dir/name and returns {"lang","path"} or None
    """
    try:
        cont = item["container"]; key = item["key"]
        lang = item.get("lang", "en")
        name = item.get("name") or Path(key).name
        out = Path(work_dir) / name
        out.parent.mkdir(parents=True, exist_ok=True)
        
        # Use streaming download for captions too (smaller size limit)
        download_blob_streaming(
            container=cont,
            blob=key,
            dest_path=str(out),
            max_mb=50,  # 50MB limit for captions (should be much smaller)
            chunk_bytes=CHUNK_BYTES,
            log=log
        )
        log(f"[captions] pulled {cont}/{key} → {out.name}")
        return {"lang": lang, "path": str(out)}
    except Exception as e:
        log(f"[captions] failed: {e}")
        return None

def _call_transcode(inp_path: str, work_dir: str, only_rungs: Optional[List[str]], log:Optional[callable] = None):
    t0 = time.time()
    audio_mp4, renditions, meta = transcode_to_cmaf_ladder(
        inp_path, work_dir, only_rungs=only_rungs, log=log
    )
    def _log(s: str): (log or print)(s)
    _log(f"[transcode] took={int(time.time()-t0)}s rungs={[r['name'] for r in renditions]}")
    return audio_mp4, renditions, meta

# Your existing heartbeat pattern (uses renew_lock from shared.storage)
def _start_heartbeat(lock_handle, ttl: int, stop_evt_flag: dict, log):
    if renew_lock is None:
        return None
    import threading
    interval = max(5, ttl // 2)
    def _beat():
        while not stop_evt_flag.get("stop"):
            try:
                time.sleep(interval)
                renew_lock(lock_handle, ttl=ttl)
                log("[lock] renewed")
            except Exception as e:
                log(f"[lock] renew failed: {e}")
    t = threading.Thread(target=_beat, name="lock-heartbeat", daemon=True)
    t.start()
    return t

# ---------- v2 Queue Trigger (transcode only; packaging deferred) ----------

@app.queue_trigger(arg_name="msg", queue_name=TRANSCODE_Q, connection="AzureWebJobsStorage")
def transcode_queue(msg: QueueMessage):
    raw = msg.get_body().decode("utf-8", errors="ignore")
    if not raw:
        LOGGER.warning("Queue payload empty; skipping.")
        return

    try:
        payload = json.loads(raw)
    except Exception as e:
        LOGGER.error(f"JSON parse failed: {e} raw={raw[:256]}")
        return

    # set up streaming logs (local + blob)
    stem_for_log = payload.get("id") or "job"
    root = Path(TMP_DIR) / stem_for_log
    dist_dir = root / "dist"; dist_dir.mkdir(parents=True, exist_ok=True)

    sl = StreamLogger(job_id=stem_for_log, dist_dir=str(dist_dir), container=LOGS, job_type="transcode")
    sl.start(interval_sec=20)
    log = bridge_logger(LOGGER, sl)

    slog, slog_exc = make_slogger(
        text_log=log,
        job_log=sl.job,
        ctx={"job_id":stem_for_log, "stage": "transcode_ingest"}
    )

    try:
        args = _normalize(payload)
        in_cont = args["in_cont"]; raw_key = args["raw_key"]; stem = args["stem"]
        only_rung = sorted(args["only_rung"]) if args["only_rung"] else None
        captions  = args["captions"]
        extra     = args["extra"]  # in case you add knobs later (e.g. trickplay)

        # workspace
        root = Path(TMP_DIR) / stem
        inp_dir  = root / "input"; inp_dir.mkdir(parents=True, exist_ok=True)
        work_dir = root / "work";  work_dir.mkdir(parents=True, exist_ok=True)
        inp_path = str(inp_dir / Path(raw_key).name)

        # ensure input exists
        if not blob_exists(in_cont, raw_key):
            raise FileNotFoundError(f"Input blob not found: {in_cont}/{raw_key}")

        # acquire lock
        lock_key = f"{in_cont}/{raw_key}"
        lock = acquire_lock_with_break(lock_key, ttl=LOCK_TTL, log=log)
        if not lock:
            log(f"[lock] busy/healthy lease present; skipping key={lock_key}")
            return

        # heartbeat
        hb_stop = {"stop": False}
        _ = _start_heartbeat(lock, LOCK_TTL, hb_stop, log=log)

        # download input with streaming and size limits
        t0 = time.time()
        download_blob_streaming(
            container=in_cont,
            blob=raw_key,
            dest_path=inp_path,
            max_mb=MAX_DOWNLOAD_MB,
            chunk_bytes=CHUNK_BYTES,
            log=log
        )
        log(f"[download] input ready path={inp_path} took={int(time.time()-t0)}s")

        # QC (tools + smoke + probe + analyze) — strict
        meta_in = ffprobe_validate(inp_path, log=log, strict=True, smoke=True)

        # captions (optional)
        t0 = time.time()
        text_tracks: List[Dict] = []
        for item in (captions or []):
            c = _pull_caption(item, str(work_dir), log)
            if c: text_tracks.append(c)
        log(f"[captions] count={len(text_tracks)} took={int(time.time()-t0)}s")

        # mezz restore first if allowed
        audio_mp4 = str(Path(work_dir) / "audio.mp4")
        labels = only_rung or ["240p","360p","480p","720p","1080p"]
        expected = [audio_mp4] + [str(Path(work_dir)/f"video_{lab[:-1]}.mp4") for lab in labels]
        missing = [p for p in expected if not Path(p).exists()]

        restored = False
        if settings.SKIP_TRANSCODE_IF_MEZZ.lower() in ("1","true","yes") and missing:
            log("[mezz] attempting restore of intermediates")
            if ensure_intermediates_from_mezz(stem=stem, work_dir=str(work_dir), only_rung=only_rung, log=log):
                missing = [p for p in expected if not Path(p).exists()]
                restored = not missing
                log("[mezz] restored intermediates" if restored else "[mezz] partial restore")

        # transcode if needed
        if not restored:
            slog("transcode", "begin", rung=only_rung)
            audio_mp4, renditions, _ = _call_transcode(inp_path, str(work_dir), only_rung, log)
            slog("transcode", "end", rung=only_rung)
        else:
            renditions = []
            for lab in labels:
                v = Path(work_dir)/f"video_{lab[:-1]}.mp4"
                if v.exists():
                    renditions.append({"name": lab, "video": str(v)})

        # upload mezz + manifest (safe both for restored and re-encoded)
        try:
            uploaded = upload_mezz_and_manifest(stem=stem, work_dir=str(work_dir), only_rung=only_rung, log=log)
            log(f"[mezz] uploaded files={len(uploaded.get('files',[]))}")
        except Exception as e:
            log(f"[mezz] upload failed (continuing): {e}")

        # enqueue packaging job (separate worker will restore from mezz and package)
        pkg_msg = {
            "id": stem,
            "only_rung": only_rung,              # None → all
            "captions": captions,                # forward original captions references
            "extra": extra or {},                # any future knobs (e.g. trickplay)
        }
        enqueue_packaging(pkg_msg, log)

        # publish a light manifest for the job state (optional)
        version = hashlib.sha1(str(time.time()).encode("utf-8")).hexdigest()[:8]
        manifest = {
            "id": stem,
            "version": version,
            "state": "queued_for_packaging",
            "rungs": [r["name"] for r in renditions] if renditions else labels,
        }
        upload_bytes(PROCESSED, f"{stem}/prepackage.json",
                     json.dumps(manifest, indent=2).encode("utf-8"),
                     "application/json")

        log("[queue] success (packaging enqueued)")

    except Exception as e:
        reason = f"{type(e).__name__}: {e}"
        slog_exc("Transcode Failed",e)
        try:
            send_to_poison(payload, reason, log)
        finally:
            raise
    finally:
        sl.stop()