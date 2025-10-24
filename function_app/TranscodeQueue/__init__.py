# function_app/TranscodeQueue/__init__.py
from __future__ import annotations
import json, time, hashlib, threading
import logging
from pathlib import Path
from typing import List, Dict, Optional, Any, Callable

import azure.functions as func
from azure.functions import QueueMessage
from pydantic import ValidationError
from azure.core.exceptions import HttpResponseError, ResourceNotFoundError

# --- shared modules from your repo ---
from ..shared.config import AppSettings
from ..shared.logger import StreamLogger, bridge_logger, make_slogger

# transcode_queue_normalize.py (example co-located helper)
from ..shared.storage import (
    blob_exists, upload_bytes,
    acquire_lock_with_break, renew_lock, download_blob_streaming,  # your existing lease helpers
)
from ..shared.mezz import (
    ensure_intermediates_from_mezz, upload_mezz_and_manifest, upload_intermediate_file,
)
from ..shared.qc import ffprobe_validate, CmdError
from ..shared.transcode import transcode_to_cmaf_ladder  # we do NOT import packaging here

from ..shared.ingest import validate_ingest, dump_normalized
from ..shared.queueing import enqueue, queue_client
from ..shared.workspace import job_paths
from ..shared.rungs import ladder_labels, mezz_video_filename, discover_renditions

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
VISIBILITY_EXTENSION_SEC = int(settings.TRANSCODE_VISIBILITY_EXTENSION_SEC or "0")

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
    enqueue(PACKAGING_Q, json.dumps(job))
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

def _call_transcode(
    inp_path: str,
    work_dir: str,
    only_rungs: Optional[List[str]],
    *,
    on_audio_done: Optional[Callable[[str], None]] = None,
    on_rung_done: Optional[Callable[[str, str], None]] = None,
    log:Optional[callable] = None
):
    t0 = time.time()
    audio_mp4, renditions, meta = transcode_to_cmaf_ladder(
        inp_path,
        work_dir,
        only_rungs=only_rungs,
        on_audio_done=on_audio_done,
        on_rung_done=on_rung_done,
        log=log,
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

def _extend_visibility(
    qc,
    message_id: str,
    pop_receipt: Optional[str],
    seconds: int,
    log,
) -> Optional[str]:
    """
    Extend the queue message visibility lease and return the new pop receipt.
    """
    if not pop_receipt:
        return pop_receipt
    try:
        updated = qc.update_message(
            message_id,
            pop_receipt=pop_receipt,
            visibility_timeout=seconds,
        )
        next_visible = getattr(updated, "next_visible_on", None)
        log(f"[queue] visibility extended by {seconds}s (next={next_visible})")
        return updated.pop_receipt
    except (HttpResponseError, ResourceNotFoundError) as e:
        code = getattr(e, "error_code", None)
        if code == "MessageNotFound" or "messagenotfound" in str(e).lower():
            log("[queue] visibility extend aborted: message no longer exists")
            return None
        log(f"[queue] visibility extend failed: {e}")
        return pop_receipt
    except Exception as e:
        log(f"[queue] visibility extend failed: {e}")
        return pop_receipt

def _start_visibility_heartbeat(
    qc,
    message_id: str,
    receipt_state: dict,
    seconds: int,
    stop_flag: dict,
    log,
):
    """
    Background thread that renews the queue message visibility while work continues.
    """
    if seconds <= 0:
        return None
    interval = max(5, min(seconds // 2, seconds - 30))
    if interval <= 0:
        interval = 5

    def _beat():
        while not stop_flag.get("stop"):
            time.sleep(interval)
            receipt_state["pop_receipt"] = _extend_visibility(
                qc,
                message_id,
                receipt_state.get("pop_receipt"),
                seconds,
                log,
            )
            if receipt_state.get("pop_receipt") is None:
                stop_flag["stop"] = True
                break

    t = threading.Thread(target=_beat, name="queue-visibility-heartbeat", daemon=True)
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
    log_paths = job_paths(stem_for_log)

    sl = StreamLogger(job_id=stem_for_log, dist_dir=str(log_paths.dist_dir), container=LOGS, job_type="transcode")
    sl.start(interval_sec=20)
    log = bridge_logger(LOGGER, sl)

    slog, slog_exc = make_slogger(
        text_log=log,
        job_log=sl.job,
        ctx={"job_id":stem_for_log, "stage": "transcode_ingest"}
    )

    qc = None
    visibility_state = {"pop_receipt": msg.pop_receipt}
    visibility_stop = None
    visibility_thread = None

    try:
        if VISIBILITY_EXTENSION_SEC > 0:
            try:
                qc = queue_client(TRANSCODE_Q)
                visibility_state["pop_receipt"] = _extend_visibility(
                    qc,
                    msg.id,
                    visibility_state.get("pop_receipt"),
                    VISIBILITY_EXTENSION_SEC,
                    log,
                )
                visibility_stop = {"stop": False}
                visibility_thread = _start_visibility_heartbeat(
                    qc,
                    msg.id,
                    visibility_state,
                    VISIBILITY_EXTENSION_SEC,
                    visibility_stop,
                    log,
                )
            except Exception as e:
                log(f"[queue] initial visibility extend failed: {e}")
                qc = None

        try:
            ingest = validate_ingest(payload)
        except ValidationError as ve:
            log(f"[validate] error: {ve}")
            send_to_poison(payload, f"ValidationError: {ve}", log)
            raise

        in_cont = ingest.in_.container
        raw_key = ingest.in_.key
        stem = ingest.id
        if not stem:
            raise CmdError("Missing job id after validation")

        only_rung = ladder_labels(ingest.only_rung) if ingest.only_rung else None
        raw_captions = payload.get("captions")
        captions = raw_captions if raw_captions else [c.model_dump() for c in (ingest.captions or [])]
        extra = payload.get("extra") or ingest.extra or {}

        paths = job_paths(stem)
        inp_dir = paths.input_dir
        work_dir = paths.work_dir
        inp_path = str(inp_dir / Path(raw_key).name)

        # ensure input exists
        if not blob_exists(in_cont, raw_key):
            raise FileNotFoundError(f"Input blob not found: {in_cont}/{raw_key}")

        # acquire lock
        lock_key = f"{in_cont}/{raw_key}"
        lock = acquire_lock_with_break(lock_key, ttl=LOCK_TTL)
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
        audio_mp4 = str(work_dir / "audio.mp4")
        labels = only_rung or ladder_labels(None)
        expected = [work_dir / "audio.mp4"] + [work_dir / mezz_video_filename(lab) for lab in labels]
        missing = [p for p in expected if not Path(p).exists()]

        restored = False
        if settings.SKIP_TRANSCODE_IF_MEZZ.lower() in ("1","true","yes") and missing:
            log("[mezz] attempting restore of intermediates")
            if ensure_intermediates_from_mezz(
                stem=stem,
                work_dir=str(work_dir),
                only_rung=only_rung,
                log=log,
            ):
                missing = [p for p in expected if not Path(p).exists()]
                restored = not missing
                log("[mezz] restored intermediates" if restored else "[mezz] partial restore")

        manifest_data: Optional[Dict] = None

        def _checkpoint_audio(local_path: str):
            nonlocal manifest_data
            manifest_data = upload_intermediate_file(
                stem=stem,
                local_path=local_path,
                manifest=manifest_data,
                log=log,
            )

        def _checkpoint_rung(label: str, local_path: str):
            nonlocal manifest_data
            manifest_data = upload_intermediate_file(
                stem=stem,
                local_path=local_path,
                manifest=manifest_data,
                log=log,
            )

        # transcode if needed
        if not restored:
            slog("transcode", "begin", rung=only_rung)
            audio_mp4, renditions, _ = _call_transcode(
                inp_path,
                str(work_dir),
                only_rung,
                on_audio_done=_checkpoint_audio,
                on_rung_done=_checkpoint_rung,
                log=log,
            )
            slog("transcode", "end", rung=only_rung)
        else:
            renditions = discover_renditions(work_dir, labels)

        # upload mezz + manifest (safe both for restored and re-encoded)
        try:
            uploaded = upload_mezz_and_manifest(
                stem=stem,
                work_dir=str(work_dir),
                manifest=manifest_data,
                log=log,
            )
            log(f"[mezz] uploaded files={len(uploaded.get('files',[]))}")
        except Exception as e:
            log(f"[mezz] upload failed (continuing): {e}")

        # enqueue packaging job (separate worker will restore from mezz and package)
        pkg_msg = dump_normalized(ingest)
        pkg_msg["only_rung"] = only_rung
        pkg_msg["captions"] = captions or []
        pkg_msg["extra"] = extra or {}
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
        if visibility_stop is not None:
            visibility_stop["stop"] = True
        if visibility_thread is not None:
            visibility_thread.join(timeout=1.0)
        sl.stop()
