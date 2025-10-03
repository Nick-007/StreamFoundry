import os
import json
import ast
import hashlib
import time
import threading
from pathlib import Path
from typing import Optional, Callable, List, Dict

import requests
import azure.functions as func
from azure.storage.queue import QueueClient

from .. import app
from ..shared.config import get
from ..shared.logger import (
    log_job as _log_job,
    log_exception as _log_exception,
    StreamLogger,
    bridge_logger,
    make_slogger
)
from ..shared.qc import (
    ensure_media_tools,
    ffprobe_validate
)
from ..shared.mezz import (
    upload_mezz_and_manifest,
    ensure_intermediates_from_mezz
)
from ..shared.transcode import (
    transcode_to_cmaf_ladder,
    package_with_shaka_ladder,
    _resolve_packager,
    CmdError,
    kill_all_children,
)
from ..shared.verify import (
    verify_transcode_outputs,
    verify_dash,
    verify_hls,
)
from ..shared.storage import (
    ensure_containers,
    blob_client,
    blob_exists,
    upload_tree_routed,
    upload_bytes,
    acquire_lock_with_break,
    renew_lock,
    release_lock,
)

import logging
LOGGER = logging.getLogger("Function.queue_ingestor")  # ← matches host.json

# ---------- tiny bridges to your blob logger ----------
def log_job(scope: str, msg: object = "", **kwargs):
    s = "" if msg is None else (msg if isinstance(msg, str) else str(msg))
    if kwargs:
        kv = ", ".join(f"{k}={v}" for k, v in kwargs.items())
        s = f"{s} | {kv}" if s else kv
    return _log_job(scope, s)

def log_exception(scope: str, msg: object = "", **kwargs):
    s = "" if msg is None else (msg if isinstance(msg, str) else str(msg))
    if kwargs:
        kv = ", ".join(f"{k}={v}" for k, v in kwargs.items())
        s = f"{s} | {kv}" if s else kv
    return _log_exception(scope, s)

# ---------- config ----------
RAW        = get("RAW_CONTAINER", "raw-videos")
MEZZ       = get("MEZZ_CONTAINER", "mezzanine")
HLS        = get("HLS_CONTAINER", "hls")
DASH       = get("DASH_CONTAINER", "dash")
LOGS       = get("LOGS_CONTAINER", "logs")
PROCESSED  = get("PROCESSED_CONTAINER", "processed")
LOCKS      = get("LOCKS_CONTAINER", "locks")
JOB_QUEUE  = get("JOB_QUEUE", "transcode-jobs")

# ---------- errors ----------
class BadMessageError(Exception): ...
class ConfigError(Exception): ...

# ---------- poison helpers ----------
def _queue_client(name: str) -> QueueClient:
    conn = os.getenv("AzureWebJobsStorage") or os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    return QueueClient.from_connection_string(conn, name)

def send_to_poison(msg: func.QueueMessage, *, queue_name: str, reason: str, details: dict | None = None):
    poison = f"{queue_name}-poison"
    qc = _queue_client(poison)
    try: qc.create_queue()
    except Exception: pass
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
    qc.send_message(json.dumps(payload))

# ---------- message parsing ----------
def _safe_json(raw: str, *, allow_plain: bool = True, max_log_chars: int = 500):
    s = (raw or "").strip()
    if not s:
        raise BadMessageError("empty queue message body")
    try:
        return json.loads(s)
    except json.JSONDecodeError:
        pass
    try:
        val = ast.literal_eval(s)
        if isinstance(val, (dict, list)):
            return val
        if isinstance(val, str) and allow_plain:
            return {"input": val}
    except Exception:
        pass
    if allow_plain:
        return {"input": s}
    preview = (s[:max_log_chars] + "…") if len(s) > max_log_chars else s
    raise BadMessageError(f"invalid JSON payload: {preview}")

def _normalize(p: dict):
    raw_key = p.get("raw_key") or p.get("blobName") or p.get("key") or p.get("blob") or p.get("input")
    if not raw_key:
        raise BadMessageError("Missing required field: raw_key/blobName/key/blob/input")
    job_id   = p.get("job_id") or p.get("jobId") or p.get("id") or Path(raw_key).stem
    container= p.get("container") or RAW
    captions = p.get("captions") or []
    return {"raw_key": raw_key, "job_id": job_id, "container": container, "captions": captions}

# ---------- captions ----------
def _pull_caption(item, tmpdir):
    src  = item.get("source")
    lang = (item.get("lang") or "en").lower()
    if not src:
        return None
    out = os.path.join(tmpdir, f"caption_{lang}.vtt")
    try:
        if src.startswith(("http://","https://")):
            r = requests.get(src, timeout=15); r.raise_for_status()
            with open(out, "wb") as f: f.write(r.content)
            return {"lang": lang, "path": out}
        if "/" in src:
            c, b = src.split("/", 1)
            bc2 = blob_client(c, b)
            with open(out, "wb") as f: f.write(bc2.download_blob().readall())
            return {"lang": lang, "path": out}
        log_job("captions", "Unrecognized caption source; skipping", source=src, lang=lang)
    except Exception as e:
        log_job("captions", f"Skip caption '{src}' ({lang}): {e}")
    return None

# ---------- flexible call shims ----------
def _call_transcode(fn, *, input_path, work_dir, segment_duration=None,
                    job_id=None, stem=None, captions=None, log=None):
    trials = (
        dict(input_path=input_path, workdir=work_dir, segment_duration=segment_duration,
             job_id=job_id, stem=stem, captions=captions, log=log),
        dict(input=input_path, workdir=work_dir, segdur=segment_duration,
             job_id=job_id, stem=stem, captions=captions, log=log),
        ("positional", (input_path, work_dir)),
    )
    last = None
    for t in trials:
        try:
            if isinstance(t, dict): return fn(**t)
            _, args = t;           return fn(*args)
        except TypeError as e:
            last = e; continue
    raise last

def _call_package(fn, *, renditions, audio_mp4, out_dash, out_hls, text_tracks=None, log=None):
    return fn(renditions=renditions, audio_mp4=audio_mp4, out_dash=out_dash,
              out_hls=out_hls, text_tracks=text_tracks, log=log)

# ---------- heartbeat ----------
def _start_heartbeat(lock_handle, ttl: int, stop_evt: threading.Event, log: Optional[Callable[[str], None]] = None):
    interval = max(5, ttl // 2)
    def _beat():
        while not stop_evt.wait(interval):
            try:
                renew_lock(lock_handle, ttl=ttl)
                (log or LOGGER.info)(f"[lock] renewed {lock_handle.get('blob','?')}")
            except Exception as e:
                (log or LOGGER.debug)(f"[lock] renew failed: {e}")
    t = threading.Thread(target=_beat, name="lock-heartbeat", daemon=True)
    t.start()
    return t

# ---------- Function ----------
@app.queue_trigger(arg_name="msg", queue_name=JOB_QUEUE, connection="AzureWebJobsStorage")
def queue_ingestor(msg: func.QueueMessage, context: func.Context):
    if not LOGGER.handlers:
        _h = logging.StreamHandler()
        _h.setFormatter(logging.Formatter("%(asctime)sZ %(levelname)s %(name)s | %(message)s"))
        LOGGER.addHandler(_h)
    LOGGER.setLevel(logging.DEBUG)

    raw = msg.get_body().decode("utf-8", errors="replace")
    log_job("queue", "Dequeued", length=len(raw), sample=raw[:256])

    # Parse or poison
    try:
        payload = _safe_json(raw)
        payload = _normalize(payload)
    except BadMessageError as e:
        log_exception("queue", f"Bad message: {e}")
        send_to_poison(msg, queue_name=JOB_QUEUE, reason="invalid_message", details={"error": str(e)})
        return

    raw_key   = payload["raw_key"]
    job_id    = payload["job_id"]
    in_cont   = payload["container"]
    captions  = payload["captions"]
    stem      = Path(raw_key).stem

    slog, slog_exc = make_slogger(
        text_log=LOGGER.info,
        job_log=_log_job,
        ctx={"job_id": job_id, "stem": stem}
    )
    log = LOGGER.info

    if not PROCESSED:
        raise ConfigError("Missing PROCESSED_CONTAINER configuration")
    ensure_containers([HLS, DASH, LOGS, PROCESSED])

    # Early packager presence check — poison if missing
    try:
        _resolve_packager(log=log)
    except CmdError as e:
        log_exception("package", f"Packager missing: {e}")
        send_to_poison(msg, queue_name=JOB_QUEUE, reason="shaka_packager_missing", details={"error": str(e)})
        return

    lock = None
    hb_stop = threading.Event()
    sl: Optional[StreamLogger] = None

    try:
        if not blob_exists(in_cont, raw_key):
            raise FileNotFoundError(f"Input blob not found: {in_cont}/{raw_key}")

        # Acquire lock (stale-aware)
        ttl = int(get("LOCK_TTL_SECONDS", "60"))
        lock_key = f"{in_cont}/{raw_key}"
        lock = acquire_lock_with_break(lock_key, ttl=ttl)
        if not lock:
            log_job("lock", "busy or healthy lease present; skipping", key=lock_key)
            return

        # Workspace
        tmp_root = get("TMP_DIR", "/tmp/ingestor")
        work_dir = os.path.join(tmp_root, stem, "work"); os.makedirs(work_dir, exist_ok=True)
        dist_dir = os.path.join(tmp_root, stem, "dist"); os.makedirs(dist_dir, exist_ok=True)
        inp_dir  = os.path.join(tmp_root, stem, "input"); os.makedirs(inp_dir,  exist_ok=True)
        inp_path = os.path.join(inp_dir, os.path.basename(raw_key))

        # Live stream log to Blob + bridge callable
        sl = StreamLogger(job_id=stem, dist_dir=dist_dir, container=LOGS)
        sl.start(interval_sec=20)
        log = bridge_logger(LOGGER, sl)  # ← use this everywhere

        # Heartbeat
        _ = _start_heartbeat(lock, ttl, hb_stop, log=log)

        # Download input
        t0 = time.time()
        bc_in = blob_client(in_cont, raw_key)
        with open(inp_path, "wb") as f:
            bc_in.download_blob(max_concurrency=4).readinto(f)
        log(f"[download] input ready path={inp_path} took={int(time.time()-t0)}s")

        # QC (tools, smoke, probe, analyze) — strict
        meta_in = ffprobe_validate(inp_path, log=log, strict=True, smoke=True)

        # Optional captions
        t0 = time.time()
        text_tracks = []
        for item in (captions or []):
            c = _pull_caption(item, work_dir)
            if c: text_tracks.append(c)
        log(f"[captions] count={len(text_tracks)} took={int(time.time()-t0)}s")
        log_job("captions", "prepared", count=len(text_tracks))

        # ---- Decide whether to skip transcode (restore from mezz) ----
        audio_mp4 = str(Path(work_dir) / "audio.mp4")
        labels = ["240p","360p","480p","720p","1080p"]  # or derive from your ladder
        expected_videos = [str(Path(work_dir) / f"video_{lab[:-1]}.mp4") for lab in labels]
        need = [audio_mp4] + expected_videos
        missing = [p for p in need if not os.path.exists(p)]

        skip_transcode = str(get("SKIP_TRANSCODE_IF_MEZZ", "true")).lower() in ("1","true","yes","on")
        restored_from_mezz = False
        did_transcode = False

        if skip_transcode and missing:
            log("[mezz] attempting restore of intermediates")
            if ensure_intermediates_from_mezz(stem=stem, work_dir=work_dir, log=log):
                missing = [p for p in need if not os.path.exists(p)]
                if not missing:
                    restored_from_mezz = True
                    log("[transcode] using restored intermediates; skipping transcode")
                    # reconstruct renditions from disk
                    renditions = []
                    for lab in labels:
                        v = str(Path(work_dir) / f"video_{lab[:-1]}.mp4")
                        if os.path.exists(v):
                            renditions.append({"name": lab, "video": v})
                else:
                    log(f"[mezz] partial restore; still missing {len(missing)} → will transcode")
                    audio_mp4, renditions, meta_in = transcode_to_cmaf_ladder(inp_path, work_dir, log=log)
                    did_transcode = True
            else:
                log("[mezz] restore unavailable → transcode")
                audio_mp4, renditions, meta_in = transcode_to_cmaf_ladder(inp_path, work_dir, log=log)
                did_transcode = True
        else:
            # either not skipping, or nothing missing → run transcode
            audio_mp4, renditions, meta_in = transcode_to_cmaf_ladder(inp_path, work_dir, log=log)
            did_transcode = True

        # ---- Mezz upload (conditional) ----
        # If we transcoded, upload mezz + manifest as before.
        # If we restored, skip upload unless manifest is missing in mezz (first-run safety).
        mezz_manifest = None
        mezz_manifest_blob = f"{stem}/manifest.json"
        if did_transcode:
            mezz_manifest = upload_mezz_and_manifest(stem=stem, work_dir=work_dir, log=log)
            log(f"[mezz] uploaded files={len(mezz_manifest.get('files', []))}")
        else:
            if not blob_exists(MEZZ, mezz_manifest_blob):
                log("[mezz] restored but manifest missing in container → uploading manifest and listing")
                mezz_manifest = upload_mezz_and_manifest(stem=stem, work_dir=work_dir, log=log)
                log(f"[mezz] uploaded files={len(mezz_manifest.get('files', []))}")
            else:
                log("[mezz] restored; manifest exists → skipping mezz re-upload")

        # Verify transcode outputs (fast)
        verify_transcode_outputs(audio_mp4, renditions, meta_in, log=log, re_probe_outputs=False)

        # Packaging (skip if manifests already there)
        pkg_root = Path(dist_dir)
        dash_path = pkg_root / "dash" / "stream.mpd"
        hls_path  = pkg_root / "hls"  / "master.m3u8"

        if dash_path.exists() and hls_path.exists():
            log("[package] already done; skipping")
        else:
            _call_package(
                package_with_shaka_ladder,
                renditions=renditions,
                audio_mp4=audio_mp4,
                out_dash=str(dash_path),
                out_hls=str(hls_path),
                text_tracks=text_tracks,
                log=log,
            )

        # Verify HLS & DASH
        verify_dash(str(dash_path), log=log)
        verify_hls(str(hls_path), log=log)
        log_job("package", "ok", out=str(dist_dir))

        # Upload routed (hls → HLS, dash → DASH) — skip if present
        t0 = time.time()
        upload_tree_routed(
            dist_dir=str(dist_dir),
            stem=job_id,
            hls_container=HLS,
            dash_container=DASH,
            strategy="if-missing",     # or "idempotent" to use your hash/etag logic
            log=log
        )
        log(f"[upload] routed took={int(time.time()-t0)}s")

        # Publish simple manifest
        version = hashlib.sha1(str(time.time()).encode("utf-8")).hexdigest()[:8]
        manifest = {
            "id": stem,
            "version": version,
            "hls": f"{HLS}/{stem}/master.m3u8",
            "dash": f"{DASH}/{stem}/stream.mpd",
            "captions": [{"lang": t["lang"], "path": f"{HLS}/{stem}/{os.path.basename(t['path'])}"} for t in (text_tracks or [])]
        }
        upload_bytes(PROCESSED, f"{stem}/manifest.json", json.dumps(manifest, indent=2).encode("utf-8"), "application/json")
        upload_bytes(PROCESSED, f"{stem}/latest.json", json.dumps({"version": version, "updatedAt": int(time.time())}).encode("utf-8"), "application/json")

        log("[queue] success")

    except BadMessageError as e:
        log_exception("queue", f"Bad message: {e}")
        send_to_poison(msg, queue_name=JOB_QUEUE, reason="invalid_message", details={"error": str(e)})
        return
    except CmdError as e:
        log_exception("queue", f"Command failed: {e}")
        raise
    except Exception as e:
        log_exception("queue", f"Unhandled error: {e}")
        raise
    finally:
        try: hb_stop.set()
        except Exception: pass
        try:
            if lock is not None: release_lock(lock)
        except Exception: pass
        try:
            if sl: sl.close()
        except Exception: pass
        kill_all_children()