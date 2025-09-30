import os
import json
import ast
import logging
import hashlib
import time
import threading
from pathlib import Path
from typing import List, Dict, Optional, Callable

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
)
from ..shared.qc import ffprobe_inspect, precheck_strict
from ..shared.transcode import (
    transcode_to_cmaf_ladder,
    package_with_shaka_ladder,
    _resolve_packager,
    CmdError,
    kill_all_children,
)
from ..shared.storage import (
    ensure_containers,
    blob_client,
    blob_exists,
    upload_tree_routed,
    upload_bytes,
    acquire_lock_with_break,   # stale-aware
    renew_lock,                # bumps lock_ts metadata
    release_lock,
)

# ---------------------- module logger (no context.get_logger) ----------------------
LOGGER = logging.getLogger("queue_ingestor")
if not LOGGER.handlers:
    _h = logging.StreamHandler()  # Functions console
    _h.setFormatter(logging.Formatter("%(asctime)sZ %(levelname)s %(name)s | %(message)s"))
    LOGGER.addHandler(_h)
LOGGER.setLevel(logging.INFO)

# ---------------------- small logging bridges ----------------------
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

# ---------------------- config snapshot ----------------------
RAW        = get("RAW_CONTAINER", "raw-videos")
MEZZ       = get("MEZZ_CONTAINER", "mezzanine")
HLS        = get("HLS_CONTAINER", "hls")
DASH       = get("DASH_CONTAINER", "dash")
LOGS       = get("LOGS_CONTAINER", "logs")
PROCESSED  = get("PROCESSED_CONTAINER", "processed")
LOCKS      = get("LOCKS_CONTAINER", "locks")
JOB_QUEUE  = get("JOB_QUEUE", "transcode-jobs")

# ---------------------- errors ----------------------
class BadMessageError(Exception): ...
class ConfigError(Exception): ...

# ---------------------- poison helpers ----------------------
def _queue_client(name: str) -> QueueClient:
    conn = os.getenv("AzureWebJobsStorage") or os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    return QueueClient.from_connection_string(conn, name)

def send_to_poison(msg: func.QueueMessage, *, queue_name: str, reason: str, details: dict | None = None):
    poison = f"{queue_name}-poison"
    qc = _queue_client(poison)
    try:
        qc.create_queue()
    except Exception:
        pass
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

# ---------------------- message parsing ----------------------
def _safe_json(raw: str, *, allow_plain: bool = True, max_log_chars: int = 500):
    s = (raw or "").strip()
    if not s:
        raise BadMessageError("empty queue message body")
    try:
        return json.loads(s)
    except json.JSONDecodeError:
        pass
    # dev-case: single-quoted dict/list or plain string
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

# ---------------------- captions pull ----------------------
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

# ---------------------- flexible call shims ----------------------
def _call_transcode(fn, *, input_path, work_dir, segment_duration=None,
                    job_id=None, stem=None, captions=None, log=None):
    """Try common kw/positional shapes so we don't break when transcode signature shifts."""
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
            if isinstance(t, dict):      return fn(**t)
            _, args = t;                 return fn(*args)
        except TypeError as e:
            last = e; continue
    raise last

def _call_package(fn, *, renditions, audio_mp4, out_dash, out_hls, text_tracks=None, log=None):
    """Single normalized entrypoint for packager; keeps call sites neat."""
    return fn(renditions=renditions, audio_mp4=audio_mp4, out_dash=out_dash,
              out_hls=out_hls, text_tracks=text_tracks, log=log)

# ---------------------- lock heartbeat (calls renew_lock, which bumps metadata) ----------------------
def _start_heartbeat(lock_handle, ttl: int, stop_evt: threading.Event, log: Optional[Callable[[str], None]] = None):
    interval = max(5, ttl // 2)
    def _beat():
        while not stop_evt.wait(interval):
            try:
                renew_lock(lock_handle, ttl=ttl)
                (log or LOGGER.info)(f"[lock] renewed {lock_handle.get('blob','?')}")
            except Exception as e:
                (log or LOGGER.info)(f"[lock] renew failed: {e}")
    t = threading.Thread(target=_beat, name="lock-heartbeat", daemon=True)
    t.start()
    return t

# ---------------------- Function entrypoint ----------------------
@app.queue_trigger(arg_name="msg", queue_name=JOB_QUEUE, connection="AzureWebJobsStorage")
def queue_ingestor(msg: func.QueueMessage, context: func.Context):
    logger = LOGGER  # no context.get_logger()

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

    # Env visibility (non-secret)
    for k in ["RAW_CONTAINER","MEZZ_CONTAINER","HLS_CONTAINER","DASH_CONTAINER","LOGS_CONTAINER","PROCESSED_CONTAINER",
              "TMP_DIR","SEGMENT_DURATION","FFMPEG_PATH","SHAKA_PACKAGER_PATH","LOCK_TTL_SECONDS","LOCKS_CONTAINER"]:
        try:
            v = get(k, default=None)
            log_job("env", f"{k}={'<set>' if v not in (None,'') else '<missing>'}")
        except Exception:
            log_job("env", f"{k}=<missing>")

    if not PROCESSED:
        raise ConfigError("Missing PROCESSED_CONTAINER configuration")
    ensure_containers([HLS, DASH, LOGS, PROCESSED])

    # Early packager presence check — poison if missing
    try:
        _resolve_packager(log=logger.info)
    except CmdError as e:
        log_exception("package", f"Packager missing: {e}")
        send_to_poison(msg, queue_name=JOB_QUEUE, reason="shaka_packager_missing", details={"error": str(e)})
        return

    lock = None
    hb_stop = threading.Event()
    sl: Optional[StreamLogger] = None
    log: Optional[Callable[[str], None]] = None

    try:
        if not blob_exists(in_cont, raw_key):
            raise FileNotFoundError(f"Input blob not found: {in_cont}/{raw_key}")

        # Acquire stale-aware lock using a stable key (storage hashes to a safe blob name)
        ttl = int(get("LOCK_TTL_SECONDS", "60"))
        lock_key = f"{in_cont}/{raw_key}"
        lock = acquire_lock_with_break(lock_key, ttl=ttl)
        if not lock:
            log_job("lock", "busy or healthy lease present; skipping", key=lock_key)
            return

        # Log actual lock blob URL for visibility
        try:
            bc_lock = blob_client(LOCKS, lock["blob"])
            log_job("lock.target", container=bc_lock.container_name, blob=bc_lock.blob_name, url=bc_lock.url)
        except Exception:
            pass

        # Workspace
        tmp_root = get("TMP_DIR", "/tmp/ingestor")
        work_dir = os.path.join(tmp_root, stem, "work"); os.makedirs(work_dir, exist_ok=True)
        dist_dir = os.path.join(tmp_root, stem, "dist"); os.makedirs(dist_dir, exist_ok=True)
        inp_dir  = os.path.join(tmp_root, stem, "input"); os.makedirs(inp_dir,  exist_ok=True)
        inp_path = os.path.join(inp_dir, os.path.basename(raw_key))

        # Live stream log to Blob + bridge callable for all progress
        sl = StreamLogger(job_id=stem, dist_dir=dist_dir, container=LOGS)
        sl.start(interval_sec=20)
        log = bridge_logger(logger, sl)
        log("[queue] workspace ready")

        # Heartbeat (uses renew_lock which now refreshes lock_ts)
        hb_thread = _start_heartbeat(lock, ttl, hb_stop, log=log)

        # Download (streaming)
        t0 = time.time()
        bc_in = blob_client(in_cont, raw_key)
        stream = bc_in.download_blob(max_concurrency=4)
        with open(inp_path, "wb") as f:
            stream.readinto(f)
        dt = int(time.time() - t0)
        log(f"[download] input ready path={inp_path} took={dt}s")

        # QC
        t0 = time.time()
        probe = ffprobe_inspect(inp_path)
        precheck_strict(probe)
        segdur = float(get("SEGMENT_DURATION", "4"))
        dt = int(time.time() - t0)
        log(f"[qc] ok took={dt}s")

        # Captions (optional)
        t0 = time.time()
        text_tracks = []
        for item in (captions or []):
            c = _pull_caption(item, work_dir)
            if c: text_tracks.append(c)
        dt = int(time.time() - t0)
        log(f"[captions] count={len(text_tracks)} took={dt}s")
        log_job("captions", "prepared", count=len(text_tracks), took=dt)

        # Transcode
        log_job("transcode", "begin", segdur=segdur); log("[transcode] begin")
        t0 = time.time()
        audio_mp4, renditions, meta = _call_transcode(
            transcode_to_cmaf_ladder,
            input_path=inp_path,
            work_dir=work_dir,
            segment_duration=segdur,
            job_id=job_id,
            stem=stem,
            captions=text_tracks,
            log=log,                 # ← unified callable → console + Blob
        )
        dt = int(time.time() - t0)
        log(f"[transcode] end took={dt}s outputs={len(renditions)}")
        log_job("transcode", "end", took=dt, outputs=len(renditions))

        # Package (idempotent)
        pkg_root = Path(dist_dir)
        dash_path = pkg_root / "dash" / "stream.mpd"
        hls_path  = pkg_root / "hls"  / "master.m3u8"

        dash_ok = dash_path.exists()
        hls_ok  = hls_path.exists()
        if dash_ok and hls_ok:
            log("[package] already done; skipping")
            log_job("package", "already done; skipping")
        else:
            t0 = time.time()
            _call_package(
                package_with_shaka_ladder,
                renditions=renditions,
                audio_mp4=audio_mp4,
                out_dash=str(dash_path),
                out_hls=str(hls_path),
                text_tracks=text_tracks,
                log=log,
            )
            dt = int(time.time() - t0)
            log(f"[package] end took={dt}s")

        # Verify outputs
        if not dash_path.exists():
            raise CmdError(f"Expected DASH MPD missing: {dash_path}")
        if not hls_path.exists():
            raise CmdError(f"Expected HLS master missing: {hls_path}")
        log(f"[verify] ok mpd={dash_path} m3u8={hls_path}")
        log_job("package", "end", out=str(dist_dir))

        # Upload artifacts (route hls/ & dash/ correctly)
        t0 = time.time()
        upload_tree_routed(dist_dir=str(dist_dir), stem=job_id, hls_container=HLS, dash_container=DASH)
        dt = int(time.time() - t0)
        log(f"[upload] routed took={dt}s")

        # Manifests
        version = hashlib.sha1(str(time.time()).encode("utf-8")).hexdigest()[:8]
        manifest = {
            "id": stem,
            "version": version,
            "hls": f"{HLS}/{stem}/master.m3u8",
            "dash": f"{DASH}/{stem}/stream.mpd",
            "thumbnails": f"{PROCESSED}/{stem}/assets/thumbnails/thumbnails.vtt",
            "captions": [{"lang": t["lang"], "path": f"{HLS}/{stem}/{os.path.basename(t['path'])}"} for t in (text_tracks or [])]
        }
        upload_bytes(PROCESSED, f"{stem}/manifest.json", json.dumps(manifest, indent=2).encode("utf-8"), "application/json")
        latest = {"version": version, "updatedAt": int(time.time())}
        upload_bytes(PROCESSED, f"{stem}/latest.json", json.dumps(latest).encode("utf-8"), "application/json")

        log_job(stem, f"QUEUE SUCCESS: Transcoded and packaged {raw_key}.")
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
        try:
            hb_stop.set()
        except Exception:
            pass
        try:
            if lock is not None:
                release_lock(lock)   # use handle, not string
        except Exception:
            pass
        try:
            if sl: sl.close()        # final flush of the live job log to Blob
        except Exception:
            pass
        # make sure ffmpeg/packager children never survive this invocation
        kill_all_children()
