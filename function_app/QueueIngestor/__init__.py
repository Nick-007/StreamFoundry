# function_app/QueueIngestor/__init__.py
from __future__ import annotations

import os, json, time, hashlib, threading, logging
from pathlib import Path
from typing import Optional, Dict, Any

import azure.functions as func
from azure.storage.queue import QueueClient
from azure.core.exceptions import ResourceNotFoundError

# ---------- Function App (v2 decorators) ----------
from .. import app

# ---------- shared helpers (your existing modules) ----------
from ..shared.config import get
from ..shared.logger import (
    log_job as _log_job_base,
    log_exception as _log_exception_base,
    StreamLogger,
    bridge_logger,
)
from ..shared.storage import (
    blob_client, blob_exists, upload_bytes,
    acquire_lock_with_break,
)
from ..shared.qc import ffprobe_validate, _safe_json
from ..shared.transcode import (
    _ffmpeg_audio_to_cmaf_segments,
    _ffmpeg_video_to_cmaf_segments,
    derive_k,
)
# Integrity checker (expects check_integrity.py to be in function_app/shared/)
from ..shared.verify import integrity_local, integrity_remote, integrity_cmaf_local
# Containers / Queues
IN       = get("RAW_CONTAINER", "raw-videos")
HLS      = get("HLS_CONTAINER",   "hls")
DASH     = get("DASH_CONTAINER",  "dash")
MEZZ     = get("MEZZ_CONTAINER",  "mezzanine")
LOGS     = get("LOGS_CONTAINER",  "logs")
INGEST_Q = get("JOB_QUEUE",    "transcode-jobs")   # queue this function listens to
PKG_Q    = get("PACKAGING_QUEUE", "packaging-jobs")
PKG_CONN = get("AzureWebJobsStorage")
# -----------------------------
# Thin wrappers to your blob logger (so we can call safely)
# -----------------------------
def _log_job(topic: str, msg: str, **kv):
    try:
        return _log_job_base.__wrapped__(topic, msg, **kv)  # type: ignore[attr-defined]
    except Exception:
        return _log_job_base(topic, msg, **kv)

def _log_exception(topic: str, msg: str, **kv):
    try:
        return _log_exception_base.__wrapped__(topic, msg, **kv)  # type: ignore[attr-defined]
    except Exception:
        return _log_exception_base(topic, msg, **kv)

# -----------------------------
# Lease heartbeat (renew until stop)
# -----------------------------
def _start_heartbeat(lock_handle, ttl: int, stop_evt: threading.Event, log):
    lease = lock_handle.get("lease") if isinstance(lock_handle, dict) else None
    if lease is None or not hasattr(lease, "renew"):
        return None
    interval = max(5, ttl // 2)

    def _beat():
        while not stop_evt.wait(interval):
            try:
                lease.renew()
                _log_job("lock", f"renewed {lock_handle.get('blob','?')}")
                log(f"[lock] renewed {lock_handle.get('blob','?')}")
            except Exception as e:
                _log_exception("lock", f"renew failed: {e}")
                log(f"[lock] renew failed: {e}")

    t = threading.Thread(target=_beat, name="lock-heartbeat", daemon=True)
    t.start()
    return t
# -----------------------------
# Upload report to blob storage
# -----------------------------
def _write_report(*, container: str, stem: str, kind: str, stage: str, report: dict, log):
    ts = int(time.time())
    key = f"reports/{stem}/{stage}-{kind}-{ts}.json"
    upload_bytes(container, key, json.dumps(report, separators=(",", ":")).encode("utf-8"), "application/json")
    log(f"[verify] uploaded {stage} {kind} report → {container}/{key}")

# -----------------------------
# Log job report (upload to blob)
# -----------------------------
def _log_job_report(*, container: str, stem: str, kind: str, stage: str, report: dict, log):
    try:
        _write_report(container=container, stem=stem, kind=kind, stage=stage, report=report, log=log)
    except Exception as e:
        _log_exception("report", f"Failed to write report for {stem} {kind} {stage}: {e}")
        log(f"[report] failed to write report for {stem} {kind} {stage}: {e}")

# -----------------------------
# Enqueue packaging job (if ready)
# -----------------------------
def _enqueue_packaging_if_ready(*, stem: str, dist_dir: str, log):
    if not PKG_CONN:
        raise RuntimeError("AzureWebJobsStorage app setting is missing")
    body = json.dumps({"stem": stem, "dist_dir": dist_dir, "ts": int(time.time())}, separators=(",", ":"))
    try:
        QueueClient.from_connection_string(PKG_CONN, PKG_Q).send_message(body)
        log(f"[queue] packaging enqueued for {stem} → {PKG_Q}")
    except ResourceNotFoundError:
        # Let this fail loudly; infra is seed-owned
        raise RuntimeError(f"Queue '{PKG_Q}' not found. Run seed script.")

# -----------------------------
# One CMAF “rung” per invocation (audio once, then least-complete video)
# -----------------------------
def _process_one_rung_cmaf(
    *,
    input_path: str,
    work_dir: str,
    meta: Dict[str, Any],
    seg_dur: int,
    log,
    rung_budget_sec: Optional[int] = None,
) -> Dict[str, Any]:
    rung_budget_sec = rung_budget_sec or int(get("RUNG_BUDGET_SEC", "1400"))  # < host timeout
    t0 = time.time()

    # --- derive duration early (from ffprobe meta) ---
    duration_sec = float(meta.get("duration_sec") or meta.get("duration") or 0.0)
    fps = float(meta.get("fps") or 24.0)

    # Audio first (idempotent)
    audio_dir = Path(work_dir) / "cmaf" / "audio"
    audio_init = audio_dir / "audio_init.m4a"
    if not audio_init.exists():
        log("[audio] begin (CMAF)")
        budget_left = max(60, rung_budget_sec - int(time.time() - t0))
        _ffmpeg_audio_to_cmaf_segments(
            input_path=input_path,
            out_dir=str(audio_dir),
            seg_dur=seg_dur,
            total_duration_sec=duration_sec,
            budget_sec=budget_left,
            log=log,
        )
        log("[audio] end (CMAF)")
        rep = integrity_cmaf_local(
            root=str(audio_dir),
            kind="audio", label=None,
            seg_prefix="audio_", init_name="audio_init.m4a",
            seg_dur=seg_dur, duration_sec=duration_sec,
            log=log
        )
        if rep["status"] == "fail":
            raise RuntimeError("audio CMAF integrity failed")
        return {"kind": "audio", "label": "stereo", "K": 0}  # audio doesn’t use GOP; keep shape

    # Ladder (same as your MP4 ladder, now CMAF’d)
    ladder = [
        {"name":"240p",  "height":240,  "bv":"300k",  "maxrate":"360k",  "bufsize":"600k"},
        {"name":"360p",  "height":360,  "bv":"650k",  "maxrate":"780k",  "bufsize":"1300k"},
        {"name":"480p",  "height":480,  "bv":"900k",  "maxrate":"1000k", "bufsize":"1800k"},
        {"name":"720p",  "height":720,  "bv":"2500k", "maxrate":"2800k", "bufsize":"5000k"},
        {"name":"1080p", "height":1080, "bv":"4200k", "maxrate":"4600k", "bufsize":"8000k"},
    ]

    def rung_progress(label: str) -> int:
        vdir = Path(work_dir) / "cmaf" / "video" / label
        if not vdir.exists():
            return 0
        return len(list(vdir.glob(f"video_{label}_*.m4s")))

    target = sorted(ladder, key=lambda r: rung_progress(r["name"]))[0]
    label = target["name"]
    vdir  = Path(work_dir) / "cmaf" / "video" / label
    vdir.mkdir(parents=True, exist_ok=True)

    budget_left = max(60, rung_budget_sec - int(time.time() - t0))
    log(f"[video:{label}] begin → {vdir} (budget~{budget_left}s)")

    res = _ffmpeg_video_to_cmaf_segments(
        input_path=input_path,
        out_dir=str(vdir),
        label=label,
        height=int(target["height"]),
        bv=target["bv"],
        maxrate=target["maxrate"],
        bufsize=target["bufsize"],
        fps=fps,
        seg_dur=seg_dur,
        total_duration_sec=duration_sec,
        budget_sec=budget_left,
        log=log,
    )
    
    log(f"[video:{label}] end")
    # Bubble up how many segments were emitted this run (res["K"])
    rep = integrity_cmaf_local(
        root=str(vdir),
        kind="video", label=label,
        seg_prefix=f"video_{label}_", init_name=f"video_{label}_init.mp4",
        seg_dur=seg_dur, duration_sec=duration_sec,
        log=log
    )
    if rep["status"] == "fail":
        raise RuntimeError(f"video {label} CMAF integrity failed")
    return {"kind": "video", "label": label, "K": res.get("K", 0)}

# -----------------------------
# Queue Trigger (v2 DECORATOR MODEL)
# -----------------------------
@app.function_name(name="queue_ingestor")
@app.queue_trigger(arg_name="msg",
                   queue_name=INGEST_Q,
                   connection="AzureWebJobsStorage")
def queue_ingestor(msg: func.QueueMessage) -> None:
    # Console logger picked up by Functions host
    pylog = logging.getLogger("Function.queue_ingestor").info

    # Stream logger to blobs (bridged), created once we know dist_dir
    sl: Optional[StreamLogger] = None
    log = pylog  # will be bridged once StreamLogger starts

    # Heartbeat control
    hb_stop: Optional[threading.Event] = None
    hb_thr:  Optional[threading.Thread] = None
    lock     = None

    # Minimal job id: first 8 chars of the message id (or time-based)
    job_id = (msg.id or hex(int(time.time()))[2:])[:8]

    try:
        raw = msg.get_body().decode("utf-8")
        payload = _safe_json(raw)

        in_cont  = payload.get("in_cont")  or IN
        raw_key  = payload.get("blob")     or payload.get("key")
        stem     = payload.get("stem")     or Path(raw_key or "").stem

        if not raw_key:
            raise ValueError("payload must contain 'blob' (path inside input container)")

        if not blob_exists(in_cont, raw_key):
            raise FileNotFoundError(f"Input blob not found: {in_cont}/{raw_key}")

        # Acquire lock (stale-aware)
        ttl = int(get("LOCK_TTL_SECONDS", "60"))
        lock_key = f"{in_cont}/{raw_key}"
        lock = acquire_lock_with_break(lock_key, ttl=ttl)
        if not lock:
            _log_job("lock", "busy or healthy lease present; skipping", key=lock_key)
            pylog(f"[lock] busy/healthy lease; skip {lock_key}")
            return

        hb_stop = threading.Event()
        hb_thr  = _start_heartbeat(lock, ttl, hb_stop, log=pylog)

        # Workspace
        tmp_root = get("TMP_DIR", "/tmp/ingestor")
        work_dir = os.path.join(tmp_root, stem, "work");  os.makedirs(work_dir, exist_ok=True)
        dist_dir = os.path.join(tmp_root, stem, "dist");  os.makedirs(dist_dir, exist_ok=True)
        inp_dir  = os.path.join(tmp_root, stem, "input"); os.makedirs(inp_dir,  exist_ok=True)
        inp_path = os.path.join(inp_dir, os.path.basename(raw_key))

        # Start StreamLogger now that we know dist_dir, bridge to console
        try:
            sl = StreamLogger(job_id=stem, dist_dir=dist_dir, container=LOGS)
            sl.start(interval_sec=20)
            log = bridge_logger(pylog, sl)   # use this everywhere
        except Exception:
            log = pylog

        # Download input (overwrite ok)
        t0 = time.time()
        bc_in = blob_client(in_cont, raw_key)
        with open(inp_path, "wb") as f:
            bc_in.download_blob(max_concurrency=4).readinto(f)
        log(f"[download] input ready path={inp_path} took={int(time.time()-t0)}s")
        pylog(f"[download] input ready path={inp_path} took={int(time.time()-t0)}s")

        # QC (tools, smoke, probe, analyze) — strict
        meta_in = ffprobe_validate(inp_path, log=log, strict=True, smoke=True)

        # One CMAF rung per invocation
        seg_dur = int(get("SEG_DUR_SEC", "4"))
        r = _process_one_rung_cmaf(
            input_path=inp_path,
            work_dir=work_dir,
            meta=meta_in,
            seg_dur=seg_dur,
            log=log,
        )
        log(f"[rung] done kind={r.get('kind')} label={r.get('label','-')} K={r.get('K','-')}")
        pylog(f"[rung] done kind={r.get('kind')} label={r.get('label','-')} K={r.get('K','-')}")

        # Enqueue packaging (parallel; packager checks readiness)
        _enqueue_packaging_if_ready(stem=stem, dist_dir=dist_dir, log=log)
        pylog(f"[queue] packaging enqueued for {stem} → {PKG_Q}")
        log("[queue] success (rung completed; more work will resume on next dequeue)")
        pylog("[queue] success (rung completed; more work will resume on next dequeue)")

    except Exception as e:
        _log_exception("queue", f"Unhandled error: {e}")
        pylog(f"Unhandled error: {e}")
        raise
    finally:
        # stop heartbeat
        try:
            if hb_stop:
                hb_stop.set()
            if hb_thr:
                hb_thr.join(timeout=2.0)
        except Exception:
            pass
        # release lease (best-effort)
        try:
            if lock and isinstance(lock, dict) and lock.get("lease"):
                try:
                    lock["lease"].release()
                except Exception:
                    pass
        except Exception:
            pass
        # stop stream logger
        try:
            if sl: sl.stop(flush=True)
        except Exception:
            pass
        pylog("[queue] function complete")