# function_app/PackageQueue/__init__.py
from __future__ import annotations
import json, time
from pathlib import Path
from typing import List, Dict, Optional, Any, Set

import logging
import azure.functions as func
from azure.functions import QueueMessage
from .. import app
# --- shared modules from your repo ---
from ..shared.config import get
from ..shared.logger import StreamLogger, bridge_logger
from ..shared.storage import (
    blob_exists, upload_tree_routed, download_blob_streaming,
)
from ..shared.mezz import ensure_intermediates_from_mezz
from ..shared.verify import check_integrity  # your consolidated checker
from ..shared.qc import CmdError
from ..shared.transcode import package_with_shaka_ladder

# ---------- Config ----------
MEZZ       = get("MEZZ_CONTAINER",       "mezzanine")
DASH       = get("DASH_CONTAINER",       "dash")
HLS        = get("HLS_CONTAINER",        "hls")
PROCESSED  = get("PROCESSED_CONTAINER",  "processed")
LOGS       = get("LOGS_CONTAINER",       "logs")

TMP_DIR        = get("TMP_DIR", "/tmp/ingestor")
PACKAGING_Q    = get("PACKAGING_QUEUE", "packaging-jobs")
POISON_Q       = get("PACKAGING_POISON_QUEUE", f"{PACKAGING_Q}-poison")
STORAGE_CONN   = get("AzureWebJobsStorage")

# File size limits (similar to SubmitJob and TranscodeQueue)
MAX_DOWNLOAD_MB = int(get("PACKAGE_MAX_DOWNLOAD_MB", "2048"))  # 2 GB default
CHUNK_BYTES     = int(get("PACKAGE_CHUNK_BYTES", "4194304"))   # 4 MiB default

# Optional remote integrity base (emulator/CDN). If absent, remote check is skipped.
DASH_BASE_URL  = get("DASH_BASE_URL")  # e.g. http://127.0.0.1:10000/devstoreaccount1/dash
HLS_BASE_URL   = get("HLS_BASE_URL")   # e.g. http://127.0.0.1:10000/devstoreaccount1/hls
VERIFY_HARD_FAIL = get("VERIFY_HARD_FAIL", "true").lower() in ("1","true","yes")


# ---------- Small helpers (local; mirrors transcode worker style) ----------
LOGGER = logging.getLogger("package")
def queue_client(queue_name: str):
    from azure.storage.queue import QueueClient
    return QueueClient.from_connection_string(STORAGE_CONN, queue_name)

def send_to_poison(queue_name: str, payload: dict, reason: str, log):
    try:
        qc = queue_client(queue_name)
        qc.send_message(json.dumps({
            "reason": reason,
            "at": int(time.time()),
            "payload": _safe_json(payload),
        }))
        log(f"[poison] sent to {queue_name}: {reason}")
    except Exception as e:
        log(f"[poison] failed to send: {e}")

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

def _parse_only_rung(v) -> List[str] | None:
    if v is None: return None
    if isinstance(v, str):
        s = v.strip(); return [s] if s else None
    if isinstance(v, (list, tuple, set)):
        out = [str(x).strip() for x in v if str(x).strip()]
        return out or None
    return None

def _discover_renditions(work_dir: Path, only_rung: List[str] | None) -> List[Dict]:
    """
    Discover video rung files produced during transcode restore:
    returns [{"name":"240p","video":"/.../video_240.mp4"}, ...]
    """
    # We expect video_{num}.mp4 naming from your transcode.
    rung_map = {
        "240p":  "video_240p.mp4",
        "360p":  "video_360p.mp4",
        "480p":  "video_480p.mp4",
        "720p":  "video_720p.mp4",
        "1080p": "video_1080p.mp4",
    }
    if only_rung:
        rung_map = {k: v for k, v in rung_map.items() if k in only_rung}

    out = []
    for name, fname in rung_map.items():
        p = work_dir / fname
        if p.exists():
            out.append({"name": name, "video": str(p)})
    return out

# ---------- v2 Queue Trigger (packaging only) ----------

@app.queue_trigger(arg_name="msg", queue_name=PACKAGING_Q, connection="AzureWebJobsStorage")
def package_queue(msg: QueueMessage):
    raw = msg.get_body().decode("utf-8", errors="ignore")
    if not raw:
        LOGGER.warning("Packaging payload empty; skipping.")
        return

    try:
        payload = json.loads(raw)
    except Exception as e:
        LOGGER.error(f"JSON parse failed: {e} raw={raw[:256]}")
        return

    stem = payload.get("id")
    if not stem:
        LOGGER.error("Packaging payload missing 'id'.")
        return

    only_rung = _parse_only_rung(payload.get("only_rung"))
    captions  = payload.get("captions") or []  # if you later want to include text tracks
    # extra     = payload.get("extra") or {}

    # logging to blob container
    root = Path(TMP_DIR) / stem
    dist = root / "dist"
    dist.mkdir(parents=True, exist_ok=True)

    sl = StreamLogger(job_id=stem, dist_dir=str(dist), container=LOGS, job_type="package")
    sl.start(interval_sec=20)
    log = bridge_logger(LOGGER, sl)

    try:
        log(f"[package] start id={stem} only_rung={only_rung or 'ALL'}")

        # Work layout
        work_dir = root / "work"
        work_dir.mkdir(parents=True, exist_ok=True)

        # Restore intermediates from mezz (must exist—this worker doesn't transcode)
        log("[mezz] restoring intermediates")
        restored = ensure_intermediates_from_mezz(
            stem=stem, work_dir=str(work_dir), only_rung=only_rung, log=log
        )
        if not restored:
            raise CmdError(f"mezz restore failed or nothing to restore for id={stem}")

        # Discover audio + requested video renditions
        audio_mp4 = str(work_dir / "audio.mp4")
        if not Path(audio_mp4).exists():
            raise CmdError("missing audio.mp4 in work_dir after restore")

        renditions = _discover_renditions(work_dir, only_rung)
        if not renditions:
            raise CmdError("no video renditions found to package")

        # Paths for outputs
        dash_path = dist / "dash" / "stream.mpd"
        hls_path  = dist / "hls"  / "master.m3u8"
        dash_path.parent.mkdir(parents=True, exist_ok=True)
        hls_path.parent.mkdir(parents=True,  exist_ok=True)

        sl.job("package", "begin",id=stem)
        # Package (Shaka) for requested rungs
        package_with_shaka_ladder(
            renditions=renditions,
            audio_mp4=audio_mp4,
            out_dash=str(dash_path),
            out_hls=str(hls_path),
            text_tracks=captions,  # optional: if you restored text into work_dir, map file paths here
            log=log,
        )
        sl.job("package", "end", id=stem)
        # Local integrity (before upload)
        try:
            log("[verify] local DASH/HLS")
            # Local, pre-upload integrity check (DASH & HLS)
            check_integrity(
                stem=stem,                         # job id (used for remote checks; harmless here)
                local_dist_dir=str(dist),      # parent dir that contains ./dash and ./hls
                mode="local",                      # "local" | "remote" | "both"
                fail_hard=True,                    # raise on any missing file
                log=log,
            )
            log("[verify] local ok")
        except Exception as e:
            if VERIFY_HARD_FAIL:
                raise
            log(f"[verify] local warning: {e}")

        # Upload routed (idempotent)
        upload_tree_routed(
            dist_dir=str(dist),
            stem=stem,
            hls_container=HLS,
            dash_container=DASH,
            strategy="idempotent",
            log=log,
        )

        # Remote integrity (after upload) if bases are configured
        if DASH_BASE_URL or HLS_BASE_URL:
            try:
                log("[verify] remote DASH/HLS")
                dash_url = DASH_BASE_URL.rstrip("/") + f"/{stem}/stream.mpd" if DASH_BASE_URL else None
                hls_url  = HLS_BASE_URL.rstrip("/")  + f"/{stem}/master.m3u8" if HLS_BASE_URL  else None
                check_integrity(
                    stem=stem,
                    base_url=get("BASE_URL", "http://127.0.0.1:10000/devstoreaccount1"),
                    containers={"dash": DASH, "hls": HLS},  # your container names
                    mode="remote",
                    fail_hard=True,
                    log=log,
                )
                log("[verify] remote ok")
            except Exception as e:
                if VERIFY_HARD_FAIL:
                    raise
                log(f"[verify] remote warning: {e}")
        else:
            log("[verify] remote skipped (no *_BASE_URL configured)")

        log("[package] success")

    except Exception as e:
        reason = f"{type(e).__name__}: {e}"
        send_to_poison(POISON_Q, payload, reason, log)
        raise
    finally:
        sl.stop()