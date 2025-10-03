import datetime as _dt
import traceback
import os
import threading
import time
from pathlib import Path
from typing import Optional, Callable, Dict, Any

from .config import get
from .storage import upload_bytes

# ---------- existing helpers (kept) ----------
def _ts() -> str:
    return _dt.datetime.now().strftime("%Y%m%dT%H%M%SZ")

def make_slogger(
    *,
    text_log: Optional[Callable[[str], None]] = None,
    job_log: Optional[Callable[[str, str, Any], None]] = None,
    ctx: Optional[Dict[str, Any]] = None,
):
    """
    Returns slog(event, msg=None, **fields) and slog_exc(event, exc, **fields)
    - text_log: e.g., logging.info
    - job_log:  e.g., shared.logger.log_job
    - ctx:      static fields to attach to every structured record (job_id, stem, etc.)
    try:
        ...
    except Exception as e:
        slog_exc("transcode", e, stage="video", rung="240p")
        raise
    """
    ctx = dict(ctx or {})

    def _merge(a: Dict[str, Any] | None, b: Dict[str, Any] | None) -> Dict[str, Any]:
        out = {}
        if a: out.update(a)
        if b: out.update(b)
        return out

    def slog(event: str, msg: Optional[str] = None, **fields):
        # 1) human text
        if text_log:
            if msg is None and fields:
                # build a compact key=val summary for readability
                kv = " ".join(f"{k}={v}" for k, v in fields.items())
                text_log(f"[{event}] {kv}")
            else:
                text_log(f"[{event}] {msg or ''}".rstrip())

        # 2) structured
        jl = job_log or log_job
        if jl:
            try:
                jl(event, msg or "", **_merge(ctx, fields))
            except Exception:
                # never let logging crash the worker
                pass

    def slog_exc(event: str, exc: BaseException, **fields):
        # text
        if text_log:
            text_log(f"[{event}] EXC: {exc}")
        # structured
        try:
            log_exception(event, str(exc), **_merge(ctx, fields))
        except Exception:
            pass

    return slog, slog_exc

# ---------- existing backwards-compatible job logger (discrete blobs) ----------
def log_job(video_name: str, content: str):
    """
    Backwards-compatible: writes a single timestamped blob per call.
    Good for discrete events and breadcrumbs.
    Blob path: {LOGS_CONTAINER}/{video_name}/{UTC_ISO_TS}.log
    """
    container = get("LOGS_CONTAINER", "logs")
    blob_path = f"{video_name}/{_ts()}.log"
    upload_bytes(container, blob_path, content.encode("utf-8"), "text/plain")

def log_exception(video_name: str, exc: Exception):
    """
    Backwards-compatible error helper.
    """
    content = f"[ERROR] {type(exc).__name__}: {exc}\n{traceback.format_exc()}"
    log_job(video_name, content)

# ---------- NEW: streaming job logger (continuous log you can tail) ----------
class StreamLogger:
    """
    Writes a rolling local log file and periodically mirrors it to Blob so you
    always have a single, up-to-date log at:
        {LOGS_CONTAINER}/logs/{job_id}.log

    Use .log(msg) anywhere you would print progress. Safe to pass as `log=...`
    into your transcode/packager functions.

    Example:
        sl = StreamLogger(job_id=stem, dist_dir=dist_dir)
        sl.start(interval_sec=20)
        sl.log("transcode begin")
        ...
        sl.close()
    """
    def __init__(self, job_id: str, dist_dir: str, container: Optional[str] = None):
        self.job_id = job_id
        self.container = container or get("LOGS_CONTAINER", "logs")
        self.local_path = Path(dist_dir) / "job.log"
        self.local_path.parent.mkdir(parents=True, exist_ok=True)
        # line-buffered write to keep file hot
        self._fh = open(self.local_path, "a", buffering=1, encoding="utf-8")
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def log(self, msg: str):
        ts = _dt.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
        try:
            self._fh.write(f"{ts} {msg}\n")
        except Exception:
            # best-effort: don't crash callers on IO hiccups
            pass

    def _upload_once(self):
        try:
            with open(self.local_path, "rb") as f:
                data = f.read()
            blob = f"logs/{self.job_id}.log"
            upload_bytes(self.container, blob, data, "text/plain")
        except Exception:
            pass

    def start(self, interval_sec: int = 20):
        def _loop():
            # initial push quickly so you see something right away
            self._upload_once()
            while not self._stop.wait(interval_sec):
                self._upload_once()
        self._thread = threading.Thread(target=_loop, name="stream-logger-uploader", daemon=True)
        self._thread.start()

    def close(self):
        # final flush + final upload
        try:
            self._fh.flush()
        except Exception:
            pass
        try:
            self._stop.set()
            if self._thread:
                self._thread.join(timeout=1.0)
        except Exception:
            pass
        try:
            self._upload_once()
        except Exception:
            pass
        try:
            self._fh.close()
        except Exception:
            pass

# ---------- NEW: convenience bridge for your module/app logger ----------
def bridge_logger(py_logger, stream: Optional[StreamLogger] = None) -> Callable[[str], None]:
    """
    Returns a callable you can pass as `log=` which will:
      1) emit to your Python logger (.info), and
      2) write to the StreamLogger (if provided).

    Example:
        from .logger import StreamLogger, bridge_logger
        sl = StreamLogger(stem, dist_dir); sl.start()
        log = bridge_logger(LOGGER, sl)
        transcode_to_cmaf_ladder(..., log=log)
    """
    def _fn(msg: str):
        try:
            py_logger.info(msg)
        except Exception:
            pass
        if stream:
            stream.log(msg)
    return _fn
