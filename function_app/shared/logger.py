# shared/logger.py
from __future__ import annotations

import io
import json
import logging
import threading
import time
import traceback
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Optional, Tuple

# --- Minimal, non-invasive storage helpers -----------------------------------
# If you already have these in shared.storage, this shim will delegate to them.
try:
    from .storage import upload_bytes, ensure_containers
except Exception:
    upload_bytes = None
    ensure_containers = None
from .config import get

# Public type for text/structured loggers
LogFn = Callable[[str], None]
JobFn = Callable[[str, str, Any], None]

# -----------------------------------------------------------------------------
# Structured log helpers (JSONL).
# -----------------------------------------------------------------------------
def _ts() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
# ---------- existing backwards-compatible job logger (discrete blobs) ----------
def log_job(video_name: str, event_or_content: str, message: Optional[str] = None, **fields: Any):
    """
    Backwards-compatible: writes a single timestamped blob per call.
    Good for discrete events and breadcrumbs.
    Blob path: {LOGS_CONTAINER}/{video_name}/{UTC_ISO_TS}.log
    """
    container = get("LOGS_CONTAINER", "logs")
    blob_path = f"{video_name}/{_ts()}.log"
    if message is None and not fields:
        payload = event_or_content
        content_type = "text/plain"
        data = payload.encode("utf-8")
    else:
        rec = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "event": event_or_content,
            "message": message or "",
            **fields,
        }
        payload = json.dumps(rec, ensure_ascii=False, separators=(",", ":"))
        content_type = "application/json"
        data = payload.encode("utf-8")
    upload_bytes(container, blob_path, data, content_type)

def log_exception(video_name: str, exc: Exception | str, **fields: Any):
    """
    Backwards-compatible error helper.
    """
    if isinstance(exc, Exception):
        content = f"[ERROR] {type(exc).__name__}: {exc}\n{traceback.format_exc()}"
    else:
        content = str(exc)
    if fields:
        log_job(video_name, "exception", content, **fields)
    else:
        log_job(video_name, content)

# -----------------------------------------------------------------------------
# Slogger: unify human text + structured job logs
# -----------------------------------------------------------------------------

def make_slogger(
    *,
    text_log: Optional[LogFn] = None,
    job_log: Optional[JobFn] = None,
    ctx: Optional[Dict[str, Any]] = None,
) -> Tuple[Callable[[str, Optional[str]], None], Callable[[str, BaseException], None]]:
    """
    Returns (slog, slog_exc):
      slog(event, msg=None, **fields)
      slog_exc(event, exc, **fields)

    - text_log: e.g., logger.info or StreamLogger.text
    - job_log:  e.g., StreamLogger.job
    - ctx:      static fields to attach to each structured record (job_id, stem, stage, etc.)
    """
    context = dict(ctx or {})

    def _merge(a: Dict[str, Any] | None, b: Dict[str, Any] | None) -> Dict[str, Any]:
        out: Dict[str, Any] = {}
        if a: out.update(a)
        if b: out.update(b)
        return out

    def slog(event: str, msg: Optional[str] = None, **fields: Any) -> None:
        # 1) human
        if text_log:
            if msg is None and fields:
                kv = " ".join(f"{k}={v}" for k, v in fields.items())
                text_log(f"[{event}] {kv}")
            else:
                text_log(f"[{event}] {msg or ''}".rstrip())

        # 2) structured
        jl = job_log or log_job
        try:
            jl(event, msg or "", **_merge(context, fields))
        except Exception:
            # Never let logging kill the worker
            pass

    def slog_exc(event: str, exc: BaseException, **fields: Any) -> None:
        if text_log:
            text_log(f"[{event}] EXC: {exc}")
        try:
            log_exception(event, str(exc), **_merge(context, fields))
        except Exception:
            pass

    return slog, slog_exc

# -----------------------------------------------------------------------------
# StreamLogger: writes local rolling files + background Blob uploads
# -----------------------------------------------------------------------------

@dataclass
class _RotConf:
    max_bytes: int = 2 * 1024 * 1024      # rotate local files after ~2 MiB
    backup_count: int = 2                 # keep a couple of rolled copies

class StreamLogger:
    """
    Stream logs to local files AND periodically sync to Blob Storage.

    Local layout (under dist_dir/logs/<job_type>/):
      - <job_id>_<ts>.log        (human-friendly text stream)
      - <job_id>_<ts>.jobs.log   (JSONL structured events)

    Blob layout (exactly as requested):
      LOGS/<job_type>/<job_id>_<ts>.log
      LOGS/<job_type>/<job_id>_<ts>.jobs.log
    """
    def __init__(
        self,
        *,
        job_id: str,
        dist_dir: str,
        container: str,
        job_type: str = "transcode_queue",
        rate_limit_sec: int = 10,
        rotation: _RotConf = _RotConf(),
        time_source: Callable[[], float] = time.time,
    ):
        self.job_id = job_id
        self.container = container
        self.job_type = job_type
        self.rate_limit_sec = max(1, int(rate_limit_sec))
        self.rotation = rotation
        self._now = time_source

        # Paths
        ts = _ts()
        root = Path(dist_dir).resolve()
        self.local_root = root / "logs" / job_type
        self.local_root.mkdir(parents=True, exist_ok=True)

        base = f"{job_id}_{ts}"
        self.text_path = self.local_root / f"{base}.log"
        self.jobs_path = self.local_root / f"{base}.jobs.log"

        # Blob destinations
        self.blob_text = f"{job_type}/{base}.log"
        self.blob_jobs = f"{job_type}/{base}.jobs.log"

        # Buffers
        self._buf_text = io.StringIO()
        self._buf_jobs = io.StringIO()

        # Rotation state
        self._text_bytes = 0
        self._jobs_bytes = 0

        # Uploader thread
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._last_flush = 0.0

        # Fast local append (create files)
        self._safe_append(self.text_path, "")
        self._safe_append(self.jobs_path, "")

        # Optionally create container
        try:
            if ensure_containers:
                ensure_containers([self.container])
        except Exception:
            pass

    # --- public facades ------------------------------------------------------

    def start(self, interval_sec: int = 20) -> None:
        """Start background flusher."""
        if self._thread and self._thread.is_alive():
            return
        self._interval = max(2, int(interval_sec))
        self._stop.clear()
        self._thread = threading.Thread(target=self._loop, name="StreamLogger", daemon=True)
        self._thread.start()

    def stop(self, *, flush: bool = False) -> None:
        if flush:
            try:
                self._flush()
            except Exception:
                pass
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=3)

    # Human text line
    def text(self, s: str) -> None:
        line = self._ensure_line(s)
        self._append_local(self.text_path, line, is_jobs=False)
        self._buf_text.write(line)

    # Structured job record
    def job(self, event: str, message: str = "", **fields: Any) -> None:
        rec = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "event": event,
            "message": message,
            **fields,
        }
        line = json.dumps(rec, separators=(",", ":"), ensure_ascii=False) + "\n"
        self._append_local(self.jobs_path, line, is_jobs=True)
        self._buf_jobs.write(line)

    # For legacy bridge_logger compatibility:
    def write(self, s: str) -> None:
        self.text(str(s))

    # --- internals -----------------------------------------------------------

    def _loop(self) -> None:
        while not self._stop.is_set():
            now = self._now()
            if now - self._last_flush >= self.rate_limit_sec:
                self._flush()
                self._last_flush = now
            self._stop.wait(self._interval)
        # final flush on stop
        self._flush()

    def _flush(self) -> None:
        t = self._buf_text.getvalue()
        j = self._buf_jobs.getvalue()
        if not t and not j:
            return
        # Clear buffers first to avoid duplicates on retry
        self._buf_text = io.StringIO()
        self._buf_jobs = io.StringIO()
        try:
            if upload_bytes:
                if t:
                    upload_bytes(self.container, self.blob_text, t.encode("utf-8"), "text/plain; charset=utf-8")
                if j:
                    upload_bytes(self.container, self.blob_jobs, j.encode("utf-8"), "application/x-ndjson")
        except Exception:
            # Swallow upload failures; we still have local files
            pass

    @staticmethod
    def _ensure_line(s: str) -> str:
        s = s.rstrip("\n")
        return s + "\n"

    def _safe_append(self, path: Path, data: str) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "a", encoding="utf-8") as f:
            f.write(data)

    def _append_local(self, path: Path, data: str, *, is_jobs: bool) -> None:
        self._safe_append(path, data)

        # Track size and rotate if needed
        if is_jobs:
            self._jobs_bytes += len(data.encode("utf-8"))
            if self._jobs_bytes >= self.rotation.max_bytes:
                self._rotate(path)
                self._jobs_bytes = 0
        else:
            self._text_bytes += len(data.encode("utf-8"))
            if self._text_bytes >= self.rotation.max_bytes:
                self._rotate(path)
                self._text_bytes = 0

    def _rotate(self, path: Path) -> None:
        # Simple N-backup rotation: file -> file.1 -> file.2, deleting oldest
        try:
            for idx in range(self.rotation.backup_count, 0, -1):
                src = Path(f"{path}.{idx}") if idx > 0 else path
            # shift
            for idx in range(self.rotation.backup_count - 1, 0, -1):
                older = Path(f"{path}.{idx}")
                newer = Path(f"{path}.{idx+1}")
                if older.exists():
                    try: newer.unlink()
                    except Exception: pass
                    older.rename(newer)
            # move current to .1
            if path.exists():
                first = Path(f"{path}.1")
                try: first.unlink()
                except Exception: pass
                path.rename(first)
            # recreate empty
            self._safe_append(path, "")
        except Exception:
            # rotation is best-effort; don't crash
            pass

# -----------------------------------------------------------------------------
# Bridge helper (keep for compatibility)
# -----------------------------------------------------------------------------

def bridge_logger(base_logger: logging.Logger, sl: StreamLogger) -> LogFn:
    """
    Returns a simple callable(text) that logs to both std logger and StreamLogger.text().
    This keeps existing call-sites working (your code that calls `log("...")`).
    """
    def _log(s: str) -> None:
        try:
            base_logger.info(s)
        except Exception:
            pass
        try:
            sl.text(s)
        except Exception:
            pass
    return _log
