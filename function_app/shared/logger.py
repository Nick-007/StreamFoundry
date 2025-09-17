import datetime as _dt, traceback
from .config import get
from .storage import upload_bytes
def _ts(): return _dt.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
def log_job(video_name: str, content: str):
    container = get("LOGS_CONTAINER", "logs")
    blob_path = f"{video_name}/{_ts()}.log"
    upload_bytes(container, blob_path, content.encode("utf-8"), "text/plain")
def log_exception(video_name: str, exc: Exception):
    content = f"[ERROR] {type(exc).__name__}: {exc}\n{traceback.format_exc()}"
    log_job(video_name, content)
