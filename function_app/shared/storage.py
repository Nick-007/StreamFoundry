import os
import mimetypes
import hashlib
from typing import Iterable, Optional, Dict, Any
from datetime import datetime, timedelta, timezone

try:
    from azure.storage.blob import BlobServiceClient, ContentSettings
except Exception as e:  # pragma: no cover
    BlobServiceClient = None
    ContentSettings = None

# Optional: use shared.config.get if present
try:
    from .config import get as _get
except Exception:  # pragma: no cover
    _get = None

def _conn_str() -> str:
    cs = os.getenv("AzureWebJobsStorage")
    if not cs and _get:
        cs = _get("AzureWebJobsStorage", default=None)
    if not cs:
        raise RuntimeError("AzureWebJobsStorage not configured")
    return cs

def _svc():
    if BlobServiceClient is None:
        raise RuntimeError("azure-storage-blob is not installed")
    return BlobServiceClient.from_connection_string(_conn_str())

def ensure_containers(names: Iterable[str]) -> None:
    svc = _svc()
    for name in names:
        try:
            svc.create_container(name)
        except Exception:
            pass  # already exists

def blob_client(container: str, blob: str):
    return _svc().get_blob_client(container=container, blob=blob)

def blob_exists(container: str, blob: str) -> bool:
    try:
        return blob_client(container, blob).exists()
    except Exception:
        return False

# add to function_app/shared/storage.py

def copy_blob(src_container: str, src_blob: str,
              dst_container: str, dst_blob: str,
              overwrite: bool = True) -> None:
    """
    Simple copy: download from source, upload to destination.
    Works in Azurite and real Azure; for large blobs switch to async service copy.
    """
    svc = _svc()
    ensure_containers([dst_container])

    src_bc = svc.get_blob_client(container=src_container, blob=src_blob)
    if not src_bc.exists():
        raise FileNotFoundError(f"Source blob not found: {src_container}/{src_blob}")

    dst_bc = svc.get_blob_client(container=dst_container, blob=dst_blob)
    if (not overwrite) and dst_bc.exists():
        return

    # Guess content-type from destination name (fall back to source props if desired)
    import mimetypes
    ct, _ = mimetypes.guess_type(dst_blob)
    kw = {"overwrite": True}
    if ct and ContentSettings:
        kw["content_settings"] = ContentSettings(content_type=ct)

    data = src_bc.download_blob().readall()
    dst_bc.upload_blob(data, **kw)


def upload_file(container: str, blob: str, path: str, content_type: Optional[str] = None) -> None:
    if content_type is None:
        content_type, _ = mimetypes.guess_type(path)
    kw = {"overwrite": True}
    if content_type and ContentSettings:
        kw["content_settings"] = ContentSettings(content_type=content_type)
    bc = blob_client(container, blob)
    with open(path, "rb") as f:
        bc.upload_blob(f, **kw)

def upload_bytes(container: str, blob: str, data: bytes, content_type: Optional[str] = None) -> None:
    kw = {"overwrite": True}
    if content_type and ContentSettings:
        kw["content_settings"] = ContentSettings(content_type=content_type)
    blob_client(container, blob).upload_blob(data, **kw)

# -------------------- TTL lease locks --------------------
def _locks_container() -> str:
    # Prefer config.get, fall back to env, default "locks"
    if _get:
        v = _get("LOCKS_CONTAINER", default=None)
        if v: return v
    return os.getenv("LOCKS_CONTAINER", "locks")

def _lease_duration(ttl: Optional[int]) -> int:
    # Blob leases support 15-60s or infinite (-1). We clamp to [15,60].
    if ttl is None: ttl = 60
    try:
        ttl = int(ttl)
    except Exception:
        ttl = 60
    return max(15, min(60, ttl))

def _lock_blob_name(key: str) -> str:
    # stable, filesystem-safe name for the lock blob
    h = hashlib.sha1(key.encode("utf-8")).hexdigest()
    return f"{h}.lock"

def acquire_lock(key: str, ttl: Optional[int] = None) -> Dict[str, Any]:
    """
    Acquire a TTL-based lock using a blob lease.
    Returns a handle dict with container/blob/lease_id/expires_at.
    Backwards-compatible: can be called with only (key).
    """
    svc = _svc()
    container = _locks_container()
    ensure_containers([container])
    bn = _lock_blob_name(key)
    bc = svc.get_blob_client(container=container, blob=bn)
    # create blob if missing
    try:
        bc.upload_blob(b"", overwrite=False)
    except Exception:
        pass
    duration = _lease_duration(ttl)
    lease = bc.acquire_lease(lease_duration=duration)  # seconds
    expires_at = datetime.now(timezone.utc) + timedelta(seconds=duration)
    return {
        "key": key,
        "container": container,
        "blob": bn,
        "lease_id": lease.id,
        "expires_at": expires_at.isoformat(),
    }

def renew_lock(lock: Dict[str, Any], ttl: Optional[int] = None) -> Dict[str, Any]:
    """
    Renew the lease and extend expires_at by ttl seconds.
    """
    svc = _svc()
    container = lock["container"]; bn = lock["blob"]
    bc = svc.get_blob_client(container=container, blob=bn)
    from azure.storage.blob import BlobLeaseClient  # type: ignore
    lease = BlobLeaseClient(client=bc, lease_id=lock["lease_id"])
    lease.renew()
    duration = _lease_duration(ttl)
    new_expiry = datetime.now(timezone.utc) + timedelta(seconds=duration)
    lock["expires_at"] = new_expiry.isoformat()
    return lock

def release_lock(lock: Dict[str, Any]) -> None:
    """
    Release the lease. Idempotent.
    """
    try:
        svc = _svc()
        container = lock["container"]; bn = lock["blob"]
        bc = svc.get_blob_client(container=container, blob=bn)
        from azure.storage.blob import BlobLeaseClient  # type: ignore
        lease = BlobLeaseClient(client=bc, lease_id=lock.get("lease_id"))
        lease.release()
    except Exception:
        # best-effort
        pass