import os, mimetypes, hashlib, socket, threading, time
from typing import Iterable, Optional, Dict, Any, Callable
from datetime import datetime, timedelta, timezone
from pathlib import Path
try:
    from azure.storage.blob import BlobServiceClient, ContentSettings, BlobLeaseClient
    from azure.core.exceptions import ResourceExistsError, HttpResponseError, ResourceNotFoundError, ServiceRequestError
except Exception as e:  # pragma: no cover
    BlobServiceClient = None
    ContentSettings = None
    ResourceExistsError = None
    HttpResponseError = None
    ResourceNotFoundError = None
    ServiceRequestError = None
# Optional: use shared.config.get if present

try:
    from .config import get as _get
except Exception:  # pragma: no cover
    _get = None

# NEW: env-tunable behavior
BREAK_STALE_LOCKS = os.getenv("BREAK_STALE_LOCKS", "true").lower() == "true"
STALE_LOCK_SECONDS = int(os.getenv("STALE_LOCK_SECONDS", "1200"))  # 20 minutes default

# NEW helpers
def _lock_owner_id() -> str:
    return f"{socket.gethostname()}:{os.getpid()}"

def _write_lock_metadata(bc, lease_id: str | None = None) -> None:
    """Record who holds the lock and the last heartbeat timestamp."""
    md = {"lock_owner": _lock_owner_id(), "lock_ts": str(int(time.time()))}
    kw = {"lease": lease_id} if lease_id else {}
    try:
        bc.set_blob_metadata(md, **kw)
    except Exception:
        # best-effort; don't fail the pipeline over metadata
        pass

# NEW: try to acquire; if a lease exists and is stale, break and retry once.
def acquire_lock_with_break(key: str, ttl: int | None = None) -> dict | None:
    """
    Returns a lock handle dict like acquire_lock() on success, or None if a healthy lease exists.
    If BREAK_STALE_LOCKS=true and the existing lease appears stale (by metadata 'lock_ts'),
    we break it and retry once.
    """
    svc = _svc()
    container = _locks_container() if "_locks_container" in globals() else _get("LOCKS_CONTAINER", "locks")
    ensure_containers([container])
    bn = _lock_blob_name(key) if "_lock_blob_name" in globals() else key  # reuse your naming

    bc = svc.get_blob_client(container=container, blob=bn)

    # Ensure the lock blob exists
    try:
        if not bc.exists():
            bc.upload_blob(b"", overwrite=True)
            _write_lock_metadata(bc, None)
    except Exception:
        pass

    duration = _lease_duration(ttl) if "_lease_duration" in globals() else (ttl or 60)

    # First attempt
    try:
        lease = bc.acquire_lease(lease_duration=duration)
        _write_lock_metadata(bc, lease.id)
        expires_at = datetime.now(timezone.utc) + timedelta(seconds=duration)
        return {"key": key, "container": container, "blob": bn, "lease_id": lease.id, "expires_at": expires_at.isoformat()}
    except HttpResponseError:
        # A lease already exists
        if not BREAK_STALE_LOCKS:
            return None

        # Inspect metadata to decide "stale"
        try:
            props = bc.get_blob_properties()
            md = props.metadata or {}
            ts = int(md.get("lock_ts", "0"))
            age = int(time.time()) - ts if ts else None
        except Exception:
            age = None

        is_stale = (age is None) or (age > STALE_LOCK_SECONDS)
        if not is_stale:
            return None  # healthy lock; back off

        # Break and retry once
        try:
            lease2 = BlobLeaseClient(client=bc)
            lease2.break_lease(0)
        except HttpResponseError:
            return None  # race or permission issue; treat as busy

        lease3 = bc.acquire_lease(lease_duration=duration)
        _write_lock_metadata(bc, lease3.id)
        expires_at = datetime.now(timezone.utc) + timedelta(seconds=duration)
        return {"key": key, "container": container, "blob": bn, "lease_id": lease3.id, "expires_at": expires_at.isoformat()}

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
        except ResourceExistsError:
            pass  # already exists

def blob_client(container: str, blob: str):
    return _svc().get_blob_client(container=container, blob=blob)

def blob_exists(container: str, blob: str) -> bool:
    try:
        return blob_client(container, blob).exists()
    except ResourceExistsError:
        return False

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

    ct, _ = mimetypes.guess_type(dst_blob)
    kw = {"overwrite": True}
    if ct and ContentSettings:
        kw["content_settings"] = ContentSettings(content_type=ct)

    data = src_bc.download_blob().readall()
    dst_bc.upload_blob(data, **kw)

def upload_file(container: str, blob: str, path: str, content_type: Optional[str] = None, skip_if_same: bool = False) -> None:
    ensure_containers([container])  # make sure container exists
    if content_type is None:
        content_type, _ = mimetypes.guess_type(path)
    kw = {"overwrite": True}
    md5 = None
    if content_type and ContentSettings:
        # include MD5 so remote props carry it (optional idempotency)
        try:
            with open(path, "rb") as fh:
                md5 = hashlib.md5(fh.read()).digest()
        except Exception:
            md5 = None
        kw["content_settings"] = ContentSettings(content_type=content_type, content_md5=md5)
    bc = blob_client(container, blob)
    if skip_if_same and bc.exists():
        try:
            props = bc.get_blob_properties()
            remote_md5 = props.content_settings.content_md5 if props and props.content_settings else None
            if remote_md5 and md5 and remote_md5 == md5:
                return  # identical -> skip
        except Exception:
            pass
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

# OPTIONAL helper (you may use your own heartbeat in the function)
def start_lock_heartbeat(lock: Dict[str, Any], ttl: int, stop_evt: threading.Event, log: Callable[[str], None] | None = None):
    """
    Renew the lease and bump lock_ts every ~ttl/2 seconds until stop_evt is set.
    """
    svc = _svc()
    bc = svc.get_blob_client(container=lock["container"], blob=lock["blob"])
    lease = BlobLeaseClient(client=bc, lease_id=lock["lease_id"])
    interval = max(5, (_lease_duration(ttl) if "_lease_duration" in globals() else (ttl or 60)) // 2)

    def _log(msg: str):
        try:
            (log or (lambda *_: None))(msg)
        except Exception:
            pass

    def _beat():
        while not stop_evt.wait(interval):
            try:
                lease.renew()
                _write_lock_metadata(bc, lock["lease_id"])
                _log("[lock] renewed")
            except Exception as e:
                _log(f"[lock] renew failed: {e}")

    t = threading.Thread(target=_beat, name="lock-heartbeat", daemon=True)
    t.start()
    return t

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
    _write_lock_metadata(bc, lease.id)
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
    Renews the lease and refreshes metadata 'lock_ts' under the lease so others can detect it's healthy.
    """
    svc = _svc()
    container = lock["container"]; bn = lock["blob"]
    bc = svc.get_blob_client(container=container, blob=bn)

    lease = BlobLeaseClient(client=bc, lease_id=lock["lease_id"])
    lease.renew()

    # refresh heartbeat timestamp under the active lease
    _write_lock_metadata(bc, lock["lease_id"])

    duration = _lease_duration(ttl) if "_lease_duration" in globals() else (ttl or 60)
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
        lease = BlobLeaseClient(client=bc, lease_id=lock.get("lease_id"))
        lease.release()
    except Exception:
        # best-effort
        pass

def upload_tree_routed(
    dist_dir: str,
    stem: str,
    hls_container: str,
    dash_container: str,
    *,
    strategy: str = "if-missing",           # "if-missing" or "idempotent"
    log=None
) -> dict:
    """
    Route uploads:
      - dist/hls/**  -> hls_container/<stem>/**
      - dist/dash/** -> dash_container/<stem>/**
    Skips everything else.

    strategy:
      - "if-missing": only upload if the target blob does NOT exist
      - "idempotent": call your existing upload_file(..) which may do etag/hash checks

    Returns simple stats: {"hls": {"uploaded": n, "skipped": m}, "dash": {...}}
    """
    def _log(msg: str):
        try:
            (log or print)(msg)
        except Exception:
            pass

    root = Path(dist_dir)
    if not root.exists():
        raise FileNotFoundError(f"dist_dir missing: {dist_dir}")

    stats = {"hls": {"uploaded": 0, "skipped": 0}, "dash": {"uploaded": 0, "skipped": 0}}

    def _route_and_upload(subdir: str, container: str, key: str):
        basedir = root / subdir
        if not basedir.exists():
            _log(f"[upload] skip {subdir}/ (missing)")
            return
        for p in basedir.rglob("*"):
            if not p.is_file():
                continue
            rel = p.relative_to(basedir).as_posix()           # e.g. segments/xxx.m4s
            blob = f"{stem}/{rel}"                             # <stem>/segments/xxx.m4s
            skipval = (strategy == "idempotent")
            if strategy == "if-missing":
                if blob_exists(container, blob):
                    stats[key]["skipped"] += 1
                    continue
                upload_file(container, blob, str(p))           # your existing helper
                stats[key]["uploaded"] += 1
            else:  # "idempotent" (use your existing hash/etag aware uploader)
                changed = upload_file(container, blob, str(p), skip_if_same=skipval) # assume it no-ops if same
                if changed:
                    stats[key]["uploaded"] += 1
                else:
                    stats[key]["skipped"] += 1

    _route_and_upload("hls",  hls_container,  "hls")
    _route_and_upload("dash", dash_container, "dash")

    _log(f"[upload] hls uploaded={stats['hls']['uploaded']} skipped={stats['hls']['skipped']} | "
         f"dash uploaded={stats['dash']['uploaded']} skipped={stats['dash']['skipped']}")
    return stats

# -------------------- Download helpers --------------------

def download_bytes(container: str, blob: str, *, start: int | None = None, end: int | None = None) -> bytes:
    """
    Download a blob as bytes.
      - If start/end are provided, does a ranged download [start, end] (inclusive).
      - Raises FileNotFoundError if the blob doesn't exist.
    """
    svc = _svc()
    bc = svc.get_blob_client(container=container, blob=blob)

    # Clear error early with a friendly message
    try:
        if not bc.exists():
            raise FileNotFoundError(f"Blob not found: {container}/{blob}")
    except ResourceNotFoundError:
        raise FileNotFoundError(f"Blob not found: {container}/{blob}")

    # Compute range length if both bounds are given
    length = None
    if start is not None and end is not None:
        if end < start:
            raise ValueError("download_bytes: end must be >= start")
        length = end - start + 1

    # Light retries for transient failures
    last_err = None
    for _ in range(3):
        try:
            stream = bc.download_blob(offset=start, length=length, max_concurrency=4)
            return stream.readall()
        except (ServiceRequestError, HttpResponseError) as e:
            last_err = e
            time.sleep(0.5)
    # If we get here, all retries failed
    raise last_err if last_err else RuntimeError("download_bytes failed unexpectedly")


def download_text(container: str, blob: str, *, encoding: str = "utf-8") -> str:
    """
    Convenience wrapper around download_bytes → str.
    """
    return download_bytes(container, blob).decode(encoding, errors="strict")


def download_to_path(container: str, blob: str, dest_path: str) -> str:
    """
    Download a blob directly to a local file path. Returns the dest_path.
    """
    data = download_bytes(container, blob)
    p = Path(dest_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_bytes(data)
    return str(p)


def download_blob_streaming(container: str, blob: str, dest_path: str, *, 
                           max_mb: int = 2048, chunk_bytes: int = 4194304, 
                           log: Optional[callable] = None) -> str:
    """
    Stream download a blob to a local file path with size limits and chunked processing.
    Similar to SubmitJob's _download_url_to_temp but for blob storage.
    
    Args:
        container: Source container name
        blob: Source blob name  
        dest_path: Local file path to write to
        max_mb: Maximum file size in MB (default 2GB)
        chunk_bytes: Chunk size for streaming (default 4MB)
        log: Optional logging function
        
    Returns:
        The dest_path
        
    Raises:
        ValueError: If file exceeds max_mb limit
        FileNotFoundError: If blob doesn't exist
    """
    svc = _svc()
    bc = svc.get_blob_client(container=container, blob=blob)
    
    # Check if blob exists first
    try:
        if not bc.exists():
            raise FileNotFoundError(f"Blob not found: {container}/{blob}")
    except ResourceNotFoundError:
        raise FileNotFoundError(f"Blob not found: {container}/{blob}")
    
    # Get blob size from properties for early size check
    try:
        props = bc.get_blob_properties()
        size_bytes = props.size
        if size_bytes > max_mb * 1024 * 1024:
            raise ValueError(f"Blob size {size_bytes} exceeds {max_mb} MB limit")
        if log:
            log(f"[download] blob size: {size_bytes} bytes")
    except Exception as e:
        if log:
            log(f"[download] could not get blob size: {e}, proceeding with streaming check")
    
    # Ensure destination directory exists
    p = Path(dest_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    
    # Stream download with size enforcement
    total = 0
    max_bytes = max_mb * 1024 * 1024
    
    with open(dest_path, "wb") as f:
        try:
            if log:
                log(f"[download] starting blob download for {container}/{blob}")
            
            # Try to get the stream first
            stream = bc.download_blob(max_concurrency=4)
            if log:
                log(f"[download] got download stream, starting chunked read")
            
            # Try using readinto() instead of chunks() to avoid potential gRPC issues
            try:
                # Read the entire stream at once (this might avoid gRPC chunking issues)
                data = stream.readall()
                if len(data) > max_bytes:
                    raise ValueError(f"Download exceeded {max_mb} MB limit")
                f.write(data)
                total = len(data)
                if log:
                    log(f"[download] downloaded {total // (1024*1024)}MB in one read")
            except Exception as chunk_error:
                if log:
                    log(f"[download] readall() failed, trying chunked approach: {chunk_error}")
                # Fallback to chunked reading
                stream = bc.download_blob(max_concurrency=4)
                for chunk in stream.chunks():
                    if not chunk:
                        continue
                    total += len(chunk)
                    if total > max_bytes:
                        f.close()
                        try:
                            os.unlink(dest_path)
                        except Exception:
                            pass
                        raise ValueError(f"Download exceeded {max_mb} MB limit")
                    f.write(chunk)
                    
                    # Log progress every 10MB
                    if total % (10 * 1024 * 1024) < len(chunk) and log:
                        log(f"[download] downloaded {total // (1024*1024)}MB so far")
                    
        except Exception as e:
            if log:
                log(f"[download] error during download: {type(e).__name__}: {e}")
            # Clean up partial file on error
            f.close()
            try:
                os.unlink(dest_path)
            except Exception:
                pass
            raise
    
    if log:
        log(f"[download] streamed {total} bytes to {dest_path}")
    
    return str(p)

# -------------------- Shaka Packager helpers --------------------