from typing import Iterable
from azure.storage.blob import BlobServiceClient, ContentSettings, BlobLeaseClient
from .config import get
def blob_client() -> BlobServiceClient:
    conn = get("AzureWebJobsStorage")
    if not conn:
        raise RuntimeError("AzureWebJobsStorage not configured")
    return BlobServiceClient.from_connection_string(conn)
def ensure_containers(container_names: Iterable[str]):
    svc = blob_client()
    for name in container_names:
        try: svc.create_container(name)
        except Exception: pass
def upload_bytes(container: str, blob_path: str, data: bytes, content_type: str = None, cache_control: str = None):
    svc = blob_client()
    bc = svc.get_blob_client(container=container, blob=blob_path)
    cs = None
    if content_type or cache_control:
        cs = ContentSettings(content_type=content_type, cache_control=cache_control)
    bc.upload_blob(data, overwrite=True, content_settings=cs)
def upload_file(container: str, blob_path: str, local_path: str, content_type: str = None, cache_control: str = None):
    with open(local_path, "rb") as f:
        upload_bytes(container, blob_path, f.read(), content_type, cache_control)
def copy_blob(src_container: str, src_blob: str, dst_container: str, dst_blob: str):
    svc = blob_client()
    src_client = svc.get_blob_client(container=src_container, blob=src_blob)
    dst_client = svc.get_blob_client(container=dst_container, blob=dst_blob)
    try: svc.create_container(dst_container)
    except Exception: pass
    try: dst_client.delete_blob()
    except Exception: pass
    dst_client.start_copy_from_url(src_client.url)
def blob_exists(container: str, blob_path: str) -> bool:
    svc = blob_client()
    bc = svc.get_blob_client(container=container, blob=blob_path)
    try:
        bc.get_blob_properties(); return True
    except Exception:
        return False
def acquire_lock(container: str, blob_path: str, lease_duration: int = 60):
    svc = blob_client()
    bc = svc.get_blob_client(container=container, blob=blob_path)
    try: svc.create_container(container)
    except Exception: pass
    try: bc.upload_blob(b"", overwrite=False)
    except Exception: pass
    lease = BlobLeaseClient(bc); lease.acquire(lease_duration=lease_duration); return lease
def release_lock(lease):
    try: lease.release()
    except Exception: pass
