from __future__ import annotations

import time
from typing import Any, Dict, Optional

from .storage import set_blob_metadata, blob_client


STATUS_KEY = "sf-status"
PIPELINE_KEY = "sf-pipeline"
VERSION_KEY = "sf-version"
FINGERPRINT_KEY = "sf-fingerprint"
MANIFEST_KEY = "sf-manifest"
CONTENT_HASH_KEY = "sf-content-hash"
UPDATED_KEY = "sf-status-updated"
REASON_KEY = "sf-status-reason"


def _now_ts() -> int:
    return int(time.time())


def set_raw_status(
    container: str,
    blob: str,
    *,
    status: str,
    pipeline: Optional[str] = None,
    version: Optional[str] = None,
    manifest: Optional[str] = None,
    fingerprint: Optional[str] = None,
    content_hash: Optional[str] = None,
    reason: Optional[str] = None,
    timestamp: Optional[int] = None,
    extra: Optional[Dict[str, Any]] = None,
) -> None:
    ts = timestamp if timestamp is not None else _now_ts()
    meta = {
        STATUS_KEY: status,
        PIPELINE_KEY: pipeline,
        VERSION_KEY: version,
        MANIFEST_KEY: manifest,
        FINGERPRINT_KEY: fingerprint,
        CONTENT_HASH_KEY: content_hash,
        UPDATED_KEY: str(ts),
        REASON_KEY: reason,
    }
    if extra:
        for k, v in extra.items():
            if v is not None:
                meta[str(k).lower()] = str(v)
    try:
        set_blob_metadata(container, blob, meta, merge=True)
    except Exception:
        raise


def get_raw_status(container: str, blob: str) -> Dict[str, Any]:
    try:
        props = blob_client(container, blob).get_blob_properties()
    except Exception:
        return {
            "status": "unknown",
            "pipeline": None,
            "version": None,
            "manifest": None,
            "fingerprint": None,
            "content_hash": None,
            "updatedAt": None,
            "metadata": {},
        }
    md = (props.metadata or {}).copy()
    def _to_int(value: Any) -> Optional[int]:
        try:
            return int(value)
        except Exception:
            return None

    return {
        "status": md.get(STATUS_KEY, "unknown"),
        "pipeline": md.get(PIPELINE_KEY),
        "version": md.get(VERSION_KEY),
        "manifest": md.get(MANIFEST_KEY),
        "fingerprint": md.get(FINGERPRINT_KEY),
        "content_hash": md.get(CONTENT_HASH_KEY),
        "updatedAt": _to_int(md.get(UPDATED_KEY)),
        "reason": md.get(REASON_KEY),
        "metadata": md,
    }
