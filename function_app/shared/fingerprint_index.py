from __future__ import annotations

import json
import time
from typing import Any, Dict, Iterable, List, Optional

from .config import get
from .storage import upload_bytes, download_bytes, ensure_containers


def _container() -> str:
    return get("PROCESSED_CONTAINER", "processed")


def _prefix() -> str:
    return get("FINGERPRINT_INDEX_PREFIX", "fingerprints").strip().strip("/")


def _blob(fingerprint: str) -> str:
    return f"{_prefix()}/{fingerprint}.json"


def _now() -> int:
    return int(time.time())


def _normalize_rungs(rungs: Iterable[str] | None) -> List[str]:
    if not rungs:
        return []
    return sorted({str(r).lower() for r in rungs})


def _normalize_captions(captions: Iterable[Dict[str, Any]] | None) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    if not captions:
        return out
    for entry in captions:
        if not isinstance(entry, dict):
            continue
        lang = (entry.get("lang") or "").strip()
        source = str(entry.get("source") or "").strip()
        out.append({"lang": lang, "source": source})
    out.sort(key=lambda d: (d.get("lang", ""), d.get("source", "")))
    return out


def load_fingerprint_record(fingerprint: str) -> Dict[str, Any]:
    cont = _container()
    blob = _blob(fingerprint)
    try:
        raw = download_bytes(cont, blob)
    except FileNotFoundError:
        return {
            "fingerprint": fingerprint,
            "contentHash": "",
            "profile": "",
            "coverage": [],
            "captions": [],
            "stems": {},
            "state": "pending",
            "createdAt": 0,
            "updatedAt": 0,
        }
    try:
        data = json.loads(raw.decode("utf-8"))
    except Exception:
        data = {}
    data.setdefault("fingerprint", fingerprint)
    data.setdefault("coverage", [])
    data.setdefault("captions", [])
    data.setdefault("stems", {})
    return data


def save_fingerprint_record(record: Dict[str, Any]) -> Dict[str, Any]:
    cont = _container()
    ensure_containers([cont])
    blob = _blob(record["fingerprint"])
    payload = dict(record)
    payload["updatedAt"] = _now()
    if not payload.get("createdAt"):
        payload["createdAt"] = payload["updatedAt"]
    upload_bytes(
        cont,
        blob,
        json.dumps(payload, indent=2, sort_keys=True).encode("utf-8"),
        "application/json",
    )
    return payload


def upsert_fingerprint_metadata(
    *,
    fingerprint: str,
    content_hash: str,
    profile_signature: str,
    coverage: Iterable[str],
    captions: Iterable[Dict[str, Any]],
    mezz_prefix: Optional[str],
    outputs: Dict[str, Any],
    encode_config: Dict[str, Any],
    state: str,
    canonical_stem: Optional[str] = None,
) -> Dict[str, Any]:
    record = load_fingerprint_record(fingerprint)
    record["fingerprint"] = fingerprint
    record["contentHash"] = content_hash
    record["profile"] = profile_signature
    record["coverage"] = _normalize_rungs(coverage)
    record["captions"] = _normalize_captions(captions)
    record["mezz"] = {
        "container": get("MEZZ_CONTAINER", "mezzanine"),
        "prefix": mezz_prefix,
    } if mezz_prefix else {}
    record["outputs"] = outputs
    record["encodeConfig"] = encode_config
    record["state"] = state
    record.setdefault("stems", {})
    if canonical_stem:
        record["canonicalStem"] = canonical_stem
    return save_fingerprint_record(record)


def record_stem_alias(
    fingerprint: str,
    *,
    stem: str,
    manifest_blob: str,
    requested_rungs: Iterable[str],
    requested_captions: Iterable[Dict[str, Any]],
) -> Dict[str, Any]:
    record = load_fingerprint_record(fingerprint)
    stems = record.setdefault("stems", {})
    stems[str(stem)] = {
        "manifest": manifest_blob,
        "requestedRungs": _normalize_rungs(requested_rungs),
        "requestedCaptions": _normalize_captions(requested_captions),
        "updatedAt": _now(),
    }
    record["stems"] = stems
    return save_fingerprint_record(record)
