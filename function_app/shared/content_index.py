from __future__ import annotations

import json
import time
from typing import Any, Dict, Iterable, List, Optional

from .config import get
from .storage import upload_bytes, download_bytes, ensure_containers


def _container() -> str:
    return get("PROCESSED_CONTAINER", "processed")


def _prefix() -> str:
    # default location processed/_hash/<content_hash>.json
    return get("CONTENT_INDEX_PREFIX", "_hash").strip().strip("/")


def _blob(content_hash: str) -> str:
    return f"{_prefix()}/{content_hash}.json"


def _now() -> int:
    return int(time.time())


def load_content_index(content_hash: str) -> Dict[str, Any]:
    cont = _container()
    blob = _blob(content_hash)
    try:
        raw = download_bytes(cont, blob)
    except FileNotFoundError:
        return {
            "contentHash": content_hash,
            "fingerprints": [],
            "updatedAt": 0,
        }
    try:
        data = json.loads(raw.decode("utf-8"))
    except Exception:
        data = {"contentHash": content_hash, "fingerprints": []}
    if "fingerprints" not in data or not isinstance(data["fingerprints"], list):
        data["fingerprints"] = []
    return data


def _normalize_caps(captions: Iterable[Dict[str, Any]] | None) -> List[Dict[str, Any]]:
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


def save_content_index(content_hash: str, data: Dict[str, Any]) -> None:
    cont = _container()
    ensure_containers([cont])
    blob = _blob(content_hash)
    data = dict(data)
    data["updatedAt"] = _now()
    upload_bytes(
        cont,
        blob,
        json.dumps(data, indent=2, sort_keys=True).encode("utf-8"),
        "application/json",
    )


def upsert_fingerprint_entry(
    content_hash: str,
    *,
    fingerprint: str,
    profile_signature: str,
    coverage: Iterable[str],
    captions: Iterable[Dict[str, Any]],
    trickplay: Optional[Dict[str, Any]] = None,
    state: str,
    stems: Optional[Iterable[str]] = None,
) -> Dict[str, Any]:
    """
    Insert or update a fingerprint entry for the given content hash.
    """
    doc = load_content_index(content_hash)
    fingerprints: List[Dict[str, Any]] = doc.setdefault("fingerprints", [])
    cov_sorted = sorted({str(r).lower() for r in coverage})
    caps_norm = _normalize_caps(captions)
    entry = next((f for f in fingerprints if f.get("fingerprint") == fingerprint), None)
    if entry is None:
        entry = {
            "fingerprint": fingerprint,
            "profile": profile_signature,
            "coverage": cov_sorted,
            "captions": caps_norm,
            "state": state,
            "createdAt": _now(),
            "stems": sorted({str(s) for s in (stems or [])}),
        }
        if trickplay is not None:
            entry["trickplay"] = trickplay
        fingerprints.append(entry)
    else:
        entry["profile"] = profile_signature
        entry["coverage"] = cov_sorted
        entry["captions"] = caps_norm
        if trickplay is not None:
            entry["trickplay"] = trickplay
        entry["state"] = state
        existing_stems = set(entry.get("stems") or [])
        for s in (stems or []):
            if s:
                existing_stems.add(str(s))
        entry["stems"] = sorted(existing_stems)
        entry.setdefault("createdAt", _now())
    entry["updatedAt"] = _now()
    doc["fingerprints"] = sorted(fingerprints, key=lambda f: f.get("createdAt", 0))
    save_content_index(content_hash, doc)
    return doc


def register_stem_alias(content_hash: str, fingerprint: str, stem: str) -> Dict[str, Any]:
    doc = load_content_index(content_hash)
    fingerprints: List[Dict[str, Any]] = doc.setdefault("fingerprints", [])
    entry = next((f for f in fingerprints if f.get("fingerprint") == fingerprint), None)
    if entry is None:
        # create a minimal entry
        entry = {
            "fingerprint": fingerprint,
            "profile": "",
            "coverage": [],
            "captions": [],
            "state": "pending",
            "createdAt": _now(),
            "stems": [],
        }
        fingerprints.append(entry)
    stems = set(entry.get("stems") or [])
    stems.add(str(stem))
    entry["stems"] = sorted(stems)
    entry["updatedAt"] = _now()
    doc["fingerprints"] = fingerprints
    save_content_index(content_hash, doc)
    return doc


def find_matching_fingerprint(
    content_doc: Dict[str, Any],
    *,
    profile_signature: str,
    requested_rungs: Iterable[str],
    requested_captions: Iterable[Dict[str, Any]],
) -> Optional[Dict[str, Any]]:
    req_rungs = {str(r).lower() for r in requested_rungs or []}
    req_caps = _normalize_caps(requested_captions)

    for entry in content_doc.get("fingerprints", []):
        if entry.get("state") != "published":
            continue
        if entry.get("profile") != profile_signature:
            continue
        coverage = set(entry.get("coverage") or [])
        if req_rungs and not req_rungs.issubset(coverage):
            continue
        if _normalize_caps(entry.get("captions")) != req_caps:
            continue
        return entry
    return None
