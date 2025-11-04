from __future__ import annotations

import hashlib
import json
from typing import Mapping, Any, Iterable


def _serialize_knobs(knobs: Mapping[str, Any] | None) -> str:
    if not knobs:
        return ""
    try:
        normalized = {str(k): knobs[k] for k in knobs.keys()}
    except AttributeError:
        normalized = {str(k): v for k, v in (knobs or {}).items()}
    return json.dumps(normalized, sort_keys=True, separators=(",", ":"))


def file_content_hash(path: str, *, chunk_size: int = 2 ** 20) -> str:
    """
    Compute a SHA-256 hash of file bytes only (no knobs).
    """
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(chunk_size), b""):
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def fingerprint_from_file(path: str, knobs: Mapping[str, Any] | None = None, *, chunk_size: int = 2 ** 20) -> str:
    """
    Compute fingerprint directly from file bytes + knobs. Useful for tooling/tests.
    """
    base = file_content_hash(path, chunk_size=chunk_size)
    return fingerprint_from_content_hash(base, knobs)


def fingerprint_from_content_hash(content_hash: str, knobs: Mapping[str, Any] | None = None) -> str:
    """
    Combine a pre-computed content hash with knobs to produce a stable fingerprint.
    """
    h = hashlib.sha256()
    h.update(content_hash.encode("utf-8"))
    serialized = _serialize_knobs(knobs)
    if serialized:
        h.update(serialized.encode("utf-8"))
    return h.hexdigest()


def profile_signature(*parts: Iterable[str | Mapping[str, Any] | Iterable[str]]) -> str:
    """
    Build a stable signature string from encode profile components (ladder, codec knobs, etc.).
    """
    collected: list[str] = []
    for part in parts:
        if part is None:
            continue
        if isinstance(part, dict):
            collected.append(_serialize_knobs(part))
        elif isinstance(part, (list, tuple, set)):
            collected.append(json.dumps(list(part), sort_keys=True, separators=(",", ":")))
        else:
            collected.append(str(part))
    return "|".join(collected)


def version_for_fingerprint(fingerprint: str) -> str:
    return f"v_{fingerprint}"
