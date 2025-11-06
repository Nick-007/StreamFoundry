from __future__ import annotations

import json
import time
from typing import Any, Dict, Iterable, Optional

from .config import get
from .storage import upload_bytes


def build_outputs(
    version: str,
    *,
    canonical_stem: str = "",
    dash_container: str,
    hls_container: str,
    dash_base_url: str = "",
    hls_base_url: str = "",
) -> Dict[str, Dict[str, str]]:
    base = version.rstrip("/")
    stem_part = canonical_stem.strip("/")
    if stem_part:
        base = f"{base}/{stem_part}" if base else stem_part

    dash_path = f"{dash_container}/{base}/stream.mpd"
    hls_path = f"{hls_container}/{base}/master.m3u8"

    outputs: Dict[str, Dict[str, str]] = {
        "dash": {"path": dash_path},
        "hls": {"path": hls_path},
    }
    if dash_base_url:
        outputs["dash"]["url"] = f"{dash_base_url.rstrip('/')}/{version}/stream.mpd"
    if hls_base_url:
        outputs["hls"]["url"] = f"{hls_base_url.rstrip('/')}/{version}/master.m3u8"
    return outputs


def upload_manifests(
    *,
    stem: str,
    version: str,
    fingerprint: str,
    source_hash: str,
    outputs: Dict[str, Dict[str, str]],
    renditions: Iterable[str],
    captions: Optional[Iterable[Dict[str, Any]]] = None,
    trickplay: Optional[Dict[str, Any]] = None,
    aliases: Optional[Iterable[str]] = None,
    log=None,
) -> Dict[str, Any]:
    """
    Write processed/<stem>/{manifest.json,latest.json} describing the published outputs.
    Returns the manifest payload written to storage.
    """
    container = get("PROCESSED_CONTAINER", "processed")
    now_ts = int(time.time())

    manifest_payload: Dict[str, Any] = {
        "id": stem,
        "source_hash": source_hash,
        "version": version,
        "fingerprint": fingerprint,
        "generatedAt": now_ts,
        "outputs": outputs,
        "renditions": sorted({str(r) for r in renditions}),
        "captions": list(captions or []),
    }
    if trickplay:
        manifest_payload["trickplay"] = trickplay
    if aliases:
        manifest_payload["aliases"] = sorted({str(a) for a in aliases})

    latest_payload = {"version": version, "updatedAt": now_ts}

    upload_bytes(
        container,
        f"{stem}/manifest.json",
        json.dumps(manifest_payload, indent=2).encode("utf-8"),
        "application/json",
    )
    upload_bytes(
        container,
        f"{stem}/latest.json",
        json.dumps(latest_payload, indent=2).encode("utf-8"),
        "application/json",
    )
    if log:
        log(f"[publish] manifest updated version={version}")
    return manifest_payload


def upload_prepackage_state(
    *,
    stem: str,
    version: str,
    fingerprint: str,
    source_hash: str,
    state: str,
    renditions: Iterable[str],
    log=None,
) -> None:
    """
    Write a light-weight prepackage status blob for the transcode stage.
    """
    container = get("PROCESSED_CONTAINER", "processed")
    payload = {
        "id": stem,
        "version": version,
        "fingerprint": fingerprint,
        "source_hash": source_hash,
        "state": state,
        "rungs": sorted({str(r) for r in renditions}),
        "updatedAt": int(time.time()),
    }
    upload_bytes(
        container,
        f"{stem}/prepackage.json",
        json.dumps(payload, indent=2).encode("utf-8"),
        "application/json",
    )
    if log:
        log(f"[publish] prepackage state={state}")
