# function_app/packager.py
from __future__ import annotations

import json
import time
import requests
from pathlib import Path
from typing import Dict, List, Optional, Iterable

# Typed payload (your shared schema model)
from .shared.schema import IngestPayload

# Existing helpers/utilities in your codebase
from .shared.mezz import ensure_intermediates_from_mezz
from .shared.verify import check_integrity
from .shared.storage import (
    upload_tree_routed,
    upload_bytes,
    download_bytes,
    download_blob_streaming,
)
from .shared.config import get
from .shared.qc import CmdError
from .shared.transcode import package_with_shaka_ladder
from .shared.workspace import job_paths
from .shared.rungs import discover_renditions, ladder_labels

CAPTION_MAX_MB = 50
CAPTION_CHUNK_BYTES = 4_194_304


def _prepare_caption_tracks(
    captions: Iterable[Dict[str, str]],
    work_dir: Path,
    log,
) -> List[Dict[str, str]]:
    """
    Ensure each caption spec is materialized locally and return [{"lang","path"}].
    Supports:
      - existing {"lang","path"} entries (left as-is)
      - {"lang","source": "https://..."}
      - {"lang","source": "container/blob.vtt"}
    """
    tracks: List[Dict[str, str]] = []
    cap_dir = work_dir / "captions"
    cap_dir.mkdir(parents=True, exist_ok=True)

    for idx, spec in enumerate(captions):
        if not isinstance(spec, dict):
            continue
        lang = spec.get("lang") or "en"
        if "path" in spec and spec["path"]:
            tracks.append({"lang": lang, "path": spec["path"]})
            continue

        src = spec.get("source")
        if not src:
            continue

        name_hint = spec.get("name") or Path(str(src)).name or f"caption_{idx}.vtt"
        dest = cap_dir / name_hint

        try:
            if "://" in src:
                # HTTP(S) download
                log(f"[captions] downloading URL {src}")
                with requests.get(src, stream=True, timeout=30) as resp:
                    resp.raise_for_status()
                    total = 0
                    with open(dest, "wb") as fh:
                        for chunk in resp.iter_content(chunk_size=CAPTION_CHUNK_BYTES):
                            if not chunk:
                                continue
                            total += len(chunk)
                            if total > CAPTION_MAX_MB * 1024 * 1024:
                                raise ValueError(f"Caption exceeds {CAPTION_MAX_MB} MB limit")
                            fh.write(chunk)
                tracks.append({"lang": lang, "path": str(dest)})
                log(f"[captions] wrote {dest.name} ({total} bytes)")
            else:
                if "/" not in src:
                    raise ValueError("Caption source must be 'container/blob' when not URL")
                container, blob = src.split("/", 1)
                log(f"[captions] restoring {container}/{blob}")
                download_blob_streaming(
                    container=container,
                    blob=blob,
                    dest_path=str(dest),
                    max_mb=CAPTION_MAX_MB,
                    chunk_bytes=CAPTION_CHUNK_BYTES,
                    log=log,
                )
                tracks.append({"lang": lang, "path": str(dest)})
                log(f"[captions] wrote {dest.name}")
        except Exception as exc:
            log(f"[captions] failed for {src}: {exc}")
    return tracks


def _handle_packaging(payload: IngestPayload, *, log) -> None:
    """
    Consolidated packaging pipeline extracted from your queue worker.

    Steps:
      - locate/restore intermediates from mezz (ensure_intermediates_from_mezz)
      - discover audio & video renditions in work_dir
      - package with Shaka to dist/{dash,hls}
      - local integrity check
      - routed upload (idempotent)
      - optional remote integrity check

    Assumptions (per your project):
      - A unique root is under TMP_DIR/<stem>, with subdirs:
          /tmp/ingestor/<stem>/work   (intermediates restored here)
          /tmp/ingestor/<stem>/dist   (outputs written here)
      - Audio file after restore is  work/audio.mp4
      - Video renditions are files like work/video_360p.mp4, work/video_720p.mp4, etc.
      - Config & constants (TMP_DIR, HLS, DASH, BASE URLs, VERIFY_HARD_FAIL) come from shared.config.get.
    """
    if not callable(log):
        log = getattr(log, "info", print)
    stem: str = payload.id or ""
    if not stem:
        raise CmdError("Missing job id (payload.id)")

    # Optional fields from payload (already normalized by your schema)
    only_rung: Optional[List[str]] = payload.only_rung  # e.g., ["360p","720p"] or None
    selected_rungs = ladder_labels(only_rung) if only_rung else None
    caption_specs: List[Dict[str, str]] = [
        {"lang": c.lang, "source": str(c.source)} for c in (payload.captions or [])
    ]

    # --- configuration / paths ---
    TMP_DIR = get("TMP_DIR", "/tmp/ingestor")             # e.g., /tmp/ingestor
    VERIFY_HARD_FAIL = bool(get("VERIFY_HARD_FAIL", True))
    HLS  = get("HLS_CONTAINER", get("HLS", "hls"))
    DASH = get("DASH_CONTAINER", get("DASH", "dash"))
    DASH_BASE_URL = get("DASH_BASE_URL", "")
    HLS_BASE_URL  = get("HLS_BASE_URL",  "")
    PROCESSED = get("PROCESSED_CONTAINER", "processed")

    paths = job_paths(stem)
    work_dir = paths.work_dir
    dist_dir = paths.dist_dir
    dist_dir.mkdir(parents=True, exist_ok=True)
    work_dir.mkdir(parents=True, exist_ok=True)

    log(f"[package] start id={stem} only_rung={selected_rungs or 'ALL'}")
    log(f"[paths] work_dir={work_dir} dist_dir={dist_dir}")

    # --- restore intermediates from mezz (this worker does NOT transcode) ---
    log("[mezz] restoring intermediates")
    restored = ensure_intermediates_from_mezz(
        stem=stem,
        work_dir=str(work_dir),
        only_rung=selected_rungs,
        log=log,
    )
    if not restored:
        raise CmdError(f"mezz restore failed or nothing to restore for id={stem}")

    # --- discover audio & renditions in work_dir ---
    audio_mp4 = str(work_dir / "audio.mp4")
    if not Path(audio_mp4).exists():
        raise CmdError("missing audio.mp4 in work_dir after restore")

    renditions = discover_renditions(work_dir, selected_rungs)
    if not renditions:
        raise CmdError("no video renditions found to package")

    caption_tracks = _prepare_caption_tracks(caption_specs, work_dir, log)

    # --- output paths ---
    dash_path = dist_dir / "dash" / "stream.mpd"
    hls_path  = dist_dir / "hls"  / "master.m3u8"
    dash_path.parent.mkdir(parents=True, exist_ok=True)
    hls_path.parent.mkdir(parents=True,  exist_ok=True)

    # --- package with Shaka ---
    log("[package] shaka begin")
    package_with_shaka_ladder(
        renditions=renditions,
        audio_mp4=audio_mp4,
        out_dash=str(dash_path),
        out_hls=str(hls_path),
        text_tracks=caption_tracks or None,
        log=log,
    )
    log("[package] shaka end")

    # --- local integrity (pre-upload) ---
    try:
        log("[verify] local DASH/HLS")
        check_integrity(
            stem=stem,
            local_dist_dir=str(dist_dir),
            mode="local",          # verify local manifests/segments exist
            fail_hard=True,        # raise on any missing file
            log=log,
        )
        log("[verify] local ok")
    except Exception as e:
        if VERIFY_HARD_FAIL:
            raise
        log(f"[verify] local warning: {e}")

    # --- uploads (routed, idempotent) ---
    log("[upload] routed begin")
    upload_tree_routed(
        dist_dir=str(dist_dir),
        stem=stem,
        hls_container=HLS,
        dash_container=DASH,
        strategy="idempotent",
        log=log,
    )
    log("[upload] routed end")

    # --- remote integrity (optional; if bases configured) ---
    if (DASH_BASE_URL or HLS_BASE_URL):
        try:
            log("[verify] remote DASH/HLS")
            # Your verifier already knows how to compose URLs/containers; just pass config
            check_integrity(
                stem=stem,
                base_url=get("BASE_URL", "http://127.0.0.1:10000/devstoreaccount1"),
                containers={"dash": DASH, "hls": HLS},
                mode="remote",
                fail_hard=True,
                log=log,
            )
            log("[verify] remote ok")
        except Exception as e:
            if VERIFY_HARD_FAIL:
                raise
            log(f"[verify] remote warning: {e}")
    else:
        log("[verify] remote skipped (no *_BASE_URL configured)")

    log("[package] success")

    # --- publish manifest metadata ---
    now_ts = int(time.time())
    version = None
    try:
        pre_bytes = download_bytes(PROCESSED, f"{stem}/prepackage.json")
        pre_data = json.loads(pre_bytes.decode("utf-8"))
        version = pre_data.get("version")
    except Exception:
        version = None
    if not version:
        version = f"v_{now_ts}"

    dash_blob = f"{DASH}/{stem}/stream.mpd"
    hls_blob = f"{HLS}/{stem}/master.m3u8"

    outputs = {
        "dash": {"path": dash_blob},
        "hls": {"path": hls_blob},
    }
    if DASH_BASE_URL:
        outputs["dash"]["url"] = f"{DASH_BASE_URL.rstrip('/')}/{stem}/stream.mpd"
    if HLS_BASE_URL:
        outputs["hls"]["url"] = f"{HLS_BASE_URL.rstrip('/')}/{stem}/master.m3u8"

    manifest_payload = {
        "id": stem,
        "version": version,
        "generatedAt": now_ts,
        "outputs": outputs,
        "renditions": sorted({r["name"] for r in renditions}),
        "captions": [{"lang": c.get("lang"), "source": c.get("source")} for c in caption_specs] if caption_specs else [],
    }
    latest_payload = {"version": version, "updatedAt": now_ts}

    try:
        upload_bytes(
            PROCESSED,
            f"{stem}/manifest.json",
            json.dumps(manifest_payload, indent=2).encode("utf-8"),
            "application/json",
        )
        upload_bytes(
            PROCESSED,
            f"{stem}/latest.json",
            json.dumps(latest_payload, indent=2).encode("utf-8"),
            "application/json",
        )
        log("[package] manifest metadata published")
    except Exception as exc:
        log(f"[package] manifest publish failed: {exc}")
