# function_app/packager.py
from __future__ import annotations

import time
import shutil
import requests
import re
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Iterable

# Typed payload (your shared schema model)
from .shared.schema import IngestPayload

# Existing helpers/utilities in your codebase
from .shared.mezz import ensure_intermediates_from_mezz
from .shared.verify import check_integrity
from .shared.storage import (
    upload_tree_routed,
    download_blob_streaming,
    list_blobs,
)
from .shared.config import get
from .shared.qc import CmdError
from .shared.transcode import package_with_shaka_ladder
from .shared.workspace import job_paths
from .shared.rungs import discover_renditions, ladder_labels
from .shared.fingerprint import version_for_fingerprint
from .shared.fingerprint_index import upsert_fingerprint_metadata, record_stem_alias, load_fingerprint_record
from .shared.content_index import upsert_fingerprint_entry, register_stem_alias, load_content_index
from .shared.publish import build_outputs, upload_manifests
from .shared.status import set_raw_status
from .shared.urls import build_asset_urls
from .shared.trickplay import generate_trickplay_assets
import pysubs2

CAPTION_MAX_MB = 50
CAPTION_CHUNK_BYTES = 4_194_304


def _maybe_convert_caption(path: Path, *, log) -> Optional[Path]:
    if path.suffix.lower() not in (".srt",):
        return None
    try:
        subs = pysubs2.load(str(path))
        out_path = path.with_suffix(".vtt")
        subs.save(str(out_path), format="webvtt")
        log(f"[captions] converted {path.name} -> {out_path.name}")
        return out_path
    except Exception as exc:
        log(f"[captions] conversion failed for {path.name}: {exc}")
        return None


def _discover_caption_sidecars(
    *,
    raw_container: str,
    raw_key: str,
    log,
) -> List[Dict[str, str]]:
    """
    Scan the raw container for STEM_<lang>.vtt|srt companions that live alongside the source.
    Prefers VTT when both are present.
    """
    if not raw_key:
        return []

    if "/" in raw_key:
        dir_prefix, filename = raw_key.rsplit("/", 1)
        dir_prefix = dir_prefix.strip("/")
        if dir_prefix:
            dir_prefix = dir_prefix + "/"
    else:
        dir_prefix = ""
        filename = raw_key

    base_name = Path(filename).stem
    prefix = f"{dir_prefix}{base_name}_"
    try:
        blobs = list_blobs(raw_container, prefix=prefix)
    except Exception as exc:
        log(f"[captions] failed to list sidecars: {exc}")
        return []

    best_per_lang: Dict[str, Dict[str, str]] = {}
    for item in blobs:
        name = getattr(item, "name", "")
        if not name or not name.lower().startswith(prefix.lower()):
            continue
        file_name = Path(name).name
        stem_part = Path(file_name).stem
        lang_part = stem_part[len(base_name) + 1 :] if stem_part.startswith(f"{base_name}_") else ""
        if len(lang_part) != 2 or not lang_part.isalpha():
            continue
        lang_code = lang_part.lower()
        suffix = Path(file_name).suffix.lower()
        if suffix not in (".vtt", ".srt"):
            continue
        entry = {"lang": lang_code, "source": f"{raw_container}/{name}"}
        current = best_per_lang.get(lang_code)
        if current:
            # Prefer VTT over SRT
            if current["source"].lower().endswith(".vtt"):
                continue
            if suffix == ".vtt":
                best_per_lang[lang_code] = entry
        else:
            best_per_lang[lang_code] = entry

    discovered = sorted(best_per_lang.values(), key=lambda d: d["lang"])
    if discovered:
        log(
            "[captions] auto-discovered sidecars: "
            + ", ".join(f"{c['lang']}:{c['source']}" for c in discovered)
        )
    return discovered


def _stage_captions_for_hls(
    caption_tracks: Iterable[Dict[str, str]],
    hls_dir: Path,
    *,
    log,
) -> Dict[str, str]:
    staged: Dict[str, str] = {}
    if not caption_tracks:
        return staged

    captions_dir = hls_dir / "captions"
    # Start from a clean captions dir each packaging run to avoid stale/duplicated VTTs.
    if captions_dir.exists():
        shutil.rmtree(captions_dir)
    captions_dir.mkdir(parents=True, exist_ok=True)

    for track in caption_tracks:
        lang = (track.get("lang") or "").strip().lower() or f"lang{len(staged)}"
        lang_key = lang
        src = Path(track.get("path") or "")
        if not src.exists():
            log(f"[captions] skipping missing file {src}")
            continue
        if src.suffix.lower() != ".vtt":
            log(f"[captions] skipping non-VTT track {src.name}")
            continue

        # ensure unique filename and map key even if multiple tracks share a language code
        counter = 1
        while lang_key in staged:
            lang_key = f"{lang}_{counter}"
            counter += 1

        target_name = f"{lang_key}.vtt"
        dest = captions_dir / target_name
        counter = 1
        while dest.exists():
            target_name = f"{lang_key}_{counter}.vtt"
            dest = captions_dir / target_name
            counter += 1

        shutil.copy2(src, dest)
        staged[lang_key] = dest.name
        log(f"[captions] staged {src.name} → {dest}")

    return staged


def _inject_captions_into_master(master_path: Path, captions_map: Dict[str, str], *, log) -> None:
    if not captions_map:
        return

    lines = master_path.read_text(encoding="utf-8").splitlines()
    group_id = "subs"
    filtered = [
        line
        for line in lines
        if not (line.startswith("#EXT-X-MEDIA") and "TYPE=SUBTITLES" in line)
    ]

    insert_idx = len(filtered)
    for idx, line in enumerate(filtered):
        if line.startswith("#EXT-X-STREAM-INF"):
            insert_idx = idx
            break

    media_lines: List[str] = []
    for lang, filename in sorted(captions_map.items()):
        uri = f"captions/{filename}"
        display = lang.upper()
        media_lines.append(
            f'#EXT-X-MEDIA:TYPE=SUBTITLES,GROUP-ID="{group_id}",NAME="{display}",DEFAULT=NO,AUTOSELECT=YES,LANGUAGE="{lang}",URI="{uri}"'
        )

    updated = filtered[:insert_idx] + media_lines + filtered[insert_idx:]

    for idx, line in enumerate(updated):
        if line.startswith("#EXT-X-STREAM-INF"):
            if 'SUBTITLES="' in line:
                # normalize to our group id if it differs
                updated[idx] = re.sub(r'SUBTITLES="[^"]+"', f'SUBTITLES="{group_id}"', line)
            elif 'CLOSED-CAPTIONS=' in line:
                updated[idx] = line.replace('CLOSED-CAPTIONS=NONE', f'CLOSED-CAPTIONS=NONE,SUBTITLES="{group_id}"')
            else:
                updated[idx] = line + f',SUBTITLES="{group_id}"'

    master_path.write_text("\n".join(updated) + "\n", encoding="utf-8")
    log(f"[captions] injected {len(captions_map)} subtitle track(s) into HLS master")


def _inject_trickplay_fallback_into_master(
    master_path: Path,
    trick_manifest: Dict[str, Any],
    hls_dir: Path,
    *,
    log,
) -> None:
    """
    Inject trickplay thumbnail sprites as HLS fallback for clients that don't support
    packager trick streams. Adds EXT-X-IMAGE-MEDIA-SEQUENCE variant playlist referencing
    thumbnails.vtt with sprite coordinates.
    
    This provides graceful degradation: clients get trick streams from packager by default,
    but older clients can still scrub via thumbnail previews.
    """
    if not trick_manifest or not trick_manifest.get("vtt"):
        return

    vtt_relative = trick_manifest["vtt"]
    sprites = trick_manifest.get("sprites", [])
    
    if not sprites:
        log("[trickplay] fallback: no sprite data; skipping HLS injection")
        return

    # Validate VTT file exists
    vtt_file = hls_dir / vtt_relative
    if not vtt_file.exists():
        log(f"[trickplay] fallback: VTT missing at {vtt_file}; skipping")
        return

    lines = master_path.read_text(encoding="utf-8").splitlines()
    
    # Find where to insert fallback variant (after regular streams, before any image streams)
    insert_idx = len(lines)
    for idx, line in enumerate(lines):
        if line.startswith("#EXT-X-IMAGE-MEDIA-SEQUENCE"):
            insert_idx = idx
            break
    
    # Build fallback image stream entry
    # Use lowest bitrate stream metadata as reference for fallback
    fallback_entries: List[str] = []
    
    # Add marker comment
    fallback_entries.append("\n# Trickplay Fallback (for clients without packager trick support)")
    
    # Add EXT-X-IMAGE-MEDIA-SEQUENCE entry pointing to thumbnails
    fallback_entries.append(
        f'#EXT-X-IMAGE-MEDIA-SEQUENCE:URI="{vtt_relative}"'
    )
    
    # Insert into master
    updated = lines[:insert_idx] + fallback_entries + lines[insert_idx:]
    
    master_path.write_text("\n".join(updated) + "\n", encoding="utf-8")
    log(f"[trickplay] fallback: injected VTT reference into HLS master (sprites={len(sprites)})")


def _select_trickplay_source(renditions: Iterable[Dict[str, str]]) -> Optional[Dict[str, str]]:
    best: Optional[Dict[str, str]] = None
    best_height = -1
    for rend in renditions or []:
        label = str(rend.get("name") or "")
        digits = "".join(ch for ch in label if ch.isdigit())
        try:
            height = int(digits) if digits else 0
        except Exception:
            height = 0
        if height >= best_height:
            best = rend
            best_height = height
    return best


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
            continue

        converted = _maybe_convert_caption(Path(dest), log=log)
        if converted:
            tracks[-1]["path"] = str(converted)
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
    extra_meta = dict(payload.extra or {})
    raw_container = (payload.in_.container or "").strip() if payload.in_ else ""
    raw_key = payload.in_.key if payload.in_ else ""
    if not raw_container:
        raw_container = get("RAW_CONTAINER", "raw")
    auto_sidecars = _discover_caption_sidecars(
        raw_container=raw_container,
        raw_key=raw_key or "",
        log=log,
    )
    existing_langs = {spec.get("lang"): spec for spec in caption_specs if spec.get("lang")}
    for entry in auto_sidecars:
        lang = entry.get("lang")
        if not lang:
            continue
        if lang not in existing_langs:
            caption_specs.append(entry)
            existing_langs[lang] = entry
    fingerprint = extra_meta.get("fingerprint")
    version = extra_meta.get("version")
    if fingerprint and not version:
        version = version_for_fingerprint(str(fingerprint))
    if not fingerprint or not version:
        raise CmdError("Packaging payload missing fingerprint/version metadata")

    content_hash = extra_meta.get("content_hash")
    if not content_hash:
        raise CmdError("Packaging payload missing content_hash metadata")
    profile_sig = extra_meta.get("profile_signature") or ""
    encode_config = extra_meta.get("encode_config") or {}
    if not profile_sig:
        profile_sig = encode_config.get("profileSignature", "")
    if not profile_sig:
        raise CmdError("Packaging payload missing profile_signature metadata")
    coverage = [str(r) for r in (extra_meta.get("coverage") or [])]

    canonical_captions = extra_meta.get("canonical_captions")
    if canonical_captions:
        norm_caps: List[Dict[str, str]] = []
        for item in canonical_captions:
            if isinstance(item, dict):
                norm_caps.append(
                    {
                        "lang": (item.get("lang") or "").strip(),
                        "source": str(item.get("source") or "").strip(),
                    }
                )
        norm_caps.sort(key=lambda d: (d.get("lang", ""), d.get("source", "")))
        canonical_captions = norm_caps
        for entry in caption_specs:
            lang = (entry.get("lang") or "").strip()
            if not lang:
                continue
            if not any(c.get("lang") == lang for c in canonical_captions):
                canonical_captions.append(
                    {
                        "lang": lang,
                        "source": str(entry.get("source") or "").strip(),
                    }
                )
        canonical_captions.sort(key=lambda d: (d.get("lang", ""), d.get("source", "")))
    else:
        canonical_captions = [
            {
                "lang": (spec.get("lang") or "").strip(),
                "source": str(spec.get("source") or "").strip(),
            }
            for spec in caption_specs
            if isinstance(spec, dict)
        ]
        canonical_captions.sort(key=lambda d: (d.get("lang", ""), d.get("source", "")))

    requested_rungs = extra_meta.get("requested_rungs")
    if requested_rungs:
        selected_rungs = ladder_labels(requested_rungs)

    if not coverage:
        coverage = selected_rungs or ladder_labels(None)
    selected_rungs = coverage


    # --- configuration / paths ---
    TMP_DIR = get("TMP_DIR", "/tmp/ingestor")             # e.g., /tmp/ingestor
    VERIFY_HARD_FAIL = bool(get("VERIFY_HARD_FAIL", True))
    HLS  = get("HLS_CONTAINER", get("HLS", "hls"))
    DASH = get("DASH_CONTAINER", get("DASH", "dash"))
    DASH_BASE_URL = get("DASH_BASE_URL", "")
    HLS_BASE_URL  = get("HLS_BASE_URL",  "")
    PROCESSED = get("PROCESSED_CONTAINER", "processed")
    RAW = get("RAW_CONTAINER", "raw")

    paths = job_paths(stem)
    work_dir = paths.work_dir
    dist_dir = paths.dist_dir
    dist_dir.mkdir(parents=True, exist_ok=True)
    work_dir.mkdir(parents=True, exist_ok=True)

    log(f"[package] start id={stem} version={version} only_rung={selected_rungs or 'ALL'}")
    log(f"[paths] work_dir={work_dir} dist_dir={dist_dir}")

    # --- restore intermediates from mezz (this worker does NOT transcode) ---
    log("[mezz] restoring intermediates")
    restored = ensure_intermediates_from_mezz(
        stem=version,
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

    # --- trick-play assets ---
    trick_manifest: Optional[Dict[str, Any]] = None
    trick_source = _select_trickplay_source(renditions)
    if trick_source:
        try:
            trick_dir = hls_path.parent / "thumbnails"
            result = generate_trickplay_assets(
                trick_source["video"],
                str(trick_dir),
                log=log,
            )
            if result.enabled and result.vtt:
                sprites_meta = [
                    {
                        "path": f"thumbnails/{artifact.sprite}",
                        "columns": artifact.columns,
                        "rows": artifact.rows,
                        "frames": artifact.frame_count,
                    }
                    for artifact in (result.sprites or [])
                ]
                trick_manifest = {
                    "vtt": f"thumbnails/{result.vtt}",
                    "interval": result.interval_sec,
                    "tile": {
                        "width": result.tile_width,
                        "height": result.tile_height,
                    },
                    "frames": result.frame_total,
                    "sprites": sprites_meta,
                }
                log("[trick-play] assets ready")
            elif result.enabled:
                log("[trick-play] generation produced no assets; skipping manifest entry")
            else:
                log("[trick-play] disabled by configuration")
        except CmdError as exc:
            log(f"[trick-play] generation failed: {exc}")
        except Exception as exc:
            log(f"[trick-play] unexpected error: {exc}")
    else:
        log("[trick-play] skipped (no suitable rendition)")

    if trick_manifest:
        encode_config["trickplay"] = trick_manifest

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

    # Shaka already emits subtitle playlists when text tracks are provided; skip manual staging/injection.
    staged_captions: Dict[str, str] = {}

    # --- inject trickplay fallback into HLS master ---
    if trick_manifest:
        _inject_trickplay_fallback_into_master(
            hls_path,
            trick_manifest,
            hls_path.parent,
            log=log,
        )

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
        stem=f"{version}/{stem}",
        hls_container=HLS,
        dash_container=DASH,
        strategy="idempotent",
        log=log,
    )
    log("[upload] routed end")

    # --- ensure DASH text AdaptationSets have labels ---
    labels_script = Path(__file__).resolve().parents[1] / "scripts" / "ops" / "add_missing_dash_labels.py"
    if labels_script.exists():
        try:
            cmd = [sys.executable or "python3", str(labels_script), str(dash_path), str(dash_path)]
            res = subprocess.run(cmd, capture_output=True, text=True, check=True)
            for line in filter(None, (res.stdout or "").splitlines()):
                log(f"[labels] {line}")
        except subprocess.CalledProcessError as exc:
            log(f"[labels] script failed: {exc.stderr or exc.stdout or exc}")
        except Exception as exc:
            log(f"[labels] unable to run add_missing_dash_labels: {exc}")
    else:
        log(f"[labels] script not found at {labels_script}; skipping")

    # --- remote integrity (optional; if bases configured) ---
    base_url = get("BASE_URL", "")
    if base_url and (DASH_BASE_URL or HLS_BASE_URL):
        try:
            log("[verify] remote DASH/HLS")
            # Your verifier already knows how to compose URLs/containers; just pass config
            remote_stem = f"{version}/{stem}"
            check_integrity(
                stem=remote_stem,
                base_url=base_url,
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
        log("[verify] remote skipped (BASE_URL not configured)")

    log("[package] success")

    outputs = build_outputs(
        version,
        canonical_stem=stem,
        dash_container=DASH,
        hls_container=HLS,
        dash_base_url=DASH_BASE_URL,
        hls_base_url=HLS_BASE_URL,
    )
    rendered_rungs = [r["name"] for r in renditions]

    urls = build_asset_urls(str(fingerprint), stem)
    dash_url = urls.get("dash_url") or ""
    hls_url = urls.get("hls_url") or ""

    encode_config.setdefault("generatedAt", int(time.time()))

    fingerprint_record = upsert_fingerprint_metadata(
        fingerprint=str(fingerprint),
        content_hash=str(content_hash),
        profile_signature=str(profile_sig),
        coverage=rendered_rungs,
        captions=canonical_captions,
        mezz_prefix=version,
        outputs=outputs,
        encode_config=encode_config,
        trickplay=trick_manifest,
        state="published",
        canonical_stem=stem,
    )

    upsert_fingerprint_entry(
        str(content_hash),
        fingerprint=str(fingerprint),
        profile_signature=str(profile_sig),
        coverage=rendered_rungs,
        captions=canonical_captions,
        trickplay=trick_manifest,
        state="published",
        stems=[stem],
    )
    register_stem_alias(str(content_hash), str(fingerprint), stem)
    record_stem_alias(
        str(fingerprint),
        stem=stem,
        manifest_blob=f"{stem}/manifest.json",
        requested_rungs=rendered_rungs,
        requested_captions=canonical_captions,
    )

    updated_fingerprint = load_fingerprint_record(str(fingerprint))
    content_doc = load_content_index(str(content_hash))
    aliases = set(updated_fingerprint.get("stems", {}).keys())
    for entry in content_doc.get("fingerprints", []):
        if entry.get("fingerprint") == str(fingerprint):
            aliases.update(entry.get("stems") or [])
    aliases.add(stem)

    try:
        upload_manifests(
            stem=stem,
            version=version,
            fingerprint=str(fingerprint),
            source_hash=str(content_hash),
            outputs=outputs,
            renditions=rendered_rungs,
            captions=canonical_captions,
            trickplay=trick_manifest,
            aliases=aliases,
            log=log,
        )
    except Exception as exc:
        log(f"[package] manifest publish failed: {exc}")
        raise

    try:
        extra_meta_status = {}
        if dash_url:
            extra_meta_status["sf_dash_url"] = dash_url
        if hls_url:
            extra_meta_status["sf_hls_url"] = hls_url
        set_raw_status(
            payload.in_.container or get("RAW_CONTAINER", "raw"),
            payload.in_.key,
            status="complete",
            pipeline=extra_meta.get("pipeline", "transcode"),
            version=version,
            manifest=f"{PROCESSED}/{stem}/manifest.json",
            fingerprint=str(fingerprint),
            content_hash=str(content_hash),
            reason="packaging_complete",
            extra=extra_meta_status,
        )
    except Exception:
        pass
