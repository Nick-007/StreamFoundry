#!/usr/bin/env python3
"""
Unified DASH/HLS integrity checker.

Modes:
  - local   : check files on the filesystem (pre-upload)
  - storage : check blobs via your storage SDK (post-upload)
  - http    : check files via HTTP HEAD/GET (post-upload; emulator or CDN)

Types:
  - dash: parse MPD (SegmentTemplate + SegmentTimeline) and verify init + segments
  - hls : parse master + media playlists, verify #EXT-X-MAP init + media segments

Exit codes:
  0 = OK, >0 = failures (hard fail)
"""

from __future__ import annotations
import argparse
import os
import re
import sys
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Dict, List, Tuple, Callable, Optional
import urllib.request

# -----------------------------
# Existence backends (pluggable)
# -----------------------------

def exists_local(root: Path) -> Callable[[str], bool]:
    root = Path(root)
    def _exists(rel: str) -> bool:
        return (root / rel).exists()
    return _exists

def exists_http(base_url: str, timeout: float = 10.0) -> Callable[[str], bool]:
    base = base_url.rstrip("/") + "/"
    def _exists(rel: str) -> bool:
        url = base + rel
        try:
            # Prefer HEAD; fall back to GET range if HEAD not supported
            req = urllib.request.Request(url, method="HEAD")
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return 200 <= resp.status < 300
        except Exception:
            try:
                req = urllib.request.Request(url, headers={"Range": "bytes=0-0"})
                with urllib.request.urlopen(req, timeout=timeout) as resp:
                    return 200 <= resp.status < 300 or resp.status == 206
            except Exception:
                return False
    return _exists

def exists_storage(container: str) -> Callable[[str], bool]:
    """
    Uses your project's blob_exists(container, blob) helper.
    Import is late-bound so this file stays standalone.
    """
    try:
        # Try both common import paths you’ve used.
        from function_app.shared.storage import blob_exists  # type: ignore
    except Exception:
        try:
            from ..shared.storage import blob_exists  # type: ignore
        except Exception as e:
            raise RuntimeError("Could not import blob_exists from your storage helpers") from e

    def _exists(rel: str) -> bool:
        # rel like "bbb/video_1080p_1.m4s" (stem + filename) or just "video_...".
        # Callers should pass full blob path relative to container. Keep rel as-is.
        return bool(blob_exists(container, rel))
    return _exists

# -----------------------
# DASH (MPD) parsing/util
# -----------------------

def _mpd_ns() -> Dict[str, str]:
    return {"mpd": "urn:mpeg:dash:schema:mpd:2011"}

def parse_mpd(mpd_path: str | Path) -> ET.Element:
    tree = ET.parse(str(mpd_path))
    return tree.getroot()

def _attr(el: ET.Element, name: str, default: Optional[str] = None) -> Optional[str]:
    v = el.attrib.get(name)
    return v if v is not None else default

def _int_attr(el: ET.Element, name: str, default: int) -> int:
    v = el.attrib.get(name)
    if v is None:
        return default
    try:
        return int(v)
    except Exception:
        return default

def enumerate_dash_expected(mpd_root: ET.Element) -> Dict[Tuple[str,int,str], List[str]]:
    """
    Returns {(kind, rep_idx, label): [filenames...]}
    kind: "video" | "audio" | "text"
    label: human label (e.g., "1080p" if derivable) or rep id
    """
    ns = _mpd_ns()
    out: Dict[Tuple[str,int,str], List[str]] = {}

    # Walk Period / AdaptationSet / Representation
    for aset in mpd_root.findall(".//mpd:Period/mpd:AdaptationSet", ns):
        contentType = _attr(aset, "contentType", "") or ""
        kind = ("video" if "video" in contentType else
                "audio" if "audio" in contentType else
                "text"  if "text"  in contentType else contentType or "unknown")

        reps = aset.findall("mpd:Representation", ns)
        for rep_idx, rep in enumerate(reps):
            rep_id = _attr(rep, "id", f"{rep_idx}")
            width  = _attr(rep, "width")
            height = _attr(rep, "height")
            label  = f"{width}x{height}" if (width and height) else rep_id

            # SegmentTemplate can be on Representation or inherited from AdaptationSet
            st = rep.find("mpd:SegmentTemplate", ns) or aset.find("mpd:SegmentTemplate", ns)
            if st is None:
                # Non-template manifests not supported in this minimal checker
                continue

            init = _attr(st, "initialization", "")
            media = _attr(st, "media", "")
            start_number = _int_attr(st, "startNumber", 1)

            files: List[str] = []
            if init:
                files.append(init)

            # Count segments from SegmentTimeline
            timeline = st.find("mpd:SegmentTimeline", ns)
            if timeline is None:
                # No timeline → we can’t derive exact count; skip strict check
                # (Shaka should always emit SegmentTimeline for CMAF here.)
                continue

            seg_num = start_number
            for s in timeline.findall("mpd:S", ns):
                # duration d and repetitions r
                r = _int_attr(s, "r", 0)
                count = r + 1
                for _ in range(count):
                    if media:
                        files.append(media.replace("$Number$", str(seg_num)))
                    seg_num += 1

            out[(kind, rep_idx, label)] = files

    return out

# -------------------
# HLS parsing/helpers
# -------------------

def _read_text(p: Path) -> str:
    return p.read_text(encoding="utf-8", errors="ignore")

def _parse_master_variants(master_text: str) -> Tuple[List[str], List[str]]:
    """
    Returns (variant_playlist_paths, audio_playlist_paths)
    Only relative URIs are expected; absolute are passed through.
    """
    variants: List[str] = []
    audios: List[str] = []
    for line in master_text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            # capture AUDIO groups if explicitly listed (#EXT-X-MEDIA:TYPE=AUDIO,URI="x")
            if line.startswith("#EXT-X-MEDIA:") and "TYPE=AUDIO" in line:
                m = re.search(r'URI="([^"]+)"', line)
                if m: audios.append(m.group(1))
            continue
        # non-tag line in master = variant playlist URI
        variants.append(line)
    return variants, audios

def _parse_media_playlist(media_text: str) -> Tuple[Optional[str], List[str]]:
    """
    Returns (init_map_uri (if present), segment_uris)
    """
    init_uri = None
    segs: List[str] = []
    for line in media_text.splitlines():
        line = line.strip()
        if line.startswith("#EXT-X-MAP:"):
            m = re.search(r'URI="([^"]+)"', line)
            if m: init_uri = m.group(1)
        elif not line.startswith("#") and line:
            segs.append(line)
    return init_uri, segs

def enumerate_hls_expected(master_path: Path) -> List[str]:
    """
    Build expected file list from HLS master + variant playlists.
    Filenames are relative to the directory containing the master.
    """
    root = master_path.parent
    master_txt = _read_text(master_path)
    variants, audio_lists = _parse_master_variants(master_txt)

    files: List[str] = [master_path.name]

    def _rel(child: Path) -> str:
        return str(child.relative_to(root)).replace("\\", "/")

    # Walk video variants
    for v in variants:
        v_path = (root / v).resolve()
        if not v_path.exists():
            files.append(v)  # still list it (checker will flag missing)
            continue
        files.append(_rel(v_path))
        init_uri, segs = _parse_media_playlist(_read_text(v_path))
        if init_uri:
            files.append(os.path.join(os.path.dirname(v), init_uri).replace("\\", "/"))
        for s in segs:
            files.append(os.path.join(os.path.dirname(v), s).replace("\\", "/"))

    # Walk audio playlists referenced in master (if any)
    for a in audio_lists:
        a_path = (root / a).resolve()
        if not a_path.exists():
            files.append(a)
            continue
        files.append(_rel(a_path))
        init_uri, segs = _parse_media_playlist(_read_text(a_path))
        if init_uri:
            files.append(os.path.join(os.path.dirname(a), init_uri).replace("\\", "/"))
        for s in segs:
            files.append(os.path.join(os.path.dirname(a), s).replace("\\", "/"))

    # Dedup while preserving order
    seen = set()
    uniq: List[str] = []
    for f in files:
        if f not in seen:
            seen.add(f)
            uniq.append(f)
    return uniq

# -----------------------
# Unified integrity check
# -----------------------

def check_dash(mode: str,
               mpd_path_or_bytes: str | Path | bytes,
               exists_fn: Callable[[str], bool],
               blob_prefix: str = "") -> Dict[str, List[str]]:
    """
    Returns {label: [missing files...]}.
    blob_prefix: prepend (e.g. "bbb/") for storage/http checks when MPD lists bare names.
    """
    if isinstance(mpd_path_or_bytes, (str, Path)):
        root = parse_mpd(mpd_path_or_bytes)  # local file path
    else:
        root = ET.fromstring(mpd_path_or_bytes)  # bytes

    expected = enumerate_dash_expected(root)
    missing: Dict[str, List[str]] = {}
    for (kind, rep_idx, label), files in expected.items():
        miss = []
        for f in files:
            rel = (blob_prefix + f) if blob_prefix else f
            if not exists_fn(rel):
                miss.append(f)
        if miss:
            missing[f"{kind}:{rep_idx}:{label}"] = miss
    return missing

def check_hls(mode: str,
              master_path: Path,
              exists_fn: Callable[[str], bool],
              base_dir_or_prefix: str) -> List[str]:
    """
    Returns missing file list.
    - local: base_dir_or_prefix should be the directory containing master
    - storage/http: base_dir_or_prefix should be something like "bbb/" (prefix)
    """
    expected = enumerate_hls_expected(master_path)
    missing: List[str] = []
    if mode == "local":
        # expected are already relative to master dir
        for f in expected:
            if not exists_fn(f):
                missing.append(f)
    else:
        # prepend prefix (e.g., stem/) for container/http
        prefix = base_dir_or_prefix.rstrip("/") + "/"
        for f in expected:
            if not exists_fn(prefix + f if not f.startswith(prefix) else f):
                missing.append(f)
    return missing

# -------------
# CLI front-end
# -------------

def main():
    p = argparse.ArgumentParser(description="Unified DASH/HLS integrity checker")
    p.add_argument("--type", choices=["dash", "hls"], required=True)
    p.add_argument("--mode", choices=["local", "storage", "http"], required=True)

    # Common-ish
    p.add_argument("--stem", help="job stem (e.g., bbb) for storage/http prefix", default="")
    p.add_argument("--container", help="Azure container name for storage mode (e.g., DASH/HLS)", default="")
    p.add_argument("--base-url", help="Base URL for http mode (e.g., http://127.0.0.1:10000/devstoreaccount1/dash/bbb/)", default="")
    p.add_argument("--root", help="Local root directory (for local mode)", default="")
    p.add_argument("--mpd", help="MPD filename (for DASH local mode)", default="stream.mpd")
    p.add_argument("--master", help="HLS master filename (for HLS local mode)", default="master.m3u8")

    args = p.parse_args()

    # Choose existence backend
    if args.mode == "local":
        if not args.root:
            print("ERROR: --root required for local mode", file=sys.stderr)
            sys.exit(2)
        exists = exists_local(Path(args.root))
    elif args.mode == "storage":
        if not (args.container and args.stem):
            print("ERROR: --container and --stem required for storage mode", file=sys.stderr)
            sys.exit(2)
        exists = exists_storage(args.container)
    else:
        if not args.base_url:
            print("ERROR: --base-url required for http mode", file=sys.stderr)
            sys.exit(2)
        exists = exists_http(args.base_url)

    failures = 0
    if args.type == "dash":
        if args.mode == "local":
            mpd_path = Path(args.root) / args.mpd
            if not mpd_path.exists():
                print(f"ERROR: MPD not found: {mpd_path}", file=sys.stderr); sys.exit(2)
            missing = check_dash("local", mpd_path, exists_fn=exists)
        elif args.mode == "storage":
            # Download MPD bytes via your storage helper.
            try:
                from function_app.shared.storage import download_bytes  # type: ignore
            except Exception:
                from ..shared.storage import download_bytes  # type: ignore
            mpd_blob = f"{args.stem}/{args.mpd}"
            mpd_bytes = download_bytes(args.container, mpd_blob)
            missing = check_dash("storage", mpd_bytes, exists_fn=exists, blob_prefix=f"{args.stem}/")
        else:
            # HTTP: fetch MPD
            url = args.base_url.rstrip("/") + "/" + args.mpd
            with urllib.request.urlopen(url) as resp:
                mpd_bytes = resp.read()
            missing = check_dash("http", mpd_bytes, exists_fn=exists)

        if missing:
            for label, segs in missing.items():
                sample = ", ".join(segs[:15]) + (" ..." if len(segs) > 15 else "")
                print(f"[check] DASH {label} missing: {sample}")
                failures += len(segs)
        else:
            print("[check] DASH OK")

    else:  # HLS
        if args.mode == "local":
            master_path = Path(args.root) / args.master
            if not master_path.exists():
                print(f"ERROR: HLS master not found: {master_path}", file=sys.stderr); sys.exit(2)
            missing = check_hls("local", master_path, exists_fn=exists, base_dir_or_prefix=str(master_path.parent))
        elif args.mode == "storage":
            # For storage/http, expected are relative to the master dir,
            # so we prepend stem/ when calling exists().
            # We still need the local master to enumerate variant/media files → download it.
            try:
                from function_app.shared.storage import download_bytes  # type: ignore
            except Exception:
                from ..shared.storage import download_bytes  # type: ignore
            master_blob = f"{args.stem}/{args.master}"
            master_bytes = download_bytes(args.container, master_blob)
            tmp = Path("/tmp/check-hls") / args.stem
            tmp.mkdir(parents=True, exist_ok=True)
            master_path = tmp / "master.m3u8"
            master_path.write_bytes(master_bytes)
            missing = check_hls("storage", master_path, exists_fn=exists, base_dir_or_prefix=args.stem)
        else:
            # HTTP mode: same trick — pull master once locally for enumeration
            url = args.base_url.rstrip("/") + "/" + args.master
            with urllib.request.urlopen(url) as resp:
                master_bytes = resp.read()
            tmp = Path("/tmp/check-hls-http")
            tmp.mkdir(parents=True, exist_ok=True)
            master_path = tmp / "master.m3u8"
            master_path.write_bytes(master_bytes)
            missing = check_hls("http", master_path, exists_fn=exists, base_dir_or_prefix="")

        if missing:
            sample = ", ".join(missing[:25]) + (" ..." if len(missing) > 25 else "")
            print(f"[check] HLS missing: {sample}")
            failures += len(missing)
        else:
            print("[check] HLS OK")

    sys.exit(1 if failures else 0)

if __name__ == "__main__":
    main()
