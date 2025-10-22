# function_app/shared/verify.py
from __future__ import annotations

import os
import re
import json
import time
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Callable, Dict, List, Tuple, Optional

# Only storage helpers; no transcode/ingestor imports → avoids circular deps
from .storage import blob_exists, download_bytes  # used for remote DASH/HLS via SDK fallback

LogFn = Optional[Callable[[str], None]]

# ---------------------------
# Public API
# ---------------------------

def check_integrity(
    stem: str,
    *,
    # Local, pre-upload (optional)
    local_dist_dir: Optional[str] = None,
    # Remote, post-upload (optional)
    base_url: Optional[str] = None,  # e.g. "http://127.0.0.1:10000/devstoreaccount1"
    containers: Dict[str, str] = None,  # {"dash": "dash", "hls": "hls"}
    mode: str = "both",  # "local" | "remote" | "both"
    fail_hard: bool = True,
    log: LogFn = print,
) -> Dict[str, Dict[str, List[str]]]:
    """
    Unified integrity check for DASH & HLS.
      - local check (pre-upload): parse dist_dir/{dash|hls} manifests; verify files exist on disk
      - remote check (post-upload): download manifests; verify files exist via HTTP HEAD (preferred)
        and fall back to SDK existence checks for blobs.

    Returns a dict of missing files per media group, e.g.:
    {
      "dash_local": {"video_1080p": ["video_1080p_2.m4s", ...], "audio": []},
      "dash_remote": {"video_1080p": [...], "audio": []},
      "hls_local": {"v1080": [...], "audio": []},
      "hls_remote": {"v1080": [...], "audio": []},
    }
    """
    containers = containers or {"dash": "dash", "hls": "hls"}
    results: Dict[str, Dict[str, List[str]]] = {}

    do_local = mode in ("local", "both") and local_dist_dir
    do_remote = mode in ("remote", "both") and base_url

    if not (do_local or do_remote):
        raise ValueError("check_integrity: nothing to do (set local_dist_dir and/or base_url, or adjust mode)")

    # ---------------------------
    # DASH
    # ---------------------------
    if do_local:
        dash_dir = Path(local_dist_dir) / "dash"
        dash_mpd = dash_dir / "stream.mpd"
        miss = _check_dash_local(dash_mpd, dash_dir, log=log)
        results["dash_local"] = miss
        _emit_missing("DASH local", miss, log)
        if fail_hard and any(miss.values()):
            raise RuntimeError("DASH local integrity failed")

    if do_remote:
        miss = _check_dash_remote(stem, base_url, containers["dash"], log=log)
        results["dash_remote"] = miss
        _emit_missing("DASH remote", miss, log)
        if fail_hard and any(miss.values()):
            raise RuntimeError("DASH remote integrity failed")

    # ---------------------------
    # HLS
    # ---------------------------
    if do_local:
        hls_dir = Path(local_dist_dir) / "hls"
        hls_master = hls_dir / "master.m3u8"
        miss = _check_hls_local(hls_master, hls_dir, log=log)
        results["hls_local"] = miss
        _emit_missing("HLS local", miss, log)
        if fail_hard and any(miss.values()):
            raise RuntimeError("HLS local integrity failed")

    if do_remote:
        miss = _check_hls_remote(stem, base_url, containers["hls"], log=log)
        results["hls_remote"] = miss
        _emit_missing("HLS remote", miss, log)
        if fail_hard and any(miss.values()):
            raise RuntimeError("HLS remote integrity failed")

    if log:
        log("[check] integrity OK")
    return results

# ---------------------------
# DASH helpers
# ---------------------------

def _parse_mpd(mpd_path: Path) -> Tuple[Dict[str, List[str]], Dict[str, str]]:
    """
    Returns (expected_files_per_label, init_map) where:
      expected_files_per_label: {"video_1080p": ["video_1080p_1.m4s", ...], "audio": ["audio_1.m4s", ...]}
      init_map: {"video_1080p": "video_1080p_init.mp4", "audio": "audio_init.m4a"}
    """
    ns = {"mpd": "urn:mpeg:dash:schema:mpd:2011"}
    tree = ET.parse(str(mpd_path))
    root = tree.getroot()

    out: Dict[str, List[str]] = {}
    init: Dict[str, str] = {}

    for aset in root.findall(".//mpd:AdaptationSet", ns):
        ctype = aset.attrib.get("contentType", "")
        for rep_i, rep in enumerate(aset.findall("./mpd:Representation", ns)):
            label = "audio" if ctype == "audio" else _dash_label_from_rep(rep, rep_i)
            st = rep.find("./mpd:SegmentTemplate", ns)
            if st is None: 
                continue
            init_tpl = st.attrib.get("initialization", "")
            media_tpl = st.attrib.get("media", "")
            start_num = int(st.attrib.get("startNumber", "1") or "1")

            # SegmentTimeline → explicit list
            tl = rep.find("./mpd:SegmentTemplate/mpd:SegmentTimeline", ns)
            numbers: List[int] = []
            if tl is not None:
                cur = 0
                for s in tl.findall("./mpd:S", ns):
                    t = s.attrib.get("t")
                    d = s.attrib.get("d")
                    r = int(s.attrib.get("r", "0") or "0")
                    # We only need COUNT, not timestamps; use r + 1 segments for each <S>
                    count = (r + 1)
                    numbers.extend(list(range(start_num + len(numbers), start_num + len(numbers) + count)))
            # If no timeline → we can’t infer exact count reliably; leave empty (checker will skip)
            files = [media_tpl.replace("$Number$", str(n)) for n in numbers] if numbers else []
            out[label] = files
            if init_tpl:
                init[label] = init_tpl
    return out, init

def _dash_label_from_rep(rep: ET.Element, rep_i: int) -> str:
    # Make a stable label matching our packager naming convention video_{height}p or fallback to index
    h = rep.attrib.get("height")
    if h and h.isdigit():
        return f"video_{h}p"
    return f"video_{rep_i}"

def _check_dash_local(mpd_path: Path, root_dir: Path, *, log: LogFn) -> Dict[str, List[str]]:
    if not mpd_path.exists():
        raise FileNotFoundError(f"Local DASH MPD not found: {mpd_path}")
    exp, init = _parse_mpd(mpd_path)
    missing: Dict[str, List[str]] = {}
    for label, segs in exp.items():
        miss = [s for s in segs if not (root_dir / s).exists()]
        if init.get(label) and not (root_dir / init[label]).exists():
            miss = [init[label]] + miss
        if miss:
            missing[label] = miss
    return missing

def _check_dash_remote(stem: str, base_url: str, dash_container: str, *, log: LogFn) -> Dict[str, List[str]]:
    """
    HTTP HEAD preferred; fallback to SDK blob_exists if HTTP not available.
    """
    mpd_url = _join_url(base_url, f"{dash_container}/{stem}/stream.mpd")
    data = _http_get_bytes(mpd_url)
    if data is None:
        # fallback to SDK
        blob = f"{stem}/stream.mpd"
        if not blob_exists(dash_container, blob):
            raise FileNotFoundError(f"Remote DASH MPD missing: {dash_container}/{blob}")
        data = download_bytes(dash_container, blob)
    tmp = Path(os.getenv("TMP_DIR", "/tmp/ingestor")) / stem / "dash-verify"
    tmp.mkdir(parents=True, exist_ok=True)
    mpd = tmp / "stream.mpd"
    mpd.write_bytes(data)

    exp, init = _parse_mpd(mpd)
    missing: Dict[str, List[str]] = {}

    for label, segs in exp.items():
        need = []
        if init.get(label):
            need.append(init[label])
        need.extend(segs)
        miss: List[str] = []
        for f in need:
            url = _join_url(base_url, f"{dash_container}/{stem}/{f}")
            ok = _http_head_ok(url)
            if ok is None:
                # http not working → fallback to SDK
                ok = blob_exists(dash_container, f"{stem}/{f}")
            if not ok:
                miss.append(f)
        if miss:
            missing[label] = miss
    return missing

# ---------------------------
# HLS helpers
# ---------------------------

def _parse_master(master_path: Path) -> Tuple[List[Tuple[str, str]], List[str]]:
    """
    Returns:
      variants: list of (label, playlist_rel_path)
      audios:   list of audio playlist paths (if any)
    """
    text = master_path.read_text(encoding="utf-8", errors="ignore")
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    variants: List[Tuple[str, str]] = []
    audios: List[str] = []

    # Simple EXT-X-STREAM-INF / EXT-X-MEDIA parser (relative URIs on next line for variants)
    i = 0
    while i < len(lines):
        ln = lines[i]
        if ln.startswith("#EXT-X-STREAM-INF"):
            # label by resolution if present
            m = re.search(r"RESOLUTION=(\d+)x(\d+)", ln)
            label = f"v{m.group(2)}" if m else f"v{i}"
            # next non-tag line should be URI
            j = i + 1
            while j < len(lines) and lines[j].startswith("#"):
                j += 1
            if j < len(lines):
                variants.append((label, lines[j]))
            i = j
        elif ln.startswith("#EXT-X-MEDIA") and "TYPE=AUDIO" in ln:
            m = re.search(r'URI="([^"]+)"', ln)
            if m:
                audios.append(m.group(1))
            i += 1
        else:
            i += 1
    return variants, audios

def _parse_media_playlist(pl_path: Path) -> List[str]:
    """
    Return list of segment filenames referenced in the media playlist.
    """
    text = pl_path.read_text(encoding="utf-8", errors="ignore")
    segs: List[str] = []
    for ln in text.splitlines():
        ln = ln.strip()
        if ln and not ln.startswith("#"):
            segs.append(ln)
    return segs

def _check_hls_local(master_path: Path, root_dir: Path, *, log: LogFn) -> Dict[str, List[str]]:
    if not master_path.exists():
        raise FileNotFoundError(f"Local HLS master not found: {master_path}")
    variants, audios = _parse_master(master_path)

    missing: Dict[str, List[str]] = {}

    # Variants
    for label, rel in variants:
        pl = root_dir / rel
        if not pl.exists():
            missing[label] = [rel]
            continue
        segs = _parse_media_playlist(pl)
        miss = [s for s in segs if not (pl.parent / s).exists()]
        if miss:
            missing[label] = miss

    # Audio renditions
    for i, rel in enumerate(audios):
        pl = root_dir / rel
        key = f"audio{i}"
        if not pl.exists():
            missing[key] = [rel]
            continue
        segs = _parse_media_playlist(pl)
        miss = [s for s in segs if not (pl.parent / s).exists()]
        if miss:
            missing[key] = miss

    return missing

def _check_hls_remote(stem: str, base_url: str, hls_container: str, *, log: LogFn) -> Dict[str, List[str]]:
    master_url = _join_url(base_url, f"{hls_container}/{stem}/master.m3u8")
    text = _http_get_text(master_url)
    if text is None:
        # fallback to SDK
        blob = f"{stem}/master.m3u8"
        if not blob_exists(hls_container, blob):
            raise FileNotFoundError(f"Remote HLS master missing: {hls_container}/{blob}")
        text = download_bytes(hls_container, blob).decode("utf-8", "ignore")

    # Use a temp dir for resolving relative paths while parsing
    tmp = Path(os.getenv("TMP_DIR", "/tmp/ingestor")) / stem / "hls-verify"
    tmp.mkdir(parents=True, exist_ok=True)
    master = tmp / "master.m3u8"
    master.write_text(text, encoding="utf-8")

    variants, audios = _parse_master(master)

    missing: Dict[str, List[str]] = {}

    # Variants
    for label, rel in variants:
        pl_url = _join_url(base_url, f"{hls_container}/{stem}/{rel}")
        pl_txt = _http_get_text(pl_url)
        if pl_txt is None:
            # fallback to SDK
            if not blob_exists(hls_container, f"{stem}/{rel}"):
                missing[label] = [rel]
                continue
            pl_txt = download_bytes(hls_container, f"{stem}/{rel}").decode("utf-8", "ignore")

        segs = [ln.strip() for ln in pl_txt.splitlines() if ln.strip() and not ln.startswith("#")]
        miss = []
        for s in segs:
            s_url = _join_url(base_url, f"{hls_container}/{stem}/{_join_rel(rel, s)}")
            ok = _http_head_ok(s_url)
            if ok is None:
                ok = blob_exists(hls_container, f"{stem}/{_join_rel(rel, s)}")
            if not ok:
                miss.append(_join_rel(rel, s))
        if miss:
            missing[label] = miss

    # Audio
    for i, rel in enumerate(audios):
        key = f"audio{i}"
        pl_url = _join_url(base_url, f"{hls_container}/{stem}/{rel}")
        pl_txt = _http_get_text(pl_url)
        if pl_txt is None:
            if not blob_exists(hls_container, f"{stem}/{rel}"):
                missing[key] = [rel]
                continue
            pl_txt = download_bytes(hls_container, f"{stem}/{rel}").decode("utf-8", "ignore")

        segs = [ln.strip() for ln in pl_txt.splitlines() if ln.strip() and not ln.startswith("#")]
        miss = []
        for s in segs:
            s_url = _join_url(base_url, f"{hls_container}/{stem}/{_join_rel(rel, s)}")
            ok = _http_head_ok(s_url)
            if ok is None:
                ok = blob_exists(hls_container, f"{stem}/{_join_rel(rel, s)}")
            if not ok:
                miss.append(_join_rel(rel, s))
        if miss:
            missing[key] = miss

    return missing

# ---------------------------
# HTTP utils (best-effort; OK if HTTP is blocked—SDK fallback covers us)
# ---------------------------

def _http_head_ok(url: str) -> Optional[bool]:
    try:
        import requests  # azure func app can include requests in requirements
        r = requests.head(url, timeout=5)
        return 200 <= r.status_code < 400
    except Exception:
        return None

def _http_get_text(url: str) -> Optional[str]:
    try:
        import requests
        r = requests.get(url, timeout=10)
        if 200 <= r.status_code < 400:
            return r.text
        return None
    except Exception:
        return None

def _http_get_bytes(url: str) -> Optional[bytes]:
    try:
        import requests
        r = requests.get(url, timeout=10)
        if 200 <= r.status_code < 400:
            return r.content
        return None
    except Exception:
        return None

def _join_url(base: str, rest: str) -> str:
    return base.rstrip("/") + "/" + rest.lstrip("/")

def _join_rel(parent_rel: str, child_rel: str) -> str:
    """
    Resolve a child path relative to the playlist (for HLS segments).
    """
    p = Path(parent_rel).parent
    return str((p / child_rel).as_posix())

def _emit_missing(prefix: str, missing: Dict[str, List[str]], log: LogFn) -> None:
    if not missing:
        if log: log(f"[check] {prefix}: OK")
        return
    for label, files in missing.items():
        head = ", ".join(files[:10])
        tail = " ..." if len(files) > 10 else ""
        if log: log(f"[check] {prefix} {label} missing: {head}{tail}")