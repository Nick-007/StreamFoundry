import os, re, math
from . import check_integrity as ci
from pathlib import Path
from typing import List, Dict, Optional, Callable, Iterable, Tuple, Any
from .errors import CmdError
from .qc import ffprobe_inspect, analyze_media 
from azure.storage.blob import BlobServiceClient

    
def verify_transcode_outputs(audio_mp4, renditions, meta, *, log=None, re_probe_outputs=False):
    # Use provided meta if it looks complete; otherwise derive once
    need = ("duration","width","height")
    if not (isinstance(meta, dict) and all(k in meta and meta[k] for k in need)):
        # derive from an output sample if asked, else from input meta you pass in
        sample = (renditions[0].get("video") or renditions[0].get("path")) if re_probe_outputs else audio_mp4
        probe2 = ffprobe_inspect(sample)
        meta = analyze_media(probe2, strict=True)

    # file existence / size checks (no analyze_media here)
    for r in renditions:
        p = r.get("video") or r.get("path")
        if not p or not os.path.exists(p) or os.path.getsize(p) <= 0:
            raise CmdError(f"Missing/empty rendition: {p}")
    if not (audio_mp4 and os.path.exists(audio_mp4) and os.path.getsize(audio_mp4) > 0):
        raise CmdError("Missing/empty audio.mp4")

    # (Optional) additional sanity checks using `meta`…
    # log(f"[verify] input {meta['width']}x{meta['height']}@{meta['fps']:.2f} {meta['duration']:.1f}s")
    return meta

def verify_dash(out_dash: str, *, log: Optional[Callable[[str], None]] = None):
    mpd = Path(out_dash)
    d = mpd.parent
    if not mpd.exists() or mpd.stat().st_size == 0:
        raise CmdError(f"Expected DASH MPD missing/empty: {mpd}")
    segs = list(d.glob("*.m4s"))
    inits = list(d.glob("*_init.mp4")) + list(d.glob("audio_init.m4a"))
    if not inits:
        raise CmdError(f"No init segments in DASH dir: {d}")
    if not segs:
        raise CmdError(f"No media segments (*.m4s) in DASH dir: {d}")
    if log: log(f"[verify] DASH ok: inits={len(inits)} segs={len(segs)} mpd={mpd.name}")

def verify_hls(out_hls: str, *, log: Optional[Callable[[str], None]] = None):
    m3u8 = Path(out_hls)
    d = m3u8.parent
    if not m3u8.exists() or m3u8.stat().st_size == 0:
        raise CmdError(f"Expected HLS master missing/empty: {m3u8}")
    variants = list(d.glob("*.m3u8"))
    segs = list(d.glob("*.m4s")) + list(d.glob("*.ts"))
    if len(variants) <= 1:
        raise CmdError(f"HLS has no variant playlists in: {d}")
    if not segs:
        raise CmdError(f"HLS has no media segments in: {d}")
    if log: log(f"[verify] HLS ok: playlists={len(variants)} segs={len(segs)} master={m3u8.name}")


_SEG_RE = re.compile(r"^(?P<prefix>.+)_(?P<num>\d+)\.m4s$")

def _sniff_bytes(p: Path, needles: tuple[bytes, ...], read=2048) -> bool:
    try:
        with p.open("rb") as f:
            b = f.read(read)
        return all(n in b for n in needles)
    except Exception:
        return False

# CMAF integrity checkers
def integrity_cmaf_local(
    root: str, *, kind: str,
    label: str | None,
    seg_prefix: str,
    init_name: str,
    seg_dur: int,
    duration_sec: float | None,
    log,
) -> dict:
    issues: list[str] = []
    ok = True
    r = Path(root)

    if not r.exists():
        return {"ok": False, "status": "fail", "issues": [f"missing dir {root}"], "segments": 0}

    # init
    init = r / init_name
    if not init.exists() or init.stat().st_size <= 0:
        issues.append(f"missing or empty init: {init_name}")
        ok = False
    else:
        if kind == "audio":
            if not _sniff_bytes(init, (b"ftyp", b"moov")):
                issues.append("audio init missing ftyp/moov")
                ok = False
        else:
            if not _sniff_bytes(init, (b"ftyp", b"moov")):
                issues.append("video init missing ftyp/moov")
                ok = False

    # segments
    segs = []
    for p in r.glob(f"{seg_prefix}*.m4s"):
        m = _SEG_RE.match(p.name)
        if m and m.group("prefix") == seg_prefix.rstrip("_"):
            if p.stat().st_size <= 0:
                issues.append(f"zero-byte seg: {p.name}")
                ok = False
            segs.append(int(m.group("num")))
    segs.sort()
    n = len(segs)

    if n == 0:
        issues.append("no segments found")
        ok = False
    else:
        # continuity
        expected_first = 1
        if segs[0] != expected_first or any(segs[i] != segs[i-1] + 1 for i in range(1, n)):
            issues.append("segment index gap detected")
            ok = False
        # structure sniff on first/last seg
        first = r / f"{seg_prefix}{segs[0]}.m4s"
        last  = r / f"{seg_prefix}{segs[-1]}.m4s"
        if not _sniff_bytes(first, (b"moof", b"mdat")):
            issues.append("first segment missing moof/mdat")
            ok = False
        if not _sniff_bytes(last, (b"moof", b"mdat")):
            issues.append("last segment missing moof/mdat")
            ok = False

    # “not_ready” classification if we know the total duration
    status = "ok" if ok else "fail"
    if duration_sec and n > 0:
        expected = int(math.ceil(max(0.0, float(duration_sec)) / float(seg_dur)))
        if n < expected and ok:
            status = "not_ready"  # healthy so far, just incomplete

    if not ok and "segment index gap detected" in issues and duration_sec:
        # treat index gaps as hard fail even mid-job
        status = "fail"

    if ok:
        log(f"[cmaf] {kind}{('/'+label) if label else ''} OK, segments={n}")
    else:
        log(f"[cmaf] {kind}{('/'+label) if label else ''} {status}: {', '.join(issues[:4])}")

    return {"ok": ok, "status": status, "issues": issues, "segments": n}

# Dash + HLS integrity checkers
def integrity_local(dist_dir: str, log: Callable[[str], None]) -> None:
    """
    Pre-upload: verify DASH + HLS produced locally under dist_dir.
    Raises CmdError on any missing required files.
    """
    dash_root = Path(dist_dir) / "dash"
    hls_root  = Path(dist_dir) / "hls"
    mpd_path  = dash_root / "stream.mpd"
    master    = hls_root / "master.m3u8"

    if not mpd_path.exists():
        raise CmdError(f"[check] local DASH MPD missing: {mpd_path}")
    if not master.exists():
        raise CmdError(f"[check] local HLS master missing: {master}")

    # DASH
    missing_dash = ci.check_dash(
        mode="local",
        mpd_path_or_bytes=str(mpd_path),
        exists_fn=ci.exists_local(dash_root),
    )
    if missing_dash:
        for label, segs in missing_dash.items():
            sample = ", ".join(segs[:15]) + (" ..." if len(segs) > 15 else "")
            log(f"[check] DASH local {label} missing: {sample}")
        raise CmdError("[check] Local DASH integrity failed")
    else:
        log("[check] DASH local OK")

    # HLS
    missing_hls = ci.check_hls(
        mode="local",
        master_path=master,
        exists_fn=ci.exists_local(hls_root),
        base_dir_or_prefix=str(hls_root),
    )
    if missing_hls:
        sample = ", ".join(missing_hls[:25]) + (" ..." if len(missing_hls) > 25 else "")
        log(f"[check] HLS local missing: {sample}")
        raise CmdError("[check] Local HLS integrity failed")
    else:
        log("[check] HLS local OK")


def integrity_remote(
    stem: str,
    log: Callable[[str], None],
    *,
    mode: Optional[str] = None,
    dash_base_url: Optional[str] = None,
    hls_base_url: Optional[str] = None,
) -> None:
    """
    Post-upload: verify remote DASH + HLS.
      mode:
        - 'storage' (default): uses Azure SDK via your storage helpers
        - 'http'             : uses HTTP against base URLs (emulator/CDN)
      dash_base_url / hls_base_url:
        Required when mode='http'. If omitted, will read from env:
        DASH_BASE_URL, HLS_BASE_URL
    Raises CmdError on any missing required files.
    """
    mode = (mode or os.getenv("CHECK_REMOTE_MODE", "storage")).strip().lower()

    if mode == "http":
        dash_base_url = (dash_base_url or os.getenv("DASH_BASE_URL") or "").rstrip("/")
        hls_base_url  = (hls_base_url  or os.getenv("HLS_BASE_URL")  or "").rstrip("/")
        if not dash_base_url or not hls_base_url:
            raise CmdError("CHECK_REMOTE_MODE=http requires DASH_BASE_URL and HLS_BASE_URL")

        # DASH via HTTP (fetch MPD then check all URLs exist with HEAD/GET)
        exists_dash = ci.exists_http(dash_base_url)
        mpd_url = f"{dash_base_url}/stream.mpd"
        try:
            import urllib.request
            with urllib.request.urlopen(mpd_url, timeout=15) as resp:
                mpd_bytes = resp.read()
        except Exception as e:
            raise CmdError(f"[check] cannot fetch DASH MPD at {mpd_url}: {e}")

        missing_dash = ci.check_dash(
            mode="http",
            mpd_path_or_bytes=mpd_bytes,
            exists_fn=exists_dash,
        )
        if missing_dash:
            for label, segs in missing_dash.items():
                sample = ", ".join(segs[:15]) + (" ..." if len(segs) > 15 else "")
                log(f"[check] DASH remote(http) {label} missing: {sample}")
            raise CmdError("[check] DASH remote(http) integrity failed")
        else:
            log("[check] DASH remote(http) OK")

        # HLS via HTTP
        exists_hls = ci.exists_http(hls_base_url)
        # Pull master locally (only for listing)
        try:
            import urllib.request
            with urllib.request.urlopen(f"{hls_base_url}/master.m3u8", timeout=15) as resp:
                master_bytes = resp.read()
        except Exception as e:
            raise CmdError(f"[check] cannot fetch HLS master at {hls_base_url}/master.m3u8: {e}")

        tmp = Path(os.getenv("TMP_DIR", "/tmp/ingestor")) / stem / "hls-http-check"
        tmp.mkdir(parents=True, exist_ok=True)
        master_path = tmp / "master.m3u8"
        master_path.write_bytes(master_bytes)

        missing_hls = ci.check_hls(
            mode="http",
            master_path=master_path,
            exists_fn=exists_hls,
            base_dir_or_prefix="",  # exists_fn already includes base URL
        )
        if missing_hls:
            sample = ", ".join(missing_hls[:25]) + (" ..." if len(missing_hls) > 25 else "")
            log(f"[check] HLS remote(http) missing: {sample}")
            raise CmdError("[check] HLS remote(http) integrity failed")
        else:
            log("[check] HLS remote(http) OK")
        return

    # STORAGE mode (default) — lazy import to avoid cycles
    from . import storage  # import here, not top-level

    # DASH via storage
    mpd_blob = f"{stem}/stream.mpd"
    if not storage.blob_exists("DASH", mpd_blob):
        raise CmdError(f"[check] remote DASH MPD missing: DASH/{mpd_blob}")
    mpd_bytes = storage.download_bytes("DASH", mpd_blob)

    missing_dash = ci.check_dash(
        mode="storage",
        mpd_path_or_bytes=mpd_bytes,
        exists_fn=ci.exists_storage(container="DASH"),
        blob_prefix=f"{stem}/",
    )
    if missing_dash:
        for label, segs in missing_dash.items():
            sample = ", ".join(segs[:15]) + (" ..." if len(segs) > 15 else "")
            log(f"[check] DASH remote(storage) {label} missing: {sample}")
        raise CmdError("[check] DASH remote(storage) integrity failed")
    else:
        log("[check] DASH remote(storage) OK")

    # HLS via storage
    master_blob = f"{stem}/master.m3u8"
    if not storage.blob_exists("HLS", master_blob):
        raise CmdError(f"[check] remote HLS master missing: HLS/{master_blob}")
    master_bytes = storage.download_bytes("HLS", master_blob)

    tmp = Path(os.getenv("TMP_DIR", "/tmp/ingestor")) / stem / "hls-storage-check"
    tmp.mkdir(parents=True, exist_ok=True)
    master_path = tmp / "master.m3u8"
    master_path.write_bytes(master_bytes)

    missing_hls = ci.check_hls(
        mode="storage",
        master_path=master_path,
        exists_fn=ci.exists_storage(container="HLS"),
        base_dir_or_prefix=stem,  # prefix relative paths
    )
    if missing_hls:
        sample = ", ".join(missing_hls[:25]) + (" ..." if len(missing_hls) > 25 else "")
        log(f"[check] HLS remote(storage) missing: {sample}")
        raise CmdError("[check] HLS remote(storage) integrity failed")
    else:
        log("[check] HLS remote(storage) OK")

# =============================================================
# Consolidated CMAF readiness (local filesystem | Azure Blob)
# =============================================================

_SEG_RE2 = re.compile(r"^(?P<prefix>.+)_(?P<num>\d+)\.m4s$")

def _contiguous(_nums: Iterable[int]) -> bool:
    _nums = list(_nums)
    return (not _nums) or (_nums[0] == 1 and all(_nums[i] == _nums[i-1] + 1 for i in range(1, len(_nums))))

def _count_local(dirpath: Path, prefix: str) -> Tuple[int, list[int], list[str]]:
    issues: list[str] = []
    nums: list[int] = []
    if not dirpath.exists():
        return 0, [], [f"missing dir {dirpath}"]
    for p in dirpath.glob(f"{prefix}*.m4s"):
        m = _SEG_RE2.match(p.name)
        if not m or m.group("prefix") != prefix.rstrip("_"):
            continue
        if p.stat().st_size <= 0:
            issues.append(f"zero-byte seg: {p.name}")
        try:
            nums.append(int(m.group("num")))
        except Exception:
            issues.append(f"bad seg index: {p.name}")
    nums.sort()
    return len(nums), nums, issues

def _count_blob(bsc: "BlobServiceClient", container: str, prefix: str, seg_prefix: str) -> Tuple[int, list[int], list[str]]:
    issues: list[str] = []
    nums: list[int] = []
    cc = bsc.get_container_client(container)
    for b in cc.list_blobs(name_starts_with=f"{prefix}/"):
        name = b.name.split("/")[-1]
        if not name.startswith(seg_prefix) or not name.endswith(".m4s"):
            continue
        m = _SEG_RE2.match(name)
        if not m or m.group("prefix") != seg_prefix.rstrip("_"):
            continue
        try:
            nums.append(int(m.group("num")))
        except Exception:
            issues.append(f"bad seg index: {name}")
    nums.sort()
    return len(nums), nums, issues

def check_cmaf_readiness(
    *,
    mode: str,                            # "local" | "blob"
    audio_root: str,                      # local path or blob prefix (no trailing slash)
    video_roots: Dict[str, str],          # label -> local path or blob prefix
    seg_dur: int,
    duration_sec: float,
    audio_init: str = "audio_init.m4a",
    video_init_fmt: str = "video_{label}_init.mp4",
    audio_seg_prefix: str = "audio_",
    video_seg_prefix_fmt: str = "video_{label}_",
    container: Optional[str] = None,      # required for mode="blob"
    conn_str: Optional[str] = None,       # required for mode="blob"
    ffprobe: bool = False,
    ffprobe_on_init_only: bool = True,
) -> Dict[str, Any]:
    """
    Unified CMAF completeness/integrity gate for local FS and Azure Blob.
    Returns a dict with:
      {
        "ok": bool,
        "status": "ok" | "not_ready" | "fail",
        "segments_expected": int,
        "audio": {"present": bool, "segments": int, "issues": [], "status": "..."},
        "video": {"labels": { "<label>": {"present": bool, "segments": int, "issues": [], "status": "..."} } },
        "issues": []
      }
    """
    out: Dict[str, Any] = {
        "ok": True, "status": "ok", "issues": [],
        "segments_expected": int(math.ceil(max(0.0, float(duration_sec)) / float(seg_dur))),
        "audio": {}, "video": {"labels": {}},
    }
    expected = out["segments_expected"]

    bsc = None
    if mode == "blob":
        if BlobServiceClient is None:
            raise RuntimeError("azure-storage-blob not available")
        if not container or not conn_str:
            raise RuntimeError("mode='blob' requires container and conn_str")
        bsc = BlobServiceClient.from_connection_string(conn_str)

    # ---------- audio ----------
    a_present = False
    a_segments = 0
    a_issues: list[str] = []
    if mode == "local":
        rootp = Path(audio_root)
        a_present = (rootp / audio_init).exists()
        n, nums, issues = _count_local(rootp, audio_seg_prefix)
    else:
        cc = bsc.get_container_client(container)  # type: ignore
        a_present = any(True for _ in cc.list_blobs(name_starts_with=f"{audio_root}/{audio_init}"))
        n, nums, issues = _count_blob(bsc, container, audio_root, audio_seg_prefix)  # type: ignore
    a_segments, a_issues = n, list(issues)
    if n and not _contiguous(nums):
        a_issues.append("segment index gap")

    if mode == "local" and ffprobe and a_present:
        try:
            # reuse your existing lightweight probe
            from .qc import ffprobe_inspect
            meta = ffprobe_inspect(str(Path(audio_root) / audio_init))
            if not any(s.get("codec_type") == "audio" for s in meta.get("streams", [])):
                a_issues.append("ffprobe: no audio stream in init")
            if not ffprobe_on_init_only and n > 0:
                first = Path(audio_root) / f"{audio_seg_prefix}{nums[0]}.m4s"
                last  = Path(audio_root) / f"{audio_seg_prefix}{nums[-1]}.m4s"
                for pth, tag in ((first, "first-seg"), (last, "last-seg")):
                    m2 = ffprobe_inspect(str(pth))
                    if not any(s.get("codec_type") == "audio" for s in m2.get("streams", [])):
                        a_issues.append(f"ffprobe: no audio stream in {tag}")
        except Exception:
            pass  # ffprobe optional

    if not a_present or a_segments == 0:
        a_status = "not_ready"
    elif a_segments < expected:
        a_status = "not_ready"
    elif a_issues:
        a_status = "fail"
    else:
        a_status = "ok"
    out["audio"] = {"present": a_present, "segments": a_segments, "issues": a_issues, "status": a_status}

    # ---------- video per label ----------
    v_any_fail = False
    v_any_not_ready = (a_status != "ok")
    for label, v_root in video_roots.items():
        seg_prefix = video_seg_prefix_fmt.format(label=label)
        init_name  = video_init_fmt.format(label=label)
        if mode == "local":
            vdir = Path(v_root)
            present = (vdir / init_name).exists()
            n, nums, seg_issues = _count_local(vdir, seg_prefix)
        else:
            cc = bsc.get_container_client(container)  # type: ignore
            present = any(True for _ in cc.list_blobs(name_starts_with=f"{v_root}/{init_name}"))
            n, nums, seg_issues = _count_blob(bsc, container, v_root, seg_prefix)  # type: ignore

        if n and not _contiguous(nums):
            seg_issues.append("segment index gap")

        if mode == "local" and ffprobe and present:
            try:
                from .qc import ffprobe_inspect
                vm = ffprobe_inspect(str((Path(v_root) / init_name)))
                if not any(s.get("codec_type") == "video" for s in vm.get("streams", [])):
                    seg_issues.append("ffprobe: no video stream in init")
                if not ffprobe_on_init_only and n > 0:
                    first = Path(v_root) / f"{seg_prefix}{nums[0]}.m4s"
                    last  = Path(v_root) / f"{seg_prefix}{nums[-1]}.m4s"
                    for pth, tag in ((first, "first-seg"), (last, "last-seg")):
                        m2 = ffprobe_inspect(str(pth))
                        if not any(s.get("codec_type") == "video" for s in m2.get("streams", [])):
                            seg_issues.append(f"ffprobe: no video stream in {tag}")
            except Exception:
                pass

        status = "ok"
        if not present or n == 0 or n < expected:
            status = "not_ready"
            v_any_not_ready = True
        if seg_issues:
            status = "fail"
            v_any_fail = True

        out["video"]["labels"][label] = {
            "present": present, "segments": n, "issues": seg_issues, "status": status
        }

    if v_any_fail or a_status == "fail":
        out["ok"] = False
        out["status"] = "fail"
    elif v_any_not_ready:
        out["ok"] = False
        out["status"] = "not_ready"
    else:
        out["ok"] = True
        out["status"] = "ok"

    return out

# Clear public wrappers (ergonomic)
def check_cmaf_local(*, audio_root, video_roots, seg_dur, duration_sec, **kw):
    return check_cmaf_readiness(
        mode="local",
        audio_root=audio_root,
        video_roots=video_roots,
        seg_dur=seg_dur,
        duration_sec=duration_sec,
        **kw
    )

def check_cmaf_remote(*, container, conn_str, audio_root, video_roots, seg_dur, duration_sec, **kw):
    return check_cmaf_readiness(
        mode="blob",
        container=container,
        conn_str=conn_str,
        audio_root=audio_root,
        video_roots=video_roots,
        seg_dur=seg_dur,
        duration_sec=duration_sec,
        **kw
    )

# Clear names for manifest verifiers (pass-through to existing)
def verify_manifests_local(dist_dir: str, log: Callable[[str], None]):
    return integrity_local(dist_dir=dist_dir, log=log)

def verify_manifests_remote(stem: str, log: Callable[[str], None], **opts):
    return integrity_remote(stem=stem, log=log, **opts)

# Back-compat shim (deprecated): keep old name working
def integrity_cmaf_local(*args, **kwargs):
    return check_cmaf_local(*args, **kwargs)