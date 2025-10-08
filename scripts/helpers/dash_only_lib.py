import shutil
from pathlib import Path
from typing import Optional, List, Dict, Callable

def package_dash_only(
    work_dir: str,
    out_dash: str,
    *,
    renditions: Optional[List[Dict]] = None,
    audio_mp4: Optional[str] = None,
    text_tracks: Optional[List[Dict]] = None,
    log: Optional[Callable[[str], None]] = None,
):
    """
    Rebuilds ONLY DASH using existing MP4s in work_dir.
    Ensures VOD/static MPD (no live flags).
    """
    def _log(s: str): (log or print)(s)

    dash_path = Path(out_dash)
    dash_dir = dash_path.parent
    dash_dir.mkdir(parents=True, exist_ok=True)

    # Nuke old DASH outputs so you can't serve a stale/corrupt MPD
    try:
        for x in dash_dir.glob("*"):
            if x.is_file(): x.unlink()
            else: shutil.rmtree(x, ignore_errors=True)
    except Exception as e:
        _log(f"[package] warn: could not clean dash dir: {e}")

    # Inputs
    if renditions is None:
        renditions = _discover_renditions(work_dir)
    if audio_mp4 is None:
        audio_mp4 = _find_audio_mp4(work_dir)

    if not renditions:
        raise CmdError("No renditions found to package into DASH")

    packager = _resolve_packager(log=_log)
    seg_dur = _get_int("PACKAGER_SEG_DUR_SEC", 4)

    # Build DASH parts
    parts = []
    for r in renditions:
        label = r["name"]
        base = f'video_{label}'
        seg_tpl = f'{base}_$Number$.m4s'
        parts.append(
            f'in="{r["video"]}",stream=video,init_segment={base}_init.mp4,segment_template={seg_tpl}'
        )
    parts.append(
        f'in="{audio_mp4}",stream=audio,init_segment=audio_init.m4a,segment_template=audio_$Number$.m4s'
    )
    if text_tracks:
        for t in text_tracks:
            parts.append(f'in="{t["path"]}",stream=text,language={t.get("lang","en")}')

    # Strict VOD/static (no live flags)
    # Note: drop flags like --generate_static_live_mpd / live profile / time shift, etc.
    dash_cmd = (
        f'{packager} ' + " ".join(parts) +
        f' --segment_duration {seg_dur} --generate_static_mpd --mpd_output=stream.mpd'
    )

    # Stream logs (stderr merged via _run_stream’s popen override)
    logs = []
    def _ln(s: str):
        logs.append(s); _log(s)

    rc = _run_stream(
        dash_cmd,
        on_line=_ln,
        cwd=str(dash_dir),
        idle_timeout_sec=600,   # shaka can be quiet for a bit
    )
    if rc != 0:
        tail = "\n".join(logs[-60:]) if logs else "(no output)"
        raise CmdError(f"Shaka DASH packaging failed rc={rc}\n--- packager output tail ---\n{tail}")

    # Optional: quick directory snapshot
    try:
        listing = ", ".join(p.name for p in sorted(dash_dir.glob("*")))
        _log(f"[package] dash wrote: {listing or '(empty)'}")
    except Exception:
        pass
    # Move MPD into place (atomic on same fs)
    tmp_mpd = dash_dir / "stream.mpd"
    if not tmp_mpd.exists():
        raise FileNotFoundError(f"Expected output MPD not found: {tmp_mpd}")
    shutil.move(str(tmp_mpd), str(dash_path))
    _log(f"[package] dash wrote: {dash_path} (seg_dur={seg_dur}s)")
    return True # indicate success