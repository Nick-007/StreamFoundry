import os
from dash_only_lib import *
from pathlib import Path
from typing import Callable, Optional, List, Dict
from shared.config import get as _get
from shared.transcode import (
    package_with_shaka_ladder,
    _call_package,
    CmdError,
)
from logging import log_job
from .dash_only_lib import package_dash_only
dist_dir = _get("DIST_DIR", "/tmp/dist")
work_dir = _get("WORK_DIR", "/tmp/work")
text_tracks: Optional[List[Dict]] = None  # e.g. [{"path": "/tmp/work/captions.vtt", "lang": "en"}]
renditions: Optional[List[Dict]] = None  # e.g. [{"name": "1080p", "video": "/tmp/work/video_1080p.mp4"}, ...]
audio_mp4: Optional[str] = None            # e.g. "/tmp/work/audio.mp4"
log: Optional[Callable[[str], None]] = print
# If you want to repackage DASH only (leaving HLS as-is), set REPACKAGE_DASH_ONLY=true
# This is useful if you need to fix a broken MPD or change text tracks without redoing HLS
# Note: if you want to always repackage DASH only, set REPACKAGE_DASH_ONLY=always instead of true

redo_dash_only = (os.getenv("REPACKAGE_DASH_ONLY", "false").lower() == "true")

pkg_root = Path(dist_dir)
dash_path = pkg_root / "dash" / "stream.mpd"
hls_path  = pkg_root / "hls"  / "master.m3u8"

if redo_dash_only:
    # Use existing work_dir outputs (video_*.mp4, audio.mp4)
    log("[package] DASH repackage requested; leaving HLS as-is")
    package_dash_only(
        work_dir=work_dir,
        out_dash=str(dash_path),
        renditions=None,           # auto-discover from work_dir
        audio_mp4=None,            # auto-find audio.mp4 in work_dir
        text_tracks=text_tracks,   # keep captions if you had them
        log=log,
    )
else:
    # your existing combined packaging path
    dash_ok = dash_path.exists()
    hls_ok  = hls_path.exists()
    if dash_ok and hls_ok:
        log("[package] already done; skipping")
        log_job("package", "already done; skipping")
    else:
        _call_package(
            package_with_shaka_ladder,
            renditions=renditions,
            audio_mp4=audio_mp4,
            out_dash=str(dash_path),
            out_hls=str(hls_path),
            text_tracks=text_tracks,
            log=log,
        )
        dash_ok = dash_path.exists()
        hls_ok  = hls_path.exists()
        if not dash_ok or not hls_ok:
            raise CmdError("Packaging failed: missing output(s)")
        log_job("package", "outputs", dash=str(dash_path), hls=str(hls_path))