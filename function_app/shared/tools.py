# function_app/shared/tools.py
from __future__ import annotations
import os, shutil

from .config import get
from .errors import CmdError

def ffmpeg_path() -> str:
    p = get("FFMPEG_PATH", "").strip()
    if p:
        return p
    for cand in ("ffmpeg", "/usr/bin/ffmpeg", "/usr/local/bin/ffmpeg"):
        if shutil.which(cand) or os.path.exists(cand):
            return cand
    raise CmdError("ffmpeg not found. Set FFMPEG_PATH or put ffmpeg on PATH.")

def ffprobe_path() -> str:
    p = get("FFPROBE_PATH", "").strip()
    if p:
        return p

    # sibling of ffmpeg if FFMPEG_PATH provided
    fp = get("FFMPEG_PATH", "").strip()
    if fp:
        base = fp if os.path.basename(fp) else os.path.join(fp, "ffmpeg")
        dirn = os.path.dirname(base) or "."
        cand = os.path.join(dirn, "ffprobe")
        if os.path.exists(cand) or shutil.which(cand):
            return cand

    for cand in ("ffprobe", "/usr/bin/ffprobe", "/usr/local/bin/ffprobe"):
        if shutil.which(cand) or os.path.exists(cand):
            return cand
    raise CmdError("ffprobe not found. Set FFPROBE_PATH or put ffprobe on PATH.")

def packager_path() -> str: return get("SHAKA_PACKAGER_PATH", "packager")