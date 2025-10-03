import os
from pathlib import Path
from typing import List, Dict, Optional, Callable

from .errors import CmdError
from .qc import ffprobe_inspect, analyze_media 
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
