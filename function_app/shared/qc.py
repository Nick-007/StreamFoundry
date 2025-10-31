from __future__ import annotations
import json, subprocess, shlex, time, ast
from .errors import BadMessageError
from typing import Dict, Optional, Any
from .tools import ffmpeg_path, ffprobe_path
from .errors import CmdError
from .logger import log_exception as _log_exception
from pathlib import Path

def _run_json(cmd: str) -> Dict:
    p = subprocess.Popen(shlex.split(cmd), stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    out, err = p.communicate()
    if p.returncode != 0:
        raise RuntimeError(f"ffprobe failed: {err.strip()}")
    return json.loads(out)

# -----------------------------
# Safe JSON
# -----------------------------
def _safe_json(raw: str, *, allow_plain: bool = True, max_log_chars: int = 500):
    s = (raw or "").strip()
    if not s:
        raise BadMessageError("empty queue message body")
    try:
        return json.loads(s)
    except json.JSONDecodeError:
        pass
    try:
        val = ast.literal_eval(s)
        if isinstance(val, (dict, list)):
            return val
        if isinstance(val, str) and allow_plain:
            return {"input": val}
    except Exception:
        pass
    if allow_plain:
        return {"input": s}
    preview = (s[:max_log_chars] + "…") if len(s) > max_log_chars else s
    raise BadMessageError(f"invalid JSON payload: {preview}")


def parse_fps(avg_frame_rate: str) -> float:
    if not avg_frame_rate or avg_frame_rate == "0/0": return 30.0
    if "/" in avg_frame_rate:
        a,b = avg_frame_rate.split("/")
        try: return float(a)/float(b)
        except Exception: return 30.0
    try: return float(avg_frame_rate)
    except Exception: return 30.0

def _tool_version(bin_path: str) -> str:
    try:
        import subprocess
        out = subprocess.run([bin_path, "-version"], capture_output=True, text=True)
        s = (out.stdout or out.stderr or "").splitlines()[0]
        return s.strip()
    except Exception:
        return "(version unknown)"

def ensure_media_tools(log=None):
    ffm = ffmpeg_path()
    ffp = ffprobe_path()
    (log or print)(f"[tools] ffmpeg: {ffm} | {_tool_version(ffm)}")
    (log or print)(f"[tools] ffprobe: {ffp} | {_tool_version(ffp)}")
    return ffm, ffp

def ffprobe_validate(
    inp_path: str,
    log,
    *,
    strict: bool = True,
    smoke: bool = True
) -> Dict:
    """
    One-stop QC:
      - verify file exists and is non-empty
      - ensure ffmpeg/ffprobe are resolvable (and log versions)
      - optional ffmpeg smoke test (syntax/codec/container sanity)
      - ffprobe → analyze_media
    Returns:
      meta dict: {duration,width,height,fps,videoCodec,audioCodec,videoBitrate,audioBitrate}
    Raises:
      CmdError on any failure when strict=True (default).
    """
    t0 = time.perf_counter()
    p = Path(inp_path)
    if not p.exists():
        raise CmdError(f"QC: input file not found: {p}")
    size = p.stat().st_size
    if size <= 0:
        raise CmdError(f"QC: downloaded file is empty: {p}")

    # Ensure tools & log versions (no-op if already on PATH)
    ffm = ffmpeg_path()
    ffp = ffprobe_path()
    try:
        v_ffm = subprocess.run([ffm, "-version"], capture_output=True, text=True, check=False)
        v_ffp = subprocess.run([ffp, "-version"], capture_output=True, text=True, check=False)
        ffm_vline = (v_ffm.stdout or v_ffm.stderr or "").splitlines()[0:1]
        ffp_vline = (v_ffp.stdout or v_ffp.stderr or "").splitlines()[0:1]
        if ffm_vline: log(f"[tools] ffmpeg: {ffm_vline[0].strip()}")
        if ffp_vline: log(f"[tools] ffprobe: {ffp_vline[0].strip()}")
    except Exception:
        # Don’t fail QC just because version logging failed
        pass

    # Optional fast smoke test: can ffmpeg open it at all?
    if smoke:
        smoke_cmd = f'{ffm} -v error -nostdin -i "{inp_path}" -f null -'
        rc = _run_stream_or_block(smoke_cmd, cwd=str(p.parent), on_line=log)
        if rc != 0:
            raise CmdError("QC: ffmpeg could not open the input; see log above for reason")

    # Probe + analyze (strict validation inside)
    probe = ffprobe_inspect(inp_path)
    meta = analyze_media(probe, strict=strict)

    dt = time.perf_counter() - t0
    log(f"[qc] ok size={size} bytes, {meta['width']}x{meta['height']}@{meta['fps']:.2f}fps took={dt*1000:.0f}ms")
    return meta


# Helper: prefer your streaming runner if present; otherwise block-run and return rc
def _run_stream_or_block(cmd: str, cwd: Optional[str] = None, on_line=None) -> int:
    try:
        # If your project defines _run_stream, use it (merges stderr→stdout for diagnostics)
        from .transcode import _run_stream  # safe import; only used for smoke test
        return _run_stream(cmd, on_line=on_line, idle_timeout_sec=60, cwd=cwd)
    except Exception:
        # Fallback: blocking run; stream after the fact
        proc = subprocess.run(shlex.split(cmd), cwd=cwd, capture_output=True, text=True)
        out = (proc.stdout or "") + "\n" + (proc.stderr or "")
        if on_line:
            for ln in out.splitlines():
                try:
                    on_line(ln)
                except Exception:
                    pass
        return proc.returncode

def ffprobe_inspect(path: str) -> Dict:
    from pathlib import Path
    p = Path(path)
    if not p.exists():
        raise CmdError(f"ffprobe: input not found: {path}")
    if p.stat().st_size <= 0:
        raise CmdError(f"ffprobe: input is empty: {path}")

    cmd = f'{ffprobe_path()} -v error -print_format json -show_format -show_streams "{path}"'
    proc = subprocess.run(shlex.split(cmd), capture_output=True, text=True)
    out = proc.stdout or ""
    err = proc.stderr or ""
    if proc.returncode != 0:
        tail = "\n".join((err or out).splitlines()[-40:])
        raise CmdError(f"ffprobe failed rc={proc.returncode}\n--- ffprobe tail ---\n{tail}")
    try:
        data = json.loads(out)
    except Exception:
        tail = "\n".join(out.splitlines()[-40:])
        raise CmdError(f"ffprobe returned non-JSON\n--- ffprobe stdout tail ---\n{tail}")
    if not data.get("streams") and not data.get("format"):
        tail = "\n".join(out.splitlines()[-40:])
        raise CmdError(f"ffprobe produced empty metadata\n--- ffprobe stdout tail ---\n{tail}")
    return data

def analyze_media(
    probe: Dict,
    *,
    strict: bool = True,
    min_duration: float = 0.5,
) -> Dict:
    """
    Build a complete meta dict from an ffprobe 'probe' and optionally validate it.
    Returns:
      {
        duration, width, height, fps,
        videoCodec, audioCodec,
        videoBitrate, audioBitrate
      }
    Raises CmdError when strict=True and derived values are invalid.
    """
    fmt = probe.get("format", {}) or {}
    streams = probe.get("streams", []) or []
    v = next((s for s in streams if s.get("codec_type") == "video"), {}) or {}
    a = next((s for s in streams if s.get("codec_type") == "audio"), {}) or {}

    # width/height
    try:  w = int(v.get("width") or 0)
    except: w = 0
    try:
        h = int(v.get("height") or 0)
    except Exception:
        h = 0
    except Exception:
        h = 0

    # fps
    def _fps_of(s):
        r = s.get("avg_frame_rate") or s.get("r_frame_rate") or "0/0"
        try:
            num, den = r.split("/")
            num = float(num); den = float(den) if float(den) != 0 else 1.0
            return num / den if den else 0.0
        except Exception:
            return 0.0
    fps = _fps_of(v)

    # duration (format → fallback nb_frames/fps)
    dur = 0.0
    try:
        dur = float(fmt.get("duration", 0.0))
    except Exception:
        pass
    if dur <= 0:
        try:
            nf = float(v.get("nb_frames") or 0)
            if nf > 0 and fps > 0:
                dur = nf / fps
        except Exception:
            pass

    # codecs
    vcodec = (v.get("codec_name") or v.get("codec_tag_string") or "?")
    acodec = (a.get("codec_name") or a.get("codec_tag_string") or "?")

    # bitrates
    def _to_int(x):
        try: return int(x)
        except: return 0
    vbr = _to_int(v.get("bit_rate") or fmt.get("bit_rate") or 0)
    abr = _to_int(a.get("bit_rate") or 0)

    meta = {
        "duration": dur,
        "width": w,
        "height": h,
        "fps": fps,
        "videoCodec": vcodec,
        "audioCodec": acodec,
        "videoBitrate": vbr,
        "audioBitrate": abr,
    }

    if strict:
        if dur <= 0 or w <= 0 or h <= 0:
            raise CmdError(f"Invalid media metadata (duration/width/height): dur={dur}, w={w}, h={h}")
    return meta