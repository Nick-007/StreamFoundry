import time, subprocess, shlex, re, os, shutil, signal
from pathlib import Path
from typing import Callable, Optional, Tuple, List, Dict
from .config import get
from .qc import precheck_strict, ffprobe_inspect

class CmdError(RuntimeError): pass

_PROCS = set()

def _popen(cmd: str, *, cwd: str | None = None, text: bool = True, bufsize: int = 1, **popen_kwargs):
    """Start a subprocess in its own process group so we can kill it reliably."""
    kwargs = dict(cwd=cwd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=text, bufsize=bufsize)
    if os.name != "nt":
        kwargs["start_new_session"] = True  # new PGID on POSIX
    else:
        kwargs["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP  # Windows
    # allow overrides like stderr=subprocess.STDOUT from callers
    kwargs.update(popen_kwargs)
    p = subprocess.Popen(shlex.split(cmd), **kwargs)
    _PROCS.add(p)
    # Optional: print/log PIDs for postmortems (replace print with your logger if you prefer)
    try:
        print(f"[spawn] pid={p.pid} cwd={cwd or os.getcwd()} cmd={cmd}")
    except Exception:
        pass
    return p

def _kill_proc(p: subprocess.Popen):
    try:
        if os.name != "nt":
            os.killpg(p.pid, signal.SIGTERM)
        else:
            p.terminate()
    except Exception:
        pass

def kill_all_children():
    """Best-effort: terminate all processes we started."""
    for p in list(_PROCS):
        _kill_proc(p)
    _PROCS.clear()

def _assert_unique_outputs(descriptors: List[str]):
    import re, collections
    outs = []
    for d in descriptors:
        outs += re.findall(r'(?:init_segment|segment_template)=([^\s,"]+|"[^"]+")', d)
    outs = [o.strip('"') for o in outs]
    dup = [k for k, v in collections.Counter(outs).items() if v > 1]
    if dup:
        raise CmdError(f"Duplicate outputs in descriptors: {dup}")

def _run(cmd: str, cwd: str = None, timeout_sec: float | None = None):
    """Run a short-lived command with optional hard timeout and group kill."""
    p = _popen(cmd, cwd=cwd, text=True, bufsize=1)
    try:
        out, err = p.communicate(timeout=timeout_sec)
        return p.returncode, out, err
    except subprocess.TimeoutExpired:
        _kill_proc(p)
        return 124, "", f"timeout after {timeout_sec}s"
    finally:
        _PROCS.discard(p)

def _run_stream(cmd: str,
                on_line: Optional[Callable[[str], None]] = None,
                on_progress: Optional[Callable[[dict], None]] = None,
                cwd: Optional[str] = None,
                timeout_sec: float | None = None,
                idle_timeout_sec: float = 300.0,
                heartbeat_sec: float = 30.0) -> int:
    """
    Stream a long-lived tool (ffmpeg/shaka) with:
      - optional hard timeout (timeout_sec)
      - idle timeout (no output/progress for idle_timeout_sec)
      - clean group termination on timeout
    """
    p = _popen(cmd, cwd=cwd, text=True, bufsize=1, stderr=subprocess.STDOUT)
    start = last_activity = time.time()
    last_heartbeat = 0.0

        # Never let callbacks kill the worker (e.g., BrokenPipe on rotated logs)
    def _safe_call(fn, *args, **kwargs):
        if not fn:
            return
        try:
            fn(*args, **kwargs)
        except (BrokenPipeError, IOError, OSError):
            # Logging/pipe closed; drop the line and continue.
            pass
        except Exception:
            # Silent fail for any unexpected callback error.
            pass

    def _emit(line: str):
        nonlocal last_activity
        last_activity = time.time()
        if "=" in line and on_progress:
            k, v = line.split("=", 1)
            _safe_call(on_progress, {k: v})
        elif on_line:
            _safe_call(on_line, line)

    try:
        # Use non-blocking iteration across stdout; drain stderr at the end
        while True:
            line = p.stdout.readline()
            now = time.time()

            # Hard timeout
            if timeout_sec and (now - start) > timeout_sec:
                _safe_call(on_line, "[watchdog] hard timeout; killing process group")
                _kill_proc(p); return 124

            if line:
                _emit(line.rstrip())
            else:
                # Idle timeout
                if p.poll() is None and (now - last_activity) > idle_timeout_sec:
                    _safe_call(on_line, "[watchdog] idle timeout; killing process group")
                    _kill_proc(p); return 125
                # Heartbeat if it's quiet but not yet idle
                if (p.poll() is None and heartbeat_sec
                        and (now - last_activity) >= heartbeat_sec
                        and (now - last_heartbeat) >= heartbeat_sec):
                    last_heartbeat = now
                    _safe_call(on_line, "[watchdog] packager/ffmpeg still running…")
                if p.poll() is not None:
                    break
                time.sleep(0.1)

        # If we merged stderr into stdout, nothing to drain here.
        # (If you ever remove the merge, keep this drain block.)
        return p.returncode
    finally:
        _PROCS.discard(p)


def ffmpeg_path() -> str: return get("FFMPEG_PATH", "ffmpeg")
def packager_path() -> str: return get("SHAKA_PACKAGER_PATH", "packager")

def _resolve_packager(log=None) -> str:
    """
    Returns the absolute path to Shaka Packager if available.
    Priority:
      1) SHAKA_PACKAGER_PATH config (absolute or relative)
      2) PATH lookup for 'packager'
      3) Common fallback location: /home/site/tools/packager (Azure Functions Linux)
    Raises CmdError with a clear message if not found or not runnable.
    """
    def _log(msg: str):
        try:
            (log or print)(msg)
        except (BrokenPipeError, IOError, OSError):
            pass
        except Exception:
            pass

    cand = get("SHAKA_PACKAGER_PATH", "").strip()
    candidates = []

    if cand:
        candidates.append(cand)  # honor explicit config first

    # PATH lookup
    w = shutil.which(cand or "packager")
    if w:
        candidates.append(w)

    # Well-known local tools dir (if you ship the binary with your app)
    candidates.append("/home/site/tools/packager")
    candidates.append("/usr/local/bin/packager")
    candidates.append("/usr/bin/packager")

    tried = []
    for c in candidates:
        if not c:
            continue
        tried.append(c)
        # If relative, make absolute based on CWD
        path = os.path.abspath(c) if os.path.sep in c else c
        if os.path.isfile(path) and os.access(path, os.X_OK):
            # sanity check: run --version
            try:
                out = subprocess.run([path, "--version"], capture_output=True, text=True, timeout=5)
                if out.returncode == 0:
                    _log(f"[package] using Shaka Packager: {path} ({out.stdout.strip() or 'version ok'})")
                else:
                    _log(f"[package] WARNING: {path} --version returned rc={out.returncode}")
                return path
            except Exception as e:
                _log(f"[package] WARNING: failed to exec {path}: {e}")
                # try the next candidate
                continue

    # If we got here, not found or not runnable
    hint = (
        "Shaka Packager was not found. Install it on the host OS or set SHAKA_PACKAGER_PATH "
        "to an absolute path to the 'packager' binary. "
        "On Azure Functions (Linux), you can place it at /home/site/tools/packager and set SHAKA_PACKAGER_PATH=/home/site/tools/packager."
    )
    tried_list = ", ".join(tried) if tried else "(no candidates)"
    raise CmdError(f"{hint} Tried: {tried_list}")

def _get_bool(x: str) -> bool: return str(get(x, "false")).lower() in ("1","true","yes","on")
def _get_int(x: str, d: int) -> int:
    try: return int(str(get(x, str(d))))
    except Exception: return d
def _parse_out_time_seconds(kv: dict) -> Optional[float]:
    """
    Accepts ffmpeg -progress dict and returns seconds as float, or None.
    Handles out_time_ms (microseconds), out_time_us, or out_time (HH:MM:SS.micro).
    """
    v = kv.get("out_time_ms") or kv.get("out_time_us") or kv.get("out_time")
    if not v or v == "N/A":
        return None
    # Numeric microseconds?
    if v.isdigit():
        # out_time_ms and out_time_us are both integers; detect scale by key
        if "out_time_us" in kv and kv["out_time_us"] == v:
            return int(v) / 1_000_000.0
        # default assume microseconds (as per out_time_ms in many builds)
        return int(v) / 1_000_000.0
    # Timecode "HH:MM:SS[.micro]"
    m = re.match(r"^(\d{2}):([0-5]\d):([0-5]\d)(?:\.(\d+))?$", v)
    if m:
        hh, mm, ss, frac = m.groups()
        base = int(hh) * 3600 + int(mm) * 60 + int(ss)
        if frac:
            # treat as fractional seconds (ffmpeg uses microseconds precision)
            return base + float(f"0.{frac}")
        return float(base)
    return None

def _ffmpeg_video(input_path: str, out_mp4: str, height: int, bv: str, maxrate: str, bufsize: str, fps: float, seg_dur: int, total_duration_sec: float = 0.0, log: Optional[Callable[[str], None]] = None):
    def _log(msg: str): (log or print)(msg)
    gop = max(1, int(round(fps * seg_dur)))
    enc = get("VIDEO_CODEC", "h264_nvenc").strip().lower()
    bt709 = _get_bool("SET_BT709_TAGS")
    nv_preset = get("NVENC_PRESET", "p5")
    nv_rc = get("NVENC_RC", "vbr_hq")
    nv_look = get("NVENC_LOOKAHEAD", "32")
    nv_aq = "1" if _get_bool("NVENC_AQ") else "0"

    # Common flags
    common = (
        f'-vf "scale=-2:{height}" '
        f'-pix_fmt yuv420p '
        + (f'-color_primaries bt709 -color_trc bt709 -colorspace bt709 ' if bt709 else '')
        + f'-g {gop} -keyint_min {gop} -sc_threshold 0 -bf 3 -coder cabac '
    )

    # Decide encoder & build encoder-specific options
    # NOTE: NVENC-only flags (-rc, -rc-lookahead, -spatial_aq, -temporal_aq) are NOT valid for libx264/videotoolbox.
    if enc == "h264_nvenc":
        # NVENC path (keeps -rc and AQ/lookahead)
        v_opts = (
            f'-c:v h264_nvenc -preset {nv_preset} -rc {nv_rc} '
            f'-spatial_aq {nv_aq} -temporal_aq 1 -rc-lookahead {nv_look} '
            f'-b:v {bv} -maxrate {maxrate} -bufsize {bufsize} -profile:v high -level 4.1 '
        )
    elif enc in ("h264_videotoolbox", "hevc_videotoolbox"):
        # Apple VideoToolbox: bitrate-based RC; NO -rc/-aq/-lookahead flags
        vcodec = enc  # keep user-selected vt encoder
        v_opts = (
            f'-c:v {vcodec} '
            f'-b:v {bv} -maxrate {maxrate} -bufsize {bufsize} '
            f'-profile:v high -level 4.1 '
        )
    elif enc == "libx264":
        # Software x264: bitrate + VBV (or swap to CRF if you prefer)
        v_opts = (
            f'-c:v libx264 -preset medium -tune film '
            f'-b:v {bv} -maxrate {maxrate} -bufsize {bufsize} '
            f'-profile:v high -level 4.1 '
        )
    else:
        # Fallback to libx264 if unknown encoder is configured
        v_opts = (
            f'-c:v libx264 -preset medium -tune film '
            f'-b:v {bv} -maxrate {maxrate} -bufsize {bufsize} '
            f'-profile:v high -level 4.1 '
        )

    # Build full command
    v_cmd = (
        f'{ffmpeg_path()} -y -nostdin -hide_banner -stats_period 1 '
        f'-progress pipe:1 '
        f'-i "{input_path}" -map 0:v:0 '
        + common + v_opts +
        f'-movflags +faststart -f mp4 "{out_mp4}"'
    )

    # Defensive sanitizer: if final command is not using NVENC, strip any stray NVENC-only tokens
    if "-c:v h264_nvenc" not in v_cmd:
        # remove patterns: "-rc <mode>", "-rc-lookahead <N>", "-spatial_aq <0/1>", "-temporal_aq <0/1>"
        def _strip_pair(cmd: str, flag: str) -> str:
            # remove "<flag> VALUE" safely
            import re
            return re.sub(rf'(?:\s+{flag}\s+\S+)', '', cmd)
        v_cmd = _strip_pair(v_cmd, "-rc")
        v_cmd = _strip_pair(v_cmd, "-rc-lookahead")
        v_cmd = _strip_pair(v_cmd, "-spatial_aq")
        v_cmd = _strip_pair(v_cmd, "-temporal_aq")

    # Stream progress into the caller's logger (if provided)
    start_ts = time.time()
    def _on_prog(kv: dict):
        sec = _parse_out_time_seconds(kv)
        if sec is None:
            return  # too early; no usable timestamp yet
        pct = (sec / total_duration_sec * 100.0) if total_duration_sec else None
        try:
            pct_txt = f"{pct:5.1f}%" if pct is not None else f"{sec:.1f}s"
        except Exception:
            # guard against %-formatting on None
            pct_txt = f"{sec:.1f}s"
        _log(f"[{height}p] {pct_txt}  fps={kv.get('fps','?')}  speed={kv.get('speed','?')}")
        if kv.get("progress") == "end":
            _log(f"[{height}p] 100.0% done in {time.time()-start_ts:.1f}s → {out_mp4}")

    rc = _run_stream(v_cmd, on_line=_log, on_progress=_on_prog, idle_timeout_sec=600)
    if rc != 0:
        raise CmdError(f"FFmpeg video transcode failed ({height}p): rc={rc}")


def _ffmpeg_audio(input_path: str, out_mp4: str, log: Optional[Callable[[str], None]] = None):
    def _log(msg: str):
        try:
            (log or print)(msg)
        except (BrokenPipeError, IOError, OSError):
            pass
        except Exception:
            pass
    _log("[audio] starting")
    a_cmd = (f'{ffmpeg_path()} -y -i "{input_path}" -map 0:a:0 '
             f'-c:a aac -b:a {get("AUDIO_MAIN_KBPS","128")}k -ac 2 -ar 48000 '
             f'-movflags +faststart -f mp4 "{out_mp4}"')
    rc = _run_stream(a_cmd, on_line=_log, idle_timeout_sec=180)
    if rc != 0: raise CmdError(f"FFmpeg audio transcode failed: rc={rc}")
    _log(f"[audio] done → {out_mp4}")

def transcode_to_cmaf_ladder(input_path: str, workdir: str, log: Optional[Callable[[str], None]] = None) -> Tuple[str, List[Dict], Dict]:
    def _log(msg: str): (log or print)(msg)
    Path(workdir).mkdir(parents=True, exist_ok=True)
    audio_mp4 = str(Path(workdir)/"audio.mp4")
    _ffmpeg_audio(input_path, audio_mp4, log)
    meta = ffprobe_inspect(input_path)
    fps, _, _ = precheck_strict(meta)
    try:
        duration_sec = float(meta["format"]["duration"])
    except Exception:
        duration_sec = 0.0
    seg_dur = _get_int('SEG_DUR_SEC', 4)
    ladder = [
        {"name":"240p","height":240,"bv":"300k","maxrate":"360k","bufsize":"600k"},
        {"name":"360p","height":360,"bv":"650k","maxrate":"780k","bufsize":"1300k"},
        {"name":"480p","height":480,"bv":"900k","maxrate":"1000k","bufsize":"1800k"},
        {"name":"720p","height":720,"bv":"2500k","maxrate":"2800k","bufsize":"5000k"},
        {"name":"1080p","height":1080,"bv":"4200k","maxrate":"4600k","bufsize":"8000k"},
    ]
    outs = []
    for r in ladder:
        label_num = r["name"].split("p")[0]
        out_mp4 = str(Path(workdir)/f"video_{label_num}.mp4")
        _ffmpeg_video(input_path, out_mp4, r["height"], r["bv"], r["maxrate"], r["bufsize"], fps, seg_dur, total_duration_sec=duration_sec, log=log)
        outs.append({"name": r["name"], "height": r["height"], "bitrate": r["bv"], "video": out_mp4})
    return audio_mp4, outs, {"fps": fps}

def package_with_shaka_ladder(renditions: List[Dict], audio_mp4: str, out_dash: str, out_hls: str, text_tracks: List[Dict] = None, log: Optional[Callable[[str], None]] = None):
    """
    renditions: list of dicts from transcode step; each item should include path to the mp4 (e.g., key 'video')
    audio_mp4: path to audio-only MP4
    out_dash/out_hls: FILE paths (e.g., /.../dash/stream.mpd, /.../hls/master.m3u8)
    text_tracks: optional list of subtitle tracks
    """
    def _log(msg: str): (log or print)(msg)

    def _ls(dirpath: Path) -> str:
        try:
            items = []
            for p in sorted(dirpath.glob("*")):
                items.append(p.name + ("/" if p.is_dir() else f" ({p.stat().st_size} bytes)"))
            return ", ".join(items) if items else "(empty)"
        except Exception as e:
            return f"(ls failed: {e})"
    
    dash_path = Path(out_dash)
    hls_path  = Path(out_hls)
    # If a directory accidentally arrives, append default filenames
    
    dash_path.parent.mkdir(parents=True, exist_ok=True)
    hls_path.parent.mkdir(parents=True, exist_ok=True)
    dash_dir = dash_path.parent # Path(.../dist/dash)
    hls_dir = hls_path.parent  # Path(.../dist/hls)
    # ✅ Resolve Shaka Packager or fail fast with a clear error
    packager = _resolve_packager(log=_log)

    _log(f"[package] DASH → {dash_path}")
    _log(f"[package] HLS  → {hls_path}")

    seg_dur = _get_int("PACKAGER_SEG_DUR_SEC", 4)
    trick = _get_bool("ENABLE_TRICKPLAY")
    trick_factor = _get_int("TRICKPLAY_FACTOR", 4)
    # DASH
    parts = []
    for r in renditions:
        label = r["name"]
        base  = f'video_{label}'
        parts.append(
            f'in="{r["video"]}",stream=video,'
            f'init_segment="{base}_init.mp4",'
            f'segment_template="{base}_$Number$.m4s"' +
            (f',trick_play_factor={trick_factor}' if trick else '')
        )
    parts.append(
        f'in="{audio_mp4}",stream=audio,'
        f'init_segment="audio_init.m4a",'
        f'segment_template="audio_$Number$.m4s"'
    )
    if text_tracks:
        for tti in text_tracks:
            parts.append(f'in="{tti["path"]}",stream=text,language={tti.get("lang","en")}')
    _assert_unique_outputs(parts)
    dash_cmd = (
        f'{packager} ' + " ".join(parts) +
        f' --segment_duration {seg_dur} --generate_static_live_mpd --mpd_output=stream.mpd'
        f' --v=2 --stderrthreshold=0 --minloglevel=0'
    )
    # run IN the dash directory so the relative outputs land there
    logs = []
    def _logln(s: str):
        logs.append(s)
        (log or print)(s)
    rc = _run_stream(
        dash_cmd,
        on_line=_logln,
        on_progress=None,
        cwd=str(dash_path.parent),
        idle_timeout_sec=600,           # shaka can be quiet
        heartbeat_sec=30
    )
    if rc != 0:
        tail = "\n".join(logs[-60:]) if logs else "(no output)"
        raise CmdError(f"Shaka DASH packaging failed rc={rc}\n--- packager output tail ---\n{tail}")
    if not Path(dash_path).exists():
        raise CmdError(f"Expected DASH manifest missing: {dash_path}")
    _log(f"[package] DASH output dir: {_ls(dash_dir)}")
    # HLS
    hls_dir = hls_path.parent  # ok
    parts = []
      
    for r in renditions:
        label = r["name"]
        base  = f'video_{label}'
        parts.append(
            f'in="{r["video"]}",stream=video,'
            f'init_segment="{base}_init.mp4",'
            f'segment_template="{base}_$Number$.m4s"'
        )
    parts.append(
        f'in="{audio_mp4}",stream=audio,'
        f'init_segment="audio_init.m4a",'
        f'segment_template="audio_$Number$.m4s"'
    )
    if text_tracks:
        for tti in text_tracks:
            parts.append(f'in="{tti["path"]}",stream=text,language={tti.get("lang","en")}')
    _assert_unique_outputs(parts)
    hls_cmd = (
        f'{packager} ' + " ".join(parts) +
        f' --segment_duration {seg_dur} --hls_playlist_type VOD --hls_master_playlist_output=master.m3u8'
        f' --v=2 --stderrthreshold=0 --minloglevel=0'
    )
    logs2 = []
    def _logln2(s: str):
        logs2.append(s)
        (log or print)(s)
    rc = _run_stream(
        hls_cmd,
        on_line=_logln2,
        on_progress=None,
        cwd=str(hls_path.parent),
        idle_timeout_sec=600,
        heartbeat_sec=30
    )
    if rc != 0:
        tail = "\n".join(logs2[-60:]) if logs2 else "(no output)"
        raise CmdError(f"Shaka HLS packaging failed rc={rc}\n--- packager output tail ---\n{tail}")
    if not hls_path.exists():
        listing = ", ".join(sorted(p.name for p in hls_dir.iterdir())) if hls_dir.exists() else "(missing dir)"
        raise CmdError(f"Expected HLS master missing at {hls_path}; dir contains: {listing}")
    _log(f"[package] HLS output dir: {_ls(hls_dir)}")
    _log(f"[package] done")