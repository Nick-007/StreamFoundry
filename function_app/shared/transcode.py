import time, subprocess, shlex, re, os, shutil, signal
from pathlib import Path
from typing import Callable, Optional, Tuple, List, Dict
from .progress import handle_progress
from .config import get
from .errors import CmdError
from .tools import ffmpeg_path
from .qc import ffprobe_inspect, analyze_media
from .normalize import normalize_rung_selector

_PROCS = set()

def _popen(cmd: str, *, cwd: str | None = None, text: bool = True, bufsize: int = 1, **popen_kwargs):
    kwargs = dict(cwd=cwd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=text, bufsize=bufsize)
    if os.name != "nt":
        kwargs["start_new_session"] = True
    else:
        kwargs["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP
    kwargs.update(popen_kwargs)
    p = subprocess.Popen(shlex.split(cmd), **kwargs)
    _PROCS.add(p)
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
    for p in list(_PROCS):
        _kill_proc(p)
    _PROCS.clear()

def _assert_unique_outputs(descriptors: List[str]):
    import collections
    outs = []
    for d in descriptors:
        outs += re.findall(r'(?:init_segment|segment_template)=([^\s,"]+|"[^"]+")', d)
    outs = [o.strip('"') for o in outs]
    dup = [k for k, v in collections.Counter(outs).items() if v > 1]
    if dup:
        raise CmdError(f"Duplicate outputs in descriptors: {dup}")

def _run(cmd: str, cwd: str = None, timeout_sec: float | None = None):
    p = _popen(cmd, cwd=cwd, text=True, bufsize=1)
    try:
        out, _ = p.communicate(timeout=timeout_sec)
        return p.returncode, out, ""
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
                heartbeat_sec: float = 30.0,
                debug: bool = False) -> int:

    def debug_log(msg):
        if debug and on_line:
            timestamp = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
            _safe_call(on_line, f"[debug] {timestamp} {msg}")

    p = _popen(cmd, cwd=cwd, text=True, bufsize=1)
    start = last_activity = time.time()
    last_heartbeat = 0.0
    progress_state ={}

    def _safe_call(fn, *args, **kwargs):
        if not fn:
            return
        try:
            fn(*args, **kwargs)
        except (BrokenPipeError, IOError, OSError):
            pass
        except Exception:
            pass

    def _emit(line: str):
        nonlocal last_activity
        last_activity = time.time()
        debug_log(f"Received line: {line}")
        if "=" in line:
            k, v = line.split("=", 1)
            progress_state[k] = v  # Accumulate progress data

            # Trigger full progress callback only when FFmpeg signals a complete update
            if k == "progress" and v in ("continue", "end") and on_progress:
                debug_log(f"Progress update: {k}={v}")
                _safe_call(on_progress, progress_state.copy())
                progress_state.clear()  # Reset for next update
        elif on_line:
            _safe_call(on_line, line)

    try:
        while True:
            line = p.stdout.readline()
            now = time.time()

            if timeout_sec and (now - start) > timeout_sec:
                _safe_call(on_line, "[watchdog] hard timeout; killing process group")
                _kill_proc(p); return 124

            if line:
                _emit(line.rstrip())
            else:
                if p.poll() is None and (now - last_activity) > idle_timeout_sec:
                    _safe_call(on_line, "[watchdog] idle timeout; killing process group")
                    _kill_proc(p); return 125
                if p.poll() is None and heartbeat_sec and (now - last_activity) >= heartbeat_sec and (now - last_heartbeat) >= heartbeat_sec:
                    last_heartbeat = now
                    _safe_call(on_line, "[watchdog] packager/ffmpeg still running…")
                if p.poll() is not None:
                    debug_log(f"Process exited with code {p.returncode}")
                    break
                time.sleep(0.1)
        return p.returncode
    finally:
        debug_log("Cleaning up process reference")
        _PROCS.discard(p)

def _get_bool(k: str, default: bool = False) -> bool:
    v = get(k)
    if v is None: return default
    return str(v).lower() in ("1", "true", "yes", "on")

def _get_int(k: str, default: int) -> int:
    try:
        return int(get(k, str(default)))
    except Exception:
        return default

def _make_line_logger(log: Optional[Callable[[str], None]], sink: List[str]):
    def _on_line(s: str):
        sink.append(s)
        (log or print)(s)
    return _on_line


_DEFAULT_LADDER: List[Dict[str, str]] = [
    {"name": "240p", "height": 240, "bv": "300k", "maxrate": "360k", "bufsize": "600k"},
    {"name": "360p", "height": 360, "bv": "650k", "maxrate": "780k", "bufsize": "1300k"},
    {"name": "480p", "height": 480, "bv": "900k", "maxrate": "1000k", "bufsize": "1800k"},
    {"name": "720p", "height": 720, "bv": "2500k", "maxrate": "2800k", "bufsize": "5000k"},
    {"name": "1080p", "height": 1080, "bv": "4200k", "maxrate": "4600k", "bufsize": "8000k"},
]


def default_ladder() -> List[Dict[str, str]]:
    return [dict(r) for r in _DEFAULT_LADDER]

def _ffmpeg_video(input_path: str, out_mp4: str, height: int, bv: str, maxrate: str, bufsize: str,
                  fps: float, seg_dur: int, total_duration_sec: float = 0.0,
                  log: Optional[Callable[[str], None]] = None):
    Path(out_mp4).parent.mkdir(parents=True, exist_ok=True)

    
    enc = get("VIDEO_CODEC", "h264_nvenc").strip().lower()
    bt709 = _get_bool("SET_BT709_TAGS")
    nv_preset = get("NVENC_PRESET", "p5")
    nv_rc     = get("NVENC_RC", "vbr_hq")
    nv_look   = get("NVENC_LOOKAHEAD", "32")
    nv_aq     = "1" if _get_bool("NVENC_AQ") else "0"

    gop = max(1, int(round(fps * seg_dur)))
    common = (
        f'-vf "scale=-2:{height}" '
        f'-pix_fmt yuv420p '
        + (f'-color_primaries bt709 -color_trc bt709 -colorspace bt709 -color_range tv ' if bt709 else '')
        + f'-r {fps} '                         # <— lock framerate (e.g., 24000/1001)
        + f'-g {gop} -keyint_min {gop} -sc_threshold 0 -bf 3 -coder cabac '
        f'-video_track_timescale 90000 '
        # -fps_mode vfr  ← REMOVE
        f'-force_key_frames "expr:gte(n,n_forced*{gop})" '   # <— frame-based forcing
    )

    log(f"[video] rung={height}p fps={fps:.4f} seg_dur={seg_dur}s gop={gop}")

    def _opts_for(codec: str) -> str:
        if codec == "h264_nvenc":
            return (f'-c:v h264_nvenc -preset {nv_preset} -rc {nv_rc} '
                    f'-spatial_aq {nv_aq} -temporal_aq 1 -rc-lookahead {nv_look} '
                    f'-b:v {bv} -maxrate {maxrate} -bufsize {bufsize} -profile:v high -level 4.1 ')
        elif codec in ("h264_videotoolbox", "hevc_videotoolbox"):
            return (f'-c:v {codec} -b:v {bv} -maxrate {maxrate} -bufsize {bufsize} '
                    f'-profile:v high -level 4.1 ')
        else:
            return (f'-c:v libx264 -preset medium '
                    f'-b:v {bv} -maxrate {maxrate} -bufsize {bufsize} '
                    f'-profile:v high -level 4.1 ')

    def _build_cmd(codec: str) -> str:
        v_opts = _opts_for(codec)
        cmd = (
            f'{ffmpeg_path()} -hide_banner -nostdin -y -i "{input_path}" -map 0:v:0 '
            + common + v_opts +
            f'-movflags +faststart -progress pipe:1 -stats_period 1 '
            f'-f mp4 "{out_mp4}"'
        )
        if "-c:v h264_nvenc" not in cmd:
            for flag in ("-rc-lookahead", "-spatial_aq", "-temporal_aq", "-rc"):
                cmd = re.sub(r'(?:\s' + flag + r'\s+\S+)', '', cmd)
        return cmd

    def _run_once(codec: str) -> tuple[int, List[str]]:
        cmd = _build_cmd(codec)
        (log or print)(f"[video] codec={codec} out={Path(out_mp4).name}")
        logs: List[str] = []

        start_t = time.time()
        _on_prog = lambda d: handle_progress(
            d,
            media_type="video",
            total_duration_sec=total_duration_sec,
            t0=start_t,
            filename=str(Path(out_mp4).name),
            log=log,
        )
        on_line = _make_line_logger(log, logs)
        rc = _run_stream(cmd, on_line=on_line, on_progress=_on_prog, idle_timeout_sec=300, cwd=str(Path(out_mp4).parent))
        return rc, logs

    rc, logs = _run_once(enc)
    if rc == 0:
        return

    hw_like = enc in ("h264_nvenc", "hevc_nvenc", "h264_videotoolbox", "hevc_videotoolbox")
    if hw_like and _get_bool("VIDEO_FALLBACK_SW", True) and enc != "libx264":
        (log or print)(f"[video] {enc} failed (rc={rc}); retrying with libx264")
        try:
            if Path(out_mp4).exists(): Path(out_mp4).unlink()
        except Exception:
            pass
        rc2, logs2 = _run_once("libx264")
        if rc2 == 0:
            return
        tail = "\n".join((logs + ["----"] + logs2)[-80:]) if (logs or logs2) else "(no output)"
        raise CmdError(f"FFmpeg video transcode failed ({height}p): rc={rc2}\n--- ffmpeg tail ---\n{tail}")

    tail = "\n".join(logs[-80:]) if logs else "(no output)"
    raise CmdError(f"FFmpeg video transcode failed ({height}p): rc={rc}\n--- ffmpeg tail ---\n{tail}")

def _ffmpeg_audio(input_path: str, out_mp4: str, total_duration_sec: float = 0.0, log: Optional[Callable[[str], None]] = None):
    Path(out_mp4).parent.mkdir(parents=True, exist_ok=True)
    probe = ffprobe_inspect(input_path)
    a_streams = [s for s in probe.get("streams", []) if s.get("codec_type") == "audio"]
    if not a_streams:
        raise CmdError("Input has no audio stream; cannot create audio.mp4")
    a_index = a_streams[0].get("index", 0)

    a_cmd = (
        f'{ffmpeg_path()} -hide_banner -nostdin -y -i "{input_path}" '
        f"-map 0:{a_index} -c:a aac -b:a 128k -ac 2 "
        f"-movflags +faststart -progress pipe:1 -stats_period 1 "
        f'"{out_mp4}"'
    )

    logs: List[str] = []
    start_t = time.time()
    _on_prog = lambda d: handle_progress(
        d,
        media_type="audio",
        total_duration_sec=total_duration_sec,
        t0=start_t,
        filename=str(Path(out_mp4).name),
        log=log,
    )
    on_line = _make_line_logger(log, logs)
    rc = _run_stream(a_cmd, on_line=on_line, on_progress=_on_prog, idle_timeout_sec=300, cwd=str(Path(out_mp4).parent))
    if rc != 0:
        tail = "\n".join(logs[-60:]) if logs else "(no output)"
        raise CmdError(f"FFmpeg audio transcode failed rc={rc}\n--- ffmpeg audio tail ---\n{tail}")
    (log or print)(f"[audio] done → {out_mp4}")

def transcode_to_cmaf_ladder(
    input_path: str,
    work_dir: str,
    *,
    only_rungs: Optional[List[str]] = None,
    on_audio_done: Optional[Callable[[str], None]] = None,
    on_rung_done: Optional[Callable[[str, str], None]] = None,
    log: Optional[callable] = None
) -> Tuple[str, List[Dict], Dict]:
    """
    Transcodes input to CMAF-ready MP4 intermediates for the requested rungs.
    Returns:
      audio_mp4: str -> <work_dir>/audio.mp4
      renditions: List[ {name,height,bitrate,video:<path>} ]
      meta: Dict -> at least {"fps": float, "duration": float, "width": int, "height": int}
    """
    def _log(s: str): (log or print)(s)

    Path(work_dir).mkdir(parents=True, exist_ok=True)
    audio_mp4 = str(Path(work_dir) / "audio.mp4")

    # --- Probe & QC ---
    # Use your existing helpers; fall back gently if needed.
    try:
        probe = ffprobe_inspect(input_path)
    except Exception as e:
        raise CmdError(f"ffprobe failed: {e}")

    # Derive normalized meta once (duration, width, height, fps, codecs, bitrates…)
    try:
        meta = analyze_media(probe, strict=True)
    except Exception:
        # Minimal, non-strict fallback if analyze_media raises (should be rare)
        fmt = (probe or {}).get("format", {}) if isinstance(probe, dict) else {}
        v = next((s for s in (probe or {}).get("streams", []) if s.get("codec_type") == "video"), {})
        meta = {
            "duration": float(fmt.get("duration") or 0.0),
            "width":    int(v.get("width") or 0),
            "height":   int(v.get("height") or 0),
            "fps":      float(v.get("fps") or 0.0),  # may be 0.0; we'll patch below
        }

    # Single, simple fallback for fps only (no re-parsing avg_frame_rate etc.)
    if not meta.get("fps") or float(meta["fps"]) <= 0.0:
        meta["fps"] = float(get("FALLBACK_FPS", "24.0"))

    fps = float(meta["fps"])
    _log(f"[meta] duration={float(meta.get('duration',0.0)):.1f}s w={int(meta.get('width',0))} h={int(meta.get('height',0))} fps={fps:.2f}")


    # --- Audio first (AAC .mp4) ---
    _log("[audio] begin")
    rc_a = _ffmpeg_audio(input_path, audio_mp4, meta["duration"], log)
    if rc_a not in (None, 0):
        raise CmdError(f"FFmpeg audio transcode failed: rc={rc_a}")
    _log("[audio] end")
    if on_audio_done:
        try:
            on_audio_done(audio_mp4)
        except Exception as cb_exc:
            raise CmdError(f"Audio completion callback failed: {cb_exc}") from cb_exc

    # --- Ladder (filter by only_rungs if provided) ---
    seg_dur = _get_int("SEG_DUR_SEC", 4)
    ladder = default_ladder()
    if only_rungs:
        only = normalize_rung_selector(only_rungs=only_rungs)
        ladder = [r for r in ladder if r["name"].lower() in only]
        if not ladder:
            raise CmdError(f"Requested only_rungs={only_rungs} not in ladder")

    # --- Video rungs ---
    outs: List[Dict] = []
    for r in ladder:
        label_num = r["name"] #.rstrip("p")     # "240p" -> "240"
        out_mp4 = str(Path(work_dir) / f"video_{label_num}.mp4")
        _log(f"[video:{r['name']}] begin → {Path(out_mp4).name}")
        _ffmpeg_video(
            input_path, out_mp4, r["height"], r["bv"], r["maxrate"], r["bufsize"],
            fps, seg_dur, total_duration_sec=meta["duration"], log=log
        )
        _log(f"[video:{r['name']}] end")
        if on_rung_done:
            try:
                on_rung_done(r["name"], out_mp4)
            except Exception as cb_exc:
                raise CmdError(f"Rung completion callback failed ({r['name']}): {cb_exc}") from cb_exc
        outs.append({
            "name": r["name"],
            "height": r["height"],
            "bitrate": r["bv"],
            "video": out_mp4
        })

    # Quick sanity
    if not Path(audio_mp4).exists():
        raise CmdError("Audio output missing after transcode")
    if not outs:
        raise CmdError("No video renditions produced")

    _log(f"[transcode] audio_mp4={audio_mp4} renditions={len(outs)}")
    return audio_mp4, outs, meta

def _resolve_packager(log=None) -> str:
    def _log(msg: str):
        try:
            (log or print)(msg)
        except Exception:
            pass
    cand = get("SHAKA_PACKAGER_PATH", "").strip()
    candidates = []
    if cand:
        candidates.append(cand)
    w = shutil.which(cand or "packager")
    if w:
        candidates.append(w)
    candidates += ["/home/site/tools/packager", "/usr/local/bin/packager", "/usr/bin/packager"]

    tried = []
    for c in candidates:
        if not c: continue
        tried.append(c)
        path = os.path.abspath(c) if os.path.sep in c else c
        if os.path.isfile(path) and os.access(path, os.X_OK):
            try:
                out = subprocess.run([path, "--version"], capture_output=True, text=True, timeout=5)
                if out.returncode == 0:
                    _log(f"[package] using Shaka Packager: {path} ({out.stdout.strip() or 'version ok'})")
                else:
                    _log(f"[package] WARNING: {path} --version returned rc={out.returncode}")
                return path
            except Exception as e:
                _log(f"[package] WARNING: failed to exec {path}: {e}")
                continue
    tried_list = ", ".join(tried) if tried else "(no candidates)"
    raise CmdError(f"Shaka Packager not found. Set SHAKA_PACKAGER_PATH or install 'packager'. Tried: {tried_list}")

def _assert_unique_outputs(parts: List[str]) -> None:
    """Minimal duplicate-output guard for shaka descriptors."""
    # extract init_segment= and segment_template= targets and ensure they don't collide
    seen = set()
    for p in parts:
        for key in ("init_segment=", "segment_template="):
            if key in p:
                frag = p.split(key, 1)[1]
                # up to comma or end
                val = frag.split(",", 1)[0].strip().strip('"').strip("'")
                k = (key, val)
                if k in seen:
                    raise CmdError(f"Seeing duplicated outputs '{val}' in stream descriptors. Every output must be unique.")
                seen.add(k)


def package_with_shaka_ladder(
    renditions: List[Dict],
    audio_mp4: str,
    out_dash: str,
    out_hls: str,
    *,
    text_tracks: Optional[List[Dict]] = None,
    log: Optional[callable] = None
):
    """
    Packages given CMAF-ready MP4s into DASH+HLS with Shaka Packager.
    - Keeps your filename conventions: video_{name}_init.mp4 + video_{name}_$Number$.m4s, audio_init.m4a, audio_$Number$.m4s
    - Uses --generate_static_live_mpd for VOD-friendly static outputs in current Shaka versions.
    """
    def _ls(dirpath: Path) -> str:
        try:
            items = []
            for p in sorted(dirpath.glob("*")):
                items.append(p.name + ("/" if p.is_dir() else f" ({p.stat().st_size} bytes)"))
            return ", ".join(items) if items else "(empty)"
        except Exception as e:
            return f"(ls failed: {e})"

    dash_path = Path(out_dash); dash_path.parent.mkdir(parents=True, exist_ok=True)
    hls_path  = Path(out_hls);  hls_path.parent.mkdir(parents=True,  exist_ok=True)
    dash_dir = dash_path.parent
    hls_dir  = hls_path.parent

    packager = _resolve_packager(log=log)
    (log or print)(f"[package] using Shaka Packager: {packager}")
    (log or print)(f"[package] DASH → {dash_path}")
    (log or print)(f"[package] HLS  → {hls_path}")

    seg_dur = int(get("PACKAGER_SEG_DUR_SEC", "4"))

    dash_text_parts: List[str] = []
    hls_text_parts: List[str] = []
    if text_tracks:
        counters: Dict[str, int] = {}
        for track in text_tracks:
            track_path = track.get("path", "")
            if not track_path or not Path(track_path).exists():
                (log or print)(f"[package] WARNING: text track missing {track_path}; skipping")
                continue
            lang = (track.get("lang") or "und").strip().lower() or "und"
            safe_lang = re.sub(r'[^a-z0-9]', '', lang) or "lang"
            count = counters.get(safe_lang, 0) + 1
            counters[safe_lang] = count
            name = safe_lang if count == 1 else f"{safe_lang}_{count}"
            dash_text_parts.append(
                f'in="{track_path}",stream=text,language={lang},format=webvtt,'
                f'init_segment="dash/text/{name}/init.mp4",'
                f'segment_template="dash/text/{name}/$Number$.m4s",dash_only=1'
            )
            hls_text_parts.append(
                f'in="{track_path}",stream=text,language={lang},format=webvtt,'
                f'segment_template="text_{name}_$Number$.vtt",'
                f'hls_group_id=subs,hls_name={name},hls_only=1'
            )

    # ---- DASH ----
    parts = []
    for r in renditions:
        # r["name"] like "720p" → base "video_720p"
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
    parts.extend(dash_text_parts)
    _assert_unique_outputs(parts)

    dash_cmd = (
        f'{packager} ' + " ".join(parts) +
        f' --segment_duration {seg_dur}'
        f' --generate_static_live_mpd'         # static VOD semantics with current Shaka
        f' --mpd_output=stream.mpd'
        f' --io_cache_size 134217728'          # 128 MiB buffer; helps when IO bursts
        f' --io_block_size 1048576'            # 1 MiB block
        f' --v=2'
    )

    dash_logs: List[str] = []
    dash_on_line = _make_line_logger(log, dash_logs)
    rc = _run_stream(dash_cmd, on_line=dash_on_line, cwd=str(dash_dir), idle_timeout_sec=600, heartbeat_sec=30)
    if rc != 0:
        tail = "\n".join(dash_logs[-60:]) if dash_logs else "(no output)"
        raise CmdError(f"Shaka DASH packaging failed rc={rc}\n--- packager output tail ---\n{tail}")
    if not dash_path.exists():
        raise CmdError(f"Expected DASH manifest missing: {dash_path}")
    (log or print)(f"[package] DASH output dir: {_ls(dash_dir)}")

    # ---- HLS ----
    parts = []
    for r in renditions:
        label = r["name"]; base = f'video_{label}'
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
    parts.extend(hls_text_parts)
    _assert_unique_outputs(parts)

    hls_cmd = (
        f'{packager} ' + " ".join(parts) +
        f' --segment_duration {seg_dur}'
        f' --hls_playlist_type VOD'
        f' --hls_master_playlist_output=master.m3u8'
        f' --io_cache_size 134217728 --io_block_size 1048576'
        f' --v=2'
    )
    hls_logs: List[str] = []
    hls_on_line = _make_line_logger(log, hls_logs)
    rc = _run_stream(hls_cmd, on_line=hls_on_line, cwd=str(hls_dir), idle_timeout_sec=600, heartbeat_sec=30)
    if rc != 0:
        tail = "\n".join(hls_logs[-60:]) if hls_logs else "(no output)"
        raise CmdError(f"Shaka HLS packaging failed rc={rc}\n--- packager output tail ---\n{tail}")
    if not hls_path.exists():
        listing = ", ".join(sorted(p.name for p in hls_dir.iterdir())) if hls_dir.exists() else "(missing dir)"
        raise CmdError(f"Expected HLS master missing at {hls_path}; dir contains: {listing}")
    (log or print)(f"[package] HLS output dir: {_ls(hls_dir)}")
    (log or print)("[package] done")
