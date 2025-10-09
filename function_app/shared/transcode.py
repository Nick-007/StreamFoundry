import time, subprocess, shlex, re, os, shutil, signal
from pathlib import Path
from typing import Callable, Optional, Tuple, List, Dict
from math import floor, ceil
from fractions import Fraction
from .config import get
from .errors import CmdError
from .tools import ffmpeg_path
from .qc import ffprobe_inspect, analyze_media

_PROCS = set()

_SEG_RE = re.compile(r'.*_(\d+)\.m4s$')

def _highest_segment_index(dir_path: str, base_prefix: str) -> int:
    """
    Return highest N among {base_prefix}{N}.m4s in dir_path, or 0 if none exist.
    """
    p = Path(dir_path)
    if not p.exists():
        return 0
    hi = 0
    for f in p.glob(f"{base_prefix}*.m4s"):
        m = _SEG_RE.match(f.name)
        if m:
            try:
                hi = max(hi, int(m.group(1)))
            except Exception:
                pass
    return hi

def _sec_from_segments(n: int, seg_dur: int) -> float:
    """
    Translate segment count -> media seconds.
    """
    return float(n * seg_dur)

# in function_app/shared/transcode.py (replace your existing derive_k)

def derive_k(*args, fps: float | None = None, seg_dur: int | None = None, **kwargs) -> int:
    """
    Derive GOP/keyint ~= fps * seg_dur, aligned to segment boundaries.
    Back-compat:
      - accepts legacy positional: (fps, seg_dur)
      - accepts legacy kwargs: frame_rate=..., segment_duration=...
    """

    # ---- harvest fps ----
    if fps is None:
        # legacy positional: derive_k(29.97, 4)
        if len(args) >= 1:
            try:
                fps = float(args[0])
            except Exception:
                fps = None
    if fps is None:
        # legacy kw: frame_rate=...
        fr = kwargs.get("frame_rate")
        if fr is not None:
            try:
                fps = float(fr)
            except Exception:
                fps = None
    if fps is None:
        # final fallback
        try:
            # if you have shared.config.get available
            from ..shared.config import get as _get  # adjust import if needed
            fps = float(_get("DEFAULT_FPS", "24"))
        except Exception:
            fps = 24.0

    # stabilize common NTSC floats like 29.97/59.94
    try:
        fps = float(Fraction(fps).limit_denominator(1001))
    except Exception:
        fps = float(fps)

    # ---- harvest seg_dur ----
    if seg_dur is None:
        if len(args) >= 2:
            try:
                seg_dur = int(args[1])
            except Exception:
                seg_dur = None
    if seg_dur is None:
        seg_dur = kwargs.get("segment_duration") or kwargs.get("seg_duration") or kwargs.get("seg")
        if seg_dur is not None:
            try:
                seg_dur = int(seg_dur)
            except Exception:
                seg_dur = None
    if seg_dur is None:
        try:
            from ..shared.config import get as _get
            seg_dur = int(_get("SEG_DUR_SEC", "4"))
        except Exception:
            seg_dur = 4

    # ---- compute & clamp ----
    k = int(round(fps * seg_dur))
    # clamp defensively (avoid absurd GOPs)
    if k < 2:
        k = 2
    elif k > 300:
        k = 300
    return k

# --- DROP-IN: resumable CMAF VIDEO rung (add; do not replace your MP4 path) ---

def derive_run_quota(
    *,
    total_duration_sec: float,
    seg_dur: int,
    n_done: int,
    budget_sec: float,
    rate_media_per_wall: float | None = None,
) -> int:
    """
    Decide how many whole segments to encode in this invocation, respecting:
      - remaining media (based on already completed segments)
      - available time budget
      - optional throughput hint (rate_media_per_wall), used for a small headroom
    Returns an integer #segments (>= 0).
    """
    # Remaining media after already-completed segments
    t0 = max(0, n_done) * seg_dur
    remaining = max(0.0, float(total_duration_sec) - float(t0))
    if remaining <= 0:
        return 0

    # Cap by budget; apply a conservative headroom if we have a measured rate
    if budget_sec is None or float(budget_sec) <= 0:
        span_cap = remaining
    else:
        headroom = 0.90 if (rate_media_per_wall and rate_media_per_wall > 0) else 1.00
        span_cap = min(remaining, float(budget_sec) * headroom)

    # Encode only whole segments
    return int(span_cap // seg_dur)


def _ffmpeg_video_to_cmaf_segments(
    input_path: str,
    out_dir: str,
    label: str,
    height: int,
    bv: str,
    maxrate: str,
    bufsize: str,
    fps: float,
    seg_dur: int,
    total_duration_sec: float,
    budget_sec: float,
    rate_media_per_wall: float | None = None,
    log=None,
) -> dict:
    """
    Produce CMAF video segments for a single rung, with:
      - resume (start_number = last+1)
      - time-budget cap (encode only N segments this run)
      - IDR alignment to segment boundaries via GOP and force_key_frames
    Returns dict describing the rung’s init + pattern.
    """
    def _log(s): (log or print)(s)

    Path(out_dir).mkdir(parents=True, exist_ok=True)

    # Progress & resume
    base = f"video_{label}_"
    n_done  = _highest_segment_index(out_dir, base)   # last existing $Number$
    resumeN = n_done + 1

    # Decide how many segments to emit this run (quota)
    n_to_make = derive_run_quota(
        total_duration_sec=total_duration_sec,
        seg_dur=seg_dur,
        n_done=n_done,
        budget_sec=budget_sec,
        rate_media_per_wall=rate_media_per_wall,
    )
    if n_to_make <= 0:
        _log(f"[video:{label}] nothing to do (n_done={n_done})")
        return {
            "label": label,
            "init": str(Path(out_dir) / f"video_{label}_init.mp4"),
            "pattern": f"video_{label}_$Number$.m4s",
            "kind": "video",
            "resumeN": resumeN,
            "K": 0,  # K = quota for this run (kept for shape-compat)
        }

    # Time slice for this invocation (seek to exact segment boundary)
    t0   = n_done * seg_dur
    span = n_to_make * seg_dur

    # GOP/key-int for codec (align IDR to segment boundaries)
    try:
        # Stabilize common fractional fps (e.g., 30000/1001)
        f = float(Fraction(fps).limit_denominator(1001))
    except Exception:
        f = float(fps)
    gop = max(2, int(round(f * seg_dur)))

    enc   = get("VIDEO_CODEC", "libx264").strip().lower()
    bt709 = _get_bool("SET_BT709_TAGS")

    vf = f'scale=-2:{height}'
    force_kf = f'-force_key_frames "expr:gte(t,n_forced*{seg_dur})" '

    common = (
        f'-pix_fmt yuv420p '
        + (f'-color_primaries bt709 -color_trc bt709 -colorspace bt709 ' if bt709 else '')
        + f'-g {gop} -keyint_min {gop} -sc_threshold 0 '
        + force_kf
    )

    if enc == "h264_nvenc":
        vcodec = (
            f'-c:v h264_nvenc '
            f'-preset {get("NVENC_PRESET","p5")} '
            f'-rc {get("NVENC_RC","vbr_hq")} '
            f'-rc-lookahead {get("NVENC_LOOKAHEAD","32")} '
            f'-spatial_aq {"1" if _get_bool("NVENC_AQ") else "0"} -temporal_aq 1 '
        )
    elif enc in ("h264_videotoolbox", "hevc_videotoolbox"):
        vcodec = f'-c:v {enc} '
    elif enc in ("libx265", "hevc", "hevc_nvenc"):
        # Optional: handle HEVC similarly; keep simple if not used
        vcodec = f'-c:v {("hevc_nvenc" if enc=="hevc_nvenc" else "libx265")} '
    else:
        vcodec = '-c:v libx264 -preset medium -tune film '

    init_name = f'video_{label}_init.mp4'
    media_pat = f'video_{label}_$Number$.m4s'

    # DASH muxer writes CMAF segments; the .mpd it produces here is not consumed
    dash_opts = (
        f'-f dash -use_timeline 1 -use_template 1 '
        f'-seg_duration {seg_dur} -min_seg_duration {seg_dur} '
        f'-single_file 0 -remove_at_exit 0 -window_size 0 -extra_window_size 0 '
        f'-init_seg_name {init_name} -media_seg_name {media_pat} '
        f'-hls_playlist 0 '
        f'-start_number {resumeN} '
    )

    v_cmd = (
        f'{ffmpeg_path()} -hide_banner -nostdin -y '
        f'-ss {t0} -i "{input_path}" -map 0:v:0 '
        f'-vf "{vf}" {common} {vcodec} '
        f'-b:v {bv} -maxrate {maxrate} -bufsize {bufsize} '
        f'-profile:v high -level 4.1 '
        f'{dash_opts} '
        f'-t {span} '
        f'"{Path(out_dir)/f"video_{label}.mpd"}"'
    )

    _log(f"[video:{label}] resumeN={resumeN} n_to_make={n_to_make} gop={gop} "
         f"t0={t0}s span={span}s → {out_dir}")

    rc = _run_stream(v_cmd, on_line=_log, on_progress=None, cwd=out_dir, idle_timeout_sec=600)
    if rc != 0:
        raise CmdError(f"FFmpeg CMAF video rung failed ({label}): rc={rc}")

    return {
        "label": label,
        "init": str(Path(out_dir) / init_name),
        "pattern": media_pat,
        "kind": "video",
        "resumeN": resumeN,
        "K": n_to_make,  # K now clearly = segments emitted this run
    }


# --- DROP-IN: resumable CMAF AUDIO (add; separate from your MP4 audio) --------
def _ffmpeg_audio_to_cmaf_segments(
    input_path: str,
    out_dir: str,
    seg_dur: int,
    total_duration_sec: float,
    budget_sec: float,
    rate_media_per_wall: float | None = None,
    log=None,
) -> dict:
    def _log(s): (log or print)(s)
    outp = Path(out_dir)
    outp.mkdir(parents=True, exist_ok=True)

    # How many segments already exist?
    n_done  = _highest_segment_index(out_dir, "audio_")
    resumeN = n_done + 1

    # Remaining media duration from current offset
    t0 = n_done * seg_dur
    remaining_media = max(0.0, float(total_duration_sec) - t0)

    if remaining_media <= 0:
        _log(f"[audio] nothing to do (n_done={n_done})")
        return {
            "label": "audio",
            "init": str(outp / "audio_init.m4a"),
            "pattern": "audio_$Number$.m4s",
            "kind": "audio",
            "resumeN": resumeN,
            "K": 0,  # kept for shape-compat; audio doesn’t use GOP
        }

    # Timebox by budget: encode only as many whole segments as we can
    # If budget is tiny (< seg_dur), we’ll do 0 and exit early.
    max_span_by_budget = max(0.0, float(budget_sec))
    span_cap = remaining_media if max_span_by_budget <= 0 else min(remaining_media, max_span_by_budget)
    n_to_make = int(span_cap // seg_dur)

    if n_to_make <= 0:
        _log(f"[audio] budget exhausted or <1 segment available (n_done={n_done}, budget_sec={budget_sec})")
        return {
            "label": "audio",
            "init": str(outp / "audio_init.m4a"),
            "pattern": "audio_$Number$.m4s",
            "kind": "audio",
            "resumeN": resumeN,
            "K": 0,
        }

    span = n_to_make * seg_dur  # encode an integral number of segments

    a_cmd = (
        f'{ffmpeg_path()} -hide_banner -nostdin -y '
        f'-ss {t0} -i "{input_path}" -map 0:a:0? '
        f'-c:a aac -b:a {get("AUDIO_BITRATE","128k")} -ac 2 -ar 48000 '
        f'-f dash -use_timeline 1 -use_template 1 '
        f'-seg_duration {seg_dur} -min_seg_duration {seg_dur} '
        f'-single_file 0 -remove_at_exit 0 -window_size 0 -extra_window_size 0 '
        f'-init_seg_name audio_init.m4a -media_seg_name audio_$Number$.m4s '
        f'-hls_playlist 0 '
        f'-start_number {resumeN} '
        f'-t {span} '
        f'"{outp/"audio.mpd"}"'
    )

    _log(f"[audio] resumeN={resumeN} t0={t0}s span={span}s → {out_dir}")
    rc = _run_stream(a_cmd, on_line=_log, on_progress=None, cwd=out_dir, idle_timeout_sec=600)
    if rc != 0:
        raise CmdError(f"FFmpeg CMAF audio failed: rc={rc}")

    return {
        "label": "audio",
        "init": str(outp / "audio_init.m4a"),
        "pattern": "audio_$Number$.m4s",
        "kind": "audio",
        "resumeN": resumeN,
        "K": 0,  # still returned for shape-compat
    }


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
                heartbeat_sec: float = 30.0) -> int:
    p = _popen(cmd, cwd=cwd, text=True, bufsize=1)
    start = last_activity = time.time()
    last_heartbeat = 0.0

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
        if "=" in line and on_progress:
            k, v = line.split("=", 1)
            _safe_call(on_progress, {k: v})
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
                    break
                time.sleep(0.1)
        return p.returncode
    finally:
        _PROCS.discard(p)

def _get_bool(x: str, default: bool = False) -> bool:
    val = str(get(x, "true" if default else "false")).lower()
    return val in ("1","true","yes","on")

def _get_int(x: str, d: int) -> int:
    try:
        return int(str(get(x, str(d))))
    except Exception:
        return d

def _make_line_logger(log: Optional[Callable[[str], None]], sink: List[str]):
    def _on_line(s: str):
        sink.append(s)
        (log or print)(s)
    return _on_line

def _ffmpeg_video(input_path: str, out_mp4: str, height: int, bv: str, maxrate: str, bufsize: str,
                  fps: float, seg_dur: int, total_duration_sec: float = 0.0,
                  log: Optional[Callable[[str], None]] = None):
    Path(out_mp4).parent.mkdir(parents=True, exist_ok=True)

    gop = max(1, int(round(fps * seg_dur)))
    enc = get("VIDEO_CODEC", "libx264").strip().lower()
    bt709 = _get_bool("SET_BT709_TAGS")
    nv_preset = get("NVENC_PRESET", "p5")
    nv_rc     = get("NVENC_RC", "vbr_hq")
    nv_look   = get("NVENC_LOOKAHEAD", "32")
    nv_aq     = "1" if _get_bool("NVENC_AQ") else "0"

    common = (
        f'-vf "scale=-2:{height}" '
        f'-pix_fmt yuv420p '
        + (f'-color_primaries bt709 -color_trc bt709 -colorspace bt709 ' if bt709 else '')
        + f'-g {gop} -keyint_min {gop} -sc_threshold 0 -bf 3 -coder cabac '
        f'-video_track_timescale 90000 '                                  # <— new
        f'-force_key_frames "expr:gte(t,n_forced*{seg_dur})" '             # <— new
        # optional but helps keep timestamps monotonic and clean for VFR sources:
        f'-fps_mode vfr '
    )

    def _opts_for(codec: str) -> str:
        if codec == "h264_nvenc":
            return (f'-c:v h264_nvenc -preset {nv_preset} -rc {nv_rc} '
                    f'-spatial_aq {nv_aq} -temporal_aq 1 -rc-lookahead {nv_look} '
                    f'-b:v {bv} -maxrate {maxrate} -bufsize {bufsize} -profile:v high -level 4.1 ')
        elif codec in ("h264_videotoolbox", "hevc_videotoolbox"):
            return (f'-c:v {codec} -b:v {bv} -maxrate {maxrate} -bufsize {bufsize} '
                    f'-profile:v high -level 4.1 ')
        else:
            return (f'-c:v libx264 -preset medium -tune film '
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

        last: Dict[str, str] = {}
        last_pct = -1
        t0 = time.time()
        def _on_prog(d: dict):
            nonlocal last, last_pct
            last.update(d)
            ot = last.get("out_time_ms") or last.get("out_time_us")
            if not ot:
                return
            try:
                sec = (int(ot) / 1000.0) if "out_time_ms" in last else (int(ot) / 1_000_000.0)
            except Exception:
                return
            if total_duration_sec > 0:
                pct = max(0, min(100, int((sec / total_duration_sec) * 100)))
                if pct != last_pct:
                    last_pct = pct
                    elapsed = max(1.0, time.time() - t0)
                    rate = sec / elapsed
                    eta = int((total_duration_sec - sec) / rate) if rate > 0 else -1
                    (log or print)(f"[video] {Path(out_mp4).name} {pct}% ({sec:0.1f}/{total_duration_sec:0.1f}s) ETA~{eta}s")
            else:
                (log or print)(f"[video] {Path(out_mp4).name} t={sec:0.1f}s")

        on_line = _make_line_logger(log, logs)
        rc = _run_stream(cmd, on_line=on_line, on_progress=_on_prog, idle_timeout_sec=900, cwd=str(Path(out_mp4).parent))
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

def _ffmpeg_audio(input_path: str, out_mp4: str, log: Optional[Callable[[str], None]] = None):
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
    def _on_prog(d: dict):
        ot = d.get("out_time_ms") or d.get("out_time_us")
        if not ot:
            return
        try:
            sec = (int(ot) / 1000.0) if "out_time_ms" in d else (int(ot) / 1_000_000.0)
            (log or print)(f"[audio] t={sec:0.1f}s")
        except Exception:
            pass

    on_line = _make_line_logger(log, logs)
    rc = _run_stream(a_cmd, on_line=on_line, on_progress=_on_prog, idle_timeout_sec=600, cwd=str(Path(out_mp4).parent))
    if rc != 0:
        tail = "\n".join(logs[-60:]) if logs else "(no output)"
        raise CmdError(f"FFmpeg audio transcode failed rc={rc}\n--- ffmpeg audio tail ---\n{tail}")
    (log or print)(f"[audio] done → {out_mp4}")

def transcode_to_cmaf_ladder(input_path: str, workdir: str, log: Optional[Callable[[str], None]] = None) -> Tuple[str, List[Dict], Dict]:
    Path(workdir).mkdir(parents=True, exist_ok=True)
    (log or print)(f"[transcode] workdir={workdir}")

    # audio first
    audio_mp4 = str(Path(workdir)/"audio.mp4")
    (log or print)("[audio] begin")
    _ffmpeg_audio(input_path, audio_mp4, log)
    (log or print)("[audio] end")

    # source meta
    probe   = ffprobe_inspect(input_path)
    meta_in = analyze_media(probe, strict=True)
    fps     = meta_in["fps"]
    try:
        duration_sec = float(meta_in["format"]["duration"])
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
        (log or print)(f"[video:{label_num}] begin → {out_mp4}")
        _ffmpeg_video(input_path, out_mp4, r["height"], r["bv"], r["maxrate"], r["bufsize"], fps, seg_dur, total_duration_sec=duration_sec, log=log)
        (log or print)(f"[video:{label_num}] end")
        outs.append({"name": r["name"], "height": r["height"], "bitrate": r["bv"], "video": out_mp4})
    return audio_mp4, outs, meta_in

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

def package_with_shaka_ladder(renditions: List[Dict], audio_mp4: str, out_dash: str, out_hls: str,
                              text_tracks: List[Dict] = None, log: Optional[Callable[[str], None]] = None):
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
    (log or print)(f"[package] DASH → {dash_path}")
    (log or print)(f"[package] HLS  → {hls_path}")

    seg_dur = _get_int("PACKAGER_SEG_DUR_SEC", 4)
    trick = _get_bool("ENABLE_TRICKPLAY")
    trick_factor = _get_int("TRICKPLAY_FACTOR", 4)

    # ---- DASH ----
    parts = []
    for r in renditions:
        label = r["name"]; base = f'video_{label}'
        parts.append(
            f'in="{r["video"]}",stream=video,init_segment="{base}_init.mp4",'
            f'segment_template="{base}_$Number$.m4s"{(",trick_play_factor="+str(trick_factor)) if trick else ""}'
        )
    parts.append('in="{a}",stream=audio,init_segment="audio_init.m4a",segment_template="audio_$Number$.m4s"'.format(a=audio_mp4))
    if text_tracks:
        for t in text_tracks:
            parts.append(f'in="{t["path"]}",stream=text,language={t.get("lang","en")}')
    _assert_unique_outputs(parts)

    dash_cmd = (
        f'{packager} ' + " ".join(parts) +
        f' --segment_duration {seg_dur} --generate_static_live_mpd --mpd_output=stream.mpd --v=2'
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
            f'in="{r["video"]}",stream=video,init_segment="{base}_init.mp4",segment_template="{base}_$Number$.m4s"'
        )
    parts.append('in="{a}",stream=audio,init_segment="audio_init.m4a",segment_template="audio_$Number$.m4s"'.format(a=audio_mp4))
    if text_tracks:
        for t in text_tracks:
            parts.append(f'in="{t["path"]}",stream=text,language={t.get("lang","en")}')
    _assert_unique_outputs(parts)

    hls_cmd = (
        f'{packager} ' + " ".join(parts) +
        f' --segment_duration {seg_dur} --hls_playlist_type VOD --hls_master_playlist_output=master.m3u8 --v=2'
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