import subprocess, shlex, json
from pathlib import Path
from typing import Tuple, List, Dict
from .config import get
from .qc import precheck_strict, ffprobe_inspect

class CmdError(RuntimeError): pass

def _run(cmd: str, cwd: str = None):
    proc = subprocess.Popen(shlex.split(cmd), cwd=cwd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    out, err = proc.communicate()
    return proc.returncode, out, err

def ffmpeg_path() -> str: return get("FFMPEG_PATH", "ffmpeg")
def packager_path() -> str: return get("SHAKA_PACKAGER_PATH", "packager")

def _get_bool(x: str) -> bool: return str(get(x, "false")).lower() in ("1","true","yes","on")
def _get_int(x: str, d: int) -> int:
    try: return int(str(get(x, str(d))))
    except Exception: return d

def _ffmpeg_video(input_path: str, out_mp4: str, height: int, bv: str, maxrate: str, bufsize: str, fps: float, seg_dur: int):
    gop = max(1, int(round(fps * seg_dur)))
    enc = get("VIDEO_CODEC", "h264_nvenc")
    bt709 = _get_bool("SET_BT709_TAGS")
    nv_preset = get("NVENC_PRESET", "p5")
    nv_rc = get("NVENC_RC", "vbr_hq")
    nv_look = get("NVENC_LOOKAHEAD", "32")
    nv_aq = "1" if _get_bool("NVENC_AQ") else "0"
    common = (f'-vf "scale=-2:{height}" ' +
              f'-pix_fmt yuv420p ' +
              (f'-color_primaries bt709 -color_trc bt709 -colorspace bt709 ' if bt709 else '') +
              f'-g {gop} -keyint_min {gop} -sc_threshold 0 -bf 3 -coder cabac ')
    if enc == "h264_nvenc":
        v_opts = (f'-c:v h264_nvenc -preset {nv_preset} -rc {nv_rc} ' +
                  f'-spatial_aq {nv_aq} -temporal_aq 1 -rc-lookahead {nv_look} ' +
                  f'-b:v {bv} -maxrate {maxrate} -bufsize {bufsize} -profile:v high -level 4.1 ')
    else:
        v_opts = (f'-c:v libx264 -preset medium -tune film ' +
                  f'-b:v {bv} -maxrate {maxrate} -bufsize {bufsize} -profile:v high -level 4.1 ')
    v_cmd = (f'{ffmpeg_path()} -y -i "{input_path}" -map 0:v:0 ' + common + v_opts + f'-movflags +faststart -f mp4 "{out_mp4}"')
    rc, out, err = _run(v_cmd)
    if rc != 0: raise CmdError(f"FFmpeg video transcode failed ({height}p): {err}")

def _ffmpeg_audio(input_path: str, out_mp4: str):
    a_cmd = (f'{ffmpeg_path()} -y -i "{input_path}" -map 0:a:0 '
             f'-c:a aac -b:a {get("AUDIO_MAIN_KBPS","128")}k -ac 2 -ar 48000 '
             f'-movflags +faststart -f mp4 "{out_mp4}"')
    rc, out, err = _run(a_cmd)
    if rc != 0: raise CmdError(f"FFmpeg audio transcode failed: {err}")

def transcode_to_cmaf_ladder(input_path: str, workdir: str) -> Tuple[str, List[Dict], Dict]:
    Path(workdir).mkdir(parents=True, exist_ok=True)
    audio_mp4 = str(Path(workdir)/"audio.mp4")
    _ffmpeg_audio(input_path, audio_mp4)
    meta = ffprobe_inspect(input_path)
    fps, _, _ = precheck_strict(meta)
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
        _ffmpeg_video(input_path, out_mp4, r["height"], r["bv"], r["maxrate"], r["bufsize"], fps, seg_dur)
        outs.append({"name": r["name"], "height": r["height"], "bitrate": r["bv"], "video": out_mp4})
    return audio_mp4, outs, {"fps": fps}

def package_with_shaka_ladder(renditions: List[Dict], audio_mp4: str, out_dash: str, out_hls: str, text_tracks: List[Dict] = None):
    Path(out_dash).mkdir(parents=True, exist_ok=True)
    Path(out_hls).mkdir(parents=True, exist_ok=True)
    seg_dur = _get_int("PACKAGER_SEG_DUR_SEC", 4)
    trick = _get_bool("ENABLE_TRICKPLAY")
    trick_factor = _get_int("TRICKPLAY_FACTOR", 4)
    # DASH
    parts = []
    for r in renditions:
        label = r["name"]; base = f'video_{label}'; seg_tpl = f'{base}_$Number$.m4s'
        trick_opt = f',trick_play_factor={trick_factor}' if trick else ''
        parts.append(f'in="{r["video"]}",stream=video,init_segment={base}_init.mp4,segment_template={seg_tpl}{trick_opt}')
    parts.append(f'in="{audio_mp4}",stream=audio,init_segment=audio_init.m4a,segment_template=audio_$Number$.m4s')
    if text_tracks:
        for tti in text_tracks:
            parts.append(f'in="{tti["path"]}",stream=text,language={tti.get("lang","en")}')
    dash_cmd = f'{packager_path()} ' + " ".join(parts) + f' --segment_duration {seg_dur} --generate_static_mpd --mpd_output=stream.mpd'
    rc, out, err = _run(dash_cmd, cwd=out_dash)
    if rc != 0: raise CmdError(f"Shaka DASH packaging failed: {err}")
    # HLS
    parts = []
    for r in renditions:
        label = r["name"]; base = f'video_{label}'; seg_tpl = f'{base}_$Number$.m4s'
        parts.append(f'in="{r["video"]}",stream=video,init_segment={base}_init.mp4,segment_template={seg_tpl}')
    parts.append(f'in="{audio_mp4}",stream=audio,init_segment=audio_init.m4a,segment_template=audio_$Number$.m4s')
    if text_tracks:
        for tti in text_tracks:
            parts.append(f'in="{tti["path"]}",stream=text,language={tti.get("lang","en")}')
    hls_cmd = f'{packager_path()} ' + " ".join(parts) + f' --segment_duration {seg_dur} --hls_master_playlist_output=master.m3u8'
    rc, out, err = _run(hls_cmd, cwd=out_hls)
    if rc != 0: raise CmdError(f"Shaka HLS packaging failed: {err}")
