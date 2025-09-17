import json, subprocess, shlex
from typing import Dict, Tuple
def _run_json(cmd: str) -> Dict:
    p = subprocess.Popen(shlex.split(cmd), stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    out, err = p.communicate()
    if p.returncode != 0:
        raise RuntimeError(f"ffprobe failed: {err.strip()}")
    return json.loads(out)
def ffprobe_inspect(path: str) -> Dict:
    cmd = f'ffprobe -v error -print_format json -show_streams -show_format "{path}"'
    return _run_json(cmd)
def parse_fps(avg_frame_rate: str) -> float:
    if not avg_frame_rate or avg_frame_rate == "0/0": return 30.0
    if "/" in avg_frame_rate:
        a,b = avg_frame_rate.split("/")
        try: return float(a)/float(b)
        except Exception: return 30.0
    try: return float(avg_frame_rate)
    except Exception: return 30.0
def precheck_strict(meta: Dict) -> Tuple[float,int,int]:
    vstreams = [s for s in meta.get("streams", []) if s.get("codec_type")=="video"]
    if not vstreams: raise RuntimeError("No video stream found")
    vs = vstreams[0]; codec = vs.get("codec_name")
    if codec not in ("h264","hevc","mpeg4","vp9","av1","mpeg2video"):
        raise RuntimeError(f"Unsupported video codec: {codec}")
    w,h = vs.get("width"), vs.get("height")
    if not w or not h: raise RuntimeError("Missing width/height")
    from_fps = vs.get("avg_frame_rate") or vs.get("r_frame_rate") or "30/1"
    # crude parse
    if "/" in from_fps:
        a,b = from_fps.split("/"); 
        try: fps = float(a)/float(b)
        except Exception: fps = 30.0
    else:
        try: fps = float(from_fps)
        except Exception: fps = 30.0
    return fps, int(w), int(h)
