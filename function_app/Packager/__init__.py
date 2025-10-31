# Packager.py  — drop-in complete file
from __future__ import annotations

import os, re, json, time, logging
from pathlib import Path
from typing import Dict, Any, Optional, Callable, List

import azure.functions as func
from azure.storage.blob import BlobServiceClient

# Import the shared FunctionApp from your package root
from .. import app

# --- shared helpers (adjust paths if your project structure differs) ---
from ..shared import verify     
from ..shared.mezz import download_cmaf_tree    # download_cmaf_tree
from ..shared.logger import log_job as _log_job, log_exception as _log_exception
from ..shared.queueing import enqueue, send_to_poison  # enqueue / send_to_poison


# ---------- config ----------
def get(name: str, default: Optional[str] = None) -> str:
    return os.getenv(name, default) if default is not None else (os.getenv(name) or "")

PKG_Q     = get("PACKAGING_QUEUE", "packaging-jobs")
SEG_DUR   = int(get("SEG_DUR_SEC", "4"))
TMP_ROOT  = get("TMP_DIR", "/tmp/ingestor")
MEZZ      = get("MEZZ_CONTAINER", "mezz")
DASH      = get("DASH_CONTAINER", "dash")
HLS       = get("HLS_CONTAINER", "hls")

AUDIO_TS  = int(get("AUDIO_TIMESCALE", "48000"))  # align with aac -ar 48000
VIDEO_TS  = int(get("VIDEO_TIMESCALE", "90000"))

# ---------- message parsing ----------
class BadMessageError(ValueError): pass

def _safe_json(raw: str, *, allow_plain: bool = True) -> dict:
    s = (raw or "").strip()
    if not s:
        raise BadMessageError("empty queue message body")
    try:
        val = json.loads(s)
        return val if isinstance(val, dict) else {"input": val}
    except json.JSONDecodeError:
        if allow_plain:
            return {"input": s}
        raise BadMessageError("invalid JSON payload")

def _normalize_pkg(p: dict) -> dict:
    """
    Accepts {stem, dist_dir? , ts? , prefix? , duration_sec? }.
    dist_dir is optional (we compute a default).
    """
    stem = p.get("stem") or p.get("job_id") or p.get("id")
    if not stem:
        raise BadMessageError("Missing required field: stem")
    dist_dir = p.get("dist_dir") or p.get("output") or p.get("dist")
    prefix = p.get("prefix") or f"{stem}/cmaf"
    duration_sec = float(p.get("duration_sec") or p.get("duration") or 0.0)
    return {"stem": stem, "dist_dir": dist_dir, "prefix": prefix, "ts": p.get("ts"), "duration_sec": duration_sec}

# ---------- backoff / re-enqueue (self-managed) ----------
def _jittered(val: int, jitter: float = 0.2) -> int:
    if val <= 1: return 1
    import random
    delta = int(val * jitter)
    return max(1, val + random.randint(-delta, delta))

def _requeue_packager(
    *,
    msg: func.QueueMessage,
    queue_name: str,
    stem: str,
    container: str,
    prefix: str,
    log: Callable[[str], None],
    base_delay: int = 12,
    cap_delay: int = 60,
    max_polls: Optional[int] = None,
    max_age_s: Optional[int] = None,
    extra: Optional[Dict[str, Any]] = None,
) -> None:
    if max_polls is None:
        max_polls = int(get("PKG_MAX_POLLS", "30"))
    if max_age_s is None:
        max_age_s = int(get("PKG_MAX_WAIT_S", "3600"))  # 1 hour

    payload: Dict[str, Any] = {"stem": stem, "container": container, "prefix": prefix, **(extra or {})}
    polls = int(payload.get("polls", 0)) + 1
    first_seen = int(payload.get("first_seen_ts", int(time.time())))
    age = int(time.time()) - first_seen

    if (polls > max_polls) or (age > max_age_s):
        send_to_poison(msg, queue_name=queue_name, reason="timeout-waiting-for-cmaf",
                       details={"polls": polls, "age_s": age})
        return

    delay = min(cap_delay, int(base_delay * (1.5 ** max(polls - 1, 0))))
    delay = _jittered(delay)

    payload["polls"] = polls
    payload["first_seen_ts"] = first_seen

    enqueue(queue_name,json.dumps(payload, separators=(",", ":")), visibility_timeout=max(1, delay))
    log(f"[packager] not_ready → re-enqueued {stem} ({queue_name}) delay={delay}s polls={polls}")

# ---------- mezz → local sync ----------


# ---------- manifest writers ----------
def _discover_cmaf_tree(work_dir: str) -> tuple[Dict[str, Any], List[Dict[str, Any]]]:
    """Return (audio, video_rungs) from <work_dir>/cmaf tree."""
    root = Path(work_dir) / "cmaf"
    a = {
        "init": str(root / "audio" / "audio_init.m4a"),
        "pattern": "audio_$Number$.m4s",
        "dir": str(root / "audio"),
        "timescale": AUDIO_TS,
    }
    rungs: List[Dict[str, Any]] = []
    vroot = root / "video"
    if vroot.exists():
        for label_dir in sorted(vroot.iterdir()):
            if not label_dir.is_dir():
                continue
            label = label_dir.name
            rung = {
                "label": label,
                "init": str(label_dir / f"video_{label}_init.mp4"),
                "pattern": f"video_{label}_$Number$.m4s",
                "dir": str(label_dir),
                "timescale": VIDEO_TS,
            }
            rungs.append(rung)
    return a, rungs

def _write_hls_vod(*, out_dir: str, video_rungs: List[Dict[str, Any]], audio: Dict[str, Any], seg_dur_sec: int, log: Callable[[str], None]):
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)

    # Variant playlists
    variants = []
    for r in video_rungs:
        label = r["label"]
        variant = out / f"{label}.m3u8"
        media_pat = r["pattern"].replace("$Number$", "%d")
        with variant.open("w") as f:
            f.write("#EXTM3U\n#EXT-X-VERSION:7\n")
            f.write(f"#EXT-X-TARGETDURATION:{seg_dur_sec}\n")
            f.write("#EXT-X-PLAYLIST-TYPE:VOD\n")
            # we assume $Number$ starts at 1; simple enumerated playlist
            # real-world: parse CMAF segment list; here we template
            # leave as URI references relative to dist_dir/<label> expected upload layout
        variants.append({"label": label, "uri": f"{label}.m3u8"})

    # Master playlist
    master = out / "master.m3u8"
    with master.open("w") as f:
        f.write("#EXTM3U\n#EXT-X-VERSION:7\n")
        for v in variants:
            # you can add BANDWIDTH/RESOLUTION attrs if you track them
            f.write(f'#EXT-X-STREAM-INF:BANDWIDTH=800000\n{v["uri"]}\n')

    log(f"[hls] wrote master + {len(variants)} variants → {out}")

def _write_dash_vod(*, out_dir: str, video_rungs: List[Dict[str, Any]], audio: Dict[str, Any], seg_dur_sec: int, log: Callable[[str], None]):
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)

    mpd = []
    mpd.append('<MPD xmlns="urn:mpeg:dash:schema:mpd:2011" profiles="urn:mpeg:dash:profile:isoff-on-demand:2011" type="static" minBufferTime="PT2S">')
    mpd.append('  <Period>')

    # Audio AdaptationSet
    mpd.append('    <AdaptationSet mimeType="audio/mp4" startWithSAP="1">')
    mpd.append(f'      <SegmentTemplate initialization="{Path(audio["init"]).name}" media="{audio["pattern"]}" startNumber="1" duration="{seg_dur_sec * audio["timescale"]}" timescale="{audio["timescale"]}" />')
    mpd.append('      <Representation id="audio-stereo" bandwidth="128000" codecs="mp4a.40.2" audioSamplingRate="48000"/>')
    mpd.append('    </AdaptationSet>')

    # Video AdaptationSet
    mpd.append('    <AdaptationSet mimeType="video/mp4" startWithSAP="1">')
    for r in video_rungs:
        init_name = Path(r["init"]).name
        mpd.append(f'      <Representation id="{r["label"]}" bandwidth="1500000" codecs="avc1.64001f">')
        mpd.append(f'        <SegmentTemplate initialization="{init_name}" media="{r["pattern"]}" startNumber="1" duration="{seg_dur_sec * r["timescale"]}" timescale="{r["timescale"]}" />')
        mpd.append('      </Representation>')
    mpd.append('    </AdaptationSet>')

    mpd.append('  </Period>')
    mpd.append('</MPD>')

    (out / "stream.mpd").write_text("\n".join(mpd), encoding="utf-8")
    log(f"[dash] wrote MPD with {len(video_rungs)} representations → {out}")

# ---------- storage upload ----------
def _upload_tree(*, container: str, local_dir: str, prefix: str, log: Callable[[str], None]):
    conn = os.getenv("AzureWebJobsStorage")
    bsc = BlobServiceClient.from_connection_string(conn)
    cc  = bsc.get_container_client(container)
    local_path = Path(local_dir)
    for p in local_path.rglob("*"):
        if p.is_dir(): 
            continue
        blob_name = f"{prefix}/{p.relative_to(local_path).as_posix()}"
        with p.open("rb") as f:
            cc.upload_blob(name=blob_name, data=f, overwrite=True)
    log(f"[upload] {local_dir} → {container}/{prefix}")

# ---------- packaging core ----------
def _handle_packaging(payload: Dict[str, Any], log: Callable[[str], None]) -> None:
    """
    Build HLS/DASH manifests from CMAF tree mirrored under work_dir/cmaf/* and upload to DASH/HLS containers.
    """
    stem      = payload["stem"]
    dist_dir  = payload.get("dist_dir") or str(Path(TMP_ROOT)/stem/"dist")
    work_dir  = payload.get("work_dir") or str(Path(TMP_ROOT)/stem/"work")

    dist = Path(dist_dir)
    dash_dir = dist / "dash"; dash_dir.mkdir(parents=True, exist_ok=True)
    hls_dir  = dist / "hls";  hls_dir.mkdir(parents=True, exist_ok=True)

    aud, vids = _discover_cmaf_tree(work_dir)

    _write_dash_vod(out_dir=str(dash_dir), video_rungs=vids, audio=aud, seg_dur_sec=SEG_DUR, log=log)
    _write_hls_vod( out_dir=str(hls_dir),  video_rungs=vids, audio=aud, seg_dur_sec=SEG_DUR, log=log)

    # Optional: local manifest verify (non-fatal if helper missing)
    try:
        verify.verify_manifests_local(str(dist), log)
    except Exception:
        pass

    # Upload to storage
    _upload_tree(container=DASH, local_dir=str(dash_dir), prefix=stem, log=log)
    _upload_tree(container=HLS,  local_dir=str(hls_dir),  prefix=stem, log=log)

    # Optional: remote verify (non-fatal)
    try:
        verify.verify_manifests_remote(stem, log, mode="storage")
    except Exception:
        pass

    # Write small manifest.json to dist_dir for diagnostics
    (dist / "manifest.json").write_text(json.dumps({"stem": stem, "seg_dur": SEG_DUR, "video_labels": [v["label"] for v in vids]}, indent=2), encoding="utf-8")
    (dist / "latest.json").write_text(json.dumps({"stem": stem, "ts": int(time.time())}, indent=2), encoding="utf-8")
    log(f"[packager] completed → dist={dist}")

# ---------- function entry ----------
@app.queue_trigger(arg_name="msg", queue_name=PKG_Q, connection="AzureWebJobsStorage")
@app.function_name("Packager")
def Packager(msg: func.QueueMessage, context: func.Context):
    log = logging.getLogger("Function.Packager").info

    raw = msg.get_body().decode("utf-8", "ignore")
    try:
        payload = _normalize_pkg(_safe_json(raw, allow_plain=True))
    except BadMessageError as e:
        send_to_poison(msg, queue_name=PKG_Q, reason="bad-payload", details={"error": str(e)})
        return

    stem       = payload["stem"]
    prefix     = payload.get("prefix") or f"{stem}/cmaf"
    duration_s = float(payload.get("duration_sec") or 0.0)

    # --- CMAF readiness on mezz -------------------------------------------------
    readiness = verify.check_cmaf_remote(
        container=MEZZ,
        conn_str=os.getenv("AzureWebJobsStorage"),
        audio_root=f"{prefix}/audio",
        video_roots={lbl: f"{prefix}/video/{lbl}" for lbl in ("240p","360p","480p","720p","1080p")},
        seg_dur=SEG_DUR,
        duration_sec=duration_s,
        ffprobe=False,
    )

    st = readiness["status"]
    if st == "not_ready":
        _requeue_packager(
            msg=msg,
            queue_name=PKG_Q,
            stem=stem,
            container=MEZZ,
            prefix=prefix,
            log=log,
            extra={"polls": payload.get("polls", 0),
                   "first_seen_ts": payload.get("first_seen_ts", int(time.time()))},
        )
        return  # success; avoid host retries

    if st == "fail":
        send_to_poison(msg, queue_name=PKG_Q, reason="cmaf-integrity-failed", details=readiness)
        return

    # --- CMAF ready → mirror mezz to local work area & package ------------------
    dist_dir = payload.get("dist_dir") or str(Path(TMP_ROOT)/stem/"dist")
    work_dir = payload.get("work_dir") or str(Path(TMP_ROOT)/stem/"work")

    Path(dist_dir).mkdir(parents=True, exist_ok=True)
    Path(work_dir).mkdir(parents=True, exist_ok=True)

    download_cmaf_tree(stem=stem, dest_work_dir=work_dir, log=log)

    # Core packaging
    try:
        _handle_packaging({**payload, "dist_dir": dist_dir, "work_dir": work_dir}, log)
        _log_job(stage="packager", job_id=stem, message="success", data={"status": "ok"})
    except Exception as e:
        _log_exception("packager", f"handler failed for {stem}: {e}")
        raise
