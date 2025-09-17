import json, os, tempfile, hashlib, time, requests
from pathlib import Path
import azure.functions as func
from ..shared.config import get
from ..shared.storage import ensure_containers, blob_client, upload_file, upload_bytes, blob_exists, acquire_lock, release_lock
from ..shared.transcode import transcode_to_cmaf_ladder, package_with_shaka_ladder, CmdError
from ..shared.logger import log_job, log_exception
from ..shared.qc import ffprobe_inspect, precheck_strict
from .. import app
RAW = get("RAW_CONTAINER", "raw-videos")
MEZZ = get("MEZZ_CONTAINER", "mezzanine")
HLS  = get("HLS_CONTAINER", "hls")
DASH = get("DASH_CONTAINER", "dash")
LOGS = get("LOGS_CONTAINER", "logs")
PROCESSED = get("PROCESSED_CONTAINER", "processed")
MAX_DEQUEUE = 5
def _ttl_for(blob_name: str):
    if blob_name.endswith(".mpd") or blob_name.endswith(".m3u8"): return "public, max-age=60"
    if blob_name.endswith(".m4s") or blob_name.endswith(".mp4"): return "public, max-age=43200"
    if blob_name.endswith(".vtt") or blob_name.endswith(".jpg") or blob_name.endswith(".webp"): return "public, max-age=43200"
    return None
def _pull_caption(item, tmpdir, svc):
    src = item.get("source"); lang = (item.get("lang") or "en").lower(); out = os.path.join(tmpdir, f"caption_{lang}.vtt")
    if not src: return None
    if src.startswith("http://") or src.startswith("https://"):
        r = requests.get(src, timeout=30); r.raise_for_status()
        with open(out, "wb") as f: f.write(r.content); return {"lang": lang, "path": out}
    if "/" in src:
        c,b = src.split("/",1); bc2 = svc.get_blob_client(container=c, blob=b)
        with open(out, "wb") as f: f.write(bc2.download_blob().readall()); return {"lang": lang, "path": out}
    return None
@app.queue_trigger(arg_name="msg", queue_name=get("JOB_QUEUE","transcode-jobs"), connection="AzureWebJobsStorage")
def queue_ingestor(msg: func.QueueMessage):
    if msg.dequeue_count and msg.dequeue_count > MAX_DEQUEUE:
        try: body = msg.get_body().decode("utf-8")
        except Exception: body = "<unreadable>"
        log_job("poison", f"POISON message exceeded retries: {body}"); return
    lock = None
    try:
        ensure_containers([RAW, MEZZ, HLS, DASH, LOGS, PROCESSED])
        payload = json.loads(msg.get_body().decode("utf-8"))
        raw_key = payload["raw_key"]; job_id = payload.get("job_id") or Path(raw_key).stem; stem = Path(raw_key).stem
        captions = payload.get("captions") or []
        if blob_exists(PROCESSED, f"{stem}/manifest.json"):
            log_job(stem, f"QUEUE SKIP: Manifest already exists for {stem}."); return
        try: lock = acquire_lock(PROCESSED, f"{stem}/_lock", lease_duration=60)
        except Exception:
            log_job(stem, "QUEUE DEFER: Could not acquire lock; likely in progress."); raise
        svc = blob_client(); bc = svc.get_blob_client(container=RAW, blob=raw_key)
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            stream = bc.download_blob(); h = hashlib.sha256()
            for chunk in stream.chunks(): tmp.write(chunk); h.update(chunk)
            in_path = tmp.name
        h.update(json.dumps({"ladder":"efficient","seg":int(get("SEG_DUR_SEC","4")),"codec":get("VIDEO_CODEC","h264_nvenc"),"preset":get("NVENC_PRESET","p5"),"rc":get("NVENC_RC","vbr_hq")}, sort_keys=True).encode("utf-8"))
        fingerprint = h.hexdigest(); version = f"v_{fingerprint}"
        meta = ffprobe_inspect(in_path); fps,_,_ = precheck_strict(meta)
        with tempfile.TemporaryDirectory() as td:
            mezz_dir = os.path.join(td, "mezz")
            a_mp4, renditions, m = transcode_to_cmaf_ladder(in_path, mezz_dir)
            upload_file(MEZZ, f"{version}/{stem}/audio.mp4", a_mp4, "audio/mp4", _ttl_for("audio.mp4"))
            for r in renditions:
                suf = r["name"].split("p")[0]
                upload_file(MEZZ, f"{version}/{stem}/video_{suf}.mp4", r["video"], "video/mp4", _ttl_for("video.mp4"))
            out_dash = os.path.join(td, "dash"); out_hls = os.path.join(td, "hls")
            text_tracks = []
            if captions:
                for it in captions:
                    got = _pull_caption(it, td, svc)
                    if got: text_tracks.append(got)
            package_with_shaka_ladder(renditions, a_mp4, out_dash, out_hls, text_tracks=text_tracks)
            for p in Path(out_dash).glob("**/*"):
                if p.is_file():
                    rel = p.relative_to(out_dash).as_posix()
                    if rel.endswith(".mpd"): ctype = "application/dash+xml"
                    elif rel.endswith(".m4s"): ctype = "video/iso.segment"
                    elif rel.endswith(".mp4"): ctype = "video/mp4"
                    else: ctype = None
                    upload_file(DASH, f"{version}/{stem}/{rel}", str(p), ctype, _ttl_for(rel))
            for p in Path(out_hls).glob("**/*"):
                if p.is_file():
                    rel = p.relative_to(out_hls).as_posix()
                    if rel.endswith(".m3u8"): ctype = "application/vnd.apple.mpegurl"
                    elif rel.endswith(".m4s"): ctype = "video/iso.segment"
                    elif rel.endswith(".mp4"): ctype = "video/mp4"
                    else: ctype = None
                    upload_file(HLS, f"{version}/{stem}/{rel}", str(p), ctype, _ttl_for(rel))
            # Thumbnails + VTT
            if str(get("ENABLE_TRICKPLAY","true")).lower() in ("1","true","yes","on"):
                try: interval = int(str(get("THUMB_INTERVAL_SEC","4")))
                except Exception: interval = 4
                thumbs_dir = os.path.join(td, "thumbs"); os.makedirs(thumbs_dir, exist_ok=True)
                tn_cmd = f'ffmpeg -y -i "{in_path}" -vf "fps=1/{interval},scale=-2:180" {thumbs_dir}/thumb_%06d.jpg'
                import subprocess, shlex
                sp = subprocess.Popen(shlex.split(tn_cmd), stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True); sp.communicate()
                vtt_lines = ["WEBVTT\n"]; tcur = 0; idx = 1
                while True:
                    fn = os.path.join(thumbs_dir, f"thumb_{idx:06d}.jpg")
                    if not os.path.exists(fn): break
                    start = tcur; end = tcur + interval
                    vtt_lines.append(f"00:{start//60:02d}:{start%60:02d}.000 --> 00:{end//60:02d}:{end%60:02d}.000")
                    vtt_lines.append(f"thumb_{idx:06d}.jpg\n"); tcur += interval; idx += 1
                vtt_path = os.path.join(thumbs_dir, "thumbnails.vtt"); open(vtt_path,"w").write("\n".join(vtt_lines))
                for root,_,files in os.walk(thumbs_dir):
                    for fn in files:
                        pth = os.path.join(root, fn); rel = os.path.basename(pth)
                        ct = "text/vtt" if rel.endswith(".vtt") else "image/jpeg"
                        upload_file(HLS, f"{version}/{stem}/thumbnails/{rel}", pth, ct, _ttl_for(rel))
            manifest = {
                "jobId": job_id,
                "version": version,
                "input": f"{RAW}/{raw_key}",
                "fps": m.get("fps"),
                "mezzanine": {
                    "audio": f"{MEZZ}/{version}/{stem}/audio.mp4",
                    "videos": [
                        f"{MEZZ}/{version}/{stem}/video_240.mp4",
                        f"{MEZZ}/{version}/{stem}/video_360.mp4",
                        f"{MEZZ}/{version}/{stem}/video_480.mp4",
                        f"{MEZZ}/{version}/{stem}/video_720.mp4",
                        f"{MEZZ}/{version}/{stem}/video_1080.mp4"
                    ]
                },
                "dash": f"{DASH}/{version}/{stem}/stream.mpd",
                "hls":  f"{HLS}/{version}/{stem}/master.m3u8",
                "thumbnails": f"{HLS}/{version}/{stem}/thumbnails/thumbnails.vtt",
                "captions": [{"lang": t["lang"], "path": f"{HLS}/{version}/{stem}/" + os.path.basename(t["path"])} for t in (text_tracks or [])]
            }
            upload_bytes(PROCESSED, f"{stem}/manifest.json", json.dumps(manifest, indent=2).encode("utf-8"), "application/json")
            latest = {"version": version, "updatedAt": int(time.time())}
            upload_bytes(PROCESSED, f"{stem}/latest.json", json.dumps(latest).encode("utf-8"), "application/json")
            log_job(stem, f"QUEUE SUCCESS: Transcoded and packaged {raw_key}.")
    except CmdError as e:
        log_exception("queue", e); raise
    except Exception as e:
        log_exception("queue", e); raise
    finally:
        try:
            if lock is not None: release_lock(lock)
        except Exception: pass
