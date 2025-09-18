import json, os, tempfile, hashlib, time, requests, threading
from pathlib import Path
import azure.functions as func
from ..shared.config import get
from ..shared.storage import ensure_containers, blob_client, upload_file, upload_bytes, blob_exists, acquire_lock, release_lock
# renew_lock might not exist yet in your installed shared module; import guardedly
try:
    from ..shared.storage import renew_lock
except Exception:
    renew_lock = None

from ..shared.transcode import transcode_to_cmaf_ladder, package_with_shaka_ladder, CmdError
from ..shared.logger import log_job as _log_job, log_exception as _log_exception
from ..shared.qc import ffprobe_inspect, precheck_strict
from .. import app

def _call_transcode(fn, *, input_path, work_dir, segment_duration, job_id, stem, captions):
    trials = [
        dict(input_path=input_path, workdir=work_dir, segment_duration=segment_duration,
             job_id=job_id, stem=stem, captions=captions),
        dict(input=input_path, workdir=work_dir, segdur=segment_duration,
             job_id=job_id, stem=stem, captions=captions),
        ("positional", [input_path, work_dir]),
    ]
    last_err = None
    for t in trials:
        try:
            if isinstance(t, dict): return fn(**t)
            _, args = t;           return fn(*args)
        except TypeError as e:
            last_err = e; continue
    raise last_err

def _call_package(fn, *, work_dir, dist_dir, job_id, stem):
    trials = [
        dict(workdir=work_dir, dist_dir=dist_dir, job_id=job_id, stem=stem),
        dict(workdir=work_dir, outdir=dist_dir, job_id=job_id, stem=stem),
        ("positional", [work_dir, dist_dir]),
    ]
    last_err = None
    for t in trials:
        try:
            if isinstance(t, dict): return fn(**t)
            _, args = t;           return fn(*args)
        except TypeError as e:
            last_err = e; continue
    raise last_err


# Wrappers to allow key=value kwargs without changing your logger API
def log_job(scope: str, msg: str, **kwargs):
    if kwargs:
        msg = f"{msg} | " + ", ".join(f"{k}={v}" for k, v in kwargs.items())
    return _log_job(scope, msg)

def log_exception(scope: str, msg: str, **kwargs):
    if kwargs:
        msg = f"{msg} | " + ", ".join(f"{k}={v}" for k, v in kwargs.items())
    return _log_exception(scope, msg)

RAW = get("RAW_CONTAINER", "raw-videos")
MEZZ = get("MEZZ_CONTAINER", "mezzanine")
HLS  = get("HLS_CONTAINER", "hls")
DASH = get("DASH_CONTAINER", "dash")
LOGS = get("LOGS_CONTAINER", "logs")
PROCESSED = get("PROCESSED_CONTAINER", "processed")
MAX_DEQUEUE = 5

class BadMessageError(Exception): ...
class ConfigError(Exception): ...

def _safe_json(raw: str):
    obj = json.loads(raw)
    if isinstance(obj, str):
        obj = json.loads(obj)
    if not isinstance(obj, dict):
        raise BadMessageError("Queue message must be a JSON object")
    return obj

def _normalize(p: dict):
    raw_key = p.get("raw_key") or p.get("blobName") or p.get("key") or p.get("blob")
    if not raw_key:
        raise BadMessageError("Missing required field: raw_key/blobName/key/blob")
    job_id   = p.get("job_id") or p.get("jobId") or p.get("id") or Path(raw_key).stem
    container= p.get("container") or RAW
    captions = p.get("captions") or []
    return {"raw_key": raw_key, "job_id": job_id, "container": container, "captions": captions}

def _pull_caption(item, tmpdir):
    src  = item.get("source")
    lang = (item.get("lang") or "en").lower()
    if not src: return None
    out = os.path.join(tmpdir, f"caption_{lang}.vtt")
    try:
        if src.startswith(("http://","https://")):
            r = requests.get(src, timeout=15); r.raise_for_status()
            with open(out, "wb") as f: f.write(r.content)
            return {"lang": lang, "path": out}
        if "/" in src:
            c, b = src.split("/", 1)
            bc2 = blob_client(c, b)
            with open(out, "wb") as f: f.write(bc2.download_blob().readall())
            return {"lang": lang, "path": out}
        log_job("captions", "Unrecognized caption source; skipping", source=src, lang=lang)
    except Exception as e:
        log_job("captions", f"Skip caption '{src}' ({lang}): {e}")
    return None

def _start_heartbeat(lock_handle, ttl: int, stop_evt: threading.Event):
    if renew_lock is None:
        return None  # no-op if renew_lock not available
    interval = max(5, ttl // 2)
    def _beat():
        while not stop_evt.wait(interval):
            try:
                renew_lock(lock_handle, ttl=ttl)
                _log_job("lock", f"renewed {lock_handle.get('blob','?')}")
            except Exception as e:
                _log_exception("lock", f"renew failed: {e}")
                # Allow loop to continue; the next attempt may succeed
    t = threading.Thread(target=_beat, name="lock-heartbeat", daemon=True)
    t.start()
    return t

@app.queue_trigger(arg_name="msg", queue_name=get("JOB_QUEUE","transcode-jobs"), connection="AzureWebJobsStorage")
def queue_ingestor(msg: func.QueueMessage):
    raw = msg.get_body().decode("utf-8", errors="replace")
    log_job("queue", "Dequeued", length=len(raw), sample=raw[:256])

    payload = _safe_json(raw)
    payload = _normalize(payload)

    raw_key   = payload["raw_key"]
    job_id    = payload["job_id"]
    in_cont   = payload["container"]
    captions  = payload["captions"]
    stem      = Path(raw_key).stem

    # configs visibility
    for k in ["RAW_CONTAINER","MEZZ_CONTAINER","HLS_CONTAINER","DASH_CONTAINER","LOGS_CONTAINER","PROCESSED_CONTAINER",
              "TMP_DIR","SEGMENT_DURATION","FFMPEG_PATH","SHAKA_PACKAGER_PATH","LOCK_TTL_SECONDS","LOCKS_CONTAINER"]:
        try:
            v = get(k, default=None)
            log_job("env", f"{k}={'<set>' if v not in (None,'') else '<missing>'}")
        except Exception:
            log_job("env", f"{k}=<missing>")

    if not PROCESSED:
        raise ConfigError("Missing PROCESSED_CONTAINER configuration")

    # ensure output containers exist before locking (in case lock uses blobs)
    ensure_containers([HLS, DASH, LOGS, PROCESSED])

    lock = None
    hb_stop = threading.Event()
    hb_thread = None
    try:
        if not blob_exists(in_cont, raw_key):
            raise FileNotFoundError(f"Input blob not found: {in_cont}/{raw_key}")

        # Acquire TTL lock
        ttl = int(get("LOCK_TTL_SECONDS", "60"))
        lock_container = get("LOCKS_CONTAINER", "locks")
        lock_blob_name = f"lock:{in_cont}/{raw_key}"
        try:
            bc = blob_client(lock_container, lock_blob_name)  # <-- correct signature
            # log the resolved target clearly
            log_job("lock.target",
                    container=bc.container_name,
                    blob=bc.blob_name,
                    url=bc.url)
        except Exception as e:
            # don't fail the job just because logging the target failed
            log_exception("lock.target.log_error", e)

        # keep your existing acquire_lock call, but pass ttl if your helper supports it
        try:
            lock = acquire_lock(lock_blob_name, ttl=ttl)
        except TypeError:
            # Back-compat: older acquire_lock without ttl
            lock = acquire_lock(lock_blob_name)
        if not lock:
            log_job("lock", "Busy; another worker holds the lock", key=f"{in_cont}/{raw_key}")
            return

        # Start heartbeat (if renew_lock available)
        hb_thread = _start_heartbeat(lock, ttl, hb_stop)

        # workspace
        tmp_root = get("TMP_DIR", "/tmp/ingestor")
        work_dir = os.path.join(tmp_root, stem, "work"); os.makedirs(work_dir, exist_ok=True)
        dist_dir = os.path.join(tmp_root, stem, "dist"); os.makedirs(dist_dir, exist_ok=True)
        inp_dir  = os.path.join(tmp_root, stem, "input"); os.makedirs(inp_dir,  exist_ok=True)
        inp_path = os.path.join(inp_dir, os.path.basename(raw_key))

        bc_in = blob_client(in_cont, raw_key)
        with open(inp_path, "wb") as f:
            f.write(bc_in.download_blob().readall())
        log_job("download", "input ready", path=inp_path)

        # QC
        try:
            probe = ffprobe_inspect(inp_path)
            precheck_strict(probe)
            segdur = float(get("SEGMENT_DURATION", "4"))
        except Exception as e:
            log_exception("qc", f"Precheck failed: {e}")
            raise

        # captions
        text_tracks = []
        for item in captions:
            c = _pull_caption(item, work_dir)
            if c: text_tracks.append(c)
        log_job("captions", "prepared", count=len(text_tracks))

        # transcode
        log_job("transcode", "begin", segdur=segdur)
        _call_transcode(
            transcode_to_cmaf_ladder,
            input_path=inp_path,
            work_dir=work_dir,
            segment_duration=segdur,
            job_id=job_id,
            stem=stem,
            captions=text_tracks,
        )
        log_job("transcode", "end")

        # package
        log_job("package", "begin")
        _call_package(
            package_with_shaka_ladder,
            work_dir=work_dir,
            dist_dir=dist_dir,
            job_id=job_id,
            stem=stem,
        )
        log_job("package", "end", out=dist_dir)

        # upload artifacts
        for dirpath, _, files in os.walk(dist_dir):
            for name in files:
                full = os.path.join(dirpath, name)
                rel  = os.path.relpath(full, dist_dir).replace("\\","/")
                blob = f"{stem}/{rel}"
                upload_file(HLS, blob, full)

        # manifests
        version = hashlib.sha1(str(time.time()).encode("utf-8")).hexdigest()[:8]
        manifest = {
            "id": stem,
            "version": version,
            "hls": f"{HLS}/{stem}/master.m3u8",
            "dash": f"{DASH}/{stem}/stream.mpd",
            "thumbnails": f"{HLS}/{stem}/thumbnails/thumbnails.vtt",
            "captions": [{"lang": t["lang"], "path": f"{HLS}/{stem}/" + os.path.basename(t["path"])} for t in (text_tracks or [])]
        }
        upload_bytes(PROCESSED, f"{stem}/manifest.json", json.dumps(manifest, indent=2).encode("utf-8"), "application/json")
        latest = {"version": version, "updatedAt": int(time.time())}
        upload_bytes(PROCESSED, f"{stem}/latest.json", json.dumps(latest).encode("utf-8"), "application/json")

        log_job(stem, f"QUEUE SUCCESS: Transcoded and packaged {raw_key}.")

    except CmdError as e:
        log_exception("queue", f"Command failed: {e}"); raise
    except Exception as e:
        log_exception("queue", f"Unhandled error: {e}"); raise
    finally:
        try:
            hb_stop.set()
            if lock is not None:
                try:
                    release_lock(lock)
                except TypeError:
                    # Back-compat signature differences
                    release_lock(f"lock:{in_cont}/{raw_key}")
        except Exception:
            pass