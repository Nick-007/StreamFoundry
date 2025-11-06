# function_app/TranscodeQueue/__init__.py
from __future__ import annotations
import json, time, threading
import logging
from pathlib import Path
from typing import List, Dict, Optional, Any, Callable

import azure.functions as func
from azure.functions import QueueMessage
from pydantic import ValidationError
from azure.core.exceptions import HttpResponseError, ResourceNotFoundError

# --- shared modules from your repo ---
from ..shared.config import AppSettings
from ..shared.logger import StreamLogger, bridge_logger, make_slogger

# transcode_queue_normalize.py (example co-located helper)
from ..shared.storage import (
    blob_exists,
    acquire_lock_with_break, renew_lock, download_blob_streaming,  # your existing lease helpers
)
from ..shared.mezz import (
    ensure_intermediates_from_mezz, upload_mezz_and_manifest, upload_intermediate_file,
)
from ..shared.qc import ffprobe_validate, CmdError
from ..shared.transcode import transcode_to_cmaf_ladder, default_ladder  # we do NOT import packaging here

from ..shared.ingest import validate_ingest, dump_normalized
from ..shared.queueing import enqueue, queue_client
from ..shared.workspace import job_paths
from ..shared.rungs import ladder_labels, mezz_video_filename, discover_renditions
from ..shared.fingerprint import (
    fingerprint_from_content_hash,
    version_for_fingerprint,
    profile_signature,
    file_content_hash,
)
from ..shared.content_index import (
    load_content_index,
    upsert_fingerprint_entry,
    register_stem_alias,
    find_matching_fingerprint,
)
from ..shared.fingerprint_index import (
    upsert_fingerprint_metadata,
    record_stem_alias,
    load_fingerprint_record,
)
from ..shared.publish import (
    build_outputs,
    upload_manifests,
    upload_prepackage_state,
)
from ..shared.status import set_raw_status
from ..shared.pipelines import select_pipeline_for_blob

settings = AppSettings()
LOGGER = logging.getLogger("transcode")

#LOGGER.info(f"RAW_CONTAINER: {settings.RAW_CONTAINER}")

# ---------- Config ----------
RAW        = settings.RAW_CONTAINER
MEZZ       = settings.MEZZ_CONTAINER
DASH       = settings.DASH_CONTAINER
HLS        = settings.HLS_CONTAINER
LOGS       = settings.LOGS_CONTAINER

TMP_DIR      = settings.TMP_DIR
LOCK_TTL     = int(settings.LOCK_TTL_SECONDS)
VISIBILITY_EXTENSION_SEC = int(settings.TRANSCODE_VISIBILITY_EXTENSION_SEC or "0")

# File size limits (similar to SubmitJob)
MAX_DOWNLOAD_MB = 2048  # 2 GB default
CHUNK_BYTES     = 4194304   # 4 MiB default

# queues
TRANSCODE_Q   = settings.TRANSCODE_QUEUE
POISON_Q      = settings.TRANSCODE_POISON_QUEUE
PACKAGING_Q   = settings.PACKAGING_QUEUE

STORAGE_CONN  = settings.AzureWebJobsStorage

from .. import app

# ---------- Helpers (restored) ----------


def _canonical_captions_list(items: Any) -> list[dict[str, str]]:
    out: list[dict[str, str]] = []
    if not items:
        return out
    for entry in items:
        if isinstance(entry, dict):
            lang = (entry.get("lang") or "").strip()
            source = str(entry.get("source") or "").strip()
            if not lang and not source:
                continue
            out.append({"lang": lang, "source": source})
    out.sort(key=lambda d: (d.get("lang", ""), d.get("source", "")))
    return out


def _ladder_for_selection(only_rung: Optional[List[str]]) -> List[Dict[str, str]]:
    ladder = default_ladder()
    if only_rung:
        allowed = {str(r).lower() for r in only_rung}
        ladder = [r for r in ladder if r["name"].lower() in allowed]
    return ladder


def _ladder_signature(ladder_cfg: Iterable[Dict[str, str]]) -> str:
    return "|".join(
        f"{r['name']}:{r['height']}:{r['bv']}:{r['maxrate']}:{r['bufsize']}"
        for r in ladder_cfg
    )


def _build_profile_components(
    *,
    only_rung: Optional[List[str]],
    captions: Any,
) -> tuple[str, List[Dict[str, str]], list[dict[str, str]], List[str], Dict[str, Any], Dict[str, Any]]:
    ladder_cfg = _ladder_for_selection(only_rung)
    canon_caps = _canonical_captions_list(captions)
    coverage = [r["name"] for r in ladder_cfg]
    profile_knobs: Dict[str, Any] = {
        "seg_dur_sec": str(settings.SEG_DUR_SEC),
        "packager_seg_dur_sec": str(settings.PACKAGER_SEG_DUR_SEC),
        "video_codec": str(settings.VIDEO_CODEC),
        "nvenc_preset": str(settings.NVENC_PRESET),
        "nvenc_rc": str(settings.NVENC_RC),
        "nvenc_lookahead": str(settings.NVENC_LOOKAHEAD),
        "nvenc_aq": str(settings.NVENC_AQ),
        "set_bt709_tags": str(settings.SET_BT709_TAGS),
        "audio_main_kbps": str(settings.AUDIO_MAIN_KBPS),
        "only_rung": ",".join(only_rung) if only_rung else "ALL",
        "ladder": _ladder_signature(ladder_cfg),
        "enable_trickplay": str(settings.ENABLE_TRICKPLAY),
        "trickplay_factor": str(settings.TRICKPLAY_FACTOR),
        "enable_captions": str(settings.ENABLE_CAPTIONS),
        "captions": json.dumps(canon_caps, sort_keys=True, separators=(",", ":")),
        "ladder_profile": str(settings.LADDER_PROFILE),
    }
    profile_sig = profile_signature(profile_knobs)
    fingerprint_knobs = {
        "profile": profile_sig,
        "coverage": coverage,
        "captions": canon_caps,
    }
    encode_config = dict(profile_knobs)
    encode_config["coverage"] = coverage
    encode_config["profileSignature"] = profile_sig
    return profile_sig, ladder_cfg, canon_caps, coverage, fingerprint_knobs, encode_config

def send_to_poison(payload: dict, reason: str, log):
    """Send failed job to poison queue with safe payload."""
    try:
        qc = queue_client(POISON_Q)
        qc.send_message(json.dumps({
            "reason": reason,
            "at": int(time.time()),
            "payload": _safe_json(payload),
        }))
        log(f"[poison] sent to {POISON_Q}: {reason}")
    except Exception as e:
        log(f"[poison] failed to send: {e}")

def enqueue_packaging(job: dict, log):
    """Enqueue a packaging job for a separate worker."""
    enqueue(PACKAGING_Q, json.dumps(job))
    log(f"[queue] enqueued packaging → {PACKAGING_Q} (id={job.get('id')}, rungs={job.get('only_rung') or 'all'})")

def _safe_json(obj: Any) -> Any:
    try:
        json.dumps(obj); return obj
    except Exception:
        if isinstance(obj, dict):
            return {str(k): _safe_json(v) for k, v in obj.items()}
        if isinstance(obj, (list, tuple, set)):
            return [_safe_json(x) for x in obj]
        try:
            return str(obj)
        except Exception:
            return f"<unserializable:{type(obj).__name__}>"

def _pull_caption(item: dict, work_dir: str, log) -> Optional[dict]:
    """
    { "container": "...", "key": "...", "lang": "en", "name": "subs_en.vtt" }
    → saves to work_dir/name and returns {"lang","path"} or None
    """
    try:
        cont = item["container"]; key = item["key"]
        lang = item.get("lang", "en")
        name = item.get("name") or Path(key).name
        out = Path(work_dir) / name
        out.parent.mkdir(parents=True, exist_ok=True)
        
        # Use streaming download for captions too (smaller size limit)
        download_blob_streaming(
            container=cont,
            blob=key,
            dest_path=str(out),
            max_mb=50,  # 50MB limit for captions (should be much smaller)
            chunk_bytes=CHUNK_BYTES,
            log=log
        )
        log(f"[captions] pulled {cont}/{key} → {out.name}")
        return {"lang": lang, "path": str(out)}
    except Exception as e:
        log(f"[captions] failed: {e}")
        return None

def _call_transcode(
    inp_path: str,
    work_dir: str,
    only_rungs: Optional[List[str]],
    *,
    on_audio_done: Optional[Callable[[str], None]] = None,
    on_rung_done: Optional[Callable[[str, str], None]] = None,
    log:Optional[callable] = None
):
    t0 = time.time()
    audio_mp4, renditions, meta = transcode_to_cmaf_ladder(
        inp_path,
        work_dir,
        only_rungs=only_rungs,
        on_audio_done=on_audio_done,
        on_rung_done=on_rung_done,
        log=log,
    )
    def _log(s: str): (log or print)(s)
    _log(f"[transcode] took={int(time.time()-t0)}s rungs={[r['name'] for r in renditions]}")
    return audio_mp4, renditions, meta

# Your existing heartbeat pattern (uses renew_lock from shared.storage)
def _start_heartbeat(lock_handle, ttl: int, stop_evt_flag: dict, log):
    if renew_lock is None:
        return None
    import threading
    interval = max(5, ttl // 2)
    def _beat():
        while not stop_evt_flag.get("stop"):
            try:
                time.sleep(interval)
                renew_lock(lock_handle, ttl=ttl)
                log("[lock] renewed")
            except Exception as e:
                log(f"[lock] renew failed: {e}")
    t = threading.Thread(target=_beat, name="lock-heartbeat", daemon=True)
    t.start()
    return t

def _extend_visibility(
    qc,
    message_id: str,
    pop_receipt: Optional[str],
    seconds: int,
    log,
) -> Optional[str]:
    """
    Extend the queue message visibility lease and return the new pop receipt.
    """
    if not pop_receipt:
        return pop_receipt
    try:
        updated = qc.update_message(
            message_id,
            pop_receipt=pop_receipt,
            visibility_timeout=seconds,
        )
        next_visible = getattr(updated, "next_visible_on", None)
        log(f"[queue] visibility extended by {seconds}s (next={next_visible})")
        return updated.pop_receipt
    except (HttpResponseError, ResourceNotFoundError) as e:
        code = getattr(e, "error_code", None)
        if code == "MessageNotFound" or "messagenotfound" in str(e).lower():
            log("[queue] visibility extend aborted: message no longer exists")
            return None
        log(f"[queue] visibility extend failed: {e}")
        return pop_receipt
    except Exception as e:
        log(f"[queue] visibility extend failed: {e}")
        return pop_receipt

def _start_visibility_heartbeat(
    qc,
    message_id: str,
    receipt_state: dict,
    seconds: int,
    stop_flag: dict,
    log,
):
    """
    Background thread that renews the queue message visibility while work continues.
    """
    if seconds <= 0:
        return None
    interval = max(5, min(seconds // 2, seconds - 30))
    if interval <= 0:
        interval = 5

    def _beat():
        while not stop_flag.get("stop"):
            time.sleep(interval)
            receipt_state["pop_receipt"] = _extend_visibility(
                qc,
                message_id,
                receipt_state.get("pop_receipt"),
                seconds,
                log,
            )
            if receipt_state.get("pop_receipt") is None:
                stop_flag["stop"] = True
                break

    t = threading.Thread(target=_beat, name="queue-visibility-heartbeat", daemon=True)
    t.start()
    return t

# ---------- v2 Queue Trigger (transcode only; packaging deferred) ----------

@app.queue_trigger(arg_name="msg", queue_name=TRANSCODE_Q, connection="AzureWebJobsStorage")
def transcode_queue(msg: QueueMessage):
    raw = msg.get_body().decode("utf-8", errors="ignore")
    if not raw:
        LOGGER.warning("Queue payload empty; skipping.")
        return

    try:
        payload = json.loads(raw)
    except Exception as e:
        LOGGER.error(f"JSON parse failed: {e} raw={raw[:256]}")
        return

    # set up streaming logs (local + blob)
    stem_for_log = payload.get("id") or "job"
    log_paths = job_paths(stem_for_log)

    sl = StreamLogger(job_id=stem_for_log, dist_dir=str(log_paths.dist_dir), container=LOGS, job_type="transcode")
    sl.start(interval_sec=20)
    log = bridge_logger(LOGGER, sl)

    slog, slog_exc = make_slogger(
        text_log=log,
        job_log=sl.job,
        ctx={"job_id":stem_for_log, "stage": "transcode_ingest"}
    )

    qc = None
    visibility_state = {"pop_receipt": msg.pop_receipt}
    visibility_stop = None
    visibility_thread = None

    try:
        if VISIBILITY_EXTENSION_SEC > 0:
            try:
                qc = queue_client(TRANSCODE_Q)
                visibility_state["pop_receipt"] = _extend_visibility(
                    qc,
                    msg.id,
                    visibility_state.get("pop_receipt"),
                    VISIBILITY_EXTENSION_SEC,
                    log,
                )
                visibility_stop = {"stop": False}
                visibility_thread = _start_visibility_heartbeat(
                    qc,
                    msg.id,
                    visibility_state,
                    VISIBILITY_EXTENSION_SEC,
                    visibility_stop,
                    log,
                )
            except Exception as e:
                log(f"[queue] initial visibility extend failed: {e}")
                qc = None

        try:
            ingest = validate_ingest(payload)
        except ValidationError as ve:
            log(f"[validate] error: {ve}")
            send_to_poison(payload, f"ValidationError: {ve}", log)
            raise

        in_cont = ingest.in_.container
        raw_key = ingest.in_.key
        stem = ingest.id
        if not stem:
            raise CmdError("Missing job id after validation")

        pipeline_id = (payload.get("extra") or {}).get("pipeline") or "transcode"
        if pipeline_id != "transcode":
            route = select_pipeline_for_blob(raw_key)
            target_queue = (route or {}).get("queue")
            if target_queue and target_queue != TRANSCODE_Q:
                enqueue(target_queue, json.dumps(payload))
                log(f"[dispatch] rerouted {raw_key} to {target_queue} (pipeline={pipeline_id})")
                try:
                    set_raw_status(
                        in_cont,
                        raw_key,
                        status="queued",
                        pipeline=pipeline_id,
                        reason="transcode_rerouted",
                    )
                except Exception:
                    pass
                sl.stop()
                return

            # Pipeline resolves to the local worker queue; continue processing here.
            if target_queue == TRANSCODE_Q:
                log(f"[dispatch] pipeline={pipeline_id} resolved to {TRANSCODE_Q}; continuing locally")
            else:
                log(f"[dispatch] pipeline={pipeline_id} no explicit queue; continuing locally")

        only_rung = ladder_labels(ingest.only_rung) if ingest.only_rung else None
        raw_captions = payload.get("captions")
        captions = raw_captions if raw_captions else [c.model_dump() for c in (ingest.captions or [])]
        extra = payload.get("extra") or ingest.extra or {}

        paths = job_paths(stem)
        inp_dir = paths.input_dir
        work_dir = paths.work_dir
        inp_path = str(inp_dir / Path(raw_key).name)

        # ensure input exists
        if not blob_exists(in_cont, raw_key):
            raise FileNotFoundError(f"Input blob not found: {in_cont}/{raw_key}")

        # acquire lock
        lock_key = f"{in_cont}/{raw_key}"
        lock = acquire_lock_with_break(lock_key, ttl=LOCK_TTL)
        if not lock:
            log(f"[lock] busy/healthy lease present; skipping key={lock_key}")
            return

        # heartbeat
        hb_stop = {"stop": False}
        _ = _start_heartbeat(lock, LOCK_TTL, hb_stop, log=log)

        # download input with streaming and size limits
        t0 = time.time()
        download_blob_streaming(
            container=in_cont,
            blob=raw_key,
            dest_path=inp_path,
            max_mb=MAX_DOWNLOAD_MB,
            chunk_bytes=CHUNK_BYTES,
            log=log
        )
        log(f"[download] input ready path={inp_path} took={int(time.time()-t0)}s")

        content_hash = file_content_hash(inp_path)
        profile_sig, ladder_cfg, canon_caps, coverage_rungs, fingerprint_knobs, encode_config = _build_profile_components(
            only_rung=only_rung,
            captions=captions,
        )
        fingerprint = fingerprint_from_content_hash(content_hash, fingerprint_knobs)
        version = version_for_fingerprint(fingerprint)
        log(f"[fingerprint] content={content_hash} profile={profile_sig} version={version}")

        content_doc = load_content_index(content_hash)
        match = find_matching_fingerprint(
            content_doc,
            profile_signature=profile_sig,
            requested_rungs=coverage_rungs,
            requested_captions=canon_caps,
        )
        if match:
            fingerprint = match["fingerprint"]
            version = version_for_fingerprint(fingerprint)
            fp_record = load_fingerprint_record(fingerprint)
            outputs = fp_record.get("outputs")
            if not outputs:
                canonical_stem = next(iter((fp_record.get("stems") or {}).keys()), stem)
                outputs = build_outputs(
                    version,
                    canonical_stem=canonical_stem,
                    dash_container=DASH,
                    hls_container=HLS,
                    dash_base_url=settings.DASH_BASE_URL,
                    hls_base_url=settings.HLS_BASE_URL,
                )
            existing_aliases = set((match.get("stems") or [])) | set((fp_record.get("stems") or {}).keys())
            existing_aliases.add(stem)
            manifest_payload = upload_manifests(
                stem=stem,
                version=version,
                fingerprint=fingerprint,
                source_hash=content_hash,
                outputs=outputs,
                renditions=match.get("coverage") or coverage_rungs,
                captions=canon_caps,
                aliases=existing_aliases,
                log=log,
            )
            record_stem_alias(
                fingerprint,
                stem=stem,
                manifest_blob=f"{stem}/manifest.json",
                requested_rungs=coverage_rungs,
                requested_captions=canon_caps,
            )
            register_stem_alias(content_hash, fingerprint, stem)
            try:
                set_raw_status(
                    in_cont,
                    raw_key,
                    status="complete",
                    pipeline=pipeline_id,
                    version=version,
                    manifest=f"{PROCESSED}/{stem}/manifest.json",
                    fingerprint=fingerprint,
                    content_hash=content_hash,
                    reason="transcode_reuse",
                )
            except Exception:
                pass
            upload_prepackage_state(
                stem=stem,
                version=version,
                fingerprint=fingerprint,
                source_hash=content_hash,
                state="reused_existing",
                renditions=manifest_payload["renditions"],
                log=log,
            )
            log("[queue] reuse success; packaging skipped")
            return

        upsert_fingerprint_entry(
            content_hash,
            fingerprint=fingerprint,
            profile_signature=profile_sig,
            coverage=coverage_rungs,
            captions=canon_caps,
            state="pending",
            stems=[stem],
        )
        register_stem_alias(content_hash, fingerprint, stem)
        try:
            set_raw_status(
                in_cont,
                raw_key,
                status="processing",
                pipeline=pipeline_id,
                fingerprint=fingerprint,
                content_hash=content_hash,
                reason="transcode_processing",
            )
        except Exception:
            pass

        # QC (tools + smoke + probe + analyze) — strict
        meta_in = ffprobe_validate(inp_path, log=log, strict=True, smoke=True)

        # captions (optional)
        t0 = time.time()
        text_tracks: List[Dict] = []
        for item in (captions or []):
            c = _pull_caption(item, str(work_dir), log)
            if c: text_tracks.append(c)
        log(f"[captions] count={len(text_tracks)} took={int(time.time()-t0)}s")

        # mezz restore first if allowed
        audio_mp4 = str(work_dir / "audio.mp4")
        labels = coverage_rungs or ladder_labels(None)
        expected = [work_dir / "audio.mp4"] + [work_dir / mezz_video_filename(lab) for lab in labels]
        missing = [p for p in expected if not Path(p).exists()]

        restored = False
        if settings.SKIP_TRANSCODE_IF_MEZZ.lower() in ("1","true","yes") and missing:
            log("[mezz] attempting restore of intermediates")
            if ensure_intermediates_from_mezz(
                stem=version,
                work_dir=str(work_dir),
                only_rung=only_rung,
                log=log,
            ):
                missing = [p for p in expected if not Path(p).exists()]
                restored = not missing
                log("[mezz] restored intermediates" if restored else "[mezz] partial restore")

        manifest_data: Optional[Dict] = None

        def _checkpoint_audio(local_path: str):
            nonlocal manifest_data
            manifest_data = upload_intermediate_file(
                stem=version,
                local_path=local_path,
                manifest=manifest_data,
                log=log,
            )

        def _checkpoint_rung(label: str, local_path: str):
            nonlocal manifest_data
            manifest_data = upload_intermediate_file(
                stem=version,
                local_path=local_path,
                manifest=manifest_data,
                log=log,
            )

        # transcode if needed
        if not restored:
            slog("transcode", "begin", rung=only_rung)
            audio_mp4, renditions, _ = _call_transcode(
                inp_path,
                str(work_dir),
                only_rung,
                on_audio_done=_checkpoint_audio,
                on_rung_done=_checkpoint_rung,
                log=log,
            )
            slog("transcode", "end", rung=only_rung)
        else:
            renditions = discover_renditions(work_dir, labels)

        # upload mezz + manifest (safe both for restored and re-encoded)
        try:
            uploaded = upload_mezz_and_manifest(
                stem=version,
                work_dir=str(work_dir),
                manifest=manifest_data,
                log=log,
            )
            log(f"[mezz] uploaded files={len(uploaded.get('files',[]))}")
        except Exception as e:
            log(f"[mezz] upload failed (continuing): {e}")

        # enqueue packaging job (separate worker will restore from mezz and package)
        pkg_msg = dump_normalized(ingest)
        pkg_msg["only_rung"] = only_rung
        pkg_msg["captions"] = captions or []
        extra_payload = dict(extra or {})
        extra_payload["fingerprint"] = fingerprint
        extra_payload["version"] = version
        extra_payload["content_hash"] = content_hash
        extra_payload["profile_signature"] = profile_sig
        extra_payload["canonical_captions"] = canon_caps
        extra_payload["ladder_signature"] = _ladder_signature(ladder_cfg)
        extra_payload["coverage"] = coverage_rungs
        extra_payload["encode_config"] = encode_config
        extra_payload["requested_rungs"] = coverage_rungs
        pkg_msg["extra"] = extra_payload
        enqueue_packaging(pkg_msg, log)

        upload_prepackage_state(
            stem=stem,
            version=version,
            fingerprint=fingerprint,
            source_hash=content_hash,
            state="queued_for_packaging",
            renditions=[r["name"] for r in renditions] if renditions else labels,
            log=log,
        )

        log("[queue] success (packaging enqueued)")

    except Exception as e:
        reason = f"{type(e).__name__}: {e}"
        slog_exc("Transcode Failed",e)
        try:
            if raw_key:
                set_raw_status(in_cont, raw_key, status="failed", pipeline=pipeline_id)
        except Exception:
            pass
        try:
            send_to_poison(payload, reason, log)
        finally:
            raise
    finally:
        if visibility_stop is not None:
            visibility_stop["stop"] = True
        if visibility_thread is not None:
            visibility_thread.join(timeout=1.0)
        sl.stop()
