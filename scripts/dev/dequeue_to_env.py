#!/usr/bin/env python3
"""
Dequeues a single message from a queue and emits an env file for run_worker_local.sh.

Usage:
  python scripts/dev/dequeue_to_env.py \
    --queue transcode-jobs \
    --output /tmp/payload.env
  ./scripts/dev/run_worker_local.sh /tmp/payload.env

By default the script deletes the message after writing the env file. Use --keep to leave it on the queue.
"""
from __future__ import annotations

import argparse
import base64
import json
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any, Tuple

from azure.storage.queue import QueueClient
from azure.storage.blob import (
    BlobServiceClient,
    generate_blob_sas,
    generate_account_sas,
    BlobSasPermissions,
    AccountSasPermissions,
    ResourceTypes,
)

ROOT = Path(__file__).resolve().parents[2]
LOCAL_SETTINGS = ROOT / "local.settings.json"


def load_connection_string() -> str:
    cs = os.getenv("AzureWebJobsStorage")
    if cs:
        return cs
    if LOCAL_SETTINGS.exists():
        data = json.loads(LOCAL_SETTINGS.read_text())
        values = data.get("Values") or {}
        cs = values.get("AzureWebJobsStorage")
        if cs:
            return cs
    raise SystemExit("AzureWebJobsStorage not found in environment or local.settings.json")


def dequeue_one(queue_name: str, conn: str) -> Tuple[str, str]:
    qc = QueueClient.from_connection_string(conn, queue_name)
    msgs = qc.receive_messages(messages_per_page=1, visibility_timeout=300)
    try:
        msg = next(msgs)
    except StopIteration:
        raise SystemExit(f"[queue] no messages available on {queue_name}")
    return msg.id, msg.content


def decode_payload(content_b64: str) -> Dict[str, Any]:
    try:
        return json.loads(base64.b64decode(content_b64).decode("utf-8"))
    except Exception as exc:
        raise SystemExit(f"[queue] failed to decode payload: {exc}")


def build_env(payload: Dict[str, Any], conn: str) -> Dict[str, str]:
    blob_svc = BlobServiceClient.from_connection_string(conn)
    account = blob_svc.account_name
    key = blob_svc.credential.account_key
    blob_endpoint = blob_svc.primary_endpoint

    raw_container = payload["in"]["container"]
    raw_key = payload["in"]["key"]
    stem = payload.get("id") or Path(raw_key).stem

    expiry = datetime.utcnow() + timedelta(hours=4)
    raw_sas = generate_blob_sas(
        account_name=account,
        container_name=raw_container,
        blob_name=raw_key,
        account_key=key,
        permission=BlobSasPermissions(read=True),
        expiry=expiry,
    )
    account_sas = generate_account_sas(
        account_name=account,
        account_key=key,
        resource_types=ResourceTypes(service=True, container=True, object=True),
        permission=AccountSasPermissions(read=True, write=True, add=True, create=True, list=True),
        expiry=expiry,
    )

    base_url = blob_endpoint.rstrip("/")
    extra = payload.get("extra") or {}
    env = {
        "RAW_URL": f"{base_url}/{raw_container}/{raw_key}?{raw_sas}",
        "DEST_BASE": base_url,
        "DEST_SAS": f"?{account_sas}",
        "STEM": stem,
        "SEG_DUR_SEC": str(extra.get("seg_dur_sec") or payload.get("seg_dur_sec") or 4),
        "AUDIO_MAIN_KBPS": str(extra.get("audio_main_kbps") or 128),
        "VIDEO_CODEC": extra.get("video_codec") or "h264_nvenc",
        "NVENC_PRESET": extra.get("nvenc_preset") or "p5",
        "NVENC_RC": extra.get("nvenc_rc") or "vbr_hq",
        "NVENC_LOOKAHEAD": str(extra.get("nvenc_lookahead") or 32),
        "NVENC_AQ": str(extra.get("nvenc_aq") or 1),
        "BT709_TAGS": str(extra.get("set_bt709_tags") or "true"),
        "ENABLE_CAPTIONS": str(extra.get("enable_captions") or True),
        "ENABLE_TRICKPLAY": str(extra.get("enable_trickplay") or True),
        "TRICKPLAY_FACTOR": str(extra.get("trickplay_factor") or 4),
        "THUMB_INTERVAL_SEC": str(extra.get("thumb_interval_sec") or 4),
    }
    return env


def write_env_file(env: Dict[str, str], path: Path) -> None:
    lines = [f"{k}={v}" for k, v in env.items()]
    path.write_text("\n".join(lines))


def main() -> None:
    parser = argparse.ArgumentParser(description="Dequeue one message and emit an env file for the transcode runner.")
    parser.add_argument("--queue", default="transcode-jobs", help="Queue name to dequeue from (default: transcode-jobs)")
    parser.add_argument("--output", default="/tmp/payload.env", help="Env file path to write")
    parser.add_argument("--keep", action="store_true", help="Keep the message on the queue (do not delete)")
    args = parser.parse_args()

    conn = load_connection_string()
    msg_id, content = dequeue_one(args.queue, conn)
    payload = decode_payload(content)
    env = build_env(payload, conn)

    out_path = Path(args.output)
    write_env_file(env, out_path)
    print(f"[env] wrote {out_path}")

    if not args.keep:
        qc = QueueClient.from_connection_string(conn, args.queue)
        qc.delete_message(msg_id, content=None)
        print(f"[queue] deleted message {msg_id}")
    else:
        print(f"[queue] kept message {msg_id}")


if __name__ == "__main__":
    main()
