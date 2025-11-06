#!/usr/bin/env python3
import json, sys, io
from pathlib import Path
from datetime import datetime, timezone

from azure.storage.queue import QueueServiceClient
from azure.storage.blob import BlobServiceClient

# ---- config (ONLY from local.settings.json) ----
SETTINGS_PATH = Path("local.settings.json")
try:
    cfg = json.loads(SETTINGS_PATH.read_text())
    CONN = cfg["Values"]["AzureWebJobsStorage"]
except Exception as e:
    print(f"[ERR] cannot read AzureWebJobsStorage from {SETTINGS_PATH}: {e}")
    sys.exit(2)

# Known/real resources in your app:
QUEUE_NAME = "transcode-jobs"
# List your BlobTrigger containers here; add more if your app uses them.
# (We saw 'raw' in your logs; do not include any azure-webjobs-* internals.)
BLOBTRIGGER_CONTAINERS = ["raw"]

# Seed blob name + content
SEED_BLOB_NAME = "__seed__.keep"
SEED_CONTENT = b"# seed blob to force BlobTrigger registration\n"
SEED_METADATA = {"seed": "true", "created": datetime.now(timezone.utc).isoformat()}

def ensure_queue():
    qs = QueueServiceClient.from_connection_string(CONN)
    q = qs.get_queue_client(QUEUE_NAME)
    try:
        q.get_queue_properties()
        print(f"[ok] queue exists: {QUEUE_NAME}")
    except Exception:
        print(f"[i] creating queue: {QUEUE_NAME}")
        q.create_queue()
        print(f"[ok] created queue: {QUEUE_NAME}")

def ensure_blobtrigger_container(name: str):
    bs = BlobServiceClient.from_connection_string(CONN)
    cc = bs.get_container_client(name)
    # Create container if missing (idempotent)
    try:
        cc.get_container_properties()
        print(f"[ok] container exists: {name}")
    except Exception:
        print(f"[i] creating container: {name}")
        cc.create_container()
        print(f"[ok] created container: {name}")

    # If container is empty, upload a seed blob to kick BlobTrigger registration
    has_any = False
    try:
        for _ in cc.list_blobs(name_starts_with=""):
            has_any = True
            break
    except Exception as e:
        print(f"[WARN] list_blobs failed on {name}: {e}")

    if not has_any:
        print(f"[i] {name} is empty -> uploading seed blob: {SEED_BLOB_NAME}")
        bc = cc.get_blob_client(SEED_BLOB_NAME)
        # Upload is idempotent: if already present, just leave it
        try:
            if not bc.exists():
                bc.upload_blob(io.BytesIO(SEED_CONTENT), overwrite=False, metadata=SEED_METADATA)
                print(f"[ok] uploaded seed blob {SEED_BLOB_NAME} in {name}")
            else:
                print(f"[ok] seed blob already present in {name}")
        except Exception as e:
            print(f"[ERR] failed to upload seed blob to {name}: {e}")

if __name__ == "__main__":
    print("[seed] using AzureWebJobsStorage from local.settings.json")
    ensure_queue()
    for cname in BLOBTRIGGER_CONTAINERS:
        ensure_blobtrigger_container(cname)
    print("[seed] done.")
