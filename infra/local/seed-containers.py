#!/usr/bin/env python3
import json, os, sys
from pathlib import Path
from azure.core.exceptions import ResourceExistsError
from azure.storage.blob import BlobServiceClient

ROOT = Path(__file__).resolve().parents[2]  # repo root
settings_path = ROOT / "local.settings.json"

try:
    cfg = json.loads(settings_path.read_text())
    values = cfg.get("Values", {})
except Exception as e:
    print(f"[seed-containers] ERROR: cannot read {settings_path}: {e}", file=sys.stderr)
    sys.exit(1)

conn = values.get("AzureWebJobsStorage") or os.environ.get("AzureWebJobsStorage")
if not conn:
    print("[seed-containers] ERROR: AzureWebJobsStorage missing in local.settings.json (Values).", file=sys.stderr)
    sys.exit(2)

# Collect container names ONLY from explicit *_CONTAINER keys
container_names = []
for k, v in values.items():
    if k.upper().endswith("_CONTAINER") and isinstance(v, str) and v.strip():
        container_names.append(v.strip())

if not container_names:
    print("[seed-containers] No *_CONTAINER keys found in local.settings.json. Nothing to do.")
    sys.exit(0)

svc = BlobServiceClient.from_connection_string(conn)

created, existed = [], []
for name in container_names:
    try:
        svc.create_container(name)
        created.append(name)
    except ResourceExistsError:
        existed.append(name)

if created:
    print("[seed-containers] created:", ", ".join(created))
if existed:
    print("[seed-containers] already existed:", ", ".join(existed))