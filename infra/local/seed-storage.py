#!/usr/bin/env python3
"""
Idempotent seeding for local Storage **queues** (no blob containers).
- Safe to re-run: existing queues are skipped.
- Defaults to queue: transcode-jobs
- Connection string sources (first found wins):
    1) env AzureWebJobsStorage
    2) env AZURE_STORAGE_CONNECTION_STRING
    3) ./local.settings.json -> Values.AzureWebJobsStorage
Usage:
    python infra/local/seed-storage.py
    python infra/local/seed-storage.py --queue transcode-jobs another-queue
"""

import argparse
import json
import os
import sys
from pathlib import Path

try:
    from azure.core.exceptions import ResourceExistsError
    from azure.storage.queue import QueueClient
except Exception as e:
    print("ERROR: Azure SDK not found. Install deps first:\n"
          "  python -m pip install azure-storage-queue azure-core",
          file=sys.stderr)
    raise

DEFAULT_QUEUES = ["transcode-jobs"]

def load_conn_from_local_settings(path: Path) -> str | None:
    if not path.exists():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return data.get("Values", {}).get("AzureWebJobsStorage")
    except Exception as e:
        print(f"WARNING: Failed to parse {path}: {e}", file=sys.stderr)
        return None

def resolve_connection_string(explicit: str | None = None) -> str:
    # explicit arg overrides everything
    if explicit:
        return explicit

    # env vars
    env_conn = os.getenv("AzureWebJobsStorage") or os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    if env_conn:
        return env_conn

    # local.settings.json in CWD
    cwd_settings = Path("local.settings.json")
    conn = load_conn_from_local_settings(cwd_settings)
    if conn:
        return conn

    # try project root if script is run from nested dir
    here = Path(__file__).resolve()
    candidate = here.parents[2] / "local.settings.json" if len(here.parents) >= 2 else None
    if candidate and candidate.exists():
        conn = load_conn_from_local_settings(candidate)
        if conn:
            return conn

    raise SystemExit(
        "Could not resolve Azure Storage connection string.\n"
        "Set env AzureWebJobsStorage (or AZURE_STORAGE_CONNECTION_STRING), "
        "or ensure local.settings.json is present with Values.AzureWebJobsStorage."
    )

def ensure_queue(conn_str: str, name: str) -> None:
    qc = QueueClient.from_connection_string(conn_str, name)
    try:
        created = qc.create_queue()
        print(f"[seed] created queue: {name}" if created else f"[seed] queue already exists: {name}")
    except ResourceExistsError:
        print(f"[seed] queue already exists: {name}")
    except Exception as e:
        # Don’t fail the whole script for one queue; report and continue
        print(f"[seed] ERROR creating queue '{name}': {e}", file=sys.stderr)

def main():
    ap = argparse.ArgumentParser(description="Idempotent seeding for Azure Storage queues (local/staging).")
    ap.add_argument("--connection-string", "-c", help="Explicit Azure Storage connection string.")
    ap.add_argument("--queue", "-q", action="append", default=[],
                    help="Queue name to create (repeatable). Default: transcode-jobs")
    args = ap.parse_args()

    conn = resolve_connection_string(args.connection_string)

    # Build final queue list (unique, preserve order)
    queues = args.queue if args.queue else DEFAULT_QUEUES
    seen = set()
    ordered = [q for q in queues if not (q in seen or seen.add(q))]

    print(f"[seed] using connection: {'(from --connection-string)' if args.connection_string else 'auto-resolved'}")
    for q in ordered:
        ensure_queue(conn, q)

    print("[seed] done.")

if __name__ == "__main__":
    main()