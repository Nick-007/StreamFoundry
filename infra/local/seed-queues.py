#!/usr/bin/env python3
import json, sys
from pathlib import Path
from typing import List, Tuple
from azure.core.exceptions import ResourceExistsError
from azure.storage.queue import QueueServiceClient

ROOT = Path(__file__).resolve().parents[2]  # repo root
settings_path = ROOT / "local.settings.json"

def load_values() -> dict:
    try:
        cfg = json.loads(settings_path.read_text())
        return cfg.get("Values", {})
    except Exception as e:
        print(f"[seed-queues] ERROR: cannot read {settings_path}: {e}", file=sys.stderr)
        sys.exit(1)

def discover_queue_names(values: dict) -> List[str]:
    names = []
    for k, v in values.items():
        if not isinstance(v, str): 
            continue
        if k.lower().startswith("azurewebjobs"):
            # ignore internal artifacts config
            continue
        if k.endswith("_QUEUE"):
            name = v.strip()
            if not name:
                continue
            # ignore internal azure-webjobs host queues if someone put them here
            if name.startswith("azure-webjobs-"):
                continue
            names.append(name)
    # add poison queues (unique-ified)
    poison = [f"{n}-poison" for n in names if not n.endswith("-poison")]
    return sorted(set(names + poison))

def create_queues(conn: str, queue_names: List[str]) -> Tuple[List[str], List[str]]:
    svc = QueueServiceClient.from_connection_string(conn)
    created, existed = [], []
    for name in queue_names:
        try:
            svc.create_queue(name)
            created.append(name)
        except ResourceExistsError:
            existed.append(name)
    return created, existed

def main():
    values = load_values()
    conn = values.get("AzureWebJobsStorage")
    if not conn:
        print("[seed-queues] ERROR: AzureWebJobsStorage missing in local.settings.json:Values", file=sys.stderr)
        sys.exit(2)

    queue_names = discover_queue_names(values)
    if not queue_names:
        print("[seed-queues] no *_QUEUE entries found in local.settings.json — nothing to do.")
        return

    created, existed = create_queues(conn, queue_names)
    if created:
        print("[seed-queues] created:", ", ".join(created))
    if existed:
        print("[seed-queues] already existed:", ", ".join(existed))

if __name__ == "__main__":
    main()
