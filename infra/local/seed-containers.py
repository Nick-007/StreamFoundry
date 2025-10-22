#!/usr/bin/env python3
import json, sys
from pathlib import Path
from typing import List, Tuple
from azure.core.exceptions import ResourceExistsError
from azure.storage.blob import BlobServiceClient

ROOT = Path(__file__).resolve().parents[2]  # repo root
settings_path = ROOT / "local.settings.json"

def load_values() -> dict:
    try:
        cfg = json.loads(settings_path.read_text())
        return cfg.get("Values", {})
    except Exception as e:
        print(f"[seed-containers] ERROR: cannot read {settings_path}: {e}", file=sys.stderr)
        sys.exit(1)

def discover_container_names(values: dict) -> List[str]:
    names = []
    for k, v in values.items():
        if not isinstance(v, str): 
            continue
        if k.lower().startswith("azurewebjobs"):
            # ignore internal artifacts config
            continue
        if k.endswith("_CONTAINER"):
            name = v.strip()
            if name:
                names.append(name)
    return sorted(set(names))

def create_containers(conn: str, container_names: List[str]) -> Tuple[List[str], List[str]]:
    svc = BlobServiceClient.from_connection_string(conn)
    created, existed = [], []
    for name in container_names:
        try:
            svc.create_container(name)
            created.append(name)
        except ResourceExistsError:
            existed.append(name)
    return created, existed

def main():
    values = load_values()
    conn = values.get("AzureWebJobsStorage")
    if not conn:
        print("[seed-containers] ERROR: AzureWebJobsStorage missing in local.settings.json:Values", file=sys.stderr)
        sys.exit(2)

    container_names = discover_container_names(values)
    if not container_names:
        print("[seed-containers] no *_CONTAINER entries found in local.settings.json — nothing to do.")
        return

    created, existed = create_containers(conn, container_names)
    if created:
        print("[seed-containers] created:", ", ".join(created))
    if existed:
        print("[seed-containers] already existed:", ", ".join(existed))

if __name__ == "__main__":
    main()
