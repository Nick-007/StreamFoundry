#!/usr/bin/env python3
"""Utility to delete blobs for a container or stem using Azure dev storage"""
from __future__ import annotations

import json
from pathlib import Path
import os
from typing import Iterable
from azure.storage.blob import BlobServiceClient

LOCAL = Path(__file__).resolve().parents[2] / 'local.settings.json'

def load_connection_string() -> str:
    data = json.loads(LOCAL.read_text()) if LOCAL.exists() else {}
    values = data.get('Values', {})
    cs = values.get('AzureWebJobsStorage')
    if not cs:
        raise SystemExit('AzureWebJobsStorage missing from local.settings.json')
    return cs


def delete_blobs(container: str, prefix: str | None = None) -> None:
    cs = load_connection_string()
    svc = BlobServiceClient.from_connection_string(cs)
    client = svc.get_container_client(container)
    blobs = client.list_blobs(name_starts_with=prefix) if prefix else client.list_blobs()
    for blob in blobs:
        client.delete_blob(blob.name)
    print(f'deleted blobs from {container}/{prefix or "*"}')


def main() -> None:
    import argparse
    parser = argparse.ArgumentParser(description='Drop blobs for dash/hls/processed stems')
    parser.add_argument('--container', '-c', required=True, help='Container name')
    parser.add_argument('--prefix', '-p', help='Optional prefix to delete')
    args = parser.parse_args()
    delete_blobs(args.container, args.prefix)


if __name__ == '__main__':
    main()
