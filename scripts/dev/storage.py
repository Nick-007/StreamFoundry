#!/usr/bin/env python3
"""
Helpers for common Azure Storage (Blob) tasks:
  - List containers and current public access
  - Toggle container public access (off/blob/container)
  - Generate SAS tokens for containers or blobs

Examples:
  python scripts/dev/storage.py containers
  python scripts/dev/storage.py set-access raw --level container
  python scripts/dev/storage.py sas raw --permissions rl --minutes 120
  python scripts/dev/storage.py sas raw --blob demo.mp4 --permissions r --minutes 15 --full-url
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import sys
from pathlib import Path
from typing import Dict, Optional, Tuple
from urllib.parse import urljoin

from azure.storage.blob import (
    BlobClient,
    BlobServiceClient,
    PublicAccess,
    ContainerSasPermissions,
    BlobSasPermissions,
    generate_container_sas,
    generate_blob_sas,
    CorsRule,
)

ROOT = Path(__file__).resolve().parents[2]
LOCAL_SETTINGS = ROOT / "local.settings.json"


def load_connection_string() -> str:
    conn = os.getenv("AzureWebJobsStorage")
    if conn:
        return conn
    if LOCAL_SETTINGS.exists():
        try:
            data = json.loads(LOCAL_SETTINGS.read_text())
            values = data.get("Values") or {}
            conn = values.get("AzureWebJobsStorage")
            if conn:
                return conn
        except Exception as exc:
            print(f"[storage] WARN: failed to read {LOCAL_SETTINGS}: {exc}", file=sys.stderr)
    raise SystemExit("AzureWebJobsStorage not found in environment or local.settings.json")


def parse_connection_string(conn: str) -> Tuple[str, str, Optional[str]]:
    parts = dict(
        item.split("=", 1)
        for item in conn.split(";")
        if item and "=" in item
    )
    try:
        account = parts["AccountName"]
        key = parts["AccountKey"]
    except KeyError as exc:
        raise SystemExit(f"Connection string missing {exc.args[0]}") from exc
    endpoint = parts.get("BlobEndpoint")
    return account, key, endpoint


def load_service() -> BlobServiceClient:
    conn = load_connection_string()
    return BlobServiceClient.from_connection_string(conn)


def _sanitize_metadata(data: Dict[str, str]) -> Dict[str, str]:
    """Ensure metadata keys/values are strings and lower-case the keys for Azure."""
    sanitized: Dict[str, str] = {}
    for key, value in data.items():
        if value is None:
            continue
        sanitized[key.replace(" ", "-").lower()] = str(value)
    return sanitized


def set_blob_metadata(container: str, blob: str, metadata: Dict[str, str], *, merge: bool = False) -> None:
    """Set or merge metadata for a blob."""
    svc = load_service()
    client: BlobClient = svc.get_blob_client(container=container, blob=blob)
    new_metadata = _sanitize_metadata(metadata)
    if merge:
        props = client.get_blob_properties()
        current = dict(props.metadata or {})
        current.update(new_metadata)
        new_metadata = _sanitize_metadata(current)
    client.set_blob_metadata(metadata=new_metadata)


def list_blobs(container: str, prefix: Optional[str] = None) -> None:
    """List blobs in a container, optionally filtered by prefix."""
    svc = load_service()
    client = svc.get_container_client(container)
    blobs = client.list_blobs(name_starts_with=prefix) if prefix else client.list_blobs()
    for blob in blobs:
        print(blob.name)


def delete_blobs(container: str, prefix: Optional[str] = None) -> None:
    """Delete blobs in a container, optionally filtered by prefix."""
    svc = load_service()
    client = svc.get_container_client(container)
    blobs = client.list_blobs(name_starts_with=prefix) if prefix else client.list_blobs()
    for blob in blobs:
        client.delete_blob(blob.name)
    print(f"[storage] deleted blobs from {container}/{prefix or '*'}")


def cmd_containers(_: argparse.Namespace) -> None:
    svc = load_service()
    print("[storage] Containers and public access:")
    for container in svc.list_containers(include_metadata=False):
        name = container["name"]
        client = svc.get_container_client(name)
        try:
            props = client.get_container_properties()
            access = props.public_access or "off"
        except Exception as exc:
            access = f"error: {exc}"
        print(f"  - {name}: {access}")


def cmd_set_access(args: argparse.Namespace) -> None:
    level = args.level
    if level == "off":
        public_access = None
    elif level == "blob":
        public_access = PublicAccess.Blob
    else:
        public_access = PublicAccess.Container

    svc = load_service()
    client = svc.get_container_client(args.container)
    client.set_container_access_policy(signed_identifiers={}, public_access=public_access)
    print(f"[storage] {args.container}: public access set to {level}")


def cmd_sas(args: argparse.Namespace) -> None:
    conn = load_connection_string()
    account, key, default_endpoint = parse_connection_string(conn)
    svc = BlobServiceClient.from_connection_string(conn)

    expires = dt.datetime.utcnow() + dt.timedelta(minutes=args.minutes)
    permissions = args.permissions or ("rl" if args.blob else "rl")

    if args.blob:
        sas = generate_blob_sas(
            account_name=account,
            container_name=args.container,
            blob_name=args.blob,
            account_key=key,
            permission=BlobSasPermissions.from_string(permissions),
            expiry=expires,
        )
        resource_path = f"{args.container}/{args.blob}"
    else:
        sas = generate_container_sas(
            account_name=account,
            container_name=args.container,
            account_key=key,
            permission=ContainerSasPermissions.from_string(permissions),
            expiry=expires,
        )
        resource_path = args.container

    if args.full_url:
        base_url = default_endpoint or svc.url
        url = urljoin(base_url.rstrip("/") + "/", resource_path) + f"?{sas}"
        print(url)
    else:
        print(sas)
    print(f"[storage] expires at {expires:%Y-%m-%d %H:%M:%S}Z, permissions={permissions}")


def cmd_cors(args: argparse.Namespace) -> None:
    svc = load_service()
    props = svc.get_service_properties()
    cors_rules = props.get("cors") or []
    if args.reset:
        props["cors"] = []
        svc.set_service_properties(**props)
        print("[storage] cleared CORS rules")
        return

    if args.allowed_origins or args.allowed_methods:
        def _normalize(value: Optional[str], default: str) -> str:
            parts = [segment.strip() for segment in (value or default).split(",")]
            return ",".join(part for part in parts if part)

        rule = CorsRule(
            allowed_origins=_normalize(args.allowed_origins, "*").split(","),
            allowed_methods=_normalize(args.allowed_methods, "GET,HEAD").split(","),
            allowed_headers=_normalize(args.allowed_headers, "*").split(","),
            exposed_headers=_normalize(args.exposed_headers, "*").split(","),
            max_age_in_seconds=args.max_age,
        )
        props["cors"] = [rule]
        svc.set_service_properties(**props)
        print("[storage] applied CORS rule:")
        print(json.dumps({
            "allowed_origins": rule.allowed_origins,
            "allowed_methods": rule.allowed_methods,
            "allowed_headers": rule.allowed_headers,
            "exposed_headers": rule.exposed_headers,
            "max_age_in_seconds": rule.max_age_in_seconds,
        }, indent=2))
    else:
        print("[storage] current CORS rules:")
        if not cors_rules:
            print("  (none)")
        for idx, rule in enumerate(cors_rules, 1):
            data = {
                "allowed_origins": rule.allowed_origins,
                "allowed_methods": rule.allowed_methods,
                "allowed_headers": rule.allowed_headers,
                "exposed_headers": rule.exposed_headers,
                "max_age_in_seconds": rule.max_age_in_seconds,
            }
            print(f"  [{idx}] {json.dumps(data)}")


def cmd_blobs(args: argparse.Namespace) -> None:
    if args.delete:
        delete_blobs(args.container, args.prefix)
    else:
        list_blobs(args.container, args.prefix)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Azure Storage (Blob) utilities for StreamFoundry.")
    sub = parser.add_subparsers(dest="command")

    p_cont = sub.add_parser("containers", help="List containers and public access level")
    p_cont.set_defaults(func=cmd_containers)

    p_set = sub.add_parser("set-access", help="Set container public access level")
    p_set.add_argument("container", help="Container name")
    p_set.add_argument("--level", choices=("off", "blob", "container"), required=True,
                       help="Public access level to apply")
    p_set.set_defaults(func=cmd_set_access)

    p_sas = sub.add_parser("sas", help="Generate a SAS token (container or blob)")
    p_sas.add_argument("container", help="Container name")
    p_sas.add_argument("--blob", help="Blob name (optional)")
    p_sas.add_argument("--permissions", help="Permission string (e.g. rl, rwdl). Defaults to rl.")
    p_sas.add_argument("--minutes", type=int, default=60, help="Lifetime in minutes (default: 60)")
    p_sas.add_argument("--full-url", action="store_true", help="Print full URL instead of token only")
    p_sas.set_defaults(func=cmd_sas)

    p_cors = sub.add_parser("cors", help="View or set blob service CORS rules")
    p_cors.add_argument("--allowed-origins", help="Comma-separated origins (default: '*')")
    p_cors.add_argument("--allowed-methods", help="Comma-separated methods (default: GET,HEAD)")
    p_cors.add_argument("--allowed-headers", help="Comma-separated allowed headers (default: '*')")
    p_cors.add_argument("--exposed-headers", help="Comma-separated exposed headers (default: '*')")
    p_cors.add_argument("--max-age", type=int, default=3600, help="Max age in seconds (default: 3600)")
    p_cors.add_argument("--reset", action="store_true", help="Remove all CORS rules")
    p_cors.set_defaults(func=cmd_cors)

    p_blobs = sub.add_parser("blobs", help="List or delete blobs in a container")
    p_blobs.add_argument("container", help="Container name")
    p_blobs.add_argument("--prefix", help="Optional prefix filter")
    p_blobs.add_argument("--delete", action="store_true", help="Delete matching blobs instead of listing")
    p_blobs.set_defaults(func=cmd_blobs)

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    if not getattr(args, "command", None):
        parser.print_help()
        sys.exit(1)
    args.func(args)


if __name__ == "__main__":
    main()
