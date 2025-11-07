#!/usr/bin/env python3
"""
sfinspect — quick CLI to inspect StreamFoundry pipeline artifacts.

Examples:
  ./scripts/sfinspect.py manifest SubmitJob/demo
  ./scripts/sfinspect.py fingerprint 5f6e7a8b...
  ./scripts/sfinspect.py hash 8f6d...
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any, Dict

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from function_app.shared.storage import download_bytes  # noqa: E402
from function_app.shared.status import get_raw_status  # noqa: E402

DEFAULT_PROCESSED = "processed"
DEFAULT_FINGERPRINT_PREFIX = "fingerprints"
DEFAULT_HASH_PREFIX = "_hash"


def _pretty(obj: Dict[str, Any]) -> str:
    return json.dumps(obj, indent=2, sort_keys=True)


def _load_json(container: str, blob: str) -> Dict[str, Any]:
    data = download_bytes(container, blob)
    return json.loads(data.decode("utf-8"))


def inspect_manifest(args: argparse.Namespace) -> None:
    stem = args.stem.strip().strip("/")
    processed = args.processed
    manifest_blob = f"{stem}/manifest.json"
    try:
        manifest = _load_json(processed, manifest_blob)
    except FileNotFoundError:
        print(f"[error] manifest not found: {processed}/{manifest_blob}", file=sys.stderr)
        sys.exit(1)

    print(f"Manifest: {processed}/{manifest_blob}")
    print(_pretty(manifest))

    fingerprint = manifest.get("fingerprint")
    source_hash = manifest.get("source_hash")
    if fingerprint and not args.skip_indexes:
        inspect_fingerprint(
            argparse.Namespace(
                fingerprint=fingerprint,
                processed=processed,
                prefix=args.fingerprint_prefix,
                skip_aliases=False,
                quiet=False,
            ),
            header="Linked fingerprint",
        )
    if source_hash and not args.skip_indexes:
        inspect_hash(
            argparse.Namespace(
                content_hash=source_hash,
                processed=processed,
                prefix=args.hash_prefix,
                quiet=False,
            ),
            header="Linked content hash",
        )


def inspect_fingerprint(args: argparse.Namespace, header: str | None = None) -> None:
    fingerprint = args.fingerprint.strip()
    blob = f"{args.prefix.strip().strip('/')}/{fingerprint}.json"
    try:
        record = _load_json(args.processed, blob)
    except FileNotFoundError:
        print(f"[error] fingerprint record not found: {args.processed}/{blob}", file=sys.stderr)
        sys.exit(1)
    if header:
        print(f"{header}: {args.processed}/{blob}")
    else:
        print(f"Fingerprint: {args.processed}/{blob}")
    print(_pretty(record))


def inspect_hash(args: argparse.Namespace, header: str | None = None) -> None:
    content_hash = args.content_hash.strip()
    blob = f"{args.prefix.strip().strip('/')}/{content_hash}.json"
    try:
        record = _load_json(args.processed, blob)
    except FileNotFoundError:
        print(f"[error] content hash record not found: {args.processed}/{blob}", file=sys.stderr)
        sys.exit(1)
    if header:
        print(f"{header}: {args.processed}/{blob}")
    else:
        print(f"Content hash: {args.processed}/{blob}")
    print(_pretty(record))


def main() -> None:
    parser = argparse.ArgumentParser(description="Inspect StreamFoundry manifests and indexes.")
    parser.add_argument(
        "--processed",
        default=os.getenv("SF_PROCESSED_CONTAINER", DEFAULT_PROCESSED),
        help="Processed container name (default: %(default)s)",
    )
    parser.add_argument(
        "--fingerprint-prefix",
        default=os.getenv("SF_FINGERPRINT_PREFIX", DEFAULT_FINGERPRINT_PREFIX),
        help="Fingerprint index prefix inside processed container (default: %(default)s)",
    )
    parser.add_argument(
        "--hash-prefix",
        default=os.getenv("SF_HASH_PREFIX", DEFAULT_HASH_PREFIX),
        help="Content-hash index prefix inside processed container (default: %(default)s)",
    )

    sub = parser.add_subparsers(dest="command")

    p_manifest = sub.add_parser("manifest", help="Inspect processed/<stem>/manifest.json")
    p_manifest.add_argument("stem", help="Processed stem (e.g., SubmitJob/demo)")
    p_manifest.add_argument(
        "--skip-indexes",
        action="store_true",
        help="Skip loading related fingerprint/content hash indexes",
    )
    p_manifest.set_defaults(func=inspect_manifest)

    p_fp = sub.add_parser("fingerprint", help="Inspect fingerprints/<fingerprint>.json")
    p_fp.add_argument("fingerprint", help="Fingerprint hash (without v_ prefix)")
    p_fp.add_argument(
        "--skip-aliases",
        action="store_true",
        help="(unused, kept for future expansion)",
    )
    p_fp.set_defaults(func=inspect_fingerprint)

    p_hash = sub.add_parser("hash", help="Inspect _hash/<content_hash>.json")
    p_hash.add_argument("content_hash", help="Content hash (sha256 of source bytes)")
    p_hash.set_defaults(func=inspect_hash)

    p_raw = sub.add_parser("raw", help="Inspect raw blob metadata status")
    p_raw.add_argument("raw_path", help="container/blob path (e.g. raw/demo.mp4)")

    def inspect_raw(args: argparse.Namespace) -> None:
        try:
            container, blob = args.raw_path.split("/", 1)
        except ValueError:
            print("[error] raw_path must be 'container/blob'", file=sys.stderr)
            sys.exit(1)
        status = get_raw_status(container, blob)
        print(f"Raw status: {container}/{blob}")
        print(_pretty(status))

    p_raw.set_defaults(func=inspect_raw)

    args = parser.parse_args()
    if not getattr(args, "command", None):
        parser.print_help()
        sys.exit(1)

    args.func(args)


if __name__ == "__main__":
    main()
