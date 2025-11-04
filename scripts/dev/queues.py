#!/usr/bin/env python3
"""
Utility helpers for managing Azure Storage queues used by StreamFoundry.

Examples:
  python scripts/dev/queues.py list
  python scripts/dev/queues.py peek transcode-jobs --limit 5
  python scripts/dev/queues.py clear transcode-jobs
  python scripts/dev/queues.py pop transcode-jobs
  python scripts/dev/queues.py push transcode-jobs '{"id":"demo","in":{"container":"raw-videos","key":"demo.mp4"}}'
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Iterable, Tuple

from azure.storage.queue import QueueServiceClient

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
            print(f"[queues] WARN: failed to read {LOCAL_SETTINGS}: {exc}", file=sys.stderr)
    raise SystemExit("AzureWebJobsStorage not found in environment or local.settings.json")


def discover_queue_names(values: dict) -> Iterable[str]:
    names = []
    for key, value in values.items():
        if not isinstance(value, str):
            continue
        if key.lower().startswith("azurewebjobs"):
            continue
        if key.endswith("_QUEUE"):
            name = value.strip()
            if name and not name.startswith("azure-webjobs-"):
                names.append(name)
    clean = []
    for name in names:
        base = name[:-7] if name.endswith("-poison") else name
        clean.append(base)
        clean.append(f"{base}-poison")
    return sorted(set(clean))


def load_queue_names() -> Tuple[str, Iterable[str]]:
    conn = load_connection_string()
    names: Iterable[str] = []
    if LOCAL_SETTINGS.exists():
        try:
            data = json.loads(LOCAL_SETTINGS.read_text())
            values = data.get("Values") or {}
            names = discover_queue_names(values)
        except Exception:
            names = []
    return conn, names


def cmd_list(_: argparse.Namespace) -> None:
    conn, names = load_queue_names()
    svc = QueueServiceClient.from_connection_string(conn)
    if names:
        print("[queues] Known queues from local.settings.json:")
        for name in names:
            print(f"  - {name}")
        print()
    print("[queues] Existing queues in storage account:")
    for queue in svc.list_queues():
        print(f"  - {queue['name']}")


def cmd_peek(args: argparse.Namespace) -> None:
    conn = load_connection_string()
    svc = QueueServiceClient.from_connection_string(conn)
    qc = svc.get_queue_client(args.name)
    messages = qc.peek_messages(max_messages=args.limit)
    if not messages:
        print(f"[queues] {args.name}: empty")
        return
    for idx, msg in enumerate(messages, 1):
        print(f"[{idx}] id={msg.id} dequeue_count={msg.dequeue_count}")
        print(f"    {msg.content}")


def cmd_clear(args: argparse.Namespace) -> None:
    conn = load_connection_string()
    svc = QueueServiceClient.from_connection_string(conn)
    qc = svc.get_queue_client(args.name)
    qc.clear_messages()
    print(f"[queues] cleared {args.name}")


def cmd_pop(args: argparse.Namespace) -> None:
    conn = load_connection_string()
    svc = QueueServiceClient.from_connection_string(conn)
    qc = svc.get_queue_client(args.name)
    msg = qc.receive_message()
    if not msg:
        print(f"[queues] {args.name}: empty")
        return
    print(f"[queues] popped {msg.id} dequeue_count={msg.dequeue_count}")
    print(msg.content)
    if args.no_delete:
        return
    qc.delete_message(msg.id, msg.pop_receipt)


def cmd_push(args: argparse.Namespace) -> None:
    conn = load_connection_string()
    svc = QueueServiceClient.from_connection_string(conn)
    qc = svc.get_queue_client(args.name)

    payload = args.payload
    if args.file:
        payload = Path(args.file).read_text()
    if args.stdin:
        payload = sys.stdin.read()
    if payload is None:
        raise SystemExit("Provide --payload, --file, or --stdin")

    if not args.skip_validate:
        try:
            json.loads(payload)
        except Exception:
            raise SystemExit("Payload is not valid JSON (use --skip-validate to bypass)")

    qc.send_message(payload)
    print(f"[queues] pushed message to {args.name}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Manage Azure Storage queues for StreamFoundry.")
    sub = parser.add_subparsers(dest="command")

    p_list = sub.add_parser("list", help="List queues (from config and storage)")
    p_list.set_defaults(func=cmd_list)

    p_peek = sub.add_parser("peek", help="Peek messages without dequeuing")
    p_peek.add_argument("name", help="Queue name")
    p_peek.add_argument("--limit", type=int, default=5, help="Number of messages to peek (default: 5)")
    p_peek.set_defaults(func=cmd_peek)

    p_clear = sub.add_parser("clear", help="Clear all messages from a queue")
    p_clear.add_argument("name", help="Queue name")
    p_clear.set_defaults(func=cmd_clear)

    p_pop = sub.add_parser("pop", help="Dequeue and optionally delete a single message")
    p_pop.add_argument("name", help="Queue name")
    p_pop.add_argument("--no-delete", action="store_true", help="Do not delete after receiving" )
    p_pop.set_defaults(func=cmd_pop)

    p_push = sub.add_parser("push", help="Send a message to a queue")
    p_push.add_argument("name", help="Queue name")
    payload_group = p_push.add_mutually_exclusive_group()
    payload_group.add_argument('--payload', help='Message payload (JSON string)')
    payload_group.add_argument('--file', help='Load payload from file')
    p_push.add_argument('--stdin', action='store_true', help='Read payload from stdin')
    p_push.add_argument('--skip-validate', action='store_true', help='Skip JSON validation')
    p_push.set_defaults(func=cmd_push)

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
