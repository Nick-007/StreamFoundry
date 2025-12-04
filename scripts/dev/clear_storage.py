#!/usr/bin/env python3
"""Deprecated wrapper: use scripts/dev/storage.py blobs instead."""
from __future__ import annotations

import argparse
from scripts.dev.storage import delete_blobs, list_blobs


def main() -> None:
    parser = argparse.ArgumentParser(description='List or delete blobs (use storage.py blobs going forward)')
    parser.add_argument('--container', '-c', required=True, help='Container name')
    parser.add_argument('--prefix', '-p', help='Optional prefix')
    parser.add_argument('--list', action='store_true', help='List instead of delete')
    args = parser.parse_args()
    if args.list:
        list_blobs(args.container, args.prefix)
    else:
        delete_blobs(args.container, args.prefix)


if __name__ == '__main__':
    main()
