#!/usr/bin/env python3
"""
Embed a SAS token into manifest files (DASH MPD or HLS master).

Usage:
  python scripts/dev/embed_sas.py --token "sv=..." dist/dash/stream.mpd \
      --base "http://127.0.0.1:10000/devstoreaccount1/dash/v_.../"

  python scripts/dev/embed_sas.py --token "sv=..." dist/hls/master.m3u8 \
      --base "http://127.0.0.1:10000/devstoreaccount1/hls/v_.../"

It creates .sas copies alongside the originals (stream.mpd.sas, master.m3u8.sas)
with absolute URLs that include the query string `?token`.
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path
from urllib.parse import urljoin
import html
import re


def _append_token(url: str, token: str) -> str:
    if token in url:
        return url
    separator = "&" if "?" in url else "?"
    return f"{url}{separator}{token}"


def embed_to_mpd(path: Path, base_url: str, token: str) -> Path:
    text = path.read_text(encoding="utf-8")
    base = base_url.rstrip("/") + "/"

    def _replace(value: str) -> str:
        unescaped = html.unescape(value)
        target = unescaped if unescaped.startswith("http") else urljoin(base, unescaped)
        updated = _append_token(target, token)
        # MPD attributes must escape '&'
        return html.escape(updated, quote=True)

    transformed = text
    for attr in ("initialization=\"", "media=\""):
        start = 0
        while True:
            idx = transformed.find(attr, start)
            if idx == -1:
                break
            begin = idx + len(attr)
            end = transformed.find("\"", begin)
            if end == -1:
                break
            original = transformed[begin:end]
            updated = _replace(original)
            transformed = transformed[:begin] + updated + transformed[end:]
            start = begin + len(updated)

    output = path.with_suffix(path.suffix + ".sas")
    output.write_text(transformed, encoding="utf-8")
    return output


def embed_to_m3u8(path: Path, base_url: str, token: str) -> Path:
    lines = []
    base = base_url.rstrip("/") + "/"
    attr_pattern = re.compile(r'(URI=")([^"]+)(")')
    with path.open(encoding="utf-8") as fh:
        for line in fh:
            stripped = line.strip()
            if stripped.startswith("#EXT-X-MAP") or stripped.startswith("#EXT-X-KEY"):
                def repl(match: re.Match[str]) -> str:
                    prefix, original, suffix = match.groups()
                    target = original if original.startswith("http") else urljoin(base, original)
                    updated = _append_token(target, token)
                    return f'{prefix}{updated}{suffix}'
                updated_line = attr_pattern.sub(repl, stripped)
                lines.append(updated_line + "\n")
            elif stripped and not stripped.startswith("#"):
                target = stripped if stripped.startswith("http") else urljoin(base, stripped)
                updated = _append_token(target, token)
                lines.append(updated + "\n")
            else:
                lines.append(line if line.endswith("\n") else line + "\n")

    output = path.with_suffix(path.suffix + ".sas")
    output.write_text("".join(lines), encoding="utf-8")
    return output


def main() -> None:
    parser = argparse.ArgumentParser(description="Embed SAS token into manifest files")
    parser.add_argument("manifest", help="Path to stream.mpd or master.m3u8")
    parser.add_argument("--token", help="SAS token (sv=...) without leading ?")
    parser.add_argument("--base", help="Base URL to prefix before relative paths")
    parser.add_argument("--container", help="Container name (dash/hls)")
    parser.add_argument("--stem", help="Stem path inside container")
    args = parser.parse_args()

    token = args.token
    base = args.base
    if not token:
        token = os.getenv("DASH_TOKEN")
    if not base and args.container and args.stem:
        base_url = os.getenv("BASE_URL", "")
        if not base_url:
            parser.error("--base or BASE_URL env must be provided when using --container/--stem")
        base = "/".join([base_url.rstrip("/"), args.container.strip("/"), args.stem.strip("/")])
    if not token or not base:
        parser.error("Provide --token/--base or --container/--stem with BASE_URL/DASH_TOKEN env")

    path = Path(args.manifest)
    if not path.exists():
        parser.error(f"Manifest not found: {path}")

    suffix = path.suffix.lower()
    if suffix == ".mpd":
        out = embed_to_mpd(path, base, token)
    elif suffix == ".m3u8":
        out = embed_to_m3u8(path, base, token)
    else:
        parser.error("Unsupported manifest type; expected .mpd or .m3u8")

    print(f"[embed] wrote {out}")


if __name__ == "__main__":
    main()
