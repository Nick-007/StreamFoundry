#!/usr/bin/env python3
"""Sanity-check DASH or HLS segments from a manifest (HTTP or local filesystem)."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
import math
import os
import re
import sys
from pathlib import Path
from typing import Dict, List, Tuple
from urllib.parse import urljoin, urlparse, unquote

import requests
import xml.etree.ElementTree as ET


@dataclass
class SegmentRecord:
    url: str
    kind: str
    template: str | None = None
    number: int | None = None


@dataclass
class TemplateInfo:
    numbers: set[int]
    step: int
    start: int


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Check DASH or HLS segments")
    parser.add_argument("manifest", help="Path or URL to stream.mpd or master.m3u8")
    parser.add_argument("--sas", help="Query string to append to every segment (sv=...&sig=...)")
    parser.add_argument("--timeout", type=float, default=10.0, help="HTTP timeout per segment (seconds)")
    parser.add_argument("--limit", type=int, help="Optional max number of segments to test")
    parser.add_argument("--save-dir", type=Path, help="Optional directory to save fetched segments")
    return parser.parse_args()


def load_text(path_or_url: str) -> Tuple[str, str]:
    parsed = urlparse(path_or_url)
    if parsed.scheme in ("http", "https"):
        resp = requests.get(path_or_url, timeout=10, proxies={"http": "", "https": ""})
        resp.raise_for_status()
        return resp.text, path_or_url
    elif parsed.scheme == "file":
        return Path(unquote(parsed.path)).read_text(encoding="utf-8"), path_or_url
    else:
        p = Path(path_or_url)
        if not p.exists():
            raise FileNotFoundError(f"Manifest not found: {p}")
        return p.read_text(encoding="utf-8"), p.as_uri()


def build_url(base: str, rel: str, sas: str | None) -> str:
    url = urljoin(base, rel)
    if sas:
        separator = "&" if "?" in url else "?"
        url = f"{url}{separator}{sas.lstrip('?&')}"
    return url


def gather_dash_segments(manifest_text: str, base: str, sas: str | None) -> List[SegmentRecord]:
    root = ET.fromstring(manifest_text)
    ns = {"mpd": root.tag.split("}")[0].strip("{")} if "}" in root.tag else {}

    def findall(elem, path):
        return elem.findall(path, ns) if ns else elem.findall(path)

    segments: List[SegmentRecord] = []
    for tmpl in findall(root, ".//mpd:SegmentTemplate") if ns else findall(root, ".//SegmentTemplate"):
        init = tmpl.attrib.get("initialization")
        media = tmpl.attrib.get("media")
        start = int(tmpl.attrib.get("startNumber", "1"))
        timeline = tmpl.find("mpd:SegmentTimeline", ns) if ns else tmpl.find("SegmentTimeline")

        if init:
            segments.append(SegmentRecord(build_url(base, init, sas), "dash"))

        if not media:
            continue

        number = start
        if timeline is not None:
            for entry in findall(timeline, "mpd:S") if ns else findall(timeline, "S"):
                repeat = int(entry.attrib.get("r", "0"))
                count = repeat + 1
                for offset in range(count):
                    url = media.replace("$Number$", str(number + offset))
                    segments.append(
                        SegmentRecord(
                            build_url(base, url, sas),
                            "dash",
                            template=media,
                            number=number + offset,
                        )
                    )
                number += count
        else:
            segments.append(
                SegmentRecord(
                    build_url(base, media.replace("$Number$", str(number)), sas),
                    "dash",
                    template=media,
                    number=number,
                )
            )
    return segments


def gather_hls_segments(master_text: str, manifest_url: str, sas: str | None) -> Tuple[List[str], List[str]]:
    media_playlists: List[str] = []
    caption_files: List[str] = []

    for line in master_text.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        if stripped.startswith("#EXT-X-MEDIA"):
            uri = None
            for part in stripped.split(","):
                if part.startswith("URI="):
                    uri = part.split("=", 1)[1].strip("\"")
                    break
            if uri:
                target = build_url(manifest_url, uri, sas)
                if uri.lower().endswith(".vtt"):
                    caption_files.append(target)
                else:
                    media_playlists.append(target)
        elif not stripped.startswith("#"):
            media_playlists.append(build_url(manifest_url, stripped, sas))

    return media_playlists, caption_files


def read_playlist(url: str) -> Tuple[str, str]:
    parsed = urlparse(url)
    if parsed.scheme in ("http", "https"):
        resp = requests.get(url, timeout=10, proxies={"http": "", "https": ""})
        resp.raise_for_status()
        return resp.text, url
    elif parsed.scheme == "file":
        path = Path(unquote(parsed.path))
        return path.read_text(encoding="utf-8"), url
    else:
        path = Path(parsed.path)
        return path.read_text(encoding="utf-8"), path.as_uri()


def fetch(url: str, timeout: float) -> Tuple[bool, int, bytes | None, Exception | None]:
    parsed = urlparse(url)
    if parsed.scheme in ("http", "https"):
        resp = requests.get(url, timeout=timeout, proxies={"http": "", "https": ""})
        ok = resp.ok
        return ok, resp.status_code, resp.content if ok else None, None if ok else Exception(f"HTTP {resp.status_code}")
    elif parsed.scheme == "file":
        path = Path(unquote(parsed.path))
        data = path.read_bytes()
        return True, 200, data, None
    else:
        path = Path(parsed.path)
        data = path.read_bytes()
        return True, 200, data, None


def build_local_dash_template_info(manifest_url: str, segments: List[SegmentRecord]) -> Dict[str, TemplateInfo]:
    parsed = urlparse(manifest_url)
    if parsed.scheme != "file":
        return {}
    mpd_path = Path(unquote(parsed.path))
    base_dir = mpd_path.parent
    infos: Dict[str, TemplateInfo] = {}
    for segment in segments:
        template = segment.template
        if not template or template in infos:
            continue
        numbers = _collect_segment_numbers(base_dir, template)
        if not numbers:
            continue
        step = _compute_step(sorted(numbers))
        infos[template] = TemplateInfo(numbers=numbers, step=step, start=min(numbers))
    return infos


def _collect_segment_numbers(base_dir: Path, template: str) -> set[int]:
    media_path = Path(template)
    folder = base_dir
    if media_path.parent != Path("."):
        folder = base_dir / media_path.parent
    pattern = media_path.name.replace("$Number$", "*")
    numbers: set[int] = set()
    for candidate in folder.glob(pattern):
        num = _segment_number_from_name(candidate.name)
        if num is not None:
            numbers.add(num)
    return numbers


def _segment_number_from_name(name: str) -> int | None:
    match = re.search(r"_(\d+)\.", name)
    if not match:
        return None
    try:
        return int(match.group(1))
    except ValueError:
        return None


def _compute_step(numbers: List[int]) -> int:
    if len(numbers) < 2:
        return 1
    diffs = []
    for prev, curr in zip(numbers, numbers[1:]):
        diff = curr - prev
        if diff > 0:
            diffs.append(diff)
    if not diffs:
        return 1
    step = diffs[0]
    for d in diffs[1:]:
        step = math.gcd(step, d)
    return max(step, 1)


def should_skip_dash_segment(segment: SegmentRecord, infos: Dict[str, TemplateInfo]) -> bool:
    if segment.kind != "dash" or not segment.template or segment.number is None:
        return False
    info = infos.get(segment.template)
    if not info or info.step <= 1:
        return False
    if segment.number in info.numbers:
        return False
    delta = segment.number - info.start
    return delta % info.step != 0


def main() -> None:
    args = parse_args()
    manifest_text, manifest_url = load_text(args.manifest)

    parsed = urlparse(args.manifest)
    ext = Path(parsed.path or args.manifest).suffix.lower()

    segments: List[SegmentRecord] = []

    if ext == ".mpd":
        base = manifest_url.rsplit("/", 1)[0] + "/"
        segments.extend(gather_dash_segments(manifest_text, base, args.sas))
    elif ext == ".m3u8":
        base = manifest_url.rsplit("/", 1)[0] + "/"
        playlists, captions = gather_hls_segments(manifest_text, base, args.sas)
        for playlist in playlists:
            playlist_text, playlist_url = read_playlist(playlist)
            base_inner = playlist_url.rsplit("/", 1)[0] + "/"
            for line in playlist_text.splitlines():
                stripped = line.strip()
                if stripped and not stripped.startswith("#"):
                    seg_url = build_url(base_inner, stripped, args.sas)
                    segments.append(SegmentRecord(seg_url, "hls"))
        for caption in captions:
            segments.append(SegmentRecord(caption, "caption"))
    else:
        print("Unsupported manifest type", file=sys.stderr)
        sys.exit(1)

    if args.limit:
        segments = segments[: args.limit]

    template_infos = build_local_dash_template_info(manifest_url, segments)

    if args.save_dir:
        args.save_dir.mkdir(parents=True, exist_ok=True)

    for idx, segment in enumerate(segments, 1):
        url = segment.url
        label = segment.kind
        if should_skip_dash_segment(segment, template_infos):
            info = template_infos.get(segment.template or "", None)
            step_info = f"step={info.step}" if info else "step=?"
            print(f"[{idx}] {label:7} SKP {0:>8} bytes {url} (trick-play {step_info})")
            continue
        try:
            ok, status, data, err = fetch(url, args.timeout)
            size = len(data) if data is not None else 0
            if ok and args.save_dir and data is not None:
                suffix = Path(urlparse(url).path).suffix or ".bin"
                (args.save_dir / f"seg_{idx:04d}{suffix}").write_bytes(data)
            print(f"[{idx}] {label:7} {status if ok else 'ERR':>3} {size:>8} bytes {url}")
        except Exception as exc:  # noqa: BLE001
            print(f"[{idx}] {label:7} ERROR {url} -> {exc}")


if __name__ == "__main__":
    main()
