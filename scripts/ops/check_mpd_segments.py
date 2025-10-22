#!/usr/bin/env python3
"""
Check that all segments referenced by a DASH MPD exist in a target location.

Usage (Azure Blob):
  CONN="..."; export CONN
  python check_mpd_segments.py --mpd /path/to/stream.mpd \
      --container my-dash-container --prefix myjob/dash/

Usage (local directory):
  python check_mpd_segments.py --mpd /path/to/stream.mpd --local-dir /tmp/ingestor/ft/dist/dash/
"""
import argparse
import sys
from pathlib import Path
from typing import Dict, List, Tuple
import xml.etree.ElementTree as ET

# ---------------- XML helpers ----------------

NS = {"mpd": "urn:mpeg:dash:schema:mpd:2011"}
CONN="DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;QueueEndpoint=http://127.0.0.1:10001/devstoreaccount1;TableEndpoint=http://127.0.0.1:10002/devstoreaccount1"

def _int(s, default=0):
    try:
        return int(s)
    except Exception:
        return default

def expand_segment_numbers(seg_tmpl: ET.Element) -> List[int]:
    """Expand SegmentTimeline into a list of segment indexes; uses startNumber (default 1)."""
    start_num = _int(seg_tmpl.get("startNumber", "1"), 1)
    timeline = seg_tmpl.find("mpd:SegmentTimeline", NS)
    if timeline is None:
        # No explicit timeline → not enough info to count; assume 0 segments
        return []
    seq = []
    n = start_num
    for s in timeline.findall("mpd:S", NS):
        # Each <S> has d (duration), optional r (repeat count), optional t (start time).
        r = _int(s.get("r", "0"), 0)
        count = (r + 1) if r >= 0 else 0
        for _ in range(count):
            seq.append(n)
            n += 1
    return seq

def parse_mpd(path: str) -> Dict:
    tree = ET.parse(path)
    root = tree.getroot()

    mpd_type = root.get("type", "static")
    mpd_duration = root.get("mediaPresentationDuration")
    out = {
        "type": mpd_type,
        "duration": mpd_duration,
        "reps": []  # list of {kind, init, media, numbers}
    }

    for period in root.findall("mpd:Period", NS):
        for aset in period.findall("mpd:AdaptationSet", NS):
            kind = aset.get("contentType") or "unknown"
            for rep in aset.findall("mpd:Representation", NS):
                # Prefer Representation-level SegmentTemplate; else AdaptationSet-level.
                seg_tmpl = rep.find("mpd:SegmentTemplate", NS) or aset.find("mpd:SegmentTemplate", NS)
                if seg_tmpl is None:
                    continue
                init = seg_tmpl.get("initialization")
                media = seg_tmpl.get("media")
                if not media:
                    continue
                nums = expand_segment_numbers(seg_tmpl)
                out["reps"].append({"kind": kind, "init": init, "media": media, "numbers": nums})
    return out

# ---------------- existence checks ----------------

def expect_files_from_mpd(mpd_info: Dict) -> List[str]:
    paths = []
    for r in mpd_info["reps"]:
        if r["init"]:
            paths.append(r["init"])
        # Replace $Number$ for each sequence
        for n in r["numbers"]:
            paths.append(r["media"].replace("$Number$", str(n)))
    # Deduplicate while preserving order
    seen = set()
    uniq = []
    for p in paths:
        if p not in seen:
            uniq.append(p); seen.add(p)
    return uniq

def check_local(dirpath: str, files: List[str]) -> Tuple[List[str], List[str]]:
    base = Path(dirpath)
    missing, present = [], []
    for f in files:
        if (base / f).exists():
            present.append(f)
        else:
            missing.append(f)
    return present, missing

def check_azure(container: str, prefix: str, files: List[str]):
    try:
        from azure.storage.blob import ContainerClient
    except ImportError:
        print("ERROR: Install azure-storage-blob (pip install azure-storage-blob)", file=sys.stderr)
        sys.exit(2)

    conn = CONN
    if not conn:
        print("ERROR: CONN not set", file=sys.stderr)
        sys.exit(2)

    # Normalize prefix to have trailing slash when not empty
    if prefix and not prefix.endswith("/"):
        prefix = prefix + "/"

    cc = ContainerClient.from_connection_string(conn, container)
    # Build a set of available blob names under the prefix
    available = set()
    for b in cc.list_blobs(name_starts_with=prefix):
        available.add(b.name)

    present, missing = [], []
    for f in files:
        key = f"{prefix}{f}" if prefix else f
        if key in available:
            present.append(f)
        else:
            missing.append(f)
    return present, missing

# ---------------- main ----------------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mpd", required=True, help="Path to local MPD file (e.g., stream.mpd)")
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--local-dir", help="Local directory where segments live")
    g.add_argument("--container", help="Azure Blob container name holding the segments")
    ap.add_argument("--prefix", default="", help="Blob virtual directory (e.g., job_id/dash/)")
    args = ap.parse_args()

    info = parse_mpd(args.mpd)
    files = expect_files_from_mpd(info)

    if info["type"] != "static":
        print(f"WARNING: MPD type is '{info['type']}', VOD players generally expect 'static' for on-demand.", file=sys.stderr)

    if args.local_dir:
        present, missing = check_local(args.local_dir, files)
    else:
        present, missing = check_azure(args.container, args.prefix, files)

    print(f"MPD type: {info['type']}, duration={info['duration']}")
    print(f"Representations: {len(info['reps'])}")
    print(f"Expected files: {len(files)}")
    print(f"Present: {len(present)}")
    print(f"Missing: {len(missing)}")

    if missing:
        print("\nMissing examples (up to 50):")
        for f in missing[:50]:
            print("  -", f)
        sys.exit(1)
    else:
        print("\nAll referenced init segments and media segments are present ✅")
        sys.exit(0)

if __name__ == "__main__":
    main()