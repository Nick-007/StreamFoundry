# function_app/shared/mezz.py
from __future__ import annotations
import os, json, hashlib, time
from pathlib import Path
from typing import Callable, Optional, List, Dict, Iterable

from .config import get
from .storage import upload_file, upload_bytes, blob_client, download_blob_streaming
from .rungs import ladder_labels, mezz_video_filename

MEZZ = get("MEZZ_CONTAINER", "mezzanine")


def _load_manifest(stem: str) -> Optional[Dict]:
    try:
        bc = blob_client(MEZZ, f"{stem}/mezz.json")
        data = bc.download_blob().readall()
        return json.loads(data.decode("utf-8"))
    except Exception:
        return None


def _empty_manifest(stem: str) -> Dict:
    now = int(time.time())
    return {
        "stem": stem,
        "container": MEZZ,
        "createdAt": now,
        "updatedAt": now,
        "files": [],
        "version": 1,
    }

def _sha256(path: str, chunk: int = 2**20) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            b = f.read(chunk)
            if not b: break
            h.update(b)
    return h.hexdigest()

def _collect_intermediates(work_dir: str) -> List[Dict]:
    """Find audio.mp4 and video_*.mp4 in work_dir."""
    w = Path(work_dir)
    outs: List[Dict] = []
    a = w / "audio.mp4"
    if a.exists():
        outs.append({"name": "audio.mp4", "path": str(a)})
    for p in sorted(w.glob("video_*.mp4")):
        outs.append({"name": p.name, "path": str(p)})
    return outs

def upload_mezz_and_manifest(
    stem: str,
    work_dir: str,
    *,
    manifest: Optional[Dict] = None,
    log: Optional[Callable[[str], None]] = None
) -> Dict:
    """Upload intermediates (audio.mp4, video_*.mp4) to MEZZ/{stem} and write MEZZ/{stem}/mezz.json."""
    def _log(s: str): (log or print)(s)
    files = _collect_intermediates(work_dir)
    if not files:
        raise FileNotFoundError(f"No intermediates found in {work_dir}")

    manifest_files = []
    for f in files:
        sha = _sha256(f["path"])
        size = os.path.getsize(f["path"])
        manifest_files.append({"name": f["name"], "sha256": sha, "size": size})
        dst = f"{stem}/{f['name']}"
        upload_file(MEZZ, dst, f["path"])  # relies on your idempotency check
        _log(f"[mezz] uploaded {dst} size={size} sha256={sha[:12]}")

    manifest = manifest or _load_manifest(stem) or _empty_manifest(stem)
    manifest["updatedAt"] = int(time.time())
    existing = {entry["name"]: entry for entry in manifest.get("files", [])}
    for item in manifest_files:
        existing[item["name"]] = item
    manifest["files"] = sorted(existing.values(), key=lambda x: x["name"])
    upload_bytes(MEZZ, f"{stem}/mezz.json", json.dumps(manifest, indent=2).encode("utf-8"), "application/json")
    _log(f"[mezz] wrote {MEZZ}/{stem}/mezz.json")
    return manifest


def upload_intermediate_file(
    stem: str,
    local_path: str,
    *,
    manifest: Optional[Dict] = None,
    log: Optional[Callable[[str], None]] = None,
) -> Dict:
    """
    Upload a single intermediate (audio.mp4 or video_###p.mp4) and update mezz.json.
    Returns the manifest dictionary reflecting the update.
    """
    def _log(s: str): (log or print)(s)

    path = Path(local_path)
    if not path.exists():
        raise FileNotFoundError(f"Local intermediate missing: {local_path}")

    sha = _sha256(str(path))
    size = os.path.getsize(str(path))
    dst = f"{stem}/{path.name}"
    upload_file(MEZZ, dst, str(path))
    _log(f"[mezz] uploaded {dst} size={size} sha256={sha[:12]}")

    manifest = manifest or _load_manifest(stem) or _empty_manifest(stem)
    manifest["updatedAt"] = int(time.time())
    files = manifest.setdefault("files", [])
    replaced = False
    for entry in files:
        if entry.get("name") == path.name:
            entry.update({"sha256": sha, "size": size})
            replaced = True
            break
    if not replaced:
        files.append({"name": path.name, "sha256": sha, "size": size})
    files.sort(key=lambda x: x["name"])

    upload_bytes(MEZZ, f"{stem}/mezz.json", json.dumps(manifest, indent=2).encode("utf-8"), "application/json")
    _log(f"[mezz] manifest updated entries={len(files)}")
    return manifest

def restore_mezz_to_workdir(stem: str, work_dir: str, *, log: Optional[Callable[[str], None]] = None) -> bool:
    """Restore intermediates listed in mezz.json into work_dir; verifies sha256. Returns True if restored."""
    def _log(s: str): (log or print)(s)
    manifest = _load_manifest(stem)
    if manifest is None:
        _log(f"[mezz] no manifest for {stem}; nothing to restore")
        return False

    Path(work_dir).mkdir(parents=True, exist_ok=True)
    ok_any = False
    for entry in manifest.get("files", []):
        name, expect_sha = entry["name"], entry.get("sha256")
        dst = Path(work_dir) / name
        if dst.exists():
            # verify existing file; skip download if correct
            if expect_sha and _sha256(str(dst)) == expect_sha:
                _log(f"[mezz] present {name} (sha OK) — skip")
                ok_any = True
                continue
            else:
                try: dst.unlink()
                except Exception: pass

        # download with streaming and size limits
        try:
            download_blob_streaming(
                container=MEZZ,
                blob=f"{stem}/{name}",
                dest_path=str(dst),
                max_mb=2048,  # 2GB limit for mezzanine files
                chunk_bytes=4194304,  # 4MB chunks
                log=_log
            )
            ok_any = True
            # verify checksum
            if expect_sha:
                got = _sha256(str(dst))
                if got != expect_sha:
                    _log(f"[mezz] checksum mismatch for {name}: got {got[:12]} expect {expect_sha[:12]}")
                    try: dst.unlink()
                    except Exception: pass
                    raise IOError(f"checksum mismatch for {name}")
            _log(f"[mezz] restored {name}")
        except Exception as e:
            _log(f"[mezz] restore failed for {name}: {e}")
            return False

    return ok_any

def ensure_intermediates_from_mezz(
    stem: str,
    work_dir: str,
    *,
    require_all: bool = True,
    only_rung: Optional[Iterable[str]] = None,
    log: Optional[Callable[[str], None]] = None,
) -> bool:
    """
    If intermediates are missing in work_dir, try to restore them from MEZZ/{stem}.
    When only_rung is provided, restrict the required video files to that subset.
    Returns True if work_dir has a usable set afterwards.
    """
    labels = ladder_labels(only_rung)
    required_videos = {mezz_video_filename(label) for label in labels}
    need = {"audio.mp4"} | required_videos
    have = {p.name for p in Path(work_dir).glob("*.mp4")}
    missing = need - have
    if missing:
        (log or print)(f"[mezz] missing in work_dir: {sorted(missing)} — attempting restore")
        restored = restore_mezz_to_workdir(stem, work_dir, log=log)
        if not restored and require_all:
            return False
    return True
