# function_app/shared/mezz.py
from __future__ import annotations
import os, json, hashlib, time
from pathlib import Path
from typing import Callable, Optional, List, Dict

from .config import get
from .storage import upload_file, upload_bytes, blob_client

MEZZ = get("MEZZ_CONTAINER", "mezzanine")

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

def upload_mezz_and_manifest(stem: str, work_dir: str, *, log: Optional[Callable[[str], None]] = None) -> Dict:
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

    manifest = {
        "stem": stem,
        "container": MEZZ,
        "createdAt": int(time.time()),
        "files": manifest_files,
        "version": 1
    }
    upload_bytes(MEZZ, f"{stem}/mezz.json", json.dumps(manifest, indent=2).encode("utf-8"), "application/json")
    _log(f"[mezz] wrote {MEZZ}/{stem}/mezz.json")
    return manifest

def restore_mezz_to_workdir(stem: str, work_dir: str, *, log: Optional[Callable[[str], None]] = None) -> bool:
    """Restore intermediates listed in mezz.json into work_dir; verifies sha256. Returns True if restored."""
    def _log(s: str): (log or print)(s)
    try:
        bc = blob_client(MEZZ, f"{stem}/mezz.json")
        data = bc.download_blob().readall()
        manifest = json.loads(data.decode("utf-8"))
    except Exception:
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

        # download
        try:
            bc = blob_client(MEZZ, f"{stem}/{name}")
            with open(dst, "wb") as f:
                bc.download_blob().readinto(f)
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

def ensure_intermediates_from_mezz(stem: str, work_dir: str, *, require_all: bool = True, log: Optional[Callable[[str], None]] = None) -> bool:
    """
    If intermediates missing in work_dir, try to restore from MEZZ/{stem}.
    Returns True if work_dir has a usable set afterwards.
    """
    need = {"audio.mp4"} | {f"video_{p}p.mp4" for p in ("240","360","480","720","1080")}
    have = {p.name for p in Path(work_dir).glob("*.mp4")}
    missing = need - have
    if missing:
        (log or print)(f"[mezz] missing in work_dir: {sorted(missing)} — attempting restore")
        restored = restore_mezz_to_workdir(stem, work_dir, log=log)
        if not restored and require_all:
            return False
    return True