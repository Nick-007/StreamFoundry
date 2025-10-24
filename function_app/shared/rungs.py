from __future__ import annotations

from pathlib import Path
from typing import Dict, Iterable, List, Optional

from .normalize import LABELS_P, normalize_label_list, normalize_only_rung

DEFAULT_RUNGS: List[str] = LABELS_P.copy()


def ladder_labels(only_rung: Optional[Iterable[str]] = None) -> List[str]:
    """
    Return the canonical ladder labels (with 'p' suffix) filtered by only_rung.
    """
    if not only_rung:
        return DEFAULT_RUNGS.copy()
    normalized = normalize_only_rung(only_rung, as_set=False, suffix_p=True)
    if not normalized:
        return DEFAULT_RUNGS.copy()
    # normalize_only_rung may preserve duplicates; normalize_label_list dedups + sorts
    return [
        lab
        for lab in normalize_label_list(normalized, suffix_p=True)
        if lab in DEFAULT_RUNGS
    ]


def mezz_video_filename(label: str) -> str:
    """
    Normalize a rung label into the canonical mezzanine video filename.
    Persist the trailing 'p' so names align with transcode outputs (video_720p.mp4).
    """
    height = str(label).strip().lower()
    if not height:
        raise ValueError("label cannot be empty")
    if not height.endswith("p"):
        height = f"{height}p"
    return f"video_{height}.mp4"


def mezz_video_paths(stem: str, mezz_container: str, only_rung: Optional[Iterable[str]] = None) -> List[str]:
    """
    Return the mezzanine video object paths for a job.
    """
    return [
        f"{mezz_container}/{stem}/{mezz_video_filename(label)}"
        for label in ladder_labels(only_rung)
    ]


def receipt_payload(
    *,
    job_id: str,
    stem: str,
    raw_container: str,
    raw_key: str,
    mezz_container: str,
    dash_container: str,
    hls_container: str,
    only_rung: Optional[Iterable[str]] = None,
) -> Dict[str, object]:
    """
    Build the HTTP receipt payload shared by submit flows.
    """
    return {
        "jobId": job_id,
        "inputBlob": f"{raw_container}/{raw_key}",
        "expectedOutputs": {
            "mezzanine": {
                "audio": f"{mezz_container}/{stem}/audio.mp4",
                "videos": mezz_video_paths(stem, mezz_container, only_rung),
            },
            "dash": f"{dash_container}/{stem}/stream.mpd",
            "hls": f"{hls_container}/{stem}/master.m3u8",
        },
    }


def discover_renditions(work_dir: Path, only_rung: Optional[Iterable[str]] = None) -> List[Dict[str, str]]:
    """
    Discover available mezzanine CMAF MP4 renditions in work_dir.
    """
    renditions: List[Dict[str, str]] = []
    for label in ladder_labels(only_rung):
        candidate = work_dir / mezz_video_filename(label)
        if candidate.exists():
            renditions.append({"name": label, "video": str(candidate)})
    return renditions
