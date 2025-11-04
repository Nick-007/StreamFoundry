from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, List, Optional

from .config import get


DEFAULT_EXTENSIONS = [
    ".mp4",
    ".m4v",
    ".mov",
    ".mkv",
    ".avi",
    ".webm",
]


def _default_routes() -> List[Dict[str, object]]:
    return [
        {
            "id": "transcode",
            "extensions": DEFAULT_EXTENSIONS,
            "queue": get("TRANSCODE_QUEUE", "transcode-jobs"),
        }
    ]


def load_routes() -> List[Dict[str, object]]:
    raw = get("PIPELINE_ROUTES", "")
    if not raw:
        return _default_routes()
    try:
        parsed = json.loads(raw)
    except Exception:
        return _default_routes()
    routes: List[Dict[str, object]] = []
    if isinstance(parsed, dict):
        items = parsed.items()
    elif isinstance(parsed, list):
        items = enumerate(parsed)
    else:
        return _default_routes()

    for key, value in items:
        if isinstance(value, dict):
            rid = str(value.get("id") or key)
            queue = str(value.get("queue") or get("TRANSCODE_QUEUE", "transcode-jobs"))
            extensions = value.get("extensions") or DEFAULT_EXTENSIONS
            if isinstance(extensions, str):
                extensions = [extensions]
            extensions = [str(ext).lower().strip() for ext in extensions]
            routes.append({"id": rid, "queue": queue, "extensions": extensions})
    if not routes:
        return _default_routes()
    return routes


def select_pipeline_for_blob(blob_path: str) -> Optional[Dict[str, object]]:
    suffix = Path(blob_path).suffix.lower()
    for route in load_routes():
        extensions = route.get("extensions") or []
        if suffix in extensions or "*" in extensions:
            return route
    return None
