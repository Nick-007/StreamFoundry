from __future__ import annotations

from typing import Dict

from .config import get


def build_asset_urls(fingerprint: str, stem: str) -> Dict[str, str]:
    stem_clean = stem.strip("/")

    dash_container = get("DASH_CONTAINER", "dash")
    hls_container = get("HLS_CONTAINER", "hls")

    base_url = get("BASE_URL", "") or ""
    dash_base = get("DASH_BASE_URL") or (
        f"{base_url.rstrip('/')}/{dash_container}" if base_url else ""
    )
    hls_base = get("HLS_BASE_URL") or (
        f"{base_url.rstrip('/')}/{hls_container}" if base_url else ""
    )

    dash_path = f"{fingerprint}/{stem_clean}/stream.mpd"
    hls_path = f"{fingerprint}/{stem_clean}/master.m3u8"

    dash_url = f"{dash_base.rstrip('/')}/{dash_path}" if dash_base else ""
    hls_url = f"{hls_base.rstrip('/')}/{hls_path}" if hls_base else ""

    return {
        "dash_path": dash_path,
        "hls_path": hls_path,
        "dash_url": dash_url,
        "hls_url": hls_url,
    }
