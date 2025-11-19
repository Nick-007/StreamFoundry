"""
Additional tests for asset-tracking helpers introduced on this branch.
Includes coverage for URL construction utilities and trickplay asset generation.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Dict
from unittest.mock import MagicMock, patch

import pytest
from PIL import Image

os.environ.setdefault("TRANSCODE_VISIBILITY_EXTENSION_SEC", "300")

from function_app.shared.errors import CmdError
from function_app.shared.trickplay import generate_trickplay_assets
from function_app.shared.urls import build_asset_urls


class TestBuildAssetUrls:
    """Verify BASE_URL/DASH/HLS overrides behave correctly."""

    def test_build_asset_urls_uses_global_base(self):
        config: Dict[str, str] = {
            "BASE_URL": "https://cdn.example.com/media/",
            "DASH_CONTAINER": "dash",
            "HLS_CONTAINER": "hls",
        }

        def fake_get(key: str, default=None):
            return config.get(key, default)

        with patch("function_app.shared.urls.get", side_effect=fake_get):
            result = build_asset_urls("abc123", "/series/stem.mp4")

        assert result["dash_path"] == "abc123/series/stem.mp4/stream.mpd"
        assert result["hls_path"] == "abc123/series/stem.mp4/master.m3u8"
        assert (
            result["dash_url"]
            == "https://cdn.example.com/media/dash/abc123/series/stem.mp4/stream.mpd"
        )
        assert (
            result["hls_url"]
            == "https://cdn.example.com/media/hls/abc123/series/stem.mp4/master.m3u8"
        )

    def test_build_asset_urls_prefers_specific_bases(self):
        config: Dict[str, str] = {
            "BASE_URL": "https://global.invalid",
            "DASH_BASE_URL": "https://dash.cdn.example.net/root/",
            "HLS_BASE_URL": "https://cdn.example.net/hls/",
            "DASH_CONTAINER": "dash",
            "HLS_CONTAINER": "hls",
        }

        def fake_get(key: str, default=None):
            return config.get(key, default)

        with patch("function_app.shared.urls.get", side_effect=fake_get):
            result = build_asset_urls("fingerprint", "nested/stem")

        assert result["dash_url"] == (
            "https://dash.cdn.example.net/root/fingerprint/nested/stem/stream.mpd"
        )
        assert result["hls_url"] == (
            "https://cdn.example.net/hls/fingerprint/nested/stem/master.m3u8"
        )


class TestGenerateTrickplayAssets:
    """Exercise the trickplay generator without invoking real ffmpeg."""

    def test_generate_trickplay_assets_disabled(self, tmp_path):
        config = {"ENABLE_TRICKPLAY": "0"}

        def fake_get(key: str, default=None):
            return config.get(key, default)

        with patch("function_app.shared.trickplay.get", side_effect=fake_get):
            result = generate_trickplay_assets(
                str(tmp_path / "video.mp4"),
                str(tmp_path / "out"),
            )

        assert result.enabled is False
        assert result.vtt is None

    def test_generate_trickplay_assets_missing_source_raises(self, tmp_path):
        config = {"ENABLE_TRICKPLAY": "1"}

        def fake_get(key: str, default=None):
            return config.get(key, default)

        with patch("function_app.shared.trickplay.get", side_effect=fake_get):
            with pytest.raises(CmdError):
                generate_trickplay_assets(
                    str(tmp_path / "missing.mp4"),
                    str(tmp_path / "out"),
                )

    def test_generate_trickplay_assets_creates_sprites_and_vtt(self, tmp_path):
        source = tmp_path / "source.mp4"
        source.write_bytes(b"fake video bytes")
        output_root = tmp_path / "trick"

        cols = 2
        rows = 2
        thumb_w = 64
        thumb_h = 36
        interval = 3
        config = {
            "ENABLE_TRICKPLAY": "1",
            "THUMB_INTERVAL_SEC": str(interval),
            "THUMB_TILE_COLS": str(cols),
            "THUMB_TILE_ROWS": str(rows),
            "THUMB_WIDTH": str(thumb_w),
            "THUMB_HEIGHT": str(thumb_h),
            "TRICKPLAY_SPRITE_FORMAT": "jpg",
            "TRICKPLAY_SPRITE_QUALITY": "85",
            "TRICKPLAY_MAX_SPRITES": "5",
        }

        def fake_get(key: str, default=None):
            return config.get(key, default)

        def fake_ffprobe(path: str):
            assert path == str(source)
            return {"format": {"duration": "30"}}

        def fake_analyze(probe, strict=False):
            assert probe["format"]["duration"] == "30"
            return {"duration": 30.0}

        def fake_ffmpeg_path():
            return "/usr/bin/ffmpeg"

        def fake_run(cmd, capture_output=True, text=True):
            pattern = Path(cmd[-1])
            pattern.parent.mkdir(parents=True, exist_ok=True)
            # Produce five thumbnails so we exercise sprite tiling
            for idx in range(5):
                img = Image.new("RGB", (thumb_w, thumb_h), color=(idx * 10, 0, 0))
                img.save(pattern.parent / f"thumb_{idx+1:05d}.jpg")

            class Result:
                returncode = 0
                stdout = ""
                stderr = ""

            return Result()

        with patch("function_app.shared.trickplay.get", side_effect=fake_get), patch(
            "function_app.shared.trickplay.ffprobe_inspect",
            side_effect=fake_ffprobe,
        ), patch(
            "function_app.shared.trickplay.analyze_media",
            side_effect=fake_analyze,
        ), patch(
            "function_app.shared.trickplay.ffmpeg_path",
            side_effect=fake_ffmpeg_path,
        ), patch(
            "function_app.shared.trickplay.subprocess.run",
            side_effect=fake_run,
        ):
            result = generate_trickplay_assets(str(source), str(output_root))

        assert result.enabled is True
        assert result.vtt == "thumbnails.vtt"
        assert result.interval_sec == interval
        assert result.tile_width == thumb_w
        assert result.tile_height == thumb_h
        assert result.frame_total == 5
        assert result.sprites is not None and len(result.sprites) >= 2

        vtt_path = output_root / "thumbnails.vtt"
        assert vtt_path.exists(), "Expected WebVTT manifest to be written"
        vtt_content = vtt_path.read_text()
        assert "sprite_0001" in vtt_content
        assert "00:00:00.000" in vtt_content

        sprite_files = [output_root / art.sprite for art in result.sprites or []]
        for sprite_path in sprite_files:
            assert sprite_path.exists(), f"sprite missing: {sprite_path}"
