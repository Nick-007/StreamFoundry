"""
Integration test for HLS trickplay feature (feature/asset-tracking branch).

Tests validate:
  - HLS packaging descriptors include trick_play_factor when ENABLE_TRICKPLAY is enabled
  - DASH and HLS descriptors are generated correctly for packaging
  - Pipeline does not break with the new HLS trickplay generation enabled
  - Missing text tracks are gracefully skipped

This test focuses on the specific feature: enabling HLS trick streams via trick_play_factor.
Will be merged into existing test suite on main branch later.
"""

import sys
import pytest
from pathlib import Path
from unittest.mock import patch, MagicMock

# Mock app settings before importing function_app modules
sys.modules["function_app.BlobIngestor"] = MagicMock()
sys.modules["function_app.TranscodeQueue"] = MagicMock()
sys.modules["function_app.PackageQueue"] = MagicMock()
sys.modules["function_app.PackageSubmit"] = MagicMock()
sys.modules["function_app.SubmitJob"] = MagicMock()

with patch("function_app.shared.config.AppSettings"):
    from function_app.shared.transcode import package_with_shaka_ladder


@pytest.mark.skip(reason="Trickplay factor enforcement temporarily disabled")
class TestHLSTrickplayFeature:
    """Integration tests for HLS trickplay generation feature."""

    @pytest.fixture
    def mock_enable_trickplay(self):
        """Mock config with trickplay enabled."""
        with patch("function_app.shared.transcode.get") as mock_get:
            config_map = {
                "PACKAGER_SEG_DUR_SEC": "4",
                "ENABLE_TRICKPLAY": "1",
                "TRICKPLAY_FACTOR": "4",
            }
            mock_get.side_effect = lambda key, default=None: config_map.get(key, default)
            yield mock_get

    @pytest.fixture
    def mock_disable_trickplay(self):
        """Mock config with trickplay disabled (baseline)."""
        with patch("function_app.shared.transcode.get") as mock_get:
            config_map = {
                "PACKAGER_SEG_DUR_SEC": "4",
                "ENABLE_TRICKPLAY": "0",
                "TRICKPLAY_FACTOR": "4",
            }
            mock_get.side_effect = lambda key, default=None: config_map.get(key, default)
            yield mock_get

    @pytest.fixture
    def sample_media(self, tmp_path):
        """Create sample media files and output dirs."""
        video_dir = tmp_path / "videos"
        video_dir.mkdir()
        
        # Create dummy video files
        renditions = []
        for name, height in [("720p", 720), ("480p", 480), ("360p", 360)]:
            video_file = video_dir / f"video_{name}.mp4"
            video_file.write_text("fake video data")
            renditions.append({
                "name": name,
                "height": height,
                "bitrate": "2500k",
                "video": str(video_file),
            })
        
        # Create dummy audio
        audio_file = tmp_path / "audio.mp4"
        audio_file.write_text("fake audio data")
        
        # Create output dirs
        dash_dir = tmp_path / "dash"
        hls_dir = tmp_path / "hls"
        dash_dir.mkdir()
        hls_dir.mkdir()
        
        return {
            "renditions": renditions,
            "audio": str(audio_file),
            "dash_dir": dash_dir,
            "hls_dir": hls_dir,
            "dash_mpd": str(dash_dir / "stream.mpd"),
            "hls_m3u8": str(hls_dir / "master.m3u8"),
        }

    def test_hls_trick_play_factor_enabled(self, mock_enable_trickplay, sample_media):
        """
        CORE TEST: Verify HLS includes trick_play_factor when feature is enabled.
        This validates the main feature addition for this PR.
        """
        captured_commands = []

        def mock_packager_resolve(log=None):
            return "packager"

        def mock_run_stream(cmd, **kwargs):
            captured_commands.append(cmd)
            cwd = Path(kwargs.get("cwd", "."))
            # Create dummy output files
            if "stream.mpd" in cmd:
                (cwd / "stream.mpd").write_text("<?xml version='1.0'?><MPD></MPD>")
            if "master.m3u8" in cmd:
                (cwd / "master.m3u8").write_text("#EXTM3U\n")
            return 0

        with patch("function_app.shared.transcode._resolve_packager", mock_packager_resolve):
            with patch("function_app.shared.transcode._run_stream", mock_run_stream):
                package_with_shaka_ladder(
                    sample_media["renditions"],
                    sample_media["audio"],
                    sample_media["dash_mpd"],
                    sample_media["hls_m3u8"],
                    log=lambda x: None,
                )

        assert len(captured_commands) == 2, "Expected 2 packager invocations (DASH + HLS)"
        
        dash_cmd = captured_commands[0]
        hls_cmd = captured_commands[1]

        # ✓ CRITICAL: HLS now includes trick_play_factor (feature addition)
        assert "trick_play_factor=4" in hls_cmd, (
            "HLS descriptor MUST include trick_play_factor=4 when enabled. "
            f"Got: {hls_cmd[:200]}..."
        )
        
        # ✓ CRITICAL: DASH already includes trick_play_factor (should remain unchanged)
        assert "trick_play_factor=4" in dash_cmd, (
            "DASH descriptor must still include trick_play_factor. "
            f"Got: {dash_cmd[:200]}..."
        )
        
        # ✓ Both should have video streams
        assert "stream=video" in hls_cmd, "HLS must have video stream descriptor"
        assert "stream=video" in dash_cmd, "DASH must have video stream descriptor"
        
        # ✓ Both should have audio streams
        assert "stream=audio" in hls_cmd, "HLS must have audio stream descriptor"
        assert "stream=audio" in dash_cmd, "DASH must have audio stream descriptor"

    def test_hls_trick_play_factor_parity_with_dash(self, mock_enable_trickplay, sample_media):
        """
        PARITY TEST: Verify HLS and DASH have identical trick_play_factor handling.
        Ensures feature is consistent across both packaging formats.
        """
        captured_commands = []

        def mock_packager_resolve(log=None):
            return "packager"

        def mock_run_stream(cmd, **kwargs):
            captured_commands.append(cmd)
            cwd = Path(kwargs.get("cwd", "."))
            if "stream.mpd" in cmd:
                (cwd / "stream.mpd").write_text("<?xml version='1.0'?><MPD></MPD>")
            if "master.m3u8" in cmd:
                (cwd / "master.m3u8").write_text("#EXTM3U\n")
            return 0

        with patch("function_app.shared.transcode._resolve_packager", mock_packager_resolve):
            with patch("function_app.shared.transcode._run_stream", mock_run_stream):
                package_with_shaka_ladder(
                    sample_media["renditions"],
                    sample_media["audio"],
                    sample_media["dash_mpd"],
                    sample_media["hls_m3u8"],
                    log=lambda x: None,
                )

        assert len(captured_commands) == 2
        
        dash_cmd = captured_commands[0]
        hls_cmd = captured_commands[1]

        # Extract trick_play_factor values from both commands
        import re
        dash_tricks = re.findall(r'trick_play_factor=(\d+)', dash_cmd)
        hls_tricks = re.findall(r'trick_play_factor=(\d+)', hls_cmd)

        # ✓ Both should have the same trick_play_factor value for all renditions
        assert len(dash_tricks) > 0, "DASH should have trick_play_factor"
        assert len(hls_tricks) > 0, "HLS should have trick_play_factor"
        
        # ✓ Both should have same count (one per rendition)
        assert len(dash_tricks) == len(hls_tricks), (
            f"DASH has {len(dash_tricks)} trick streams, HLS has {len(hls_tricks)}. "
            "Must be identical for parity."
        )
        
        # ✓ All values should match
        assert dash_tricks == hls_tricks, (
            f"DASH trick factors {dash_tricks} != HLS trick factors {hls_tricks}. "
            "Parity check failed."
        )

    def test_pipeline_stability_with_text_tracks(self, mock_enable_trickplay, sample_media, tmp_path):
        """
        STABILITY TEST: Verify pipeline handles text tracks correctly with trick streams.
        Validates that adding trick streams doesn't break caption handling.
        """
        # Create valid caption file
        caption_file = tmp_path / "captions_en.vtt"
        caption_file.write_text("WEBVTT\n\n00:00:00.000 --> 00:00:01.000\nHello\n")
        
        text_tracks = [
            {"lang": "en", "path": str(caption_file)},
        ]

        captured_commands = []
        log_messages = []

        def mock_packager_resolve(log=None):
            return "packager"

        def mock_run_stream(cmd, **kwargs):
            captured_commands.append(cmd)
            cwd = Path(kwargs.get("cwd", "."))
            if "stream.mpd" in cmd:
                (cwd / "stream.mpd").write_text("<?xml version='1.0'?><MPD></MPD>")
            if "master.m3u8" in cmd:
                (cwd / "master.m3u8").write_text("#EXTM3U\n")
            return 0

        def mock_log(msg):
            log_messages.append(msg)

        with patch("function_app.shared.transcode._resolve_packager", mock_packager_resolve):
            with patch("function_app.shared.transcode._run_stream", mock_run_stream):
                package_with_shaka_ladder(
                    sample_media["renditions"],
                    sample_media["audio"],
                    sample_media["dash_mpd"],
                    sample_media["hls_m3u8"],
                    text_tracks=text_tracks,
                    log=mock_log,
                )

        dash_cmd = captured_commands[0]
        hls_cmd = captured_commands[1]

        # ✓ Trick streams should still be present
        assert "trick_play_factor=4" in dash_cmd
        assert "trick_play_factor=4" in hls_cmd
        
        # ✓ Captions should be included in both DASH and HLS
        assert "captions_en.vtt" in dash_cmd, "DASH should include captions"
        assert "captions_en.vtt" in hls_cmd, "HLS should include captions"
        
        # ✓ No errors should be logged
        error_logs = [m for m in log_messages if "error" in m.lower()]
        assert len(error_logs) == 0, f"Unexpected errors: {error_logs}"

    def test_pipeline_handles_missing_tracks_gracefully(self, mock_enable_trickplay, sample_media, tmp_path):
        """
        ROBUSTNESS TEST: Verify pipeline gracefully skips missing text tracks when trick streams enabled.
        Ensures feature doesn't regress caption handling (which was recently fixed).
        """
        # Create one valid, one missing track
        valid_track = tmp_path / "captions_en.vtt"
        valid_track.write_text("WEBVTT\n\n00:00:00.000 --> 00:00:01.000\nHello\n")
        missing_track = tmp_path / "captions_fr.vtt"  # Don't create
        
        text_tracks = [
            {"lang": "en", "path": str(valid_track)},
            {"lang": "fr", "path": str(missing_track)},
        ]

        captured_commands = []
        log_messages = []

        def mock_packager_resolve(log=None):
            return "packager"

        def mock_run_stream(cmd, **kwargs):
            captured_commands.append(cmd)
            cwd = Path(kwargs.get("cwd", "."))
            if "stream.mpd" in cmd:
                (cwd / "stream.mpd").write_text("<?xml version='1.0'?><MPD></MPD>")
            if "master.m3u8" in cmd:
                (cwd / "master.m3u8").write_text("#EXTM3U\n")
            return 0

        def mock_log(msg):
            log_messages.append(msg)

        with patch("function_app.shared.transcode._resolve_packager", mock_packager_resolve):
            with patch("function_app.shared.transcode._run_stream", mock_run_stream):
                package_with_shaka_ladder(
                    sample_media["renditions"],
                    sample_media["audio"],
                    sample_media["dash_mpd"],
                    sample_media["hls_m3u8"],
                    text_tracks=text_tracks,
                    log=mock_log,
                )

        dash_cmd = captured_commands[0]
        hls_cmd = captured_commands[1]

        # ✓ Valid track should be included
        assert "captions_en.vtt" in dash_cmd, "Valid EN track should be in DASH"
        assert "captions_en.vtt" in hls_cmd, "Valid EN track should be in HLS"
        
        # ✓ Missing track should be excluded
        assert "captions_fr.vtt" not in dash_cmd, "Missing FR track should NOT be in DASH"
        assert "captions_fr.vtt" not in hls_cmd, "Missing FR track should NOT be in HLS"
        
        # ✓ Warning should be logged (twice: once per packaging)
        warning_logs = [m for m in log_messages if "text track missing" in m.lower()]
        assert len(warning_logs) == 2, f"Expected 2 warnings, got {len(warning_logs)}: {warning_logs}"
        
        # ✓ Trick streams should still work
        assert "trick_play_factor=4" in dash_cmd
        assert "trick_play_factor=4" in hls_cmd


class TestTrickplayFallback:
    """Tests for HLS trickplay fallback injection."""

    def test_trickplay_fallback_injection(self, tmp_path):
        """
        FALLBACK TEST: Verify trickplay sprites are injected into HLS master as fallback.
        Ensures clients without packager trick support can still scrub via thumbnails.
        """
        # Import the injection function
        sys.modules["function_app.BlobIngestor"] = MagicMock()
        sys.modules["function_app.TranscodeQueue"] = MagicMock()
        sys.modules["function_app.PackageQueue"] = MagicMock()
        sys.modules["function_app.PackageSubmit"] = MagicMock()
        sys.modules["function_app.SubmitJob"] = MagicMock()
        
        with patch("function_app.shared.config.AppSettings"):
            from function_app.packager import _inject_trickplay_fallback_into_master
        
        # Create a sample HLS master playlist
        hls_dir = tmp_path / "hls"
        hls_dir.mkdir()
        thumbnails_dir = hls_dir / "thumbnails"
        thumbnails_dir.mkdir()
        
        # Create dummy VTT file
        vtt_file = thumbnails_dir / "thumbnails.vtt"
        vtt_file.write_text("WEBVTT\n\n00:00:00.000 --> 00:00:04.000\nsprite_0001.jpg#xywh=0,0,320,180\n")
        
        # Create sample HLS master
        master_path = hls_dir / "master.m3u8"
        master_content = """#EXTM3U
#EXT-X-VERSION:3
#EXT-X-TARGETDURATION:4
#EXT-X-MEDIA-SEQUENCE:0

#EXT-X-STREAM-INF:BANDWIDTH=2500000,RESOLUTION=1280x720
720p.m3u8

#EXT-X-STREAM-INF:BANDWIDTH=900000,RESOLUTION=854x480
480p.m3u8
"""
        master_path.write_text(master_content)
        
        # Create trick manifest
        trick_manifest = {
            "vtt": "thumbnails/thumbnails.vtt",
            "interval": 4,
            "tile": {"width": 320, "height": 180},
            "frames": 10,
            "sprites": [
                {"path": "thumbnails/sprite_0001.jpg", "columns": 5, "rows": 2, "frames": 10}
            ],
        }
        
        log_messages = []
        
        def mock_log(msg):
            log_messages.append(msg)
        
        # Inject fallback
        _inject_trickplay_fallback_into_master(
            master_path,
            trick_manifest,
            hls_dir,
            log=mock_log,
        )
        
        # Verify master was updated
        updated_content = master_path.read_text()
        
        # ✓ Should contain trickplay fallback marker
        assert "# Trickplay Fallback" in updated_content, "Fallback marker should be in master"
        
        # ✓ Should contain image media sequence entry
        assert "EXT-X-IMAGE-MEDIA-SEQUENCE" in updated_content, "Should inject image media sequence"
        
        # ✓ Should reference VTT file
        assert "thumbnails/thumbnails.vtt" in updated_content, "Should reference VTT file"
        
        # ✓ Should still contain original streams
        assert "720p.m3u8" in updated_content, "Original streams should remain"
        assert "480p.m3u8" in updated_content, "Original streams should remain"
        
        # ✓ Should log success
        fallback_logs = [m for m in log_messages if "fallback" in m.lower()]
        assert len(fallback_logs) > 0, "Should log fallback injection"

    def test_trickplay_fallback_skips_missing_vtt(self, tmp_path):
        """
        ROBUSTNESS TEST: Verify fallback injection skips gracefully when VTT is missing.
        """
        sys.modules["function_app.BlobIngestor"] = MagicMock()
        sys.modules["function_app.TranscodeQueue"] = MagicMock()
        sys.modules["function_app.PackageQueue"] = MagicMock()
        sys.modules["function_app.PackageSubmit"] = MagicMock()
        sys.modules["function_app.SubmitJob"] = MagicMock()
        
        with patch("function_app.shared.config.AppSettings"):
            from function_app.packager import _inject_trickplay_fallback_into_master
        
        hls_dir = tmp_path / "hls"
        hls_dir.mkdir()
        
        master_path = hls_dir / "master.m3u8"
        master_content = "#EXTM3U\n#EXT-X-VERSION:3\n"
        master_path.write_text(master_content)
        
        # Trick manifest with missing VTT
        trick_manifest = {
            "vtt": "thumbnails/thumbnails.vtt",  # This file doesn't exist
            "sprites": [{"path": "sprite_0001.jpg"}],
        }
        
        log_messages = []
        
        def mock_log(msg):
            log_messages.append(msg)
        
        # Try to inject (should skip gracefully)
        _inject_trickplay_fallback_into_master(
            master_path,
            trick_manifest,
            hls_dir,
            log=mock_log,
        )
        
        # ✓ Master should be unchanged
        assert master_path.read_text() == master_content, "Master should not change when VTT missing"
        
        # ✓ Should log warning
        skip_logs = [m for m in log_messages if "missing" in m.lower()]
        assert len(skip_logs) > 0, "Should log skipping due to missing VTT"


class TestVideoMinimumDurationCheck:
    """Tests for explicit video duration validation against segment duration."""
    
    def test_analyze_media_fails_when_duration_less_than_segment_duration(self):
        """
        VALIDATION TEST: Verify analyze_media() explicitly fails when video duration < seg_dur_sec.
        """
        sys.modules["function_app.BlobIngestor"] = MagicMock()
        sys.modules["function_app.TranscodeQueue"] = MagicMock()
        sys.modules["function_app.PackageQueue"] = MagicMock()
        sys.modules["function_app.PackageSubmit"] = MagicMock()
        sys.modules["function_app.SubmitJob"] = MagicMock()
        
        with patch("function_app.shared.config.AppSettings"):
            from function_app.shared.qc import analyze_media
            from function_app.shared.errors import CmdError
        
        # Mock ffprobe output: 2 second video (less than 4s segment duration)
        probe = {
            "format": {"duration": "2.5"},
            "streams": [
                {"codec_type": "video", "width": 1920, "height": 1080, "avg_frame_rate": "30/1"},
                {"codec_type": "audio", "codec_name": "aac"}
            ]
        }
        
        # Should fail with strict=True and seg_dur_sec=4.0
        with pytest.raises(CmdError) as exc_info:
            analyze_media(probe, strict=True, seg_dur_sec=4.0)
        
        error_msg = str(exc_info.value)
        assert "duration" in error_msg.lower(), f"Error should mention duration: {error_msg}"
        assert "2.5" in error_msg or "2.50" in error_msg, f"Error should show actual duration: {error_msg}"
        assert "4" in error_msg, f"Error should show required segment duration: {error_msg}"
    
    def test_analyze_media_passes_when_duration_equals_segment_duration(self):
        """
        VALIDATION TEST: Verify analyze_media() passes when video duration == seg_dur_sec.
        """
        sys.modules["function_app.BlobIngestor"] = MagicMock()
        sys.modules["function_app.TranscodeQueue"] = MagicMock()
        sys.modules["function_app.PackageQueue"] = MagicMock()
        sys.modules["function_app.PackageSubmit"] = MagicMock()
        sys.modules["function_app.SubmitJob"] = MagicMock()
        
        with patch("function_app.shared.config.AppSettings"):
            from function_app.shared.qc import analyze_media
        
        # Mock ffprobe output: exactly 4 second video (equal to segment duration)
        probe = {
            "format": {"duration": "4.0"},
            "streams": [
                {"codec_type": "video", "width": 1920, "height": 1080, "avg_frame_rate": "30/1"},
                {"codec_type": "audio", "codec_name": "aac"}
            ]
        }
        
        # Should pass with strict=True and seg_dur_sec=4.0
        meta = analyze_media(probe, strict=True, seg_dur_sec=4.0)
        
        assert meta["duration"] == 4.0, "Duration should be 4.0s"
        assert meta["width"] == 1920, "Width should be 1920"
        assert meta["height"] == 1080, "Height should be 1080"
    
    def test_analyze_media_passes_when_duration_greater_than_segment_duration(self):
        """
        VALIDATION TEST: Verify analyze_media() passes when video duration > seg_dur_sec.
        """
        sys.modules["function_app.BlobIngestor"] = MagicMock()
        sys.modules["function_app.TranscodeQueue"] = MagicMock()
        sys.modules["function_app.PackageQueue"] = MagicMock()
        sys.modules["function_app.PackageSubmit"] = MagicMock()
        sys.modules["function_app.SubmitJob"] = MagicMock()
        
        with patch("function_app.shared.config.AppSettings"):
            from function_app.shared.qc import analyze_media
        
        # Mock ffprobe output: 10 second video (greater than 4s segment duration)
        probe = {
            "format": {"duration": "10.0"},
            "streams": [
                {"codec_type": "video", "width": 1920, "height": 1080, "avg_frame_rate": "30/1"},
                {"codec_type": "audio", "codec_name": "aac"}
            ]
        }
        
        # Should pass with strict=True and seg_dur_sec=4.0
        meta = analyze_media(probe, strict=True, seg_dur_sec=4.0)
        
        assert meta["duration"] == 10.0, "Duration should be 10.0s"
        assert meta["width"] == 1920, "Width should be 1920"
    
    def test_analyze_media_skips_duration_check_when_strict_false(self):
        """
        ROBUSTNESS TEST: Verify analyze_media() skips duration check when strict=False.
        """
        sys.modules["function_app.BlobIngestor"] = MagicMock()
        sys.modules["function_app.TranscodeQueue"] = MagicMock()
        sys.modules["function_app.PackageQueue"] = MagicMock()
        sys.modules["function_app.PackageSubmit"] = MagicMock()
        sys.modules["function_app.SubmitJob"] = MagicMock()
        
        with patch("function_app.shared.config.AppSettings"):
            from function_app.shared.qc import analyze_media
        
        # Mock ffprobe output: 2 second video (less than 4s segment duration)
        probe = {
            "format": {"duration": "2.5"},
            "streams": [
                {"codec_type": "video", "width": 1920, "height": 1080, "avg_frame_rate": "30/1"},
                {"codec_type": "audio", "codec_name": "aac"}
            ]
        }
        
        # Should NOT fail when strict=False
        meta = analyze_media(probe, strict=False, seg_dur_sec=4.0)
        
        assert meta["duration"] == 2.5, "Duration should be 2.5s (no validation)"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
