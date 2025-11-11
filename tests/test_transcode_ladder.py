import json
import os
import sys
import shutil
import subprocess
from pathlib import Path
from typing import Dict
import pytest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

@pytest.fixture
def work_dir():
    """Creates and cleans up the test work directory."""
    work_dir = Path(ROOT) / "tests" / "work"
    # Clean up any leftovers from previous failed runs
    if work_dir.exists():
        shutil.rmtree(work_dir)
    work_dir.mkdir(exist_ok=True)
    yield work_dir
    # Clean up after tests
    shutil.rmtree(work_dir)

# Load test environment from local.settings.json
with open(ROOT / "local.settings.json") as f:
    for key, value in json.load(f).get("Values", {}).items():
        os.environ.setdefault(key, str(value))

from function_app.shared.transcode import transcode_to_cmaf_ladder
from function_app.shared.errors import CmdError

def create_test_video(output_path: str, duration: int = 3, size: str = "640x360", fps: int = 30) -> Dict:
    """Creates a synthetic test video with known properties."""
    cmd = [
        "ffmpeg", "-y",
        "-f", "lavfi", "-i", f"testsrc=duration={duration}:size={size}:rate={fps}",
        "-f", "lavfi", "-i", f"sine=frequency=440:duration={duration}",
        "-c:v", "libx264", "-preset", "ultrafast", "-b:v", "1000k",
        "-c:a", "aac", "-b:a", "128k",
        output_path
    ]
    subprocess.run(cmd, check=True, capture_output=True)
    
    # Return expected metadata
    w, h = map(int, size.split("x"))
    return {
        "width": w,
        "height": h,
        "fps": float(fps),
        "duration": float(duration)
    }

def test_transcode_ladder_basic(work_dir):
    """Tests basic transcoding of a synthetic video with expected metadata."""
    input_path = work_dir / "test_input.mp4"
    
    # Generate test input with known properties
    expected_meta = create_test_video(str(input_path))
    
    # Run the transcode
    audio_mp4, renditions, meta = transcode_to_cmaf_ladder(
        str(input_path),
        str(work_dir),
        only_rungs=["240p", "360p"],  # Limit rungs for faster test
        log=lambda s: None  # Quiet logging
    )
    
    # Verify metadata matches input
    assert meta["width"] == expected_meta["width"]
    assert meta["height"] == expected_meta["height"]
    assert abs(meta["fps"] - expected_meta["fps"]) < 0.1
    assert abs(meta["duration"] - expected_meta["duration"]) < 0.1
    
    # Verify outputs exist and structure
    assert Path(audio_mp4).exists()
    assert Path(audio_mp4).name == "audio.mp4"
    
    # Verify renditions
    assert len(renditions) == 2
    heights = sorted(r["height"] for r in renditions)
    assert heights == [240, 360]
    
    # Verify all video files exist
    for r in renditions:
        assert Path(r["video"]).exists()
        assert Path(r["video"]).name == f"video_{r['name']}.mp4"

def test_transcode_ladder_invalid_input(work_dir):
    """Tests error handling for invalid input video."""
    invalid_path = work_dir / "invalid.mp4"
    
    # Create an invalid MP4 (just some random bytes)
    invalid_path.write_bytes(b"not a valid mp4 file")
    
    with pytest.raises(CmdError) as exc:
        transcode_to_cmaf_ladder(
            str(invalid_path),
            str(work_dir),
            log=lambda s: None
        )
    assert "ffprobe failed" in str(exc.value)

def test_transcode_ladder_missing_audio(work_dir):
    """Tests handling of input with no audio stream."""
    input_path = work_dir / "no_audio.mp4"
    
    # Create video without audio
    cmd = [
        "ffmpeg", "-y",
        "-f", "lavfi", "-i", "testsrc=duration=1:size=320x240:rate=30",
        "-c:v", "libx264", "-preset", "ultrafast",
        str(input_path)
    ]
    subprocess.run(cmd, check=True, capture_output=True)
    
    with pytest.raises(CmdError) as exc:
        transcode_to_cmaf_ladder(
            str(input_path),
            str(work_dir),
            log=lambda s: None
        )
    assert "no audio stream" in str(exc.value).lower()

def test_transcode_ladder_callbacks(work_dir):
    """Tests that completion callbacks are called correctly."""
    input_path = work_dir / "test_input.mp4"
    
    # Create test input
    create_test_video(str(input_path))
    
    audio_done = False
    rung_done = []
    
    def on_audio(path):
        nonlocal audio_done
        audio_done = True
        assert Path(path).exists()
    
    def on_rung(name, path):
        rung_done.append(name)
        assert Path(path).exists()
    
    audio_mp4, renditions, _ = transcode_to_cmaf_ladder(
        str(input_path),
        str(work_dir),
        only_rungs=["240p"],
        on_audio_done=on_audio,
        on_rung_done=on_rung,
        log=lambda s: None
    )
    
    assert audio_done
    assert len(rung_done) == 1
    assert rung_done[0] == "240p"

def test_transcode_ladder_gpu_fallback(work_dir):
    """Tests NVENC → libx264 fallback when GPU encoding fails."""
    input_path = work_dir / "test_input.mp4"
    
    # Create test input
    create_test_video(str(input_path))
    
    # Force NVENC but expect fallback
    with patch.dict(os.environ, {"VIDEO_CODEC": "h264_nvenc"}):
        audio_mp4, renditions, _ = transcode_to_cmaf_ladder(
            str(input_path),
            str(work_dir),
            only_rungs=["240p"],
            log=lambda s: None
        )
        
        # Should succeed with fallback
        assert Path(audio_mp4).exists()
        assert len(renditions) == 1

def test_transcode_ladder_invalid_rungs(work_dir):
    """Tests error handling for invalid rung selection."""
    input_path = work_dir / "test_input.mp4"
    
    create_test_video(str(input_path))
    
    with pytest.raises(CmdError) as exc:
        transcode_to_cmaf_ladder(
            str(input_path),
            str(work_dir),
            only_rungs=["invalid_rung"],
            log=lambda s: None
        )
    assert "not in ladder" in str(exc.value)