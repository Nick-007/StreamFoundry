from __future__ import annotations

import math
import subprocess
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Dict, List, Optional

from PIL import Image

from .config import get
from .errors import CmdError
from .qc import analyze_media, ffprobe_inspect
from .tools import ffmpeg_path

LogFn = Optional[Callable[[str], None]]


@dataclass
class TrickplayArtifact:
    sprite: str
    columns: int
    rows: int
    frame_count: int


@dataclass
class TrickplayResult:
    enabled: bool
    vtt: Optional[str] = None
    sprites: Optional[List[TrickplayArtifact]] = None
    tile_width: Optional[int] = None
    tile_height: Optional[int] = None
    interval_sec: Optional[int] = None
    frame_total: int = 0


def _log(logger: LogFn, message: str) -> None:
    if logger:
        try:
            logger(message)
        except Exception:
            pass


def _get_int(key: str, default: int) -> int:
    try:
        return int(str(get(key, default)).strip() or default)
    except Exception:
        return default


def _get_str(key: str, default: str) -> str:
    val = get(key, default)
    if val is None:
        return default
    return str(val).strip() or default


def _format_ts(seconds: float) -> str:
    if seconds < 0:
        seconds = 0.0
    ms = int(round(seconds * 1000))
    hrs, rem = divmod(ms, 3_600_000)
    mins, rem = divmod(rem, 60_000)
    secs, millis = divmod(rem, 1000)
    return f"{hrs:02d}:{mins:02d}:{secs:02d}.{millis:03d}"


def generate_trickplay_assets(
    source_video: str,
    output_root: str,
    *,
    log: LogFn = None,
) -> TrickplayResult:
    """
    Generate trick-play sprites + WebVTT companion from the given video.

    Returns TrickplayResult describing generated assets. When trick-play is
    disabled or generation fails gracefully, enabled=False and no assets are
    produced.
    """
    enabled_flag = str(get("ENABLE_TRICKPLAY", "true")).lower() in ("1", "true", "yes", "on")
    if not enabled_flag:
        return TrickplayResult(enabled=False)

    video_path = Path(source_video)
    if not video_path.exists():
        raise CmdError(f"Trick-play: video source missing: {source_video}")

    interval_sec = max(1, _get_int("THUMB_INTERVAL_SEC", 4))
    cols = max(1, _get_int("THUMB_TILE_COLS", 5))
    max_rows = max(1, _get_int("THUMB_TILE_ROWS", 5))
    thumb_width = max(32, _get_int("THUMB_WIDTH", 320))
    thumb_height_cfg = _get_int("THUMB_HEIGHT", 0)
    sprite_format = _get_str("TRICKPLAY_SPRITE_FORMAT", "jpg").lower()
    sprite_quality = max(1, min(100, _get_int("TRICKPLAY_SPRITE_QUALITY", 80)))
    max_sprites = max(1, _get_int("TRICKPLAY_MAX_SPRITES", 50))

    probe = ffprobe_inspect(str(video_path))
    meta = analyze_media(probe, strict=False)
    duration = float(meta.get("duration") or 0.0)
    if duration <= 0:
        # Fallback: still try to produce a couple frames; ffmpeg will clamp.
        duration = interval_sec

    output_dir = Path(output_root)
    output_dir.mkdir(parents=True, exist_ok=True)
    sprite_dir = output_dir

    with tempfile.TemporaryDirectory(prefix="sf-trick-") as tmp_dir:
        tmp_path = Path(tmp_dir)
        pattern = tmp_path / "thumb_%05d.jpg"

        scale_expr = f"scale={thumb_width}:-2"
        if thumb_height_cfg > 0:
            scale_expr = f"scale={thumb_width}:{thumb_height_cfg}"

        vf = f"fps=1/{interval_sec},{scale_expr}"
        cmd: List[str] = [
            ffmpeg_path(),
            "-hide_banner",
            "-nostdin",
            "-y",
            "-i",
            str(video_path),
            "-vf",
            vf,
            "-an",
        ]
        if sprite_format in ("jpg", "jpeg", "webp"):
            cmd += ["-q:v", str(sprite_quality)]
        elif sprite_format == "png":
            cmd += ["-compression_level", "6"]
        cmd += [str(pattern)]

        _log(log, f"[trick-play] extracting thumbs interval={interval_sec}s width={thumb_width}")
        proc = subprocess.run(cmd, capture_output=True, text=True)
        if proc.returncode != 0:
            tail = (proc.stderr or proc.stdout or "").splitlines()[-40:]
            raise CmdError(
                "ffmpeg thumbnail extraction failed "
                f"rc={proc.returncode}\n--- ffmpeg tail ---\n" + "\n".join(tail)
            )

        thumb_paths = sorted(tmp_path.glob("thumb_*.jpg"))
        if not thumb_paths:
            _log(log, "[trick-play] ffmpeg produced zero thumbnails; skipping generation")
            return TrickplayResult(enabled=True)

        # Trim to configured maximum sprite capacity
        tile_capacity = cols * max_rows
        max_frames = max_sprites * tile_capacity
        if len(thumb_paths) > max_frames:
            _log(
                log,
                f"[trick-play] limiting thumbnails {len(thumb_paths)} -> {max_frames} "
                f"(max_sprites={max_sprites}, capacity={tile_capacity})",
            )
            thumb_paths = thumb_paths[:max_frames]

        # Determine tile size from first thumbnail
        with Image.open(thumb_paths[0]) as first_img:
            tile_width, tile_height = first_img.size

        sprites: List[TrickplayArtifact] = []
        cues: List[str] = []
        frame_total = len(thumb_paths)

        for sprite_idx, chunk_start in enumerate(range(0, frame_total, tile_capacity)):
            chunk = thumb_paths[chunk_start : chunk_start + tile_capacity]
            if not chunk:
                continue
            rows = math.ceil(len(chunk) / cols)
            rows = min(rows, max_rows)
            sprite_canvas = Image.new("RGB", (tile_width * cols, tile_height * rows), color=(0, 0, 0))

            for idx, frame_path in enumerate(chunk):
                col = idx % cols
                row = idx // cols
                if row >= rows:
                    break
                with Image.open(frame_path) as thumb_img:
                    sprite_canvas.paste(thumb_img.convert("RGB"), (col * tile_width, row * tile_height))

            sprite_name = f"sprite_{sprite_idx + 1:04d}.{sprite_format}"
            sprite_path = sprite_dir / sprite_name

            save_kwargs: Dict[str, int | str] = {}
            if sprite_format in ("jpg", "jpeg"):
                save_kwargs["quality"] = sprite_quality
                save_kwargs["optimize"] = True
            elif sprite_format == "webp":
                save_kwargs["quality"] = sprite_quality
            pil_format = {
                "jpg": "JPEG",
                "jpeg": "JPEG",
                "png": "PNG",
                "webp": "WEBP",
            }.get(sprite_format, sprite_format.upper())
            sprite_canvas.save(sprite_path, format=pil_format, **save_kwargs)
            sprite_canvas.close()

            sprites.append(
                TrickplayArtifact(
                    sprite=sprite_name,
                    columns=cols,
                    rows=rows,
                    frame_count=len(chunk),
                )
            )

            for local_idx, frame_path in enumerate(chunk):
                absolute_index = chunk_start + local_idx
                start = absolute_index * interval_sec
                end = min(duration, (absolute_index + 1) * interval_sec)
                if absolute_index == frame_total - 1 and end <= start:
                    end = start + interval_sec

                if start > duration + interval_sec:
                    continue

                col = local_idx % cols
                row = local_idx // cols
                if row >= rows:
                    break
                x = col * tile_width
                y = row * tile_height
                cue = (
                    f"{_format_ts(start)} --> {_format_ts(end)}\n"
                    f"{sprite_name}#xywh={x},{y},{tile_width},{tile_height}\n"
                )
                cues.append(cue)

        if not cues:
            _log(log, "[trick-play] no cues generated; skipping output")
            for sprite in sprites:
                try:
                    (sprite_dir / sprite.sprite).unlink(missing_ok=True)
                except Exception:
                    pass
            return TrickplayResult(enabled=True)

        vtt_path = sprite_dir / "thumbnails.vtt"
        vtt_lines = ["WEBVTT\n"]
        vtt_lines.extend(cue + "\n" for cue in cues)
        vtt_path.write_text("".join(vtt_lines), encoding="utf-8")

        _log(
            log,
            f"[trick-play] generated {len(cues)} cues across {len(sprites)} "
            f"sprite(s) (tile={tile_width}x{tile_height})",
        )

        return TrickplayResult(
            enabled=True,
            vtt=vtt_path.name,
            sprites=sprites,
            tile_width=tile_width,
            tile_height=tile_height,
            interval_sec=interval_sec,
            frame_total=frame_total,
        )
