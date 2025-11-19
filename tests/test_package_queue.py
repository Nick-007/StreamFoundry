import json
import os
import sys
from types import SimpleNamespace
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

with open(ROOT / "local.settings.json") as f:
    for key, value in json.load(f).get("Values", {}).items():
        os.environ.setdefault(key, str(value))

from function_app import PackageQueue  # noqa: E402


class DummyStreamLogger:
    def __init__(self, *_, **__):
        self.job = MagicMock()

    def start(self, *_, **__):
        return None

    def stop(self, *_, **__):
        return None


def make_message(payload: dict[str, object]) -> MagicMock:
    msg = MagicMock()
    msg.get_body.return_value = json.dumps(payload).encode("utf-8")
    return msg


def test_package_queue_raises_on_invalid_json():
    msg = MagicMock()
    msg.get_body.return_value = b"{invalid"
    with patch("function_app.PackageQueue.LOGGER") as logger:
        logger.error = MagicMock()
        with pytest.raises(json.JSONDecodeError):
            PackageQueue.handle_packaging_queue(msg, context=MagicMock())


def test_package_queue_processes_message():
    payload = SimpleNamespace(
        id="job-1",
        in_=SimpleNamespace(container="raw", key="input.mp4"),
        extra={"pipeline": "transcode"},
    )
    msg = make_message({"id": "job-1"})
    with patch("function_app.PackageQueue.validate_ingest", return_value=payload) as mock_validate, \
        patch("function_app.PackageQueue._handle_packaging") as mock_packager, \
        patch("function_app.PackageQueue.set_raw_status") as mock_set_status, \
        patch("function_app.PackageQueue.StreamLogger", DummyStreamLogger), \
        patch("function_app.PackageQueue.job_paths", return_value=SimpleNamespace(dist_dir=Path("/tmp/dist"))), \
        patch("function_app.PackageQueue.bridge_logger", return_value=lambda *_a, **_k: (lambda *_: None)), \
        patch("function_app.PackageQueue.make_slogger", return_value=(lambda *_a, **_k: None, lambda *_a, **_k: None)):
        PackageQueue.handle_packaging_queue(msg, context=MagicMock())

    mock_validate.assert_called_once()
    mock_packager.assert_called_once()
    assert mock_set_status.call_count >= 1
