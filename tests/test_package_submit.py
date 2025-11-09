import json
import os
import sys
from types import SimpleNamespace
from pathlib import Path
from unittest.mock import MagicMock, patch

import azure.functions as func

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

with open(ROOT / "local.settings.json") as f:
    for key, value in json.load(f).get("Values", {}).items():
        os.environ.setdefault(key, str(value))

from function_app import PackageSubmit  # noqa: E402


class DummyOut:
    def __init__(self):
        self.value = None

    def set(self, val):
        self.value = val


def make_request(body, *, params=None, headers=None):
    payload = body if isinstance(body, (bytes, bytearray)) else json.dumps(body).encode("utf-8")
    return func.HttpRequest(
        method="POST",
        url="/api/package/submit",
        params=params or {},
        headers=headers or {},
        route_params={},
        body=payload,
    )


def test_package_submit_rejects_invalid_json():
    req = make_request(b"not-json")
    out = DummyOut()
    resp = PackageSubmit.handle_package_submit(req, out)
    assert resp.status_code == 400


@patch("function_app.PackageSubmit.validate_and_normalize", return_value=SimpleNamespace(id="job-123"))
@patch("function_app.PackageSubmit.dump_normalized", return_value={"id": "job-123"})
def test_package_submit_async_enqueues(_, __):
    req = make_request({"id": "job-123", "foo": "bar"})
    out = DummyOut()
    resp = PackageSubmit.handle_package_submit(req, out)
    assert resp.status_code == 202
    assert json.loads(out.value)["id"] == "job-123"


@patch("function_app.PackageSubmit._handle_packaging")
@patch("function_app.PackageSubmit.validate_and_normalize", return_value=SimpleNamespace(id="job-123"))
def test_package_submit_sync_calls_packager(_, mock_handle):
    req = make_request({"id": "job-123"}, params={"mode": "sync"})
    out = DummyOut()
    resp = PackageSubmit.handle_package_submit(req, out)
    assert resp.status_code == 200
    mock_handle.assert_called_once()
