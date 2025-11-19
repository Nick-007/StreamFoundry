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

from function_app import SubmitJob  # noqa: E402


class DummyStreamLogger:
    def __init__(self, *_, **__):
        self.job = MagicMock()

    def start(self, *_, **__):
        return None

    def stop(self, *_, **__):
        return None


def make_request(body: dict, *, method: str = "POST", params: dict | None = None) -> func.HttpRequest:
    return func.HttpRequest(
        method=method,
        url="/api/submit",
        headers={},
        params=params or {},
        route_params={},
        body=json.dumps(body).encode("utf-8"),
    )


@patch.object(SubmitJob, "_rate_limit_ok", return_value=True)
@patch.object(SubmitJob, "ensure_containers")
def test_submit_job_requires_source(_, __):
    req = make_request({"foo": "bar"})
    resp = SubmitJob.handle_submit_job(req)
    assert resp.status_code == 400
    body = json.loads(resp.get_body())
    assert body["error"].startswith("missing 'source'")


def test_submit_job_status_mode_returns_existing_status():
    req = make_request({"source": "raw/demo.mp4", "mode": "status"})
    with patch.object(SubmitJob, "ensure_containers"), \
        patch.object(SubmitJob, "_rate_limit_ok", return_value=True), \
        patch.object(SubmitJob, "StreamLogger", DummyStreamLogger), \
        patch.object(SubmitJob, "bridge_logger", return_value=lambda *_a, **_k: (lambda *_: None)), \
        patch.object(SubmitJob, "get_raw_status", return_value={"status": "complete"}), \
        patch.object(SubmitJob, "select_pipeline_for_blob", return_value={"id": "transcode", "queue": "transcode-jobs"}), \
        patch.object(SubmitJob, "set_raw_status"):
        resp = SubmitJob.handle_submit_job(req)
    assert resp.status_code == 200
    payload = json.loads(resp.get_body())
    assert payload["status"]["status"] == "complete"
    assert payload["pipeline"] == "transcode"


def test_submit_job_enqueues_payload():
    req = make_request({"source": "incoming/demo.mp4", "name": "uploads/demo"})
    with patch.object(SubmitJob, "ensure_containers"), \
        patch.object(SubmitJob, "_rate_limit_ok", return_value=True), \
        patch.object(SubmitJob, "enqueue") as enqueue_mock, \
        patch.object(SubmitJob, "job_paths", return_value=SimpleNamespace(dist_dir=Path("/tmp/dist"))), \
        patch.object(SubmitJob, "select_pipeline_for_blob", return_value={"id": "transcode", "queue": "transcode-jobs"}), \
        patch.object(SubmitJob, "set_raw_status"), \
        patch.object(SubmitJob, "get_raw_status", return_value={}), \
        patch.object(SubmitJob, "blob_exists", side_effect=[False, False]), \
        patch.object(SubmitJob, "copy_blob"), \
        patch.object(SubmitJob, "normalize_only_rung", return_value=["720p"]), \
        patch.object(SubmitJob, "receipt_payload", return_value={"receipt": True}) as receipt_mock, \
        patch.object(SubmitJob, "log_job"), \
        patch.object(SubmitJob, "bridge_logger", return_value=lambda *_a, **_k: (lambda *_: None)), \
        patch.object(SubmitJob, "StreamLogger", DummyStreamLogger):
        resp = SubmitJob.handle_submit_job(req)

    assert resp.status_code == 202
    queue_name, payload_json = enqueue_mock.call_args.args
    assert queue_name == "transcode-jobs"
    payload = json.loads(payload_json)
    assert payload["in"]["key"].endswith("uploads/demo.mp4")
    assert payload["extra"]["pipeline"] == "transcode"
    assert receipt_mock.called
