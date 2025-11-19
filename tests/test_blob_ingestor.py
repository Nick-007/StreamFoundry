import json
import os
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import MagicMock, patch

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# Ensure AppSettings can instantiate by loading local.settings.json defaults for tests.
with open("local.settings.json") as f:
    settings = json.load(f).get("Values", {})
for key, value in settings.items():
    os.environ.setdefault(key, str(value))

import function_app.BlobEnqueuer as blob_ingestor


def _make_event(*, subject: str, event_type: str = "Microsoft.Storage.BlobCreated"):
    """Helper to fabricate lightweight Event Grid-like events for tests."""
    return SimpleNamespace(
        id="evt-1",
        data={"test": True},
        topic="/subscriptions/test/resourceGroups/test/providers/Microsoft.Storage/storageAccounts/test",
        subject=subject,
        event_type=event_type,
        data_version="1.0",
    )


class BlobEnqueuerTests(TestCase):
    def setUp(self):
        # Ensure module constants are predictable
        self.raw_container = blob_ingestor.RAW

    @patch("function_app.BlobEnqueuer.log_exception")
    @patch("function_app.BlobEnqueuer.log_job")
    @patch("function_app.BlobEnqueuer.enqueue")
    @patch("function_app.BlobEnqueuer.set_raw_status")
    @patch("function_app.BlobEnqueuer.select_pipeline_for_blob")
    @patch("function_app.BlobEnqueuer.blob_exists")
    @patch("function_app.BlobEnqueuer.ensure_containers")
    def test_event_grid_blob_created_enqueues_transcode_job(
        self,
        mock_ensure_containers: MagicMock,
        mock_blob_exists: MagicMock,
        mock_select_pipeline: MagicMock,
        mock_set_raw_status: MagicMock,
        mock_enqueue: MagicMock,
        mock_log_job: MagicMock,
        mock_log_exception: MagicMock,
    ):
        mock_blob_exists.return_value = False
        mock_select_pipeline.return_value = {"id": "transcode", "queue": blob_ingestor.TRANSCODE_QUEUE}

        subject = f"/blobServices/default/containers/{self.raw_container}/blobs/path/to/demo.mp4"
        event = _make_event(subject=subject)

        result = blob_ingestor._handle_event_grid_notification(event)

        assert result is True
        mock_ensure_containers.assert_called_once()
        mock_blob_exists.assert_called_once_with(blob_ingestor.PROCESSED, "demo/manifest.json")
        mock_select_pipeline.assert_called_once_with("path/to/demo.mp4")
        mock_set_raw_status.assert_called_once()
        mock_enqueue.assert_called_once()

        # The queue payload should match the submit schema subset
        queued_payload = json.loads(mock_enqueue.call_args.args[1])
        assert queued_payload["id"] == "demo"
        assert queued_payload["in"] == {"container": self.raw_container, "key": "path/to/demo.mp4"}
        assert queued_payload["captions"] == []
        assert queued_payload["extra"]["pipeline"] == "transcode"

        mock_log_job.assert_called()
        mock_log_exception.assert_not_called()

    @patch("function_app.BlobEnqueuer.enqueue")
    @patch("function_app.BlobEnqueuer.select_pipeline_for_blob")
    def test_non_matching_container_is_ignored(self, mock_select_pipeline: MagicMock, mock_enqueue: MagicMock):
        other_subject = "/blobServices/default/containers/other/blobs/test.mp4"
        event = _make_event(subject=other_subject)

        result = blob_ingestor._handle_event_grid_notification(event)

        assert result is False
        mock_select_pipeline.assert_not_called()
        mock_enqueue.assert_not_called()
