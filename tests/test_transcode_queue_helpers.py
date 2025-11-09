import json
import os
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

with open(ROOT / "local.settings.json") as f:
    for key, value in json.load(f).get("Values", {}).items():
        os.environ.setdefault(key, str(value))

from function_app.TranscodeQueue import (
    _canonical_captions_list,
    enqueue_packaging,
    send_to_poison,
)


def test_canonical_captions_list_sorts_and_filters():
    caps = [
        {"lang": "es", "source": "b.vtt"},
        {"lang": "en", "source": "a.vtt"},
        {"lang": "", "source": ""},
        {"lang": "fr", "source": "c.vtt"},
    ]
    result = _canonical_captions_list(caps)
    assert [c["lang"] for c in result] == ["en", "es", "fr"]


@patch("function_app.TranscodeQueue.enqueue")
def test_enqueue_packaging_serializes_job(mock_enqueue):
    enqueue_packaging({"id": "job-7", "only_rung": ["720p"]}, log=lambda *_: None)
    args = mock_enqueue.call_args.args
    payload = json.loads(args[1])
    assert payload["id"] == "job-7"


@patch("function_app.TranscodeQueue.queue_client")
def test_send_to_poison_wraps_payload(mock_qc):
    mock_client = MagicMock()
    mock_qc.return_value = mock_client
    send_to_poison({"id": "job-err"}, reason="decode_error", log=lambda *_: None)
    call_arg = mock_client.send_message.call_args.args[0]
    decoded = json.loads(call_arg)
    assert decoded["reason"] == "decode_error"
