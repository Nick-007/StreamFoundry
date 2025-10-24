# function_app/shared/ingest.py
from __future__ import annotations

from typing import Any, Dict, List, Optional, Union
from pydantic import ValidationError

from .schema import IngestPayload  # your existing validator (with lang+source captions)

def extract_payload(body: Dict[str, Any]) -> Dict[str, Any]:
    """
    Accept either:
      { "payload": { ... } }
    or:
      { ... }
    """
    if isinstance(body, dict) and isinstance(body.get("payload"), dict):
        return body["payload"]
    return body

def validate_ingest(raw: Dict[str, Any]) -> IngestPayload:
    """
    Validate + normalize using your shared/schema.IngestPayload (Pydantic v2).
    Raises ValidationError if invalid.
    """
    return IngestPayload.model_validate(raw)

def dump_normalized(model: IngestPayload) -> Dict[str, Any]:
    """
    Dump with aliases so 'in_' stays 'in'.
    """
    return model.model_dump(by_alias=True)

def only_rung_as_list(only_rung: Optional[Union[str, int, List[Union[str, int]]]]) -> Optional[List[str]]:
    """
    Normalize only_rung to a clean list[str] or None.
    Mirrors behavior seen in your ingestor.
    Examples:
      "720p" -> ["720p"]
      720    -> ["720"]
      ["360p","720p"] -> ["360p","720p"]
      "" or [] -> None
    """
    if only_rung is None:
        return None
    if isinstance(only_rung, (str, int)):
        s = str(only_rung).strip()
        return [s] if s else None
    if isinstance(only_rung, list):
        out = [str(x).strip() for x in only_rung if str(x).strip()]
        return out or None
    # Unknown type -> best effort string
    s = str(only_rung).strip()
    return [s] if s else None

# Convenience one-liner used by both HTTP and Queue paths
def validate_and_normalize(body: Dict[str, Any]) -> IngestPayload:
    """
    Extract (unwrap {payload}) and validate.
    """
    raw = extract_payload(body)
    return validate_ingest(raw)

# Optional: a slim serializable error payload for HTTP 422
def pydantic_errors_json(err: ValidationError) -> str:
    return err.json()