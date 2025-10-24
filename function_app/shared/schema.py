from pydantic import BaseModel, Field, ConfigDict, AnyUrl, model_validator, field_validator
from typing import List, Optional, Union, Any, Dict
from pathlib import Path

class InputSource(BaseModel):
    container: Optional[str] = Field(default="raw")
    key: str

class CaptionSpec(BaseModel):
    lang: str
    # allow either a valid URL or a plain path-like string
    source: Union[AnyUrl, str]

class IngestPayload(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    id: Optional[str]
    in_: "InputSource" = Field(alias="in")
    only_rung: Optional[List[str]] = None
    captions: List["CaptionSpec"] = Field(default_factory=list)
    extra: Dict[str, Any] = Field(default_factory=dict)

    # --- NEW: validate and normalize `extra` (especially extra.renditions) ---
    @field_validator("extra", mode="before")
    @classmethod
    def _validate_extra(cls, v: Any) -> Dict[str, Any]:
        """
        Ensures:
          - extra is a dict (otherwise error)
          - if extra.renditions is present, it's a non-empty list of dicts
          - each rendition has a non-empty string 'mp4'
          - optional 'height' is coerced to int if present (error if not int-like)
          - optional audio_mp4/out_dash/out_hls are non-empty strings if provided
        """
        if v is None:
            return {}
        if not isinstance(v, dict):
            raise ValueError("extra must be an object")

        r = v.get("renditions", None)
        if r is not None:
            if not isinstance(r, list) or len(r) == 0:
                raise ValueError("extra.renditions must be a non-empty list")
            cleaned: List[Dict[str, Any]] = []
            for i, item in enumerate(r):
                if not isinstance(item, dict):
                    raise ValueError(f"extra.renditions[{i}] must be an object")
                mp4 = item.get("mp4")
                if not isinstance(mp4, str) or not mp4.strip():
                    raise ValueError(f"extra.renditions[{i}].mp4 must be a non-empty string")
                # optional: coerce height if present
                if "height" in item and item["height"] is not None:
                    try:
                        item["height"] = int(item["height"])
                    except Exception:
                        raise ValueError(f"extra.renditions[{i}].height must be an integer if provided")
                cleaned.append(item)
            v["renditions"] = cleaned  # normalized

        # Optional: sanity checks for other known keys if provided
        for key in ("audio_mp4", "out_dash", "out_hls"):
            if key in v and v[key] is not None:
                if not isinstance(v[key], str) or not v[key].strip():
                    raise ValueError(f"extra.{key} must be a non-empty string if provided")

        return v

    # keep your existing only_rung normalizer if you added it
    # @field_validator("only_rung", mode="before") ... (unchanged)

    @model_validator(mode="before")
    @classmethod
    def set_default_id_and_normalize(cls, data):
        if not isinstance(data, dict):
            return data
        if not data.get("id"):
            inp = data.get("in") or data.get("in_")
            if isinstance(inp, dict):
                raw_key = inp.get("key")
                if raw_key:
                    data["id"] = Path(raw_key).stem
        return data