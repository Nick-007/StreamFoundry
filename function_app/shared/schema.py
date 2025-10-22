from pydantic import BaseModel, Field, model_validator, ConfigDict
from typing import Optional, List, Dict, Union
from pathlib import Path

class InputSource(BaseModel):
    container: Optional[str] = Field(default="raw")
    key: str

class IngestPayload(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    
    id: Optional[str]
    in_: InputSource = Field(alias="in")
    only_rung: Optional[Union[str, int, List[Union[str, int]]]]
    captions: Optional[List[str]] = Field(default_factory=list)
    extra: Optional[Dict] = Field(default_factory=dict)

    @model_validator(mode="before")
    def set_default_id(cls, data):
        # data is the raw input dict before field validation
        if isinstance(data, dict):
            if not data.get("id"):
                inp = data.get("in") or data.get("in_")
                if isinstance(inp, dict):
                    raw_key = inp.get("key")
                    if raw_key:
                        data["id"] = Path(raw_key).stem
        return data