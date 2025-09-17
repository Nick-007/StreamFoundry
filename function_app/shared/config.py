import os, json
from pathlib import Path
_cache = {}
def _load_local_settings():
    p = Path(__file__).resolve()
    for _ in range(6):
        p = p.parent
        cand = p / "local.settings.json"
        if cand.exists():
            try:
                data = json.loads(cand.read_text())
                return data.get("Values", {}) or {}
            except Exception:
                return {}
    return {}
def get(key: str, default=None):
    if key in os.environ:
        return os.environ.get(key, default)
    if not _cache:
        _cache.update(_load_local_settings())
    return _cache.get(key, default)
