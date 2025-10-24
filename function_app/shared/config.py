import json
from pathlib import Path

#pydantic-settings
from pydantic import Field
from pydantic_settings import BaseSettings

def _load_local_settings() -> dict:
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


class AppSettings(BaseSettings):
    PACKAGING_QUEUE: str = Field(default="packaging-jobs")
    AzureWebJobsStorage: str
    FUNCTIONS_WORKER_RUNTIME: str
    RAW_CONTAINER: str
    MEZZ_CONTAINER: str
    SKIP_TRANSCODE_IF_MEZZ: str
    SKIP_MEZZ_UPLOAD_ON_RESTORE: str
    HLS_CONTAINER: str
    DASH_CONTAINER: str
    DASH_BASE_URL: str
    HLS_BASE_URL: str
    LOGS_CONTAINER: str
    PROCESSED_CONTAINER: str
    TMP_DIR: str
    TRANSCODE_QUEUE: str
    TRANSCODE_POISON_QUEUE: str
    FFMPEG_PATH: str
    FFPROBE_PATH: str
    SHAKA_PACKAGER_PATH: str
    SEG_DUR_SEC: str
    PACKAGER_SEG_DUR_SEC: str
    LADDER_PROFILE: str
    LOCKS_CONTAINER: str
    LOCK_TTL_SECONDS: str
    TRANSCODE_VISIBILITY_EXTENSION_SEC: str 
    VIDEO_CODEC: str
    NVENC_PRESET: str
    NVENC_RC: str
    NVENC_LOOKAHEAD: str
    NVENC_AQ: str
    SET_BT709_TAGS: str
    AUDIO_MAIN_KBPS: str
    ENABLE_AUDIO_LOW: str
    ENABLE_CAPTIONS: str
    ENABLE_TRICKPLAY: str
    TRICKPLAY_FACTOR: str
    THUMB_INTERVAL_SEC: str
    QC_STRICT: str
    DRM_PLACEHOLDERS: str

    class Config:
        case_sensitive = True
        env_file = None  # disable .env
        @classmethod
        def customise_sources(cls, init_settings, env_settings, file_secret_settings):
            return (
                init_settings,  # values passed directly to AppSettings()
                env_settings,   # values from os.environ
                lambda _: _load_local_settings(),  # fallback to local.settings.json
            )

def get(key: str, default=None):
    return getattr(AppSettings(), key, default)

def generate_missing_fields():
    local_values = _load_local_settings()
    declared_fields = AppSettings.model_fields.keys()
    undeclared = [k for k in local_values if k not in declared_fields]

    if undeclared:
        print("🔧 Add these fields to AppSettings:\n")
        for key in undeclared:
            value = local_values[key]
            inferred_type = type(value).__name__
            type_hint = {
                "str": "str",
                "int": "int",
                "float": "float",
                "bool": "bool"
            }.get(inferred_type, "str")  # default to str if unknown
            print(f"{key}: {type_hint}")
    else:
        print("✅ All keys in local.settings.json are already declared.")

if __name__ == "__main__":
    generate_missing_fields()
