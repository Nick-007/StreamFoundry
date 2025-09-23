# Local Infrastructure Seeding & Smoke Tests

Creates containers from *_CONTAINER and queues from *_QUEUE in local.settings.json (plus -poison). 
Internal azure-webjobs* artifacts are ignored.

This folder contains platform-friendly seeding utilities that **only** create the app’s own resources.
They read names from your `local.settings.json` so your team shares the same truth across OSes.

## What gets created
- Blob containers: `RAW_VIDEOS_CONTAINER`, `MEZZANINE_CONTAINER`, `HLS_CONTAINER`, `DASH_CONTAINER`, `LOGS_CONTAINER`, `PROCESSED_CONTAINER`
- Queue: `TRANSCODE_QUEUE`

> The *values* are taken from `local.settings.json:Values` with sensible defaults if a key is missing.


# Local Infra Setup

### Prereqs
- Python 3.10+ (for seeding scripts)
- Azure Functions Core Tools (v4)
- Azurite (or Azure Storage account)
- Azure CLI (optional but recommended)

### Steps
(see root README Quickstart for the short version)

1. `python -m pip install -r infra/local/requirements.txt`
2. `cp infra/local/.env.example infra/local/.env` and edit as needed
3. Seed storage:
   - `bash infra/local/seed-storage.sh` **or**
   - `pwsh infra/local/seed-storage.ps1` **or**
   - `python infra/local/seed-storage.py`
4. Start Functions host: `func start`
5. Smoke tests:
   - `bash scripts/dev/curl-health.sh`
   - `bash scripts/dev/curl-submit.sh`

### Notes
- Scripts read container/queue names from `local.settings.json`.
- On Windows, use PowerShell script or WSL.  
- If a script isn’t executable: `chmod +x infra/local/*.sh scripts/**/*.sh`.
- Using a shared Azurite instance? Point your connection string accordingly.
- Troubleshooting leases: `bash scripts/ops/list-locks.sh`.
