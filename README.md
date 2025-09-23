# StreamFoundry — GPU‑accelerated CMAF Transcoding (Azure Functions)

This bundle provides a **queue‑driven** transcoding pipeline that **encodes once and packages twice** (CMAF → **HLS** + **DASH**) with a scalable ingestion model. It favors **NVENC h.264** on NVIDIA T4s (fallback to `libx264`) and publishes results under **versioned paths** derived from a **stable content fingerprint**.

---

## Features
- **Ingestion**: HTTP `POST /api/submit` and **Blob Trigger** (enqueues only)
- **Scale‑out**: Storage **Queue Trigger** workers (safe for multiple instances)
- **Ladder**: 240p/360p/480p/720p/1080p (efficiency‑tuned bitrates)
- **Segments**: **4 s** CMAF segments; GOP aligned per source **fps**
- **Packaging**: **Shaka Packager** → `master.m3u8` + `stream.mpd`
- **Captions**: **WebVTT** sidecars (HLS & DASH)
- **Trick‑play**: I‑frame friendly + **thumbnails** + **VTT** index
- **Manifest hygiene**: CODECS/RESOLUTION/FRAME‑RATE/BANDWIDTH
- **Idempotency**: Content **fingerprint**, short **blob lease** lock, `latest.json`
- **Publish layout**: `.../v_<sha256>/<stem>/...` plus `processed/<stem>/manifest.json`
- **Cache**: manifests **60 s**, segments/thumbnails/captions **12 h**

---

## Prerequisites
- **Python** (Functions runtime) and **Azure Functions Core Tools v4+**
- **Azurite** (local): uses `UseDevelopmentStorage=true`
- **FFmpeg** available on PATH  
  - For NVENC: NVIDIA drivers + FFmpeg built with `--enable-nvenc`  
  - If NVENC is unavailable, pipeline falls back to `libx264`
- **Shaka Packager** available on PATH (`packager`)

> Containers are created on first use: `raw-videos`, `mezzanine`, `dash`, `hls`, `processed`, `logs`, and queue `transcode-jobs`.

**Run Azurite (Docker):**
```bash
docker run -p 10000:10000 -p 10001:10001 -p 10002:10002 mcr.microsoft.com/azure-storage/azurite
```

---

## Local Run
```bash
# from the repo root
func start
```

### Submit a job (HTTP)
```bash
curl -s -X POST http://localhost:7071/api/submit \
  -H "Content-Type: application/json" \
  -d '{
        "source":"https://commondatastorage.googleapis.com/gtv-videos-bucket/sample/BigBuckBunny.mp4",
        "name": "bbb",
        "captions": [{"lang":"en","source":"https://example.com/subs.vtt"}]
      }' | jq
```

### What to expect in Storage
```
raw-videos/
  bbb.mp4
mezzanine/
  v_<fingerprint>/bbb/{audio.mp4, video_240.mp4, ..., video_1080.mp4}
dash/
  v_<fingerprint>/bbb/{stream.mpd, video_240_init.mp4, video_240_$Number$.m4s, ...}
hls/
  v_<fingerprint>/bbb/{master.m3u8, video_240_init.mp4, video_240_$Number$.m4s, ..., thumbnails/thumbnails.vtt}
processed/
  bbb/manifest.json         # canonical pointer for clients
  bbb/latest.json           # { "version": "v_<fingerprint>", "updatedAt": <epoch> }
logs/
  bbb/2024...Z.log
```

---

## Configuration (see `local.settings.json` → `Values`)
| Key | Default | Notes |
|---|---|---|
| `AzureWebJobsStorage` | `UseDevelopmentStorage=true` | Single storage var for everything |
| `RAW_CONTAINER` | `raw-videos` | Ingest container (blob trigger enqueues) |
| `MEZZ_CONTAINER` | `mezzanine` | CMAF MP4s (audio + video rungs) |
| `DASH_CONTAINER` | `dash` | DASH MPD + segments |
| `HLS_CONTAINER` | `hls` | HLS master + segments + thumbnails |
| `PROCESSED_CONTAINER` | `processed` | Job manifests + latest pointer |
| `LOGS_CONTAINER` | `logs` | Append‑style job logs |
| `JOB_QUEUE` | `transcode-jobs` | Work queue |
| `FFMPEG_PATH` | `ffmpeg` | Override if needed |
| `SHAKA_PACKAGER_PATH` | `packager` | Override if needed |
| `SEG_DUR_SEC` | `4` | Segment duration (seconds) |
| `PACKAGER_SEG_DUR_SEC` | `4` | Must match encoder segments |
| `VIDEO_CODEC` | `h264_nvenc` | Fallback: `libx264` |
| `NVENC_PRESET` | `p5` | |
| `NVENC_RC` | `vbr_hq` | |
| `NVENC_LOOKAHEAD` | `32` | |
| `NVENC_AQ` | `1` | Spatial AQ on |
| `SET_BT709_TAGS` | `true` | SDR tagging |
| `AUDIO_MAIN_KBPS` | `128` | Single AAC track |
| `ENABLE_CAPTIONS` | `true` | Accept WebVTT sidecars |
| `ENABLE_TRICKPLAY` | `true` | Also writes thumbnails & VTT |
| `TRICKPLAY_FACTOR` | `4` | For DASH trick-play index |
| `THUMB_INTERVAL_SEC` | `4` | Thumb extraction interval |
| `QC_STRICT` | `true` | ffprobe gate (codec/fps/WH) |
| `DRM_PLACEHOLDERS` | `true` | Reserved for future DRM wiring |

**HTTP submit payload**
```jsonc
{
  "source": "<URL or 'container/blob'>",
  "name": "optional-output-stem",
  "captions": [
    { "lang": "en", "source": "<URL or 'container/blob.vtt'>" }
  ]
}
```

---

## Operational Notes
- **Scale‑out**: Add more Function workers or distribute jobs via Azure Batch; choose **1–2 concurrent encodes per T4**.
- **Idempotency**: Worker checks `processed/<stem>/manifest.json` and uses a **lease lock** (`processed/<stem>/_lock`) to avoid duplicate work.
- **Content fingerprint**: `sha256(input bytes + profile knobs)` → publish under `v_<fingerprint>/...`; update `latest.json` atomically.
- **Cache/headers**: manifests `Cache-Control: public, max-age=60`; segments/captions/thumbnails `public, max-age=43200`.
- **Observability**: Logs in `logs/`; wire Application Insights in `host.json`/portal for queue depth, failures, and GPU utilization dashboards.

---

## Troubleshooting
- **NVENC not active** → ensure ffmpeg build supports NVENC; otherwise fallback `libx264` kicks in (slower CPU encode).
- **Packager missing** → ensure `packager` on PATH.
- **Bad input** → strict precheck fails early; see `logs/<stem>/*.log` and `processed/<stem>/manifest.json` for error context.
- **Segments drift** → verify GOP = `fps × SEG_DUR_SEC` and `-sc_threshold 0` in video cmd (this bundle enforces both).

---

## Security & Networking
- Use **Managed Identity** for storage in production.
- Prefer **Private Endpoints** for Storage → Functions/Batch; put **WAF** on Front Door.
- Use least‑privilege **RBAC** and short‑lived **SAS** for client access.

---

## Cleanup
Set Storage lifecycle policies to transition old **mezzanine/segments** to cool/archival and prune stale `v_*` versions when `latest.json` moves forward.
---

## Deployment Guide (Azure Batch GPU Pool)

This section shows how to deploy **GPU compute** with **Azure Batch** (NCas T4 v3), push your container image, and submit tasks. The Functions app remains your **API/orchestrator**; the heavy transcode happens on Batch nodes.

> **Modes**  
> - **Functions‑only**: OK for CPU prototyping.  
> - **Functions + Batch (recommended)**: Queue/HTTP intake in Functions → submit GPU tasks to Batch for horizontal scale.

### 0) Build and push your container image
Use an image that includes **FFmpeg with NVENC** and **Shaka Packager**, plus a tiny wrapper (e.g., `run_transcode.sh`) that: 
1) downloads the input asset, 2) runs the ladder encode (4s GOP), 3) packages via Shaka, 4) uploads to Blob with the proper paths.

Example high‑level Dockerfile skeleton:
```dockerfile
# Choose a CUDA/NVENC-capable base (example)
FROM jrottenberg/ffmpeg:6.1-nvidia

# Install Shaka Packager (or copy your own static build)
# RUN curl -L <packager-url> -o /usr/local/bin/packager && chmod +x /usr/local/bin/packager

# Copy a small wrapper that reads env vars and runs ffmpeg+packager
COPY run_transcode.sh /usr/local/bin/run_transcode.sh
RUN chmod +x /usr/local/bin/run_transcode.sh

ENTRYPOINT ["/usr/local/bin/run_transcode.sh"]
```

Build & push to **ACR**:
```bash
az acr create -g <rg> -n <acrName> --sku Standard
az acr login -n <acrName>
docker build -t <acrName>.azurecr.io/streamfoundry:gpu .
docker push <acrName>.azurecr.io/streamfoundry:gpu
```

### 1) Create the Batch pool (NCas T4 v3)
Save as `pool.json`:
```json
{
  "id": "gpu-ffmpeg-cmaf",
  "vmSize": "Standard_NC4as_T4_v3",
  "targetDedicatedNodes": 1,
  "targetLowPriorityNodes": 4,
  "taskSlotsPerNode": 2,
  "targetNodeCommunicationMode": "simplified",
  "virtualMachineConfiguration": {
    "imageReference": {
      "publisher": "Canonical",
      "offer": "0001-com-ubuntu-server-jammy",
      "sku": "22_04-lts",
      "version": "latest"
    },
    "nodeAgentSKUId": "batch.node.ubuntu 22.04",
    "containerConfiguration": {
      "type": "dockerCompatible",
      "containerImageNames": [
        "<acrName>.azurecr.io/streamfoundry:gpu"
      ],
      "containerRegistries": [
        { "registryServer": "<acrName>.azurecr.io", "userName": "<acr-username>", "password": "<acr-password>" }
      ]
    },
    "extensions": [
      {
        "name": "nvidiaGpu",
        "publisher": "Microsoft.HpcCompute",
        "type": "NvidiaGpuDriverLinux",
        "typeHandlerVersion": "1.6",
        "autoUpgradeMinorVersion": true,
        "enableAutomaticUpgrade": true,
        "settings": { }
      }
    ]
  },
  "enableAutoScale": false,
  "taskSchedulingPolicy": { "nodeFillType": "spread" }
}
```

Create the pool:
```bash
az batch pool create --json-file pool.json \
  --account-name <batchAccount> \
  --account-endpoint <account>.<region>.batch.azure.com \
  --resource-group <rg>
```

> **Spot strategy**: set `targetLowPriorityNodes` high and keep `targetDedicatedNodes` ≥ 1 to ride out evictions. Adjust `taskSlotsPerNode` after measuring GPU & NVENC utilization.

### 2) Create a Batch job
`job.json`:
```json
{ "id": "transcode", "poolInfo": { "poolId": "gpu-ffmpeg-cmaf" } }
```
```bash
az batch job create --json-file job.json \
  --account-name <batchAccount> \
  --account-endpoint <account>.<region>.batch.azure.com \
  --resource-group <rg>
```

### 3) Submit a task (container) per video
Tasks should pass **input/output** via environment variables (SAS URLs) and call your wrapper. Save as `task.json` (template):

```json
{
  "id": "bbb-1080p",
  "commandLine": "/bin/bash -lc \"echo starting && /usr/local/bin/run_transcode.sh\"",
  "environmentSettings": [
    { "name": "RAW_URL",           "value": "https://<account>.blob.core.windows.net/raw-videos/bbb.mp4?<SAS>" },
    { "name": "DEST_BASE",         "value": "https://<account>.blob.core.windows.net" },
    { "name": "DEST_SAS",          "value": "<SAS-with-write>" },
    { "name": "STEM",              "value": "bbb" },
    { "name": "SEG_DUR_SEC",       "value": "4" },
    { "name": "BT709_TAGS",        "value": "true" },
    { "name": "AUDIO_MAIN_KBPS",   "value": "128" },
    { "name": "LADDER",            "value": "240:300k/360k/600k,360:650k/780k/1300k,480:900k/1000k/1800k,720:2500k/2800k/5000k,1080:4200k/4600k/8000k" }
  ],
  "containerSettings": {
    "imageName": "<acrName>.azurecr.io/streamfoundry:gpu",
    "containerRunOptions": "--rm"
  }
}
```

Submit the task:
```bash
az batch task create \
  --job-id transcode \
  --json-file task.json \
  --account-name <batchAccount> \
  --account-endpoint <account>.<region>.batch.azure.com \
  --resource-group <rg>
```

**What `run_transcode.sh` should do (outline):**
```bash
#!/usr/bin/env bash
set -euo pipefail
# Inputs
RAW_URL="${RAW_URL:?}"
DEST_BASE="${DEST_BASE:?}"      # e.g., https://<account>.blob.core.windows.net
DEST_SAS="${DEST_SAS:?}"        # ?sv=...
STEM="${STEM:?}"
SEG="${SEG_DUR_SEC:-4}"
LADDER="${LADDER:-"240:300k/360k/600k,360:650k/780k/1300k,480:900k/1000k/1800k,720:2500k/2800k/5000k,1080:4200k/4600k/8000k"}"

# 0) Download input
curl -sfL "$RAW_URL" -o in.mp4

# 1) Transcode ladder (NVENC) -> mezzanine/*.mp4
mkdir -p mezzanine dash hls
# …loop over LADDER, compute GOP from fps, run ffmpeg with nvenc flags…

# 2) Package with Shaka -> dash/stream.mpd & hls/master.m3u8
# packager in=<...> --segment_duration "$SEG" --generate_static_mpd ...

# 3) Upload to Blob (SAS) under versioned path v_<sha256>/<stem>/...
# Use 'azcopy' or 'curl -T' with SAS
```

> **Integration pattern**: Your **Functions** app can turn each queue message into a `task.json` (with SAS URLs) and call `az batch task create` or use the Batch Python SDK to submit. That preserves the same **fingerprint + versioned publish** model.

### 4) Optional: Autoscale formula
Enable `enableAutoScale: true` on the pool and set a formula like:
```
$TargetDedicatedNodes = 1;
$pending = $PendingTasks.GetSample(1);
$lp = min(20, max(0, $pending / 2));
$TargetLowPriorityNodes = $lp;
```
This scales Spot nodes with queue pressure while keeping 1 dedicated node for stability.

### 5) Networking & security
- Use **Managed Identity** to fetch SAS for task env vars.  
- Lock down Storage with **Private Endpoints** and allow Batch nodes via **Private Link** or VNet rules.  
- Restrict ACR pull with scoped tokens; rotate secrets regularly.

## Quickstart (Local)

```bash
# 1) Install deps for the seed script
python -m pip install -r infra/local/requirements.txt

# 2) (optional) copy env template and tweak
cp infra/local/.env.example infra/local/.env

# 3) Create containers & queue (names read from local.settings.json)
bash infra/local/seed-storage.sh
# or
pwsh infra/local/seed-storage.ps1
# or
python infra/local/seed-storage.py

# 4) Smoke tests (when your Functions host is running)
bash scripts/dev/curl-health.sh
bash scripts/dev/curl-submit.sh
