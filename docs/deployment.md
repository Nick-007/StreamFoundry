# Deployment Guide

## Overview

StreamFoundry runs as Azure Functions with Azurite for local emulation and can be containerized for ACA/Azure Batch deployments. The orchestrator (blob trigger + queue workers) dispatches to a queue, while worker containers perform transcode/packaging with ffmpeg + Shaka Packager.

## Local Setup

1. Install dependencies:
   - Python 3.12
   - Azure Functions Core Tools v4
   - Docker (for Azurite and container builds)
2. Start Azurite locally:
   ```bash
   docker run --rm -it -p 10000:10000 -p 10001:10001 -p 10002:10002 mcr.microsoft.com/azure-storage/azurite
   ```
3. Seed containers/queues:
   ```bash
   bash infra/local/seeder.sh
   ```
4. Run the Functions host:
   ```bash
   func start --verbose
   ```

## Containerized Worker (ACA/Batch Simulation)

The worker image bundles ffmpeg, Shaka Packager, and the Functions app. Files:
- `docker/Dockerfile.worker`
- `docker/Dockerfile.controller` (lightweight orchestrator)
- `docker/docker-compose.yml`
- `docker/docker-compose.split.yml` (controller/worker overlay)

### Build & Run (Compose)

```bash
docker compose -f docker/docker-compose.yml up --build
```

Services:
- `azurite`: storage emulator (ports 10000/10001/10002)
- `worker`: Functions host container connected to Azurite with ffmpeg/Shaka installed.

### Controller/worker split (local ACA mimic)

Run a lean controller (HTTP + EventGrid) and a dedicated worker (queue triggers with ffmpeg/Shaka):
```bash
docker compose -f docker/docker-compose.split.yml up --build
```
Controller exposes port 7071 and runs `submit_job`, `package_submit`, and `blob_enqueuer`. Worker runs `transcode_queue` and `packaging_queue` only. Scale workers to simulate ACA/Batch replicas:
```bash
docker compose -f docker/docker-compose.split.yml up --build --scale worker=2
```

### Per-job worker dispatch (one container per transcode)

To mimic ACA/Batch jobs where each transcode runs in its own container, use the dispatch overlay + dispatcher flag:
```bash
# build controller (light) + transcode image, and enable dispatch mode
docker compose -f docker/docker-compose.dispatch.yml up --build
# Transcode queue will launch a fresh transcode container per job (via scripts/dev/run_worker_local.sh)
```
Set `TRANSCODE_DISPATCH_MODE=docker` (default in the dispatch compose) to have the transcode queue trigger spin up a one-off worker container, pass the payload via env file, and shut it down after completion. Configure `TRANSCODE_WORKER_IMAGE` if you push the transcode image to a registry.

### Standalone Build

```bash
docker build -f docker/Dockerfile.worker -t streamfoundry-worker .
```

Run against local Azurite:
```bash
docker run --rm \
  -e AzureWebJobsStorage="DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=...;BlobEndpoint=http://host.docker.internal:10000/devstoreaccount1;QueueEndpoint=http://host.docker.internal:10001/devstoreaccount1;TableEndpoint=http://host.docker.internal:10002/devstoreaccount1" \
  -p 7071:7071 streamfoundry-worker
```

## Deployment Targets

### Azure Container Apps / Azure Batch
1. Publish `streamfoundry-worker` image to ACR.
2. Configure environment variables (AzureWebJobsStorage, SHAKA_PACKAGER_PATH, etc.).
3. Run container instances with access to Storage queues/blobs.

### Azure Functions (hosted)
Deploy the app via `func azure functionapp publish <APP_NAME>`; ensure ffmpeg/Shaka Packager are available (custom container recommended).

## Event Grid Simulation

Use `/tmp/blob-event.json` to simulate BlobCreated notifications:
```bash
curl -s -X POST "http://localhost:7071/runtime/webhooks/EventGrid?functionName=blob_enqueuer" \
  -H "Content-Type: application/json" \
  -H "aeg-event-type: Notification" \
  --data-binary @/tmp/blob-event.json
```

Ensure `subject` and `data.url` align with the actual blob path stored in `raw/`.

## Notes

- Shaka Packager must be accessible in worker containers (`SHAKA_PACKAGER_PATH`).
- Keep `local.settings.json` synchronized across host and containerized workers.
- Document updates should be mirrored in external wikis if needed.
