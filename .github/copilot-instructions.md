 postman# AI Agent Instructions for StreamFoundry

This guide helps AI agents understand StreamFoundry's architecture, workflows, and conventions.

## Core Architecture

StreamFoundry is a **queue-driven GPU-accelerated video transcoding pipeline** with these key components:

1. **Ingestion Layer** (`BlobEnqueuer`, `SubmitJob`):
   - Accepts videos via HTTP POST to `/api/submit` or blob trigger on `raw` container
   - Validates inputs and generates stable content fingerprints
   - Enqueues transcoding jobs to Azure Storage queues

2. **Processing Layer** (`TranscodeQueue`, `PackageQueue`):
   - Queue-triggered workers handle transcoding and packaging tasks
   - Supports scale-out with multiple worker instances
   - Uses NVENC on NVIDIA T4s (fallbacks to libx264)
   - Produces CMAF output with HLS+DASH packaging

3. **Storage Layout**:
   ```
   raw/            # Input videos
   mezzanine/            # Intermediate CMAF MP4s
     v_<fingerprint>/
       <stem>/
         audio.mp4
         video_{240,360,480,720,1080}.mp4
   dash/                 # DASH output
     v_<fingerprint>/
       <stem>/
         stream.mpd + segments
   hls/                  # HLS output
     v_<fingerprint>/
       <stem>/
         master.m3u8 + segments
   processed/           # Job manifests
     <stem>/
       manifest.json    # Canonical pointer
       latest.json      # {version, updatedAt}
   logs/               # Append-style job logs
   ```

## Critical Workflows

### Local Development Setup
```bash
# 1. Install dependencies
python -m pip install -r infra/local/requirements.txt
python -m pip install -r requirements.txt

# 2. Start Azurite
docker run -p 10000:10000 -p 10001:10001 -p 10002:10002 mcr.microsoft.com/azure-storage/azurite

# 3. Seed storage resources
bash infra/local/seeder.sh

# 4. Run Functions host
func start
```

### Testing
- Integration tests: `test_blob_ingestor.py` shows queue trigger patterns
- Reset local state: `bash scripts/dev/reset-azurite.sh`
- Test submission: `bash scripts/dev/curl-submit.sh`
- Inspect results: `./scripts/sfinspect.py manifest <stem>`

## Key Conventions

1. **Content Versioning**:
   - Content fingerprint = `sha256(input bytes + profile knobs)`
   - All assets published under `v_<fingerprint>/` paths
   - `processed/<stem>/latest.json` atomically tracks current version

2. **Status Tracking**:
   - Source blobs carry metadata: `sf-status`, `sf-version`, `sf-fingerprint`
   - Status transitions: submit_enqueued → transcode_processing → packaging_complete
   - Full history in `logs/<stem>/*.log`

3. **Queue Processing**:
   - Worker pools share work via queue triggers
   - Lease-based concurrency control: `processed/<stem>/_lock`
   - Queue message visibility auto-renewed during long operations

4. **Output Patterns**:
   - 4-second aligned CMAF segments for both HLS and DASH
   - Thumbnail indexes and WebVTT captions
   - Consistent cache headers (manifests: 60s, media: 12h)

## Integration Points

1. **Video Input**:
   ```json
   POST /api/submit
   {
     "source": "<URL or 'container/blob'>",
     "name": "optional-output-stem",
     "captions": [
       { "lang": "en", "source": "<URL or 'container/blob.vtt'>" }
     ]
   }
   ```

2. **Azure Batch GPU Integration**:
   - See `pool.json` and `job.json` for pool/job templates
   - Container must include FFmpeg+NVENC and Shaka Packager
   - Task environment passes SAS URLs and configuration
   - Functions app orchestrates task submission

3. **Observability**:
   - Query status: `GET /api/submit?mode=status`
   - Check logs in `logs/<stem>/*.log`
   - Monitor GPU utilization via Application Insights
   - Use `scripts/sfinspect.py` for status/manifest inspection

## Common Gotchas

1. NVENC requires:
   - FFmpeg built with `--enable-nvenc`
   - NVIDIA drivers present
   - Fallback to `libx264` is automatic

2. Output consistency requires:
   - GOP = `fps × SEG_DUR_SEC`
   - `-sc_threshold 0` in video command
   - Both enforced by pipeline settings

3. Scale-out considerations:
   - Queue triggers handle fan-out safely
   - Lease locks prevent duplicate work
   - Recommend 1-2 encodes per T4 GPU