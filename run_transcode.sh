#!/usr/bin/env bash
set -euo pipefail

# --------- Inputs (env) ---------
RAW_URL="${RAW_URL:?RAW_URL missing}"
DEST_BASE="${DEST_BASE:?DEST_BASE missing}"      # e.g., https://myacct.blob.core.windows.net
DEST_SAS="${DEST_SAS:?DEST_SAS missing}"         # starts with ?sv=...
STEM="${STEM:?STEM missing}"                     # output name (no extension)
SEG="${SEG_DUR_SEC:-4}"
AUDIO_KBPS="${AUDIO_MAIN_KBPS:-128}"
VIDEO_CODEC="${VIDEO_CODEC:-h264_nvenc}"
BT709="${BT709_TAGS:-true}"
NV_PRESET="${NVENC_PRESET:-p5}"
NV_RC="${NVENC_RC:-vbr_hq}"
NV_LOOK="${NVENC_LOOKAHEAD:-32}"
NV_AQ="${NVENC_AQ:-1}"
LADDER="${LADDER:-"240:300k/360k/600k,360:650k/780k/1300k,480:900k/1000k/1800k,720:2500k/2800k/5000k,1080:4200k/4600k/8000k"}"

RAW_CONT="${RAW_CONT:-raw}"
MEZZ_CONT="${MEZZ_CONT:-mezzanine}"
HLS_CONT="${HLS_CONT:-hls}"
DASH_CONT="${DASH_CONT:-dash}"

# --------- Pre-flight ---------
command -v ffmpeg >/dev/null || { echo "ffmpeg not found"; exit 1; }
command -v packager >/dev/null || { echo "Shaka packager not found"; exit 1; }
command -v sha256sum >/dev/null || { echo "sha256sum not found"; exit 1; }

mkdir -p work/mezz work/dash work/hls
cd work

# --------- 0) Download input ---------
echo "[DL] $RAW_URL"
curl -sfL "$RAW_URL" -o in.mp4

# --------- Helpers ---------
fps_of () {
  ffprobe -v error -select_streams v:0 -show_entries stream=avg_frame_rate -of default=nw=1:nk=1 "$1" | awk -F/ '{ if (NF==2 && $2>0) printf "%.3f\n", $1/$2; else print $1 }'
}
SAFE() { [[ -n "$1" ]] && echo "$1" || echo "30"; }

FPS="$(SAFE "$(fps_of in.mp4)")"
GOP="$(python3 - <<PY
fps=float("$FPS")
seg=int("${SEG:-4}")
print(max(1, int(round(fps*seg))))
PY
)"
echo "[INFO] FPS=$FPS SEG=$SEG GOP=$GOP"

# fingerprint = sha256(input bytes + knobs)
KSTR="seg=${SEG}|ladder=${LADDER}|codec=${VIDEO_CODEC}|preset=${NV_PRESET}|rc=${NV_RC}"
INPUT_SHA="$(sha256sum in.mp4 | awk '{print $1}')"
FINGERPRINT="$(python3 - <<PY
import hashlib, os
h=hashlib.sha256()
h.update(os.environ["INPUT_SHA"].encode()); h.update(os.environ["KSTR"].encode())
print(h.hexdigest())
PY
)"
export FINGERPRINT
VERSION="v_${FINGERPRINT}"
echo "[INFO] VERSION=$VERSION"

# --------- 1) Audio ---------
echo "[AUDIO] -> mezz/audio.mp4"
ffmpeg -y -i in.mp4 -map 0:a:0 -c:a aac -b:a ${AUDIO_KBPS}k -ac 2 -ar 48000 -movflags +faststart -f mp4 mezz/audio.mp4

# --------- 2) Video ladder ---------
IFS=',' read -ra RUNGS <<< "$LADDER"
for rung in "${RUNGS[@]}"; do
  # format: HEIGHT:bv/maxrate/bufsize
  H="${rung%%:*}"; rest="${rung#*:}"
  BV="${rest%%/*}"; rest="${rest#*/}"
  MR="${rest%%/*}"; BS="${rest#*/}"

  OUT="mezz/video_${H}.mp4"
  echo "[VIDEO] ${H}p -> $OUT"
  COMMON=(-vf "scale=-2:${H}" -pix_fmt yuv420p -g "${GOP}" -keyint_min "${GOP}" -sc_threshold 0 -bf 3 -coder cabac)
  if [[ "${BT709,,}" == "true" ]]; then
    COMMON+=(-color_primaries bt709 -color_trc bt709 -colorspace bt709)
  fi
  if [[ "$VIDEO_CODEC" == "h264_nvenc" ]]; then
    ffmpeg -y -i in.mp4 -map 0:v:0 \
      "${COMMON[@]}" \
      -c:v h264_nvenc -preset "${NV_PRESET}" -rc "${NV_RC}" \
      -spatial_aq "${NV_AQ}" -temporal_aq 1 -rc-lookahead "${NV_LOOK}" \
      -b:v "${BV}" -maxrate "${MR}" -bufsize "${BS}" -profile:v high -level 4.1 \
      -movflags +faststart -f mp4 "$OUT"
  else
    ffmpeg -y -i in.mp4 -map 0:v:0 \
      "${COMMON[@]}" \
      -c:v libx264 -preset medium -tune film \
      -b:v "${BV}" -maxrate "${MR}" -bufsize "${BS}" -profile:v high -level 4.1 \
      -movflags +faststart -f mp4 "$OUT"
  fi
done

# --------- 3) Package (DASH & HLS) ---------
cd dash
PARTS=()
for v in ../mezz/video_*.mp4; do
  base="$(basename "$v" .mp4)" # video_240 ...
  PARTS+=("in=\"${v}\",stream=video,init_segment=${base}_init.mp4,segment_template=${base}_\$Number\$.m4s")
done
PARTS+=("in=\"../mezz/audio.mp4\",stream=audio,init_segment=audio_init.m4a,segment_template=audio_\$Number\$.m4s")
CMD="packager ${PARTS[*]} --segment_duration ${SEG} --generate_static_mpd --mpd_output=stream.mpd"
echo "[DASH] $CMD"
eval "$CMD"
cd ..

cd hls
PARTS=()
for v in ../mezz/video_*.mp4; do
  base="$(basename "$v" .mp4)"
  PARTS+=("in=\"${v}\",stream=video,init_segment=${base}_init.mp4,segment_template=${base}_\$Number\$.m4s")
done
PARTS+=("in=\"../mezz/audio.mp4\",stream=audio,init_segment=audio_init.m4a,segment_template=audio_\$Number\$.m4s")
CMD="packager ${PARTS[*]} --segment_duration ${SEG} --hls_master_playlist_output=master.m3u8"
echo "[HLS] $CMD"
eval "$CMD"
cd ..

# --------- 4) Upload (mezzanine, dash, hls) ---------
# Prefer AzCopy if present
dest_mezz="${DEST_BASE}/${MEZZ_CONT}/${VERSION}/${STEM}${DEST_SAS}"
dest_dash="${DEST_BASE}/${DASH_CONT}/${VERSION}/${STEM}${DEST_SAS}"
dest_hls="${DEST_BASE}/${HLS_CONT}/${VERSION}/${STEM}${DEST_SAS}"

if command -v azcopy >/dev/null; then
  echo "[UPLOAD] Using azcopy"
  azcopy copy "mezz/*" "${dest_mezz}" --recursive
  azcopy copy "dash/*"  "${dest_dash}" --recursive
  azcopy copy "hls/*"   "${dest_hls}"  --recursive
  # Optionally set short cache for manifests (uncomment to enforce overrides)
  # azcopy set-properties "${DEST_BASE}/${DASH_CONT}/${VERSION}/${STEM}/stream.mpd${DEST_SAS}" --cache-control "public, max-age=60" --content-type "application/dash+xml"
  # azcopy set-properties "${DEST_BASE}/${HLS_CONT}/${VERSION}/${STEM}/master.m3u8${DEST_SAS}" --cache-control "public, max-age=60" --content-type "application/vnd.apple.mpegurl"
else
  echo "[UPLOAD] azcopy not found; uploading via curl (manifests only; please upload segments via azcopy later)"
  curl -X PUT -T "dash/stream.mpd"  "${DEST_BASE}/${DASH_CONT}/${VERSION}/${STEM}/stream.mpd${DEST_SAS}"  -H "x-ms-blob-type: BlockBlob" -H "Content-Type: application/dash+xml" -H "Cache-Control: public, max-age=60"
  curl -X PUT -T "hls/master.m3u8"  "${DEST_BASE}/${HLS_CONT}/${VERSION}/${STEM}/master.m3u8${DEST_SAS}" -H "x-ms-blob-type: BlockBlob" -H "Content-Type: application/vnd.apple.mpegurl" -H "Cache-Control: public, max-age=60"
fi

echo "[DONE] Published under ${VERSION}/${STEM}"
