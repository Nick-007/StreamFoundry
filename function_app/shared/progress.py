import time

def handle_progress(d: dict, media_type: str, total_duration_sec: float, t0: float, filename: str, log=None):
    ot = d.get("out_time_ms") or d.get("out_time_us")
    if not ot:
        return

    try:
        sec = int(ot) / 1_000_000.0  # Always treat as microseconds
    except Exception:
        return

    # Detect timestamp overshoot
    if total_duration_sec > 0 and sec > total_duration_sec + 1.0:
        (log or print)(f"[{media_type}] WARNING: processed timestamp {sec:.1f}s exceeds media duration {total_duration_sec:.1f}s")

    # Log progress and performance
    if total_duration_sec > 0:
        pct = max(0, min(100, int((sec / total_duration_sec) * 100)))
        elapsed = max(1.0, time.time() - t0)
        rate = sec / elapsed
        eta = int((total_duration_sec - sec) / rate) if rate > 0 else -1
        (log or print)(f"[{media_type}] {filename} {pct}% | processed={sec:.1f}s | media={total_duration_sec:.1f}s | elapsed={elapsed:.2f}s | speed={rate:.1f}x | ETA~{eta}s")
    else:
        (log or print)(f"[{media_type}] {filename} processed={sec:.1f}s")
