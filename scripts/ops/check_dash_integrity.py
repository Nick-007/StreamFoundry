import xml.etree.ElementTree as ET
import os, sys, re
from typing import List, Tuple, Dict, Iterable

def parse_mpd(mpd_path: str) -> Dict[str, Dict]:
    ns = {"mpd": "urn:mpeg:dash:schema:mpd:2011"}
    tree = ET.parse(mpd_path)
    root = tree.getroot()

    out = {}
    period = root.find("mpd:Period", ns)
    for aset in period.findall("mpd:AdaptationSet", ns):
        ctype = aset.get("contentType")
        reps = aset.findall("mpd:Representation", ns)
        for rep in reps:
            rep_id = rep.get("id")
            st = rep.find("mpd:SegmentTemplate", ns)
            if st is None:
                continue
            init = st.get("initialization")
            media = st.get("media")
            start = int(st.get("startNumber", "1"))

            # Expand SegmentTimeline into a list of $Number$ values
            timeline = st.find("mpd:SegmentTimeline", ns)
            numbers = []
            if timeline is not None:
                n = start
                for S in timeline.findall("mpd:S", ns):
                    d = int(S.get("d"))
                    r = int(S.get("r", "0"))
                    # One entry + r repeats → r+1 segments
                    seg_count = r + 1
                    for _ in range(seg_count):
                        numbers.append(n)
                        n += 1

            out[(ctype or "unknown", rep_id)] = {
                "initialization": init,
                "media_template": media,   # e.g. video_1080p_$Number$.m4s
                "numbers": numbers,
            }
    return out

def filenames_for_rep(rep: Dict) -> List[str]:
    templ = rep["media_template"]
    return [templ.replace("$Number$", str(n)) for n in rep["numbers"]]

def list_local(path: str) -> Iterable[str]:
    try:
        return set(os.listdir(path))
    except FileNotFoundError:
        return set()

def main(local_dir: str = None):
    mpd_path = os.path.join(local_dir,"stream.mpd")
    info = parse_mpd(mpd_path)
    any_missing = False
    for (ctype, rep_id), rep in info.items():
        want = set(filenames_for_rep(rep))
        if not want:
            print(f"[warn] no timeline for {ctype} rep {rep_id}")
            continue
        init = rep["initialization"]
        if local_dir:
            have = list_local(local_dir)
            missing = sorted([f for f in want if f not in have])
            init_missing = (init not in have)
            print(f"[check] {ctype} rep={rep_id} expect {len(want)} segments + init")
            if init_missing:
                print(f"  - MISSING init: {init}")
                any_missing = True
            if missing:
                any_missing = True
                # show just a few to avoid spam
                head = ", ".join(missing[:10])
                more = f" (+{len(missing)-10} more)" if len(missing) > 10 else ""
                print(f"  - MISSING segments: {head}{more}")
            else:
                print("  - OK all segments present")
        else:
            # Just print expected names
            print(f"[expect] {ctype} rep={rep_id} init={init} first10={list(want)[:10]} … total={len(want)}")
    sys.exit(1 if any_missing else 0)

if __name__ == "__main__":
    if len(sys.argv) < 1:
        print("Usage: python check_dash_integrity.py /path/to/stream.mpd /path/to/local/dash_dir")
        sys.exit(2)
    main(sys.argv[1])