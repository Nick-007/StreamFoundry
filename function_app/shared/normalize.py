# shared/normalize.py
from __future__ import annotations
import re, json
from typing import Iterable, List, Set, Union

# -------- Ladder constants (single source of truth) --------
# Base labels (no 'p')
LABELS: List[str] = ["240", "360", "480", "720", "1080"]
# With 'p' suffix
LABELS_P: List[str] = [f"{x}p" for x in LABELS]

LABELS_SET = set(LABELS)
LABELS_P_SET = set(LABELS_P)

# -------- tiny helpers --------
def _to_list(x: Union[str, int, Iterable[Union[str, int]]]) -> List[str]:
    if x is None:
        return []
    if isinstance(x, (str, int)):
        x = [x]
    out: List[str] = []
    for v in x:
        s = str(v).strip()
        if s:
            out.append(s)
    return out

def _strip_p(s: Union[str, int]) -> str:
    s = str(s).strip().lower()
    return s[:-1] if s.endswith("p") else s

def _with_p(s: Union[str, int]) -> str:
    s = _strip_p(s)
    return f"{s}p"

def _numeric_key(s: str) -> int:
    try:
        return int(_strip_p(s))
    except Exception:
        return 10**9

def _coerce_to_list(x) -> list[str]:
    """Accept str / list / tuple / set / None and return a flat list of strings."""
    if x is None:
        return []
    if isinstance(x, (list, tuple, set)):
        return [str(v) for v in x if str(v).strip()]
    if isinstance(x, str):
        s = x.strip()
        if not s:
            return []
        # If someone passed a JSON array string, try to parse it
        if s.startswith("["):
            try:
                arr = json.loads(s)
                if isinstance(arr, list):
                    return [str(v) for v in arr if str(v).strip()]
            except Exception:
                pass
        # Otherwise split by commas/whitespace
        return [p for p in re.split(r"[,\s]+", s) if p]
    # Fallback: single scalar
    return [str(x)]

# -------- public APIs you already use --------
def normalize_label_list(labels: Iterable[Union[str, int]], *, suffix_p: bool = True) -> List[str]:
    """
    Normalize a list of rung labels to either all with 'p' or all without.
    Dedup + numeric sort. (If you need to preserve order, remove the sort.)
    """
    base = {_strip_p(x) for x in _to_list(labels)}
    out = {_with_p(x) if suffix_p else _strip_p(x) for x in base}
    return sorted(out, key=_numeric_key)

def normalize_only_rung(*args,
                        value=None,
                        values=None,
                        as_set: bool = False,
                        suffix_p: bool = True):
    """
    Compatibility normalizer for rung labels.

    Accepts:
      - legacy positional: normalize_only_rung(items)
      - new kw style:      normalize_only_rung(value=..., values=...)
        (if both provided, they're merged)

    Behavior:
      - Converts inputs like "720", "720p", ["360","1080p"] to canonical labels
      - If suffix_p=True  -> "720" -> "720p"
      - If suffix_p=False -> "720p" -> "720"
      - Returns set or list depending on as_set
    """
    # Determine source items (supports both legacy and new styles)
    if args and (value is None and values is None):
        items = args[0]
    else:
        merged = []
        merged += _coerce_to_list(value)
        merged += _coerce_to_list(values)
        items = merged

    labels = _coerce_to_list(items)

    out: list[str] = []
    for lab in labels:
        s = str(lab).strip().lower()
        # keep only digits and optional 'p'
        s = re.sub(r"[^0-9p]", "", s)
        m = re.fullmatch(r"(\d+)(p)?", s)
        if not m:
            continue
        num = m.group(1)
        norm = f"{num}p" if suffix_p else num
        out.append(norm)

    return set(out) if as_set else out


def normalize_rung_selector(*,
                            only_rungs: Union[str, int, Iterable[Union[str, int]], None],
                            labels: Iterable[str] | None = None,
                            suffix_p: bool = True):
    """
    Intersect 'only_rungs' with the allowed ladder.
    - If 'labels' is omitted, uses the built-in ladder (LABELS/LABELS_P).
    - Returns a set of normalized labels (with or without 'p' per suffix_p).
    """
    # default to built-in ladder
    if labels is None:
        labels = LABELS_P if suffix_p else LABELS

    allowed = set(normalize_label_list(labels, suffix_p=suffix_p))
    if not only_rungs:
        return allowed  # nothing requested → everything allowed

    requested = set(normalize_only_rung(only_rungs, as_set=True, suffix_p=suffix_p))
    return allowed.intersection(requested)

# -------- (optional) light compatibility shims you might still import --------
def rung_to_variants(r) -> Set[str]:
    """
    Return both forms for a rung token: base and 'p' suffixed.
    Ex: '720p' -> {'720', '720p'} ; 720 -> {'720', '720p'}
    """
    b = _strip_p(r)
    return {b, _with_p(b)}

def present_matrix(labels: Iterable[Union[str, int]],
                   include_base: bool = True,
                   include_p: bool = True):
    """
    Map each rung to its base/p variants:
      present_matrix(['240','360p'])
      -> {'240': {'base': '240','p': '240p'}, '360': {'base': '360','p': '360p'}}
    """
    bases = {_strip_p(x) for x in _to_list(labels)}
    matrix = {}
    for b in sorted(bases, key=_numeric_key):
        row = {}
        if include_base: row["base"] = b
        if include_p:    row["p"] = _with_p(b)
        matrix[b] = row
    return matrix