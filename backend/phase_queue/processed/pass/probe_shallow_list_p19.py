"""Probe v2: dump full list of shallow families using verifier's own classification.

Imports final_state_verifier to reuse canonical alias collapse + rich/shallow
bucket classification, so counts match the dashboard exactly.
"""
# --- ensure backend_dir is on sys.path ---
import os as _os
import sys as _sys
from pathlib import Path as _Path
_HERE = _Path(__file__).resolve().parent
_cand = _HERE
for _ in range(4):
    if (_cand / "final_state_verifier.py").exists():
        if str(_cand) not in _sys.path:
            _sys.path.insert(0, str(_cand))
        break
    _cand = _cand.parent
for _p in _os.environ.get("PYTHONPATH", "").split(_os.pathsep):
    if _p and _p not in _sys.path:
        _sys.path.insert(0, _p)

import argparse
import datetime as dt
import json
import sqlite3
import sys
from pathlib import Path

import final_state_verifier as fsv


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True)
    ap.add_argument("--report-dir", required=True)
    ns = ap.parse_args()

    conn = fsv.connect_db(Path(ns.db))
    raw_names = fsv.distinct_pattern_names(conn)
    alias_groups, alias_collapse_events = fsv.build_alias_groups(raw_names)
    pair_map = fsv.pair_map_from_extract_molecules(conn)
    recent = set(fsv.recent_completion_families(conn))

    summaries = []
    for canonical, raws in alias_groups.items():
        s = fsv.summarize_canonical_family(
            conn, canonical, raws, pair_map,
        )
        summaries.append(s)

    shallow = [r for r in summaries if r["completion_bucket"] == "shallow"]
    rich = [r for r in summaries if r["completion_bucket"] == "rich"]
    missing = [r for r in summaries if r["completion_bucket"] == "missing"]

    # sort shallow by (extract_count asc, pair asc) — most-empty first
    shallow.sort(key=lambda r: (r["extract_count"], r["unique_queryable_pair_count"], r["family"]))

    out_dir = Path(ns.report_dir) / dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    out_dir.mkdir(parents=True, exist_ok=True)
    out_json = out_dir / "shallow_list_full.json"
    out_json.write_text(json.dumps({
        "generated_at": dt.datetime.now().isoformat(),
        "db": ns.db,
        "shallow_count": len(shallow),
        "rich_count": len(rich),
        "missing_count": len(missing),
        "shallow_families": shallow,
        "rich_families": [r["family"] for r in rich],
    }, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"[probe_v2] shallow={len(shallow)} rich={len(rich)} missing={len(missing)}  wrote {out_json}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
