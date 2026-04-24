"""probe_shallow_list_p28 — list top shallow families after phase28."""
from __future__ import annotations
import sys, json
sys.path.insert(0, "C:\\chemlens\\backend")
import final_state_verifier as fsv
from pathlib import Path

def main(db_path: str, report_dir: str):
    import sqlite3
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    alias_groups = fsv.build_alias_groups(cur)
    pair_map = fsv.pair_map_from_extract_molecules(cur)

    rows = []
    for canonical, members in alias_groups.items():
        summary = fsv.summarize_canonical_family(cur, canonical, members, pair_map)
        rows.append(summary)

    missing = [r for r in rows if r["bucket"] == "missing"]
    shallow = [r for r in rows if r["bucket"] == "shallow"]
    rich = [r for r in rows if r["bucket"] == "rich"]

    shallow.sort(key=lambda r: (r["extract_count"], r["unique_queryable_pair_count"], r["canonical_family"]))
    top = shallow[:20]

    out_path = Path(report_dir) / "probe_shallow_list_p28.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps({
        "totals": {"missing": len(missing), "shallow": len(shallow), "rich": len(rich)},
        "top_shallow": [
            {
                "family": r["canonical_family"],
                "extract_count": r["extract_count"],
                "overview_count": r["overview_count"],
                "application_count": r["application_count"],
                "unique_queryable_pair_count": r["unique_queryable_pair_count"],
            }
            for r in top
        ],
    }, indent=2))
    print(f"wrote {out_path}")

if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--db", required=True)
    p.add_argument("--report-dir", required=True)
    args = p.parse_args()
    main(args.db, args.report_dir)
