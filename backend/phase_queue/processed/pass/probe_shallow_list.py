"""Probe: dump full list of shallow families to JSON for Claude to read.

Read-only. No DB writes. Exits 0 on success so daemon verifier/dashboard stages
pass trivially (they operate on existing state).
"""
# --- ensure backend_dir is on sys.path (defensive, daemon also injects PYTHONPATH) ---
import os as _os
import sys as _sys
from pathlib import Path as _Path
_HERE = _Path(__file__).resolve().parent
_cand = _HERE
for _ in range(4):
    if (_cand / "smiles_guard.py").exists():
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
from pathlib import Path


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True)
    ap.add_argument("--report-dir", required=True)
    ns = ap.parse_args()

    conn = sqlite3.connect(ns.db)
    conn.row_factory = sqlite3.Row

    # Mirror the dashboard's bucket classification logic:
    #   rich: overview>=1 AND application>=2 AND queryable_R>=3 AND queryable_P>=3 AND pair>=3
    #   missing: no extracts
    #   shallow: has extracts but not rich
    # (We join reaction_extracts + extract_molecules per family.)
    sql = """
    WITH per_fam AS (
        SELECT
            re.reaction_family_name AS fam,
            SUM(CASE WHEN re.extract_kind='overview' THEN 1 ELSE 0 END)            AS overview_count,
            SUM(CASE WHEN re.extract_kind='application_example' THEN 1 ELSE 0 END) AS application_count,
            SUM(CASE WHEN re.extract_kind='mechanism' THEN 1 ELSE 0 END)           AS mechanism_count,
            COUNT(*)                                                               AS extract_count
        FROM reaction_extracts re
        GROUP BY re.reaction_family_name
    ),
    q AS (
        SELECT
            em.reaction_family_name AS fam,
            COUNT(DISTINCT CASE WHEN em.role='reactant' AND em.queryable=1 THEN em.smiles END) AS qR,
            COUNT(DISTINCT CASE WHEN em.role='product'  AND em.queryable=1 THEN em.smiles END) AS qP
        FROM extract_molecules em
        GROUP BY em.reaction_family_name
    ),
    pairs AS (
        SELECT reaction_family_name AS fam, COUNT(*) AS pair_count FROM (
            SELECT re.reaction_family_name,
                   re.reactant_smiles, re.product_smiles
            FROM reaction_extracts re
            WHERE re.reactant_smiles IS NOT NULL AND re.product_smiles IS NOT NULL
            GROUP BY re.reaction_family_name, re.reactant_smiles, re.product_smiles
        ) GROUP BY reaction_family_name
    )
    SELECT
        p.fam,
        p.overview_count, p.application_count, p.mechanism_count, p.extract_count,
        COALESCE(q.qR, 0) AS qR,
        COALESCE(q.qP, 0) AS qP,
        COALESCE(pr.pair_count, 0) AS pair_count
    FROM per_fam p
    LEFT JOIN q ON q.fam = p.fam
    LEFT JOIN pairs pr ON pr.fam = p.fam
    """
    rows = [dict(r) for r in conn.execute(sql).fetchall()]
    classified = []
    for r in rows:
        is_rich = (r["overview_count"] >= 1 and r["application_count"] >= 2
                   and r["qR"] >= 3 and r["qP"] >= 3 and r["pair_count"] >= 3)
        bucket = "rich" if is_rich else "shallow"
        r["bucket"] = bucket
        classified.append(r)
    shallow = [r for r in classified if r["bucket"] == "shallow"]
    rich = [r for r in classified if r["bucket"] == "rich"]
    # sort shallow by extract_count asc (most shallow first)
    shallow.sort(key=lambda r: (r["extract_count"], r["application_count"], r["fam"]))

    out_dir = Path(ns.report_dir) / dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    out_dir.mkdir(parents=True, exist_ok=True)
    out_json = out_dir / "shallow_list_full.json"
    out_json.write_text(json.dumps({
        "generated_at": dt.datetime.now().isoformat(),
        "db": ns.db,
        "shallow_count": len(shallow),
        "rich_count": len(rich),
        "shallow_families": shallow,
        "rich_families": [r["fam"] for r in rich],
    }, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"[probe] shallow={len(shallow)} rich={len(rich)}  wrote {out_json}")
    return 0


if __name__ == "__main__":
    sys.exit(main()) if False else __import__("sys").exit(main())
