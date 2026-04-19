from __future__ import annotations

import argparse
import json
import os
import shutil
import sqlite3
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Tuple

SCRIPT_DIR = Path(__file__).resolve().parent

TARGET_FAMILIES: list[str] = [
    "Claisen Condensation / Claisen Reaction",
    "Horner-Wadsworth-Emmons Olefination",
    "Krapcho Dealkoxycarbonylation",
    "Michael Addition Reaction",
    "Regitz Diazo Transfer",
    "Enyne Metathesis",
    "Hofmann-Loffler-Freytag Reaction",
    "Mitsunobu Reaction",
]

ROLE_FALLBACK_ORDER = ["reactant", "product", "reagent", "intermediate", "catalyst", "solvent", "unknown"]


def _connect(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=OFF")
    return conn


def _table_cols(conn: sqlite3.Connection, table: str) -> list[str]:
    return [r[1] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()]


def _get_state(conn: sqlite3.Connection) -> dict:
    q = conn.execute("SELECT COUNT(*) FROM extract_molecules WHERE queryable=1").fetchone()[0]
    fc = conn.execute(
        "SELECT COUNT(DISTINCT reaction_family_name) FROM extract_molecules WHERE queryable=1"
    ).fetchone()[0]
    re = conn.execute("SELECT COUNT(*) FROM reaction_extracts").fetchone()[0]
    em = conn.execute("SELECT COUNT(*) FROM extract_molecules").fetchone()[0]
    return {"queryable": q, "family_coverage": fc, "reaction_extracts": re, "extract_molecules_total": em}


def _family_in_canonical(canonical_conn: sqlite3.Connection, family: str) -> bool:
    cnt = canonical_conn.execute(
        "SELECT COUNT(*) FROM extract_molecules WHERE reaction_family_name=? AND queryable=1",
        (family,),
    ).fetchone()[0]
    return cnt > 0


def _fetch_family_data(stage_conn: sqlite3.Connection, canonical_conn: sqlite3.Connection, family: str) -> Tuple[Optional[sqlite3.Row], List[sqlite3.Row]]:
    re_rows = stage_conn.execute(
        """
        SELECT DISTINCT re.*
        FROM reaction_extracts re
        JOIN extract_molecules em ON em.extract_id = re.id
        WHERE re.reaction_family_name = ?
          AND em.queryable = 1
          AND em.structure_source = 'gemini_auto_seed'
        ORDER BY re.id
        LIMIT 1
        """,
        (family,),
    ).fetchall()
    if not re_rows:
        re_rows = stage_conn.execute(
            """
            SELECT DISTINCT re.*
            FROM reaction_extracts re
            JOIN extract_molecules em ON em.extract_id = re.id
            WHERE re.reaction_family_name = ?
              AND em.queryable = 1
            ORDER BY re.id
            LIMIT 1
            """,
            (family,),
        ).fetchall()
    if not re_rows:
        return None, []
    re_row = re_rows[0]
    extract_id = re_row["id"]
    em_rows = stage_conn.execute(
        "SELECT * FROM extract_molecules WHERE extract_id=? AND queryable=1 ORDER BY CASE role "
        "WHEN 'reactant' THEN 1 WHEN 'product' THEN 2 WHEN 'reagent' THEN 3 WHEN 'intermediate' THEN 4 WHEN 'catalyst' THEN 5 WHEN 'solvent' THEN 6 ELSE 99 END, id",
        (extract_id,),
    ).fetchall()
    canonical_smiles = {r[0] for r in canonical_conn.execute(
        "SELECT smiles FROM extract_molecules WHERE reaction_family_name=? AND queryable=1", (family,)
    ).fetchall() if r[0]}
    em_rows = [r for r in em_rows if r["smiles"] not in canonical_smiles]
    return re_row, em_rows


def _pick_role(row: sqlite3.Row) -> str:
    role = row["role"] if "role" in row.keys() else None
    if role and str(role).strip():
        return str(role).strip()
    source_field = row["source_field"] if "source_field" in row.keys() else None
    if source_field and str(source_field).strip():
        sf = str(source_field).strip().lower()
        for cand in ROLE_FALLBACK_ORDER:
            if cand in sf:
                return cand
    note_text = row["note_text"] if "note_text" in row.keys() else None
    if note_text:
        nt = str(note_text).lower()
        for cand in ROLE_FALLBACK_ORDER:
            if cand in nt:
                return cand
    return "unknown"


def _insert_family(canonical_conn: sqlite3.Connection, stage_conn: sqlite3.Connection, family: str) -> Tuple[Optional[int], List[int], dict]:
    re_row, em_rows = _fetch_family_data(stage_conn, canonical_conn, family)
    if re_row is None or not em_rows:
        return None, [], {"reason": "not_found_or_no_new_queryable"}

    can_re_cols = set(_table_cols(canonical_conn, "reaction_extracts"))
    stage_re_cols = _table_cols(stage_conn, "reaction_extracts")
    shared_re_cols = [c for c in stage_re_cols if c in can_re_cols and c != "id"]

    re_vals = {c: re_row[c] for c in shared_re_cols}
    re_vals["reaction_family_name"] = family
    if "reaction_family_name_norm" in can_re_cols:
        re_vals["reaction_family_name_norm"] = re_row["reaction_family_name_norm"] or family.lower().strip()
    if "created_at" in can_re_cols and not re_vals.get("created_at"):
        re_vals["created_at"] = datetime.utcnow().isoformat()
    if "updated_at" in can_re_cols:
        re_vals["updated_at"] = datetime.utcnow().isoformat()
    if re_vals.get("scheme_candidate_id") is None:
        sc_min = canonical_conn.execute("SELECT MIN(id) FROM scheme_candidates").fetchone()[0]
        re_vals["scheme_candidate_id"] = sc_min if sc_min is not None else 1

    placeholders = ", ".join(["?"] * len(re_vals))
    col_str = ", ".join(re_vals.keys())
    canonical_conn.execute(f"INSERT INTO reaction_extracts ({col_str}) VALUES ({placeholders})", list(re_vals.values()))
    new_re_id = canonical_conn.execute("SELECT last_insert_rowid()").fetchone()[0]

    can_em_cols = set(_table_cols(canonical_conn, "extract_molecules"))
    stage_em_cols = _table_cols(stage_conn, "extract_molecules")
    shared_em_cols = [c for c in stage_em_cols if c in can_em_cols and c not in ("id", "extract_id")]

    inserted_em_ids: list[int] = []
    roles_seen: list[str] = []
    for em in em_rows:
        em_vals = {c: em[c] for c in shared_em_cols}
        em_vals["extract_id"] = new_re_id
        em_vals["structure_source"] = "gemini_auto_seed"
        em_vals["reaction_family_name"] = family
        em_vals["queryable"] = 1
        em_vals["quality_tier"] = em["quality_tier"] or 1
        em_vals["created_at"] = datetime.utcnow().isoformat()
        if "role" in can_em_cols:
            em_vals["role"] = _pick_role(em)
        roles_seen.append(em_vals.get("role", ""))
        if "role_confidence" in can_em_cols and em_vals.get("role_confidence") is None:
            em_vals["role_confidence"] = em["role_confidence"] if "role_confidence" in em.keys() else None
        p2 = ", ".join(["?"] * len(em_vals))
        c2 = ", ".join(em_vals.keys())
        canonical_conn.execute(f"INSERT INTO extract_molecules ({c2}) VALUES ({p2})", list(em_vals.values()))
        inserted_em_ids.append(canonical_conn.execute("SELECT last_insert_rowid()").fetchone()[0])

    return new_re_id, inserted_em_ids, {
        "stage_extract_id": re_row["id"],
        "new_extract_id": new_re_id,
        "inserted_molecule_count": len(inserted_em_ids),
        "roles": roles_seen,
    }


def _rollback_family(canonical_conn: sqlite3.Connection, re_id: Optional[int], em_ids: List[int]) -> None:
    if em_ids:
        placeholders = ",".join(["?"] * len(em_ids))
        canonical_conn.execute(f"DELETE FROM extract_molecules WHERE id IN ({placeholders})", em_ids)
    if re_id is not None:
        canonical_conn.execute("DELETE FROM reaction_extracts WHERE id=?", (re_id,))


def _run_benchmark(canonical_path: Path, benchmark_path: Path, out_dir: Path, label: str) -> dict:
    json_out = out_dir / f"bench_{label}.json"
    csv_out = out_dir / f"bench_{label}.csv"
    md_out = out_dir / f"bench_{label}.md"
    runner = SCRIPT_DIR / "run_named_reaction_benchmark_small.py"
    env = os.environ.copy(); env["PYTHONIOENCODING"] = "utf-8"; env["LABINT_DB_PATH"] = str(canonical_path)
    result = subprocess.run([sys.executable, str(runner), "--db", str(canonical_path), "--benchmark", str(benchmark_path), "--json-out", str(json_out), "--csv-out", str(csv_out), "--report-md", str(md_out)], capture_output=True, text=True, encoding="utf-8", errors="replace", timeout=300, env=env)
    if json_out.exists():
        raw = json.loads(json_out.read_text(encoding="utf-8"))
        summary = raw.get("summary", raw)
        return {"top1_accuracy": float(summary.get("top1_accuracy", 0.0)), "top3_accuracy": float(summary.get("top3_accuracy", 0.0)), "disallow_top3_violations": int(summary.get("disallow_top3_violations", 0)), "db_used": str(canonical_path)}
    raise RuntimeError(f"benchmark failed (rc={result.returncode}):\nSTDOUT:\n{result.stdout[-2000:]}")


def _save_summary(path: Path, obj: dict) -> None:
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")


def main() -> int:
    ap = argparse.ArgumentParser(description="Inject rejected 8 families from v5_stage into canonical")
    ap.add_argument("--canonical", default="app/labint.db")
    ap.add_argument("--stage", default="app/labint_v5_stage.db")
    ap.add_argument("--benchmark", default="benchmark/named_reaction_benchmark_small.json")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--families", default="")
    ap.add_argument("--report-dir", default="reports/inject_rejected_8")
    args = ap.parse_args()
    if not args.dry_run and not args.apply:
        print("[ERROR] --dry-run 또는 --apply 중 하나를 지정하세요")
        return 1
    canonical_path = (SCRIPT_DIR / args.canonical).resolve()
    stage_path = (SCRIPT_DIR / args.stage).resolve()
    benchmark_path = (SCRIPT_DIR / args.benchmark).resolve()
    report_dir = (SCRIPT_DIR / args.report_dir / datetime.now().strftime("%Y%m%d_%H%M%S")).resolve(); report_dir.mkdir(parents=True, exist_ok=True)
    target_families = [f.strip() for f in args.families.split(",") if f.strip()] if args.families else TARGET_FAMILIES
    mode = "DRY-RUN" if args.dry_run else "APPLY"
    print("="*72)
    print("INJECT REJECTED 8 FAMILIES v3")
    print("="*72)
    print(f"mode:       {mode}")
    print(f"canonical:  {canonical_path}")
    print(f"stage:      {stage_path}")
    print(f"benchmark:  {benchmark_path}")
    print(f"report dir: {report_dir}")
    print(f"families:   {len(target_families)}")
    for f in target_families:
        print(f"  - {f}")
    print()
    if args.apply:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_path = canonical_path.parent / f"labint.backup_before_inject_rejected8_v3_{ts}.db"
        shutil.copy2(str(canonical_path), str(backup_path))
        print(f"[backup] {backup_path.name}\n")
    canonical_conn = _connect(canonical_path)
    stage_conn = _connect(stage_path)
    before_state = _get_state(canonical_conn)
    print("[STEP 1] baseline benchmark")
    print(f"  state: {before_state}")
    baseline = _run_benchmark(canonical_path, benchmark_path, report_dir, "baseline")
    print(f"  top1={baseline['top1_accuracy']:.4f}  top3={baseline['top3_accuracy']:.4f}  violations={baseline['disallow_top3_violations']}\n")

    results = []
    if args.dry_run:
        print("[STEP 2] DRY-RUN: actual insert rehearsal with ROLLBACK")
        for family in target_families:
            print(f"  [{family}]")
            if _family_in_canonical(canonical_conn, family):
                print("    -> SKIP (already in canonical)")
                results.append({"family": family, "status": "skipped_already_present"})
                continue
            re_row, em_rows = _fetch_family_data(stage_conn, canonical_conn, family)
            if re_row is None or not em_rows:
                print("    -> NOT FOUND in stage")
                results.append({"family": family, "status": "not_found_in_stage"})
                continue
            canonical_conn.execute("BEGIN")
            try:
                new_re_id, new_em_ids, meta = _insert_family(canonical_conn, stage_conn, family)
                print(f"    -> READY: stage_extract_id={meta['stage_extract_id']} insert_molecules={meta['inserted_molecule_count']} roles={meta['roles']}")
                results.append({"family": family, "status": "ready", **meta})
            except Exception as e:
                print(f"    -> INSERT_REHEARSAL_ERROR: {e}")
                results.append({"family": family, "status": "insert_rehearsal_error", "error": str(e)})
            finally:
                canonical_conn.execute("ROLLBACK")
        summary = {"mode": "dry-run", "before_state": before_state, "baseline": baseline, "results": results}
        _save_summary(report_dir / "inject_summary.json", summary)
        print("\n[DRY-RUN done] inject_summary.json 확인 후 --apply 로 재실행하세요")
        canonical_conn.close(); stage_conn.close(); return 0

    print("[STEP 2] INSERT + benchmark-guard (family별)")
    for family in target_families:
        print(f"\n  [{family}]")
        if _family_in_canonical(canonical_conn, family):
            print("    -> already in canonical, skip")
            results.append({"family": family, "status": "skipped_already_present"})
            continue
        re_row, em_rows = _fetch_family_data(stage_conn, canonical_conn, family)
        if re_row is None or not em_rows:
            print("    -> NOT FOUND in stage")
            results.append({"family": family, "status": "not_found_in_stage"})
            continue
        print(f"    found: {len(em_rows)} queryable molecules in stage")
        canonical_conn.execute("BEGIN")
        try:
            new_re_id, new_em_ids, meta = _insert_family(canonical_conn, stage_conn, family)
        except Exception as e:
            canonical_conn.execute("ROLLBACK")
            print(f"    insert error: {e}")
            results.append({"family": family, "status": "insert_error", "error": str(e)})
            continue
        canonical_conn.execute("COMMIT")
        safe_name = family.replace("/", "_").replace(" ", "_").replace("-", "_")
        try:
            bench = _run_benchmark(canonical_path, benchmark_path, report_dir, f"after_{safe_name}")
        except Exception as e:
            canonical_conn.execute("BEGIN"); _rollback_family(canonical_conn, new_re_id, new_em_ids); canonical_conn.execute("COMMIT")
            print(f"    benchmark error → ROLLBACK: {e}")
            results.append({"family": family, "status": "benchmark_error", "error": str(e), **meta})
            continue
        t1, t3, vio = bench["top1_accuracy"], bench["top3_accuracy"], bench["disallow_top3_violations"]
        print(f"    benchmark: top1={t1:.4f}  top3={t3:.4f}  violations={vio}")
        if t1 < baseline["top1_accuracy"] - 0.001 or vio > baseline["disallow_top3_violations"]:
            canonical_conn.execute("BEGIN"); _rollback_family(canonical_conn, new_re_id, new_em_ids); canonical_conn.execute("COMMIT")
            print(f"    REJECTED")
            results.append({"family": family, "status": "rejected_regression", "benchmark": bench, **meta})
        else:
            print(f"    ACCEPTED")
            results.append({"family": family, "status": "accepted", "benchmark": bench, **meta})
    after_state = _get_state(canonical_conn)
    print("\n" + "="*72)
    print("[RESULT]")
    accepted = [r for r in results if r["status"] == "accepted"]
    rejected = [r for r in results if r["status"] == "rejected_regression"]
    skipped = [r for r in results if r["status"] not in ("accepted", "rejected_regression")]
    print(f"  accepted:  {len(accepted)}")
    print(f"  rejected:  {len(rejected)}")
    print(f"  skipped:   {len(skipped)}")
    print(f"  state before: {before_state}")
    print(f"  state after:  {after_state}")
    print(f"  delta:  queryable +{after_state['queryable']-before_state['queryable']}  family_coverage +{after_state['family_coverage']-before_state['family_coverage']}")
    summary = {"mode": "apply", "before_state": before_state, "after_state": after_state, "baseline": baseline, "results": results}
    _save_summary(report_dir / "inject_summary.json", summary)
    print(f"\n[summary] {report_dir / 'inject_summary.json'}")
    print("="*72)
    canonical_conn.close(); stage_conn.close(); return 0


if __name__ == "__main__":
    raise SystemExit(main())
