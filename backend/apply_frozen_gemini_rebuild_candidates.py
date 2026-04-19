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
from typing import Any, Iterable

SCRIPT_DIR = Path(__file__).resolve().parent


def _connect(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=OFF")
    return conn


def _table_cols(conn: sqlite3.Connection, table: str) -> list[str]:
    return [r[1] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()]


def _ts() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def _norm_family(name: str) -> str:
    return " ".join((name or "").lower().split())


def _pick_latest_summary(root: Path, mode: str) -> Path | None:
    candidates: list[Path] = []
    if not root.exists():
        return None
    for p in root.glob("*/gemini_rebuild_rejected8_summary.json"):
        try:
            data = json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            continue
        if str(data.get("mode", "")).upper() == mode.upper():
            candidates.append(p)
    return sorted(candidates)[-1] if candidates else None


def _get_state(conn: sqlite3.Connection) -> dict[str, int]:
    queryable = conn.execute("SELECT COUNT(*) FROM extract_molecules WHERE queryable=1").fetchone()[0]
    fam_cov = conn.execute(
        "SELECT COUNT(DISTINCT reaction_family_name) FROM extract_molecules WHERE queryable=1 AND COALESCE(reaction_family_name,'')<>''"
    ).fetchone()[0]
    rxn_extracts = conn.execute("SELECT COUNT(*) FROM reaction_extracts").fetchone()[0]
    em_total = conn.execute("SELECT COUNT(*) FROM extract_molecules").fetchone()[0]
    return {
        "queryable": int(queryable),
        "family_coverage": int(fam_cov),
        "reaction_extracts": int(rxn_extracts),
        "extract_molecules_total": int(em_total),
    }


def _run_benchmark(db_path: Path, benchmark_path: Path, out_dir: Path, label: str) -> dict[str, Any]:
    json_out = out_dir / f"bench_{label}.json"
    csv_out = out_dir / f"bench_{label}.csv"
    md_out = out_dir / f"bench_{label}.md"
    runner = SCRIPT_DIR / "run_named_reaction_benchmark_small.py"
    env = os.environ.copy()
    env["PYTHONIOENCODING"] = "utf-8"
    env["LABINT_DB_PATH"] = str(db_path)
    result = subprocess.run(
        [
            sys.executable,
            str(runner),
            "--db",
            str(db_path),
            "--benchmark",
            str(benchmark_path),
            "--json-out",
            str(json_out),
            "--csv-out",
            str(csv_out),
            "--report-md",
            str(md_out),
        ],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=300,
        env=env,
    )
    if not json_out.exists():
        raise RuntimeError(f"benchmark failed rc={result.returncode}\nSTDOUT:\n{result.stdout[-2000:]}\nSTDERR:\n{result.stderr[-2000:]}")
    raw = json.loads(json_out.read_text(encoding="utf-8"))
    summary = raw.get("summary", raw)
    return {
        "top1_accuracy": float(summary.get("top1_accuracy", 0.0)),
        "top3_accuracy": float(summary.get("top3_accuracy", 0.0)),
        "disallow_top3_violations": int(summary.get("disallow_top3_violations", 0)),
        "db_used": str(db_path),
    }


def _save_json(path: Path, obj: Any) -> None:
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")


def _fetch_stage_context(stage_conn: sqlite3.Connection, family: str, stage_extract_id: int) -> tuple[sqlite3.Row, list[sqlite3.Row]]:
    re_row = stage_conn.execute(
        "SELECT * FROM reaction_extracts WHERE id=? AND lower(trim(reaction_family_name))=lower(trim(?))",
        (stage_extract_id, family),
    ).fetchone()
    if re_row is None:
        raise RuntimeError(f"stage extract not found: family={family} id={stage_extract_id}")
    em_rows = stage_conn.execute(
        "SELECT * FROM extract_molecules WHERE extract_id=? ORDER BY id",
        (stage_extract_id,),
    ).fetchall()
    return re_row, em_rows


def _insert_frozen_candidate(canonical_conn: sqlite3.Connection, stage_conn: sqlite3.Connection, family: str, stage_extract_id: int, candidate: dict[str, Any], model: str) -> tuple[int, list[int], dict[str, Any]]:
    re_row, stage_em_rows = _fetch_stage_context(stage_conn, family, stage_extract_id)
    can_re_cols = set(_table_cols(canonical_conn, "reaction_extracts"))
    stage_re_cols = _table_cols(stage_conn, "reaction_extracts")
    shared_re_cols = [c for c in stage_re_cols if c in can_re_cols and c != "id"]
    re_vals = {c: re_row[c] for c in shared_re_cols}
    now = datetime.utcnow().isoformat()
    re_vals["reaction_family_name"] = family
    if "reaction_family_name_norm" in can_re_cols:
        re_vals["reaction_family_name_norm"] = _norm_family(family)
    if "reactant_smiles" in can_re_cols:
        re_vals["reactant_smiles"] = ".".join(candidate.get("substrate_smiles", []) or [])
    if "product_smiles" in can_re_cols:
        re_vals["product_smiles"] = ".".join(candidate.get("product_smiles", []) or [])
    if "reagents_text" in can_re_cols and candidate.get("optional_reagent_smiles"):
        re_vals["reagents_text"] = "; ".join(candidate.get("optional_reagent_smiles") or [])
    if "smiles_confidence" in can_re_cols:
        re_vals["smiles_confidence"] = 1.0
    if "extraction_confidence" in can_re_cols:
        re_vals["extraction_confidence"] = 1.0
    if "parse_status" in can_re_cols:
        re_vals["parse_status"] = "promoted"
    if "promote_decision" in can_re_cols:
        re_vals["promote_decision"] = "accept"
    if "rejection_reason" in can_re_cols:
        re_vals["rejection_reason"] = None
    if "extractor_model" in can_re_cols:
        re_vals["extractor_model"] = model
    if "extractor_prompt_version" in can_re_cols:
        re_vals["extractor_prompt_version"] = "rebuild_rejected8_freeze_v1"
    if "extraction_raw_json" in can_re_cols:
        re_vals["extraction_raw_json"] = json.dumps(candidate, ensure_ascii=False)
    if "created_at" in can_re_cols:
        re_vals["created_at"] = re_vals.get("created_at") or now
    if "updated_at" in can_re_cols:
        re_vals["updated_at"] = now
    if re_vals.get("scheme_candidate_id") is None:
        sc_min = canonical_conn.execute("SELECT MIN(id) FROM scheme_candidates").fetchone()[0]
        re_vals["scheme_candidate_id"] = sc_min if sc_min is not None else 1

    placeholders = ", ".join(["?"] * len(re_vals))
    canonical_conn.execute(
        f"INSERT INTO reaction_extracts ({', '.join(re_vals.keys())}) VALUES ({placeholders})",
        list(re_vals.values()),
    )
    new_re_id = int(canonical_conn.execute("SELECT last_insert_rowid()").fetchone()[0])

    can_em_cols = set(_table_cols(canonical_conn, "extract_molecules"))
    proto = stage_em_rows[0] if stage_em_rows else None
    page_no = proto["page_no"] if proto is not None and "page_no" in proto.keys() else None
    source_zip = proto["source_zip"] if proto is not None and "source_zip" in proto.keys() else None

    molecules: list[tuple[str, str]] = []
    for s in candidate.get("substrate_smiles", []) or []:
        molecules.append(("reactant", s))
    for s in candidate.get("product_smiles", []) or []:
        molecules.append(("product", s))
    for s in candidate.get("optional_reagent_smiles", []) or []:
        molecules.append(("reagent", s))

    inserted_ids: list[int] = []
    for role, smiles in molecules:
        em_vals: dict[str, Any] = {
            "extract_id": new_re_id,
            "role": role,
            "smiles": smiles,
            "quality_tier": 1,
            "reaction_family_name": family,
            "queryable": 1,
            "structure_source": "gemini_rebuild_seed",
        }
        if "smiles_kind" in can_em_cols:
            em_vals["smiles_kind"] = "smiles"
        if "source_zip" in can_em_cols:
            em_vals["source_zip"] = source_zip
        if "page_no" in can_em_cols:
            em_vals["page_no"] = page_no
        if "note_text" in can_em_cols:
            em_vals["note_text"] = f"frozen apply from dry-run accepted candidate ({role})"
        if "normalized_text" in can_em_cols:
            em_vals["normalized_text"] = smiles
        if "source_field" in can_em_cols:
            em_vals["source_field"] = role
        if "fg_tags" in can_em_cols:
            em_vals["fg_tags"] = None
        if "role_confidence" in can_em_cols:
            em_vals["role_confidence"] = 1.0
        if "created_at" in can_em_cols:
            em_vals["created_at"] = now
        cols = [c for c in em_vals.keys() if c in can_em_cols]
        vals = [em_vals[c] for c in cols]
        placeholders = ", ".join(["?"] * len(cols))
        canonical_conn.execute(
            f"INSERT INTO extract_molecules ({', '.join(cols)}) VALUES ({placeholders})",
            vals,
        )
        inserted_ids.append(int(canonical_conn.execute("SELECT last_insert_rowid()").fetchone()[0]))
    return new_re_id, inserted_ids, {"stage_extract_id": stage_extract_id, "inserted_molecule_count": len(inserted_ids)}


def _rollback_family(canonical_conn: sqlite3.Connection, new_extract_id: int | None, inserted_molecule_ids: Iterable[int]) -> None:
    ids = list(inserted_molecule_ids)
    if ids:
        placeholders = ",".join(["?"] * len(ids))
        canonical_conn.execute(f"DELETE FROM extract_molecules WHERE id IN ({placeholders})", ids)
    if new_extract_id is not None:
        canonical_conn.execute("DELETE FROM reaction_extracts WHERE id=?", (new_extract_id,))


def main() -> int:
    ap = argparse.ArgumentParser(description="Apply frozen accepted candidates from rebuild dry-run summary")
    ap.add_argument("--canonical", default="app/labint.db")
    ap.add_argument("--stage", default="app/labint_v5_stage.db")
    ap.add_argument("--benchmark", default="benchmark/named_reaction_benchmark_small.json")
    ap.add_argument("--summary", default=None, help="Dry-run summary json to use. Defaults to latest DRY-RUN summary.")
    ap.add_argument("--exclude-apply-summary", default=None, help="Apply summary json whose applied_families should be excluded. Defaults to latest APPLY summary.")
    ap.add_argument("--family", action="append", default=None, help="Limit to specific family/families.")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    canonical_path = (SCRIPT_DIR / args.canonical).resolve()
    stage_path = (SCRIPT_DIR / args.stage).resolve()
    benchmark_path = (SCRIPT_DIR / args.benchmark).resolve()
    reports_root = SCRIPT_DIR / "reports" / "gemini_rebuild_rejected8"
    summary_path = Path(args.summary).resolve() if args.summary else _pick_latest_summary(reports_root, "DRY-RUN")
    exclude_summary_path = Path(args.exclude_apply_summary).resolve() if args.exclude_apply_summary else _pick_latest_summary(reports_root, "APPLY")

    if summary_path is None or not summary_path.exists():
        raise SystemExit("Dry-run summary not found.")

    data = json.loads(summary_path.read_text(encoding="utf-8"))
    accepted_entries = data.get("accepted", [])
    applied_families = set()
    if exclude_summary_path and exclude_summary_path.exists():
        try:
            apply_data = json.loads(exclude_summary_path.read_text(encoding="utf-8"))
            applied_families.update(apply_data.get("applied_families", []))
        except Exception:
            pass
    requested = set(args.family or [])
    if requested:
        accepted_entries = [e for e in accepted_entries if e.get("family") in requested]
    accepted_entries = [e for e in accepted_entries if e.get("family") not in applied_families]

    out_dir = SCRIPT_DIR / "reports" / "gemini_rebuild_rejected8_frozen_apply" / _ts()
    out_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 72)
    print("APPLY FROZEN GEMINI REBUILD CANDIDATES")
    print("=" * 72)
    print(f"mode:          {'DRY-RUN' if args.dry_run else 'APPLY'}")
    print(f"canonical:     {canonical_path}")
    print(f"stage:         {stage_path}")
    print(f"benchmark:     {benchmark_path}")
    print(f"summary:       {summary_path}")
    print(f"exclude_apply: {exclude_summary_path}")
    print(f"report dir:    {out_dir}")
    print(f"families:      {len(accepted_entries)}")
    for e in accepted_entries:
        print(f"  - {e['family']} (attempt={e.get('attempt')}, stage_extract_id={e.get('stage_extract_id')})")

    can_conn = _connect(canonical_path)
    stage_conn = _connect(stage_path)
    baseline = _run_benchmark(canonical_path, benchmark_path, out_dir, "baseline")
    print(f"[baseline] top1={baseline['top1_accuracy']:.4f} top3={baseline['top3_accuracy']:.4f} violations={baseline['disallow_top3_violations']}")
    state_before = _get_state(can_conn)

    if not args.dry_run:
        backup_path = canonical_path.with_name(f"labint.backup_before_frozen_rebuild_apply_{_ts()}.db")
        shutil.copy2(canonical_path, backup_path)
        print(f"[backup] {backup_path.name}")

    accepted: list[dict[str, Any]] = []
    rejected: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []

    for entry in accepted_entries:
        fam = entry["family"]
        # skip if already there as rebuild seed
        existing = can_conn.execute(
            "SELECT COUNT(*) FROM extract_molecules WHERE queryable=1 AND reaction_family_name=? AND COALESCE(structure_source,'')='gemini_rebuild_seed'",
            (fam,),
        ).fetchone()[0]
        if existing:
            print(f"\n[{fam}]\n  already present as gemini_rebuild_seed -> SKIP")
            skipped.append({"family": fam, "reason": "already_present"})
            continue
        print(f"\n[{fam}]")
        new_re_id = None
        new_em_ids: list[int] = []
        try:
            new_re_id, new_em_ids, info = _insert_frozen_candidate(
                can_conn,
                stage_conn,
                fam,
                int(entry["stage_extract_id"]),
                entry["candidate"],
                data.get("model", "gemini-2.5-pro") if isinstance(data, dict) else "gemini-2.5-pro",
            )
            can_conn.commit()
            bench = _run_benchmark(canonical_path, benchmark_path, out_dir, f"{fam}_after".replace("/", "_").replace(" ", "_"))
            ok = (
                bench["top1_accuracy"] >= 1.0
                and bench["top3_accuracy"] >= 1.0
                and bench["disallow_top3_violations"] == 0
            )
            print(f"  benchmark: top1={bench['top1_accuracy']:.4f} top3={bench['top3_accuracy']:.4f} violations={bench['disallow_top3_violations']}")
            if ok:
                print("  APPLIED")
                accepted.append({"family": fam, "benchmark": bench, **info})
            else:
                can_conn.execute("BEGIN")
                _rollback_family(can_conn, new_re_id, new_em_ids)
                can_conn.commit()
                print("  REJECTED")
                rejected.append({"family": fam, "benchmark": bench, **info})
        except Exception as exc:
            try:
                can_conn.execute("BEGIN")
                _rollback_family(can_conn, new_re_id, new_em_ids)
                can_conn.commit()
            except Exception:
                pass
            print(f"  ERROR: {exc}")
            rejected.append({"family": fam, "error": str(exc)})

    final_bench = _run_benchmark(canonical_path, benchmark_path, out_dir, "final")
    state_after = _get_state(can_conn)
    print("\n" + "=" * 72)
    print("[RESULT]")
    print(f"  accepted: {len(accepted)}")
    print(f"  rejected: {len(rejected)}")
    print(f"  skipped:  {len(skipped)}")
    if accepted:
        print("  applied:  " + ", ".join(x["family"] for x in accepted))
    print(f"  state before: {state_before}")
    print(f"  state after:  {state_after}")
    print(f"  final bench: top1={final_bench['top1_accuracy']:.4f} top3={final_bench['top3_accuracy']:.4f} violations={final_bench['disallow_top3_violations']}")
    summary = {
        "mode": "DRY-RUN" if args.dry_run else "APPLY",
        "summary": str(summary_path),
        "exclude_apply_summary": str(exclude_summary_path) if exclude_summary_path else None,
        "accepted": accepted,
        "rejected": rejected,
        "skipped": skipped,
        "state_before": state_before,
        "state_after": state_after,
        "final_benchmark": final_bench,
    }
    _save_json(out_dir / "frozen_rebuild_apply_summary.json", summary)
    print(f"  summary:   {out_dir / 'frozen_rebuild_apply_summary.json'}")
    print("=" * 72)
    can_conn.close(); stage_conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
