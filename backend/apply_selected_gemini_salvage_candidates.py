from __future__ import annotations

import argparse
import json
import os
import shutil
import sqlite3
import subprocess
import sys
import tempfile
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

try:
    from rdkit import Chem
    from rdkit.Chem import AllChem
except Exception:  # pragma: no cover
    Chem = None
    AllChem = None


def now_ts() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def ensure_parent(p: Path) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)


def load_json(p: Path) -> Any:
    return json.loads(p.read_text(encoding="utf-8"))


def resolve_user_path(backend_root: Path, raw: str) -> Path:
    norm = raw.replace("\\", "/")
    path = Path(norm)
    return path if path.is_absolute() else (backend_root / path).resolve()


def canonicalize_smiles(smiles: str) -> str | None:
    if not smiles:
        return None
    if Chem is None:
        return smiles.strip()
    mol = Chem.MolFromSmiles(smiles)
    if not mol:
        return None
    return Chem.MolToSmiles(mol, canonical=True)


def morgan_fp_blob(smiles: str) -> bytes | None:
    if not smiles or Chem is None or AllChem is None:
        return None
    mol = Chem.MolFromSmiles(smiles)
    if not mol:
        return None
    fp = AllChem.GetMorganFingerprintAsBitVect(mol, 2, nBits=2048)
    return "".join("1" if fp.GetBit(i) else "0" for i in range(2048)).encode("ascii")


def next_id(conn: sqlite3.Connection, table: str) -> int:
    return int(conn.execute(f"SELECT COALESCE(MAX(id), 0) FROM {table}").fetchone()[0]) + 1


def table_columns(conn: sqlite3.Connection, table: str) -> list[str]:
    return [r[1] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()]


def backup_file(src: Path, backup_dir: Path) -> Path:
    backup_dir.mkdir(parents=True, exist_ok=True)
    dst = backup_dir / f"{src.stem}_{now_ts()}{src.suffix}"
    shutil.copy2(src, dst)
    return dst


def find_latest_salvage_summary(report_root: Path) -> Path:
    cands = sorted(report_root.glob("*/gemini_salvage_summary.json"))
    if not cands:
        raise FileNotFoundError(f"No gemini salvage summary found under {report_root}")
    return cands[-1]


def run_benchmark(backend_root: Path, db_path: Path, benchmark_path: Path, out_dir: Path, label: str) -> dict[str, Any]:
    out_dir.mkdir(parents=True, exist_ok=True)
    json_out = out_dir / f"benchmark_{label}.json"
    csv_out = out_dir / f"benchmark_{label}.csv"
    md_out = out_dir / f"benchmark_{label}.md"
    cmd = [
        sys.executable,
        str(backend_root / "run_named_reaction_benchmark_small.py"),
        "--db", str(db_path),
        "--benchmark", str(benchmark_path),
        "--json-out", str(json_out),
        "--csv-out", str(csv_out),
        "--report-md", str(md_out),
    ]
    env = os.environ.copy()
    env["LABINT_DB_PATH"] = str(db_path)
    cp = subprocess.run(
        cmd,
        cwd=str(backend_root),
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        env=env,
    )
    if cp.returncode != 0:
        raise RuntimeError(f"benchmark failed\nSTDOUT:\n{cp.stdout}\nSTDERR:\n{cp.stderr}")
    data = load_json(json_out)
    summary = data.get("summary", data)
    return {
        "db_used": str(db_path),
        "top1_accuracy": float(summary.get("top1_accuracy", 0.0)),
        "top3_accuracy": float(summary.get("top3_accuracy", 0.0)),
        "disallow_top3_violations": int(summary.get("disallow_top3_violations", 0)),
        "json_out": str(json_out),
        "csv_out": str(csv_out),
        "md_out": str(md_out),
    }


def benchmark_ok(before: dict[str, Any], after: dict[str, Any]) -> tuple[bool, str]:
    if after["top1_accuracy"] < before["top1_accuracy"]:
        return False, f"top1 regressed: {before['top1_accuracy']:.4f} -> {after['top1_accuracy']:.4f}"
    if after["top3_accuracy"] < before["top3_accuracy"]:
        return False, f"top3 regressed: {before['top3_accuracy']:.4f} -> {after['top3_accuracy']:.4f}"
    if after["disallow_top3_violations"] > before["disallow_top3_violations"]:
        return False, (
            "violations regressed: "
            f"{before['disallow_top3_violations']} -> {after['disallow_top3_violations']}"
        )
    return True, ""


def extract_summary_rows(summary_path: Path) -> dict[str, dict[str, Any]]:
    data = load_json(summary_path)
    out: dict[str, dict[str, Any]] = {}
    for fam in data.get("families", []):
        out[str(fam.get("family_name", "")).strip()] = fam
    return out


def dedupe_exists(conn: sqlite3.Connection, family: str, role: str, smiles: str, structure_source: str) -> bool:
    row = conn.execute(
        """
        SELECT 1
        FROM extract_molecules
        WHERE reaction_family_name=? AND role=? AND smiles=? AND structure_source=?
        LIMIT 1
        """,
        (family, role, smiles, structure_source),
    ).fetchone()
    return row is not None


def insert_selection(conn: sqlite3.Connection, source_conn: sqlite3.Connection, selection: dict[str, Any]) -> dict[str, Any]:
    family = selection["family_name"]
    stage_extract_id = int(selection["stage_extract_id"])
    page_no = int(selection.get("template_page_no") or 0) or None
    candidate = deepcopy(selection["candidate"])

    template_row = source_conn.execute(
        "SELECT * FROM reaction_extracts WHERE id=?",
        (stage_extract_id,),
    ).fetchone()
    if template_row is None:
        raise RuntimeError(f"stage extract not found for {family}: {stage_extract_id}")
    template = dict(template_row)

    reactants = [canonicalize_smiles(s) for s in candidate.get("substrate_smiles", [])]
    products = [canonicalize_smiles(s) for s in candidate.get("product_smiles", [])]
    reagents = [canonicalize_smiles(s) for s in candidate.get("optional_reagent_smiles", [])]

    for role_name, arr in (("substrate_smiles", reactants), ("product_smiles", products), ("optional_reagent_smiles", reagents)):
        if any(s is None for s in arr):
            raise RuntimeError(f"invalid smiles in {family} / {role_name}")

    structure_source = "gemini_salvage_seed"
    # family-level idempotency: if same reactant+product already exists, skip the whole family.
    if reactants and products:
        react_ok = all(dedupe_exists(conn, family, "reactant", s, structure_source) for s in reactants)
        prod_ok = all(dedupe_exists(conn, family, "product", s, structure_source) for s in products)
        if react_ok and prod_ok:
            return {
                "family_name": family,
                "status": "skipped_existing",
                "selected_candidate_id": selection["selected_candidate_id"],
                "inserted_extract_id": None,
                "inserted_molecule_ids": [],
            }

    extract_id = next_id(conn, "reaction_extracts")
    cols = table_columns(conn, "reaction_extracts")
    notes = (
        f"[gemini_salvage_seed] {candidate.get('rationale', '')}\n"
        f"[collision_avoidance] {candidate.get('collision_avoidance_note', '')}"
    ).strip()
    template.update(
        {
            "id": extract_id,
            "reaction_family_name": family,
            "reaction_family_name_norm": family.lower(),
            "extract_kind": "canonical_overview",
            "transformation_text": f"{family} salvage canonical example",
            "reactants_text": "; ".join(reactants) if reactants else None,
            "products_text": "; ".join(products) if products else None,
            "intermediates_text": None,
            "reagents_text": "; ".join(reagents) if reagents else None,
            "catalysts_text": None,
            "solvents_text": None,
            "temperature_text": None,
            "time_text": None,
            "yield_text": None,
            "workup_text": None,
            "conditions_text": None,
            "notes_text": notes,
            "reactant_smiles": "; ".join(reactants) if reactants else None,
            "product_smiles": "; ".join(products) if products else None,
            "smiles_confidence": 0.95,
            "extraction_confidence": 0.95,
            "parse_status": "promoted",
            "promote_decision": "accepted",
            "rejection_reason": None,
            "extractor_model": "gemini-2.5-pro",
            "extractor_prompt_version": "salvage_v1",
            "extraction_raw_json": json.dumps(candidate, ensure_ascii=False),
            "created_at": datetime.now().isoformat(timespec="seconds"),
            "updated_at": datetime.now().isoformat(timespec="seconds"),
        }
    )
    conn.execute(
        f"INSERT INTO reaction_extracts ({','.join(cols)}) VALUES ({','.join('?' * len(cols))})",
        [template.get(c) for c in cols],
    )

    inserted_molecule_ids: list[int] = []
    role_specs = [
        ("reactant", reactants, 1, "reactants_text"),
        ("product", products, 1, "products_text"),
        ("reagent", reagents, 0, "reagents_text"),
    ]
    for role, smiles_list, queryable, source_field in role_specs:
        for smiles in smiles_list:
            if dedupe_exists(conn, family, role, smiles, structure_source):
                continue
            mol_id = next_id(conn, "extract_molecules")
            conn.execute(
                """
                INSERT INTO extract_molecules (
                    id, extract_id, role, smiles, smiles_kind, quality_tier,
                    reaction_family_name, source_zip, page_no, queryable, note_text,
                    morgan_fp, normalized_text, source_field, structure_source,
                    alias_id, fg_tags, role_confidence, created_at
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    mol_id,
                    extract_id,
                    role,
                    smiles,
                    "explicit",
                    1,
                    family,
                    None,
                    page_no,
                    queryable,
                    "[gemini_salvage_seed]",
                    morgan_fp_blob(smiles),
                    None,
                    source_field,
                    structure_source,
                    None,
                    None,
                    1.0,
                    datetime.now().isoformat(timespec="seconds"),
                ),
            )
            inserted_molecule_ids.append(mol_id)

    return {
        "family_name": family,
        "status": "inserted",
        "selected_candidate_id": selection["selected_candidate_id"],
        "inserted_extract_id": extract_id,
        "inserted_molecule_ids": inserted_molecule_ids,
    }


def apply_to_db(db_path: Path, source_db: Path, selections: list[dict[str, Any]]) -> list[dict[str, Any]]:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    source_conn = sqlite3.connect(str(source_db))
    source_conn.row_factory = sqlite3.Row
    applied: list[dict[str, Any]] = []
    try:
        conn.execute("BEGIN")
        for sel in selections:
            applied.append(insert_selection(conn, source_conn, sel))
        conn.commit()
        return applied
    except Exception:
        conn.rollback()
        raise
    finally:
        source_conn.close()
        conn.close()


def main() -> int:
    ap = argparse.ArgumentParser(description="Apply selected Gemini salvage candidates with benchmark guard.")
    ap.add_argument("--canonical-db", default=r"app\labint.db")
    ap.add_argument("--work-db", default=r"app\labint_round9_bridge_work.db")
    ap.add_argument("--source-db", default=r"app\labint_v5_stage.db")
    ap.add_argument("--benchmark", default=r"benchmark\named_reaction_benchmark_small.json")
    ap.add_argument("--selection-json", default="gemini_salvage_selected_candidates.json")
    ap.add_argument("--salvage-summary", default="")
    ap.add_argument("--report-dir", default=r"reports\gemini_salvage_apply")
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()

    backend_root = Path.cwd()
    canonical_db = resolve_user_path(backend_root, args.canonical_db)
    work_db = resolve_user_path(backend_root, args.work_db)
    source_db = resolve_user_path(backend_root, args.source_db)
    benchmark_path = resolve_user_path(backend_root, args.benchmark)
    selection_json = resolve_user_path(backend_root, args.selection_json)
    report_root = resolve_user_path(backend_root, args.report_dir)
    report_dir = report_root / now_ts()
    report_dir.mkdir(parents=True, exist_ok=True)

    if not canonical_db.exists():
        raise SystemExit(f"canonical db not found: {canonical_db}")
    if not source_db.exists():
        raise SystemExit(f"source db not found: {source_db}")
    if not benchmark_path.exists():
        raise SystemExit(f"benchmark not found: {benchmark_path}")
    if not selection_json.exists():
        raise SystemExit(f"selection json not found: {selection_json}")

    if args.salvage_summary:
        salvage_summary = resolve_user_path(backend_root, args.salvage_summary)
    else:
        salvage_summary = find_latest_salvage_summary(backend_root / "reports" / "gemini_rejected_salvage")
    salvage_rows = extract_summary_rows(salvage_summary) if salvage_summary.exists() else {}

    sel_data = load_json(selection_json)
    selections = sel_data.get("selections", [])

    print("=" * 76)
    print("APPLY SELECTED GEMINI SALVAGE CANDIDATES")
    print("=" * 76)
    print(f"mode:          {'APPLY' if args.apply else 'DRY-RUN'}")
    print(f"canonical_db:  {canonical_db}")
    print(f"work_db:       {work_db if work_db.exists() else '(missing / skip)'}")
    print(f"source_db:     {source_db}")
    print(f"benchmark:     {benchmark_path}")
    print(f"summary:       {salvage_summary}")
    print(f"selection:     {selection_json}")
    print(f"report dir:    {report_dir}")
    print(f"families:      {len(selections)}")
    for sel in selections:
        fam = sel["family_name"]
        cand = sel["selected_candidate_id"]
        changed = salvage_rows.get(fam, {}).get("changed_cases")
        print(f"  - {fam} <- {cand} (changed_cases={changed})")

    baseline = run_benchmark(backend_root, canonical_db, benchmark_path, report_dir, "baseline")
    print("[baseline]")
    print(json.dumps(baseline, ensure_ascii=False, indent=2))

    with tempfile.TemporaryDirectory(prefix="gemini_salvage_apply_") as td:
        temp_db = Path(td) / canonical_db.name
        shutil.copy2(canonical_db, temp_db)
        temp_apply = apply_to_db(temp_db, source_db, selections)
        temp_result = run_benchmark(backend_root, temp_db, benchmark_path, report_dir, "temp_after")
        ok, reason = benchmark_ok(baseline, temp_result)
        print("[temp benchmark]")
        print(json.dumps(temp_result, ensure_ascii=False, indent=2))
        print(f"[guard] {'PASS' if ok else 'FAIL'} {reason}")

        report_payload = {
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "mode": "apply" if args.apply else "dry-run",
            "canonical_db": str(canonical_db),
            "work_db": str(work_db) if work_db.exists() else None,
            "source_db": str(source_db),
            "benchmark": str(benchmark_path),
            "salvage_summary": str(salvage_summary),
            "selection_json": str(selection_json),
            "baseline": baseline,
            "temp_after": temp_result,
            "guard_pass": ok,
            "guard_reason": reason,
            "temp_apply": temp_apply,
            "selections": selections,
        }

        if not ok:
            (report_dir / "gemini_salvage_apply_summary.json").write_text(
                json.dumps(report_payload, ensure_ascii=False, indent=2), encoding="utf-8"
            )
            raise SystemExit(f"benchmark guard failed: {reason}")

        if args.apply:
            backup_dir = report_dir / "backups"
            canonical_backup = backup_file(canonical_db, backup_dir)
            work_backup = backup_file(work_db, backup_dir) if work_db.exists() else None
            apply_results = apply_to_db(canonical_db, source_db, selections)
            work_results = apply_to_db(work_db, source_db, selections) if work_db.exists() else []
            final_result = run_benchmark(backend_root, canonical_db, benchmark_path, report_dir, "final_after")
            ok2, reason2 = benchmark_ok(baseline, final_result)
            report_payload.update(
                {
                    "canonical_backup": str(canonical_backup),
                    "work_backup": str(work_backup) if work_backup else None,
                    "apply_results": apply_results,
                    "work_apply_results": work_results,
                    "final_after": final_result,
                    "final_guard_pass": ok2,
                    "final_guard_reason": reason2,
                }
            )
            if not ok2:
                raise SystemExit(f"post-apply benchmark guard failed: {reason2}")
        else:
            report_payload.update({"apply_results": [], "work_apply_results": []})

        (report_dir / "gemini_salvage_apply_summary.json").write_text(
            json.dumps(report_payload, ensure_ascii=False, indent=2), encoding="utf-8"
        )

    print("=" * 76)
    print("[DONE] selected gemini salvage apply finished")
    print(f"summary: {report_dir / 'gemini_salvage_apply_summary.json'}")
    print("=" * 76)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
