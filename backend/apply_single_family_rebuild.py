
from __future__ import annotations

import argparse
import json
import shutil
import sqlite3
import subprocess
import sys
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any

SCRIPT_DIR = Path(__file__).resolve().parent


def connect(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=OFF")
    return conn


def next_id(conn: sqlite3.Connection, table: str) -> int:
    return int(conn.execute(f"SELECT COALESCE(MAX(id), 0) + 1 FROM {table}").fetchone()[0])


def table_info_map(conn: sqlite3.Connection, table: str) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for row in conn.execute(f"PRAGMA table_info({table})").fetchall():
        out[row[1]] = {"notnull": bool(row[3]), "default": row[4], "type": (row[2] or "").upper()}
    return out


def normalize_benchmark_payload(payload: Any) -> dict[str, Any]:
    if isinstance(payload, dict) and "summary" in payload and isinstance(payload.get("summary"), dict):
        data = dict(payload["summary"])
        data["rows"] = payload.get("rows", [])
        return data
    if isinstance(payload, dict):
        return payload
    return {"raw": payload}


def run_benchmark(backend_root: Path, db_path: Path, benchmark_path: Path, report_dir: Path, prefix: str) -> dict[str, Any]:
    json_out = report_dir / f"{prefix}.json"
    csv_out = report_dir / f"{prefix}.csv"
    md_out = report_dir / f"{prefix}.md"
    cmd = [
        sys.executable,
        str(backend_root / "run_named_reaction_benchmark_small.py"),
        "--db", str(db_path),
        "--benchmark", str(benchmark_path),
        "--json-out", str(json_out),
        "--csv-out", str(csv_out),
        "--report-md", str(md_out),
    ]
    cp = subprocess.run(cmd, cwd=str(backend_root), capture_output=True, text=True, encoding="utf-8", errors="replace")
    if cp.returncode != 0:
        raise RuntimeError(f"benchmark failed\nSTDOUT:\n{cp.stdout}\nSTDERR:\n{cp.stderr}")
    data = normalize_benchmark_payload(json.loads(json_out.read_text(encoding="utf-8")))
    data["db_used"] = str(db_path)
    return data


def baseline_ok(bench: dict[str, Any]) -> bool:
    return float(bench.get("top1_accuracy") or 0.0) >= 0.9999 and float(bench.get("top3_accuracy") or 0.0) >= 0.9999 and int(bench.get("disallow_top3_violations") or 0) == 0


def set_if_exists(row: dict[str, Any], info: dict[str, dict[str, Any]], key: str, value: Any) -> None:
    if key in info:
        row[key] = value


def set_first_existing(row: dict[str, Any], info: dict[str, dict[str, Any]], keys: list[str], value: Any) -> None:
    for key in keys:
        if key in info:
            row[key] = value
            return


def fill_missing_required(row: dict[str, Any], info: dict[str, dict[str, Any]]) -> dict[str, Any]:
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    if "created_at" in info and "created_at" not in row:
        row["created_at"] = now
    if "updated_at" in info and "updated_at" not in row:
        row["updated_at"] = now
    for col, meta in info.items():
        if col in row:
            continue
        default = meta.get("default")
        if default is not None:
            # let sqlite handle expressions like CURRENT_TIMESTAMP by omitting them
            if isinstance(default, str) and default.upper() == "CURRENT_TIMESTAMP":
                continue
            row[col] = default
        elif meta.get("notnull"):
            typ = meta.get("type") or ""
            row[col] = 0 if any(x in typ for x in ("INT", "REAL", "NUM")) else ""
    return row


def build_extract_row(conn: sqlite3.Connection, family: str, family_norm: str, candidate: dict[str, Any], cfg: dict[str, Any], model: str) -> dict[str, Any]:
    info = table_info_map(conn, "reaction_extracts")
    row: dict[str, Any] = {}
    set_if_exists(row, info, "id", next_id(conn, "reaction_extracts"))
    set_if_exists(row, info, "scheme_candidate_id", int(conn.execute("SELECT COALESCE(MIN(id),1) FROM scheme_candidates").fetchone()[0] or 1))
    set_first_existing(row, info, ["reaction_family_name", "family_name"], family)
    set_first_existing(row, info, ["reaction_family_name_norm", "family_name_norm"], family_norm)
    set_if_exists(row, info, "extract_kind", "canonical_overview")
    set_if_exists(row, info, "transformation_text", cfg.get("transformation_text") or family)
    set_if_exists(row, info, "reactants_text", "; ".join(candidate.get("substrate_smiles") or []))
    set_if_exists(row, info, "products_text", "; ".join(candidate.get("product_smiles") or []))
    set_if_exists(row, info, "reagents_text", candidate.get("reagents_text") or cfg.get("reagents_text") or "")
    set_if_exists(row, info, "conditions_text", candidate.get("conditions_text") or cfg.get("conditions_text") or "")
    notes = (candidate.get("rationale") or "")
    if candidate.get("collision_avoidance_note"):
        notes += (" | " if notes else "") + candidate.get("collision_avoidance_note")
    set_first_existing(row, info, ["notes_text", "note_text"], notes)
    set_if_exists(row, info, "reactant_smiles", ".".join(candidate.get("substrate_smiles") or []))
    set_if_exists(row, info, "product_smiles", ".".join(candidate.get("product_smiles") or []))
    set_if_exists(row, info, "smiles_confidence", 0.95)
    set_if_exists(row, info, "extraction_confidence", 0.8)
    set_if_exists(row, info, "parse_status", "parsed")
    set_if_exists(row, info, "promote_decision", "candidate")
    set_if_exists(row, info, "extractor_model", model)
    set_if_exists(row, info, "extractor_prompt_version", "single_family_round1")
    set_if_exists(row, info, "extraction_raw_json", json.dumps(candidate, ensure_ascii=False))
    # Optional columns that may exist in other schema variants
    set_if_exists(row, info, "source_type", "gemini_single_family_rebuild")
    set_if_exists(row, info, "source_label", f"{family} canonical rebuild")
    set_if_exists(row, info, "page_num", 0)
    set_if_exists(row, info, "bbox_json", "{}")
    return fill_missing_required(row, info)


def build_molecule_rows(conn: sqlite3.Connection, extract_id: int, candidate: dict[str, Any], family: str, family_norm: str, model: str) -> list[dict[str, Any]]:
    info = table_info_map(conn, "extract_molecules")
    roles: list[tuple[str, str]] = []
    for s in candidate.get("substrate_smiles") or []:
        roles.append(("reactant", s))
    for s in candidate.get("product_smiles") or []:
        roles.append(("product", s))
    for s in candidate.get("optional_reagent_smiles") or []:
        roles.append(("reagent", s))
    rows: list[dict[str, Any]] = []
    nextmol = next_id(conn, "extract_molecules")
    for idx, (role, smiles) in enumerate(roles):
        row: dict[str, Any] = {}
        set_if_exists(row, info, "id", nextmol + idx)
        set_if_exists(row, info, "extract_id", extract_id)
        set_if_exists(row, info, "role", role)
        set_if_exists(row, info, "smiles", smiles)
        set_if_exists(row, info, "smiles_confidence", 0.95)
        set_if_exists(row, info, "smiles_kind", "exact")
        set_if_exists(row, info, "quality_tier", 1)
        set_if_exists(row, info, "queryable", 1)
        set_first_existing(row, info, ["reaction_family_name", "family_name"], family)
        set_if_exists(row, info, "normalized_text", family_norm)
        set_if_exists(row, info, "note_text", f"{family} canonical rebuild ({role})")
        set_if_exists(row, info, "source_field", role)
        set_first_existing(row, info, ["structure_source", "source"], "gemini_single_family_rebuild")
        set_if_exists(row, info, "role_confidence", 0.95)
        set_if_exists(row, info, "source_zip", "gemini_single_family_rebuild")
        set_if_exists(row, info, "page_no", 0)
        rows.append(fill_missing_required(row, info))
    return rows


def insert_candidate(conn: sqlite3.Connection, family: str, family_norm: str, candidate: dict[str, Any], cfg: dict[str, Any], model: str) -> tuple[int, list[int]]:
    extract_row = build_extract_row(conn, family, family_norm, candidate, cfg, model)
    cols = list(extract_row.keys())
    conn.execute(
        f"INSERT INTO reaction_extracts ({','.join(cols)}) VALUES ({','.join(['?']*len(cols))})",
        [extract_row[c] for c in cols],
    )
    mol_rows = build_molecule_rows(conn, extract_row.get("id") or conn.execute("SELECT last_insert_rowid()").fetchone()[0], candidate, family, family_norm, model)
    for row in mol_rows:
        cols = list(row.keys())
        conn.execute(
            f"INSERT INTO extract_molecules ({','.join(cols)}) VALUES ({','.join(['?']*len(cols))})",
            [row[c] for c in cols],
        )
    return int(extract_row.get("id") or conn.execute("SELECT last_insert_rowid()").fetchone()[0]), [int(r.get("id", 0)) for r in mol_rows if r.get("id") is not None]


def latest_summary(root: Path) -> Path:
    d = root / "reports" / "gemini_single_family_rebuild"
    if not d.exists():
        raise FileNotFoundError("reports/gemini_single_family_rebuild not found")
    hits = sorted([p / "gemini_single_family_rebuild_summary.json" for p in d.iterdir() if p.is_dir() and (p / "gemini_single_family_rebuild_summary.json").exists()])
    if not hits:
        raise FileNotFoundError("No gemini_single_family_rebuild_summary.json found")
    return hits[-1]


def backup_file(path: Path, backup_root: Path) -> Path:
    backup_root.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out = backup_root / f"{path.stem}.backup_before_single_family_apply_{ts}{path.suffix}"
    shutil.copy2(path, out)
    return out


def maybe_work_db(root: Path) -> Path | None:
    cand = root / "app" / "labint_round9_bridge_work.db"
    return cand if cand.exists() else None


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--backend-root", default=str(SCRIPT_DIR))
    ap.add_argument("--summary", default="")
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()

    backend_root = Path(args.backend_root).resolve()
    summary_path = Path(args.summary).resolve() if args.summary else latest_summary(backend_root)
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    accepted = summary.get("accepted")
    if not accepted:
        raise SystemExit("No accepted candidate found in summary; do not apply.")

    family = summary["family"]
    family_norm = summary["family_norm"]
    candidate = accepted["candidate"]
    cfg = json.loads(Path(summary["config"]).read_text(encoding="utf-8"))
    model = summary.get("model") or "gemini-2.5-pro"

    canonical = backend_root / "app" / "labint.db"
    work_db = maybe_work_db(backend_root)
    benchmark = backend_root / "benchmark" / "named_reaction_benchmark_small.json"
    report_dir = backend_root / "reports" / "gemini_single_family_apply" / datetime.now().strftime("%Y%m%d_%H%M%S")
    report_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 72)
    print("APPLY SINGLE-FAMILY REBUILD")
    print("=" * 72)
    print(f"mode:       {'APPLY' if args.apply else 'DRY-RUN'}")
    print(f"family:     {family}")
    print(f"summary:    {summary_path}")
    print(f"canonical:  {canonical}")
    print(f"work_db:    {work_db if work_db else '(none)'}")
    print(f"report dir: {report_dir}")

    baseline = run_benchmark(backend_root, canonical, benchmark, report_dir, "benchmark_baseline")
    print(f"[baseline] top1={baseline['top1_accuracy']:.4f} top3={baseline['top3_accuracy']:.4f} violations={baseline['disallow_top3_violations']}")

    tmpdir = Path(tempfile.mkdtemp(prefix="single_family_apply_"))
    tmpdb = tmpdir / "labint.db"
    shutil.copy2(canonical, tmpdb)
    tmpconn = connect(tmpdb)
    try:
        extract_id, mol_ids = insert_candidate(tmpconn, family, family_norm, candidate, cfg, model)
        tmpconn.commit()
        temp_bench = run_benchmark(backend_root, tmpdb, benchmark, report_dir, "benchmark_temp_after")
    finally:
        try:
            tmpconn.close()
        except Exception:
            pass
        shutil.rmtree(tmpdir, ignore_errors=True)

    print(f"[temp benchmark] top1={temp_bench['top1_accuracy']:.4f} top3={temp_bench['top3_accuracy']:.4f} violations={temp_bench['disallow_top3_violations']}")
    if not baseline_ok(temp_bench):
        raise SystemExit("[guard] FAIL - candidate does not preserve benchmark")
    print("[guard] PASS")

    backups: dict[str, str] = {}
    applied = False
    if args.apply:
        backups_dir = backend_root / "backups" / "single_family_apply"
        backups["canonical"] = str(backup_file(canonical, backups_dir))
        cconn = connect(canonical)
        try:
            insert_candidate(cconn, family, family_norm, candidate, cfg, model)
            cconn.commit()
        finally:
            cconn.close()
        if work_db:
            backups["work_db"] = str(backup_file(work_db, backups_dir))
            wconn = connect(work_db)
            try:
                insert_candidate(wconn, family, family_norm, candidate, cfg, model)
                wconn.commit()
            finally:
                wconn.close()
        applied = True
        final_bench = run_benchmark(backend_root, canonical, benchmark, report_dir, "benchmark_final_after")
        print(f"[final benchmark] top1={final_bench['top1_accuracy']:.4f} top3={final_bench['top3_accuracy']:.4f} violations={final_bench['disallow_top3_violations']}")
        if not baseline_ok(final_bench):
            raise SystemExit("[post-apply guard] FAIL - investigate immediately")
    else:
        final_bench = None

    summary_out = {
        "family": family,
        "mode": "APPLY" if args.apply else "DRY-RUN",
        "source_summary": str(summary_path),
        "candidate": candidate,
        "baseline": baseline,
        "temp_benchmark": temp_bench,
        "applied": applied,
        "final_benchmark": final_bench,
        "backups": backups,
        "report_dir": str(report_dir),
    }
    out = report_dir / "gemini_single_family_apply_summary.json"
    out.write_text(json.dumps(summary_out, ensure_ascii=False, indent=2), encoding="utf-8")
    print("=" * 72)
    print(f"[DONE] summary: {out}")
    print("=" * 72)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
