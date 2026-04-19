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
from tempfile import TemporaryDirectory
from typing import Any

ALLOWED_SOURCES = {"gemini_auto_seed", "deterministic_gemini_seed", "deterministic_seed_from_existing"}
SUMMARY_REJECT_KEYS = {"rejected", "still_rejected", "rejected_families", "still_rejected_families"}
LIKELY_FAMILY_KEYS = {"family", "family_name", "name", "candidate_family", "exact_pattern_family"}
LIKELY_QUERYABLE_COLS = ("queryable", "is_queryable")
LIKELY_ROLE_COLS = ("role", "molecule_role", "mol_role")
LIKELY_FAMILY_COLS = (
    "reaction_family_name",
    "reaction_family_name_norm",
    "exact_pattern_family",
    "family_name",
    "pattern_family",
    "normalized_family",
    "family",
)


def _json_load(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _pick_existing_col(cols: list[str], candidates: tuple[str, ...] | list[str]) -> str | None:
    cols_set = {c.lower(): c for c in cols}
    for cand in candidates:
        if cand.lower() in cols_set:
            return cols_set[cand.lower()]
    return None


def _table_cols(conn: sqlite3.Connection, table: str) -> list[str]:
    return [r[1] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()]


def _normalize_family_name(name: str) -> str:
    return " ".join((name or "").strip().lower().split())


def _collect_rejected(obj: Any, out: list[str]) -> None:
    if isinstance(obj, dict):
        for k, v in obj.items():
            lk = str(k).lower()
            if lk in SUMMARY_REJECT_KEYS:
                if isinstance(v, list):
                    for item in v:
                        if isinstance(item, str):
                            out.append(item)
                        elif isinstance(item, dict):
                            for fk in LIKELY_FAMILY_KEYS:
                                if fk in item and item[fk]:
                                    out.append(str(item[fk]))
                elif isinstance(v, dict):
                    _collect_rejected(v, out)
            else:
                _collect_rejected(v, out)
    elif isinstance(obj, list):
        for item in obj:
            _collect_rejected(item, out)


def _extract_family_names_from_items(items: Any) -> list[str]:
    out: list[str] = []
    if not isinstance(items, list):
        return out
    for item in items:
        if isinstance(item, str):
            val = item.strip()
            if val:
                out.append(val)
        elif isinstance(item, dict):
            for key in ("family_name", "family", "name", "exact_pattern_family"):
                val = item.get(key)
                if val:
                    out.append(str(val).strip())
                    break
    return out


def _dedupe_keep_order(items: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for x in items:
        if x and x not in seen:
            out.append(x)
            seen.add(x)
    return out


def load_rejected_families(summary_path: Path) -> list[str]:
    data = _json_load(summary_path)
    filename = summary_path.name.lower()

    if filename == "rejected_retry_summary.json":
        still = _extract_family_names_from_items(data.get("still_rejected"))
        if still:
            return _dedupe_keep_order(still)
        # fallback: loaded - accepted
        loaded = _extract_family_names_from_items(data.get("rejected_candidates_loaded"))
        accepted = set(_extract_family_names_from_items(data.get("accepted")))
        return _dedupe_keep_order([x for x in loaded if x not in accepted])

    if filename == "selective_merge_summary.json":
        direct = _extract_family_names_from_items(data.get("rejected_families"))
        if direct:
            return _dedupe_keep_order(direct)

    out: list[str] = []
    _collect_rejected(data, out)
    return _dedupe_keep_order([x.strip() for x in out if str(x).strip()])


def load_benchmark_json(path: Path) -> dict[str, Any]:
    data = _json_load(path)
    if isinstance(data, dict) and "summary" in data and isinstance(data["summary"], dict):
        return data
    raise RuntimeError(f"unexpected benchmark JSON shape: {path}")


def _user_path(s: str | None) -> Path | None:
    if s is None:
        return None
    return Path(s.replace("\\", os.sep).replace("/", os.sep))


def create_report_dir(base_dir: Path) -> Path:
    base_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    candidate = base_dir / stamp
    counter = 1
    while candidate.exists():
        candidate = base_dir / f"{stamp}_{counter:02d}"
        counter += 1
    candidate.mkdir(parents=True, exist_ok=False)
    return candidate


def find_latest_summary(backend_root: Path) -> Path:
    retry_root = backend_root / "reports" / "v5_rejected_retry"
    retry = sorted(retry_root.glob("*/rejected_retry_summary.json"))
    if retry:
        return retry[-1]
    sel_root = backend_root / "reports" / "v5_selective_merge"
    sel = sorted(sel_root.glob("*/selective_merge_summary.json"))
    if sel:
        return sel[-1]
    raise FileNotFoundError("No retry/selective summary found under reports/")

def find_latest_apply_summary(backend_root: Path) -> Path | None:
    root = backend_root / "reports" / "gemini_salvage_apply"
    if not root.exists():
        return None
    candidates = sorted(root.glob("*/gemini_salvage_apply_summary.json"), reverse=True)
    for path in candidates:
        try:
            data = _json_load(path)
        except Exception:
            continue
        mode = str(data.get("mode", "")).strip().lower()
        guard_pass = bool(data.get("guard_pass"))
        final_guard_pass = bool(data.get("final_guard_pass", guard_pass))
        if mode == "apply" and guard_pass and final_guard_pass:
            return path
    return None


def load_applied_families(apply_summary_path: Path | None) -> list[str]:
    if not apply_summary_path or not Path(apply_summary_path).exists():
        return []
    data = _json_load(Path(apply_summary_path))
    mode = str(data.get("mode", "")).strip().lower()
    guard_pass = bool(data.get("guard_pass"))
    final_guard_pass = bool(data.get("final_guard_pass", guard_pass))
    if mode != "apply" or not guard_pass or not final_guard_pass:
        return []

    out: list[str] = []
    acceptable_status = {"inserted", "updated", "exists", "already_present", "skipped_exists"}
    for key in ("apply_results", "temp_apply", "selections"):
        items = data.get(key)
        if not isinstance(items, list):
            continue
        for item in items:
            if not isinstance(item, dict):
                continue
            fam = str(item.get("family_name") or item.get("family") or "").strip()
            if not fam:
                continue
            if key == "selections":
                out.append(fam)
                continue
            status = str(item.get("status") or "").strip().lower()
            if status in acceptable_status:
                out.append(fam)
    return _dedupe_keep_order(out)


def run_benchmark(backend_root: Path, benchmark_path: Path, db_path: Path, out_prefix: Path) -> dict[str, Any]:
    env = os.environ.copy()
    env["LABINT_DB_PATH"] = str(db_path)
    json_path = out_prefix.with_suffix(".json")
    csv_path = out_prefix.with_suffix(".csv")
    md_path = out_prefix.with_suffix(".md")
    json_path.parent.mkdir(parents=True, exist_ok=True)
    cmd = [
        sys.executable,
        "run_named_reaction_benchmark_small.py",
        "--db", str(db_path),
        "--benchmark", str(benchmark_path),
        "--json-out", str(json_path),
        "--csv-out", str(csv_path),
        "--report-md", str(md_path),
    ]
    cp = subprocess.run(
        cmd,
        cwd=str(backend_root),
        env=env,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    if cp.returncode != 0:
        raise RuntimeError(
            "benchmark failed\n"
            f"CMD: {' '.join(cmd)}\n"
            f"STDOUT:\n{cp.stdout}\n"
            f"STDERR:\n{cp.stderr}"
        )
    data = load_benchmark_json(json_path)
    data["runner_stdout_tail"] = cp.stdout[-4000:]
    data["runner_stderr_tail"] = cp.stderr[-4000:]
    data["_diag_db_used"] = str(db_path)
    data["_diag_json_out"] = str(json_path)
    data["_diag_csv_out"] = str(csv_path)
    data["_diag_md_out"] = str(md_path)
    return data


def build_case_map(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    return {str(r.get("case_id")): r for r in rows if r.get("case_id")}


def diff_rows(base_rows: list[dict[str, Any]], cur_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    base = build_case_map(base_rows)
    cur = build_case_map(cur_rows)
    changed = []
    for case_id, brow in base.items():
        crow = cur.get(case_id, {})
        b_ok = bool(brow.get("top1_correct"))
        c_ok = bool(crow.get("top1_correct"))
        b_top1 = brow.get("top1_family")
        c_top1 = crow.get("top1_family")
        b_top3 = brow.get("top3_families")
        c_top3 = crow.get("top3_families")
        if (b_ok != c_ok) or (b_top1 != c_top1) or (b_top3 != c_top3):
            changed.append({
                "case_id": case_id,
                "expected_family": brow.get("expected_family"),
                "baseline_top1_family": b_top1,
                "current_top1_family": c_top1,
                "baseline_top1_correct": b_ok,
                "current_top1_correct": c_ok,
                "baseline_top3_families": b_top3,
                "current_top3_families": c_top3,
                "baseline_top3_scores": brow.get("top3_scores"),
                "current_top3_scores": crow.get("top3_scores"),
                "baseline_notes": brow.get("notes"),
                "current_notes": crow.get("notes"),
            })
    return changed


def _family_extract_ids(src_conn: sqlite3.Connection, family_name: str) -> list[int]:
    re_cols = _table_cols(src_conn, "reaction_extracts")
    family_col = _pick_existing_col(re_cols, LIKELY_FAMILY_COLS)
    if not family_col:
        raise RuntimeError("reaction_extracts family column not found")

    rows = src_conn.execute(
        f"SELECT id FROM reaction_extracts WHERE TRIM({family_col}) = ? ORDER BY id",
        (family_name,),
    ).fetchall()
    if rows:
        return [int(r[0]) for r in rows]

    norm_target = _normalize_family_name(family_name)
    for cand in ("reaction_family_name_norm", "normalized_family", "family_name", "reaction_family_name"):
        if cand in re_cols:
            rows = src_conn.execute(f"SELECT id, {cand} FROM reaction_extracts ORDER BY id").fetchall()
            out = []
            for r in rows:
                if _normalize_family_name(str(r[1] or "")) == norm_target:
                    out.append(int(r[0]))
            if out:
                return out
    return []


def _extract_molecule_rows(src_conn: sqlite3.Connection, extract_ids: list[int]) -> list[sqlite3.Row]:
    if not extract_ids:
        return []
    src_conn.row_factory = sqlite3.Row
    marks = ",".join("?" * len(extract_ids))
    rows = src_conn.execute(
        f"SELECT * FROM extract_molecules WHERE extract_id IN ({marks}) AND structure_source IN ({','.join('?'*len(ALLOWED_SOURCES))}) ORDER BY extract_id, id",
        tuple(extract_ids) + tuple(sorted(ALLOWED_SOURCES)),
    ).fetchall()
    return rows


def _variant_ids(rows: list[sqlite3.Row], variant: str) -> list[int]:
    if not rows:
        return []
    row_dicts = [dict(r) for r in rows]
    em_cols = list(row_dicts[0].keys())
    qcol = next((c for c in LIKELY_QUERYABLE_COLS if c in em_cols), None)
    rcol = next((c for c in LIKELY_ROLE_COLS if c in em_cols), None)

    def is_queryable(d: dict[str, Any]) -> bool:
        if not qcol:
            return False
        try:
            return int(d.get(qcol) or 0) == 1
        except Exception:
            return False

    reactants = [d for d in row_dicts if rcol and str(d.get(rcol, "")).lower() == "reactant"]
    products = [d for d in row_dicts if rcol and str(d.get(rcol, "")).lower() == "product"]
    reagents = [d for d in row_dicts if rcol and str(d.get(rcol, "")).lower() in {"reagent", "catalyst", "solvent", "agent"}]

    if variant == "all_original":
        return [int(d["id"]) for d in row_dicts]

    if variant == "queryable_only":
        chosen = [d for d in row_dicts if is_queryable(d)]
        return [int(d["id"]) for d in chosen]

    if variant == "minimal_pair":
        chosen = []
        if reactants:
            chosen.append(reactants[0])
        if products:
            chosen.append(products[0])
        if not chosen:
            chosen = row_dicts[:2]
        return [int(d["id"]) for d in chosen]

    if variant == "queryable_pair":
        qr = [d for d in reactants if is_queryable(d)]
        qp = [d for d in products if is_queryable(d)]
        chosen = []
        if qr:
            chosen.append(qr[0])
        if qp:
            chosen.append(qp[0])
        if not chosen:
            return _variant_ids(rows, "minimal_pair")
        return [int(d["id"]) for d in chosen]

    if variant == "core_pair_set":
        chosen = []
        if reactants:
            chosen.extend(reactants[:2])
        if products:
            chosen.extend(products[:2])
        if not chosen:
            chosen = row_dicts[:4]
        seen = set()
        out = []
        for d in chosen:
            if d["id"] not in seen:
                out.append(int(d["id"]))
                seen.add(d["id"])
        return out

    if variant == "core_plus_intermediate":
        core = _variant_ids(rows, "core_pair_set")
        chosen = [d for d in row_dicts if int(d["id"]) in set(core)]
        extra = [d for d in row_dicts if d not in chosen][:1]
        return core + [int(d["id"]) for d in extra]

    if variant == "core_plus_reagent":
        core = _variant_ids(rows, "core_pair_set")
        chosen_ids = set(core)
        extra = [int(d["id"]) for d in reagents if int(d["id"]) not in chosen_ids][:1]
        if not extra:
            extra = [int(d["id"]) for d in row_dicts if int(d["id"]) not in chosen_ids][:1]
        return core + extra

    raise RuntimeError(f"unknown variant: {variant}")


def _table_common_cols(conn: sqlite3.Connection, source_alias: str, table: str) -> list[str]:
    target_cols = _table_cols(conn, table)
    src_cols = [r[1] for r in conn.execute(f"PRAGMA {source_alias}.table_info({table})").fetchall()]
    return [c for c in target_cols if c in src_cols]


def attach_and_insert(temp_db: Path, source_db: Path, extract_ids: list[int], mol_ids: list[int]) -> dict[str, int]:
    conn = sqlite3.connect(str(temp_db))
    try:
        before_total = conn.total_changes
        before_extracts = int(conn.execute("SELECT COUNT(*) FROM reaction_extracts").fetchone()[0])
        before_mols = int(conn.execute("SELECT COUNT(*) FROM extract_molecules").fetchone()[0])
        conn.execute("ATTACH DATABASE ? AS src", (str(source_db),))
        if extract_ids:
            cols = _table_common_cols(conn, "src", "reaction_extracts")
            marks = ",".join("?" * len(extract_ids))
            col_sql = ",".join(cols)
            sql = f"INSERT OR IGNORE INTO reaction_extracts ({col_sql}) SELECT {col_sql} FROM src.reaction_extracts WHERE id IN ({marks})"
            conn.execute(sql, tuple(extract_ids))
        if mol_ids:
            cols = _table_common_cols(conn, "src", "extract_molecules")
            marks = ",".join("?" * len(mol_ids))
            col_sql = ",".join(cols)
            sql = f"INSERT OR IGNORE INTO extract_molecules ({col_sql}) SELECT {col_sql} FROM src.extract_molecules WHERE id IN ({marks})"
            conn.execute(sql, tuple(mol_ids))
        conn.commit()
        conn.execute("DETACH DATABASE src")
        after_extracts = int(conn.execute("SELECT COUNT(*) FROM reaction_extracts").fetchone()[0])
        after_mols = int(conn.execute("SELECT COUNT(*) FROM extract_molecules").fetchone()[0])
        after_total = conn.total_changes
        return {
            "inserted_extract_rows": max(0, after_extracts - before_extracts),
            "inserted_molecule_rows": max(0, after_mols - before_mols),
            "total_changes_delta": max(0, after_total - before_total),
        }
    finally:
        conn.close()


def make_report(report_dir: Path, family_name: str, variant: str, summary: dict[str, Any], changed: list[dict[str, Any]], extract_ids: list[int], mol_ids: list[int], insert_stats: dict[str, int], identical_to_baseline: bool) -> None:
    safe = "".join(ch if ch.isalnum() or ch in "-_ ." else "_" for ch in family_name).strip().replace(" ", "_")[:80]
    payload = {
        "family_name": family_name,
        "variant": variant,
        "summary": summary,
        "changed_cases": changed,
        "reaction_extract_ids": extract_ids,
        "extract_molecule_ids": mol_ids,
        "insert_stats": insert_stats,
        "identical_to_baseline": identical_to_baseline,
    }
    (report_dir / f"{safe}__{variant}.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--canonical-db", default=r"app\labint.db")
    ap.add_argument("--source-db", default=r"app\labint_v5_stage.db")
    ap.add_argument("--benchmark", default=r"benchmark\named_reaction_benchmark_small.json")
    ap.add_argument("--summary", default=None, help="Path to selective/retry summary JSON. If omitted, latest retry summary is preferred.")
    ap.add_argument("--apply-summary", default=None, help="Path to gemini_salvage_apply_summary.json. If omitted, latest successful APPLY summary is auto-detected and applied families are excluded.")
    ap.add_argument("--report-dir", default=r"reports\v5_rejected_diagnose")
    args = ap.parse_args()

    backend_root = Path.cwd()
    canonical_db = (backend_root / _user_path(args.canonical_db)).resolve()
    source_db = (backend_root / _user_path(args.source_db)).resolve()
    benchmark = (backend_root / _user_path(args.benchmark)).resolve()
    summary_path = ((backend_root / _user_path(args.summary)).resolve() if args.summary else find_latest_summary(backend_root))
    apply_summary_path = ((backend_root / _user_path(args.apply_summary)).resolve() if args.apply_summary else find_latest_apply_summary(backend_root))
    report_dir = create_report_dir((backend_root / _user_path(args.report_dir)).resolve())

    print("=" * 76)
    print("DIAGNOSE REJECTED FAMILIES (CASE-LEVEL)")
    print("=" * 76)
    print(f"canonical:         {canonical_db}")
    print(f"source:            {source_db}")
    print(f"benchmark:         {benchmark}")
    print(f"summary:           {summary_path}")
    print(f"apply summary:     {apply_summary_path if apply_summary_path else '(none)'}")
    print(f"report dir:        {report_dir}")
    print()

    summary_rejected = load_rejected_families(summary_path)
    applied_families = load_applied_families(apply_summary_path)
    applied_norm = {_normalize_family_name(x) for x in applied_families}
    rejected = [fam for fam in summary_rejected if _normalize_family_name(fam) not in applied_norm]

    print("[STEP 1] rejected families")
    if applied_families:
        print(f"  excluded already-applied salvage families: {len(applied_families)}")
        for fam in applied_families:
            print(f"    * {fam}")
    print(f"  total rejected candidates: {len(rejected)}")
    for fam in rejected:
        print(f"  - {fam}")
    print()

    base_bench = run_benchmark(backend_root, benchmark, canonical_db, report_dir / "benchmark_baseline")
    base_summary = base_bench["summary"]
    base_rows = base_bench["rows"]
    baseline_json_bytes = Path(base_bench["_diag_json_out"]).read_bytes()

    if float(base_summary.get("top1_accuracy", 0.0)) <= 0.0:
        raise RuntimeError("baseline benchmark failed or parsed as 0.0; aborting diagnosis")

    print("[STEP 2] baseline benchmark")
    print(json.dumps({
        "top1_accuracy": base_summary.get("top1_accuracy"),
        "top3_accuracy": base_summary.get("top3_accuracy"),
        "disallow_top3_violations": base_summary.get("disallow_top3_violations"),
        "db_used": base_bench.get("_diag_db_used"),
    }, ensure_ascii=False, indent=2))
    print()

    src_conn = sqlite3.connect(str(source_db))
    try:
        variants = [
            "minimal_pair",
            "queryable_pair",
            "core_pair_set",
            "core_plus_intermediate",
            "core_plus_reagent",
            "queryable_only",
            "all_original",
        ]
        summary_rows = []
        for family_name in rejected:
            extract_ids = _family_extract_ids(src_conn, family_name)
            rows = _extract_molecule_rows(src_conn, extract_ids)
            if not extract_ids or not rows:
                summary_rows.append({"family_name": family_name, "status": "missing_in_stage"})
                print(f"[DIAGNOSE] {family_name}\n  - missing_in_stage")
                continue
            print(f"[DIAGNOSE] {family_name}")
            family_reports = []
            for variant in variants:
                mol_ids = _variant_ids(rows, variant)
                if not mol_ids:
                    continue
                with TemporaryDirectory(prefix="chemlens_diag_") as td:
                    temp_db = Path(td) / "temp.db"
                    shutil.copy2(canonical_db, temp_db)
                    insert_stats = attach_and_insert(temp_db, source_db, extract_ids, mol_ids)
                    bench = run_benchmark(
                        backend_root,
                        benchmark,
                        temp_db,
                        report_dir / f"bench_{''.join(ch if ch.isalnum() else '_' for ch in family_name)[:40]}_{variant}",
                    )
                    current_json_bytes = Path(bench["_diag_json_out"]).read_bytes()
                    identical_to_baseline = current_json_bytes == baseline_json_bytes
                    changed = diff_rows(base_rows, bench["rows"])
                    s = bench["summary"]
                    top1 = float(s.get("top1_accuracy", 0.0))
                    top3 = float(s.get("top3_accuracy", 0.0))
                    vio = int(s.get("disallow_top3_violations", 0))
                    status = "accepted"
                    if top1 < float(base_summary.get("top1_accuracy", 0.0)):
                        status = "rejected"
                    elif top3 < float(base_summary.get("top3_accuracy", 0.0)):
                        status = "rejected"
                    elif vio > int(base_summary.get("disallow_top3_violations", 0)):
                        status = "rejected"
                    warning = ""
                    if identical_to_baseline and (insert_stats["inserted_extract_rows"] > 0 or insert_stats["inserted_molecule_rows"] > 0):
                        warning = " [identical-to-baseline-json]"
                    make_report(report_dir, family_name, variant, s, changed, extract_ids, mol_ids, insert_stats, identical_to_baseline)
                    first_case = changed[0]["case_id"] if changed else ""
                    first_shift = changed[0]["current_top1_family"] if changed else ""
                    print(
                        f"  - {variant}: {status.upper()} top1={top1:.4f} top3={top3:.4f} "
                        f"changed_cases={len(changed)} first_case={first_case} first_top1={first_shift} "
                        f"delta_extracts={insert_stats['inserted_extract_rows']} delta_mols={insert_stats['inserted_molecule_rows']}"
                        f"{warning}"
                    )
                    family_reports.append({
                        "variant": variant,
                        "status": status,
                        "top1_accuracy": top1,
                        "top3_accuracy": top3,
                        "violations": vio,
                        "changed_case_count": len(changed),
                        "first_changed_case": first_case,
                        "first_changed_top1": first_shift,
                        "insert_stats": insert_stats,
                        "identical_to_baseline": identical_to_baseline,
                        "db_used": bench.get("_diag_db_used"),
                    })
            summary_rows.append({"family_name": family_name, "variants": family_reports})
    finally:
        src_conn.close()

    out = {
        "summary_path": str(summary_path),
        "apply_summary_path": str(apply_summary_path) if apply_summary_path else None,
        "summary_rejected_count": len(summary_rejected),
        "applied_family_exclusions": applied_families,
        "effective_rejected_count": len(rejected),
        "baseline": {
            "top1_accuracy": base_summary.get("top1_accuracy"),
            "top3_accuracy": base_summary.get("top3_accuracy"),
            "disallow_top3_violations": base_summary.get("disallow_top3_violations"),
            "db_used": base_bench.get("_diag_db_used"),
        },
        "summary_path_used": str(summary_path),
        "rejected_count": len(rejected),
        "families": summary_rows,
    }
    (report_dir / "rejected_diagnosis_summary.json").write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    print()
    print("=" * 76)
    print("[DONE] diagnosis finished")
    print(f"summary: {report_dir / 'rejected_diagnosis_summary.json'}")
    print("=" * 76)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
