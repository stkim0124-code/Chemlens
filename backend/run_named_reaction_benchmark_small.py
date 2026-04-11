from __future__ import annotations

import argparse
import csv
import importlib.util
import json
from pathlib import Path
import sys
import types
import typing
from collections import Counter, defaultdict


def _install_fastapi_shim_if_needed() -> None:
    try:
        import fastapi  # noqa: F401
        return
    except Exception:
        pass

    fastapi_stub = types.ModuleType("fastapi")

    class HTTPException(Exception):
        def __init__(self, status_code: int = 500, detail: str | None = None):
            self.status_code = status_code
            self.detail = detail
            super().__init__(detail or f"HTTPException(status_code={status_code})")

    class APIRouter:
        def get(self, *args, **kwargs):
            def decorator(fn):
                return fn
            return decorator

        def post(self, *args, **kwargs):
            def decorator(fn):
                return fn
            return decorator

    def Query(default=None, *args, **kwargs):
        return default

    fastapi_stub.APIRouter = APIRouter
    fastapi_stub.HTTPException = HTTPException
    fastapi_stub.Query = Query
    sys.modules["fastapi"] = fastapi_stub


def load_module(module_path: Path):
    _install_fastapi_shim_if_needed()
    spec = importlib.util.spec_from_file_location("chemlens_evidence_search", str(module_path))
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Could not load module: {module_path}")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    if hasattr(mod.StructureEvidenceRequest, "model_rebuild"):
        mod.StructureEvidenceRequest.model_rebuild(_types_namespace={"Optional": typing.Optional})
    return mod


def _markdown_report(summary: dict, rows: list[dict], benchmark_name: str) -> str:
    fam_counter = Counter(row["expected_family"] for row in rows)
    fam_success = defaultdict(lambda: {"top1": 0, "top3": 0, "n": 0})
    boundary_case_count = 0
    for row in rows:
        fam = row["expected_family"]
        fam_success[fam]["n"] += 1
        fam_success[fam]["top1"] += int(bool(row["top1_correct"]))
        fam_success[fam]["top3"] += int(bool(row["top3_correct"]))
        boundary_case_count += int(bool(row.get("acceptance_override_used")))

    lines: list[str] = []
    lines.append(f"# {benchmark_name} report")
    lines.append("")
    lines.append("## Summary")
    lines.append("")
    lines.append(f"- total_cases: {summary['total_cases']}")
    lines.append(f"- top1_accuracy: {summary['top1_accuracy']:.4f}")
    lines.append(f"- top3_accuracy: {summary['top3_accuracy']:.4f}")
    lines.append(f"- disallow_top3_violations: {summary['disallow_top3_violations']}")
    lines.append(f"- unique_families: {summary['unique_families']}")
    lines.append(f"- boundary_cases_with_acceptance_override: {boundary_case_count}")
    lines.append("")
    lines.append("## Family coverage")
    lines.append("")
    lines.append("| family | cases | top1 | top3 |")
    lines.append("|---|---:|---:|---:|")
    for fam in sorted(fam_counter):
        fs = fam_success[fam]
        lines.append(f"| {fam} | {fs['n']} | {fs['top1']}/{fs['n']} | {fs['top3']}/{fs['n']} |")
    lines.append("")
    lines.append("## Case table")
    lines.append("")
    lines.append("| case_id | expected | top1 | top3 families | query types | mismatch pruned | boundary override |")
    lines.append("|---|---|---|---|---|---:|---|")
    for row in rows:
        lines.append(
            f"| {row['case_id']} | {row['expected_family']} | {row['top1_family'] or ''} | {row['top3_families'].replace(' | ', ', ')} | {row['query_reaction_types_ko'].replace(' | ', ', ')} | {row['pruned_mismatch_count']} | {'yes' if row.get('acceptance_override_used') else ''} |"
        )
    return "\n".join(lines) + "\n"


def main():
    here = Path(__file__).resolve().parent
    parser = argparse.ArgumentParser(description="Run CHEMLENS small named-reaction benchmark.")
    parser.add_argument("--db", default=str(here / "app" / "labint.db"), help="Path to labint.db")
    parser.add_argument("--benchmark", default=str(here / "benchmark" / "named_reaction_benchmark_small.json"), help="Benchmark JSON")
    parser.add_argument("--top-k", type=int, default=5)
    parser.add_argument("--min-tanimoto", type=float, default=0.25)
    parser.add_argument("--csv-out", default=str(here / "benchmark" / "named_reaction_benchmark_small_results.csv"))
    parser.add_argument("--json-out", default=str(here / "benchmark" / "named_reaction_benchmark_small_results.json"))
    parser.add_argument("--report-md", default=str(here / "benchmark" / "named_reaction_benchmark_small_report.md"))
    args = parser.parse_args()

    module = load_module(here / "app" / "evidence_search.py")
    module.DB_PATH = Path(args.db)

    bench = json.loads(Path(args.benchmark).read_text(encoding="utf-8"))
    cases = bench.get("cases") or []
    benchmark_name = bench.get("name") or "CHEMLENS small benchmark"

    rows = []
    top1_correct = 0
    top3_correct = 0
    disallow_violations = 0

    for case in cases:
        try:
            req = module.StructureEvidenceRequest(
                reaction_smiles=case["reaction_smiles"],
                top_k=args.top_k,
                min_tanimoto=args.min_tanimoto,
            )
            res = module._search_by_reaction(req)
        except Exception as e:
            msg = str(e)
            if "RDKit import failed" in msg:
                print("RDKit import failed in the current shell. Run this inside the CHEMLENS conda env, e.g. `conda activate chemlens`.")
            raise
        results = res.get("results") or []
        top3 = [item.get("reaction_family_name") for item in results[:3]]
        scores = [round(float(item.get("match_score") or 0.0), 3) for item in results[:3]]
        top1 = top3[0] if top3 else None

        expected = case["expected_family"]
        acceptable_top1 = set(case.get("acceptable_top1") or []) | {expected}
        acceptable_top3 = set(case.get("acceptable_top3") or []) | {expected}
        acceptance_override_used = bool((case.get("acceptable_top1") or []) or (case.get("acceptable_top3") or []))

        hit_top1 = top1 in acceptable_top1
        hit_top3 = bool(acceptable_top3.intersection(x for x in top3 if x))

        top1_correct += int(hit_top1)
        top3_correct += int(hit_top3)

        disallow = case.get("disallow_top3") or []
        violated = [fam for fam in disallow if fam in top3]
        disallow_violations += int(bool(violated))

        rows.append({
            "case_id": case["case_id"],
            "source_extract_id": case.get("source_extract_id"),
            "expected_family": expected,
            "top1_family": top1,
            "top3_families": " | ".join([x for x in top3 if x]),
            "top3_scores": " | ".join(map(str, scores)),
            "top1_correct": hit_top1,
            "top3_correct": hit_top3,
            "acceptance_override_used": acceptance_override_used,
            "disallow_violated": " | ".join(violated),
            "query_reaction_types_ko": " | ".join(res.get("query_reaction_types_ko") or []),
            "pruned_mismatch_count": res.get("pruned_mismatch_count", 0),
            "notes": case.get("notes", ""),
        })

    total = len(rows)
    unique_families = len(set(row["expected_family"] for row in rows))
    summary = {
        "benchmark_name": benchmark_name,
        "total_cases": total,
        "top1_correct": top1_correct,
        "top1_accuracy": round(top1_correct / total, 4) if total else 0.0,
        "top3_correct": top3_correct,
        "top3_accuracy": round(top3_correct / total, 4) if total else 0.0,
        "disallow_top3_violations": disallow_violations,
        "disallow_violation_rate": round(disallow_violations / total, 4) if total else 0.0,
        "unique_families": unique_families,
    }

    csv_path = Path(args.csv_out)
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    with csv_path.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()) if rows else [])
        if rows:
            writer.writeheader()
            writer.writerows(rows)

    json_path = Path(args.json_out)
    json_path.write_text(json.dumps({"summary": summary, "rows": rows}, ensure_ascii=False, indent=2), encoding="utf-8")

    report_path = Path(args.report_md)
    report_path.write_text(_markdown_report(summary, rows, benchmark_name), encoding="utf-8")

    print(benchmark_name)
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    print(f"CSV:  {csv_path}")
    print(f"JSON: {json_path}")
    print(f"MD:   {report_path}")


if __name__ == "__main__":
    main()
