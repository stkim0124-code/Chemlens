"""run_family_admission_benchmark.py — Phase 2b runner.

Consumes ``benchmark/family_admission_benchmark.json`` (produced by
``benchmark_builder.py``) and runs each case through
``app.evidence_search._search_by_reaction``. Emits:

  * ``<out-dir>/family_admission_results.csv``
  * ``<out-dir>/family_admission_results.json``
  * ``<out-dir>/family_admission_report.md``
  * ``<out-dir>/family_admission_confusion.csv`` — confusion matrix
                                                   (expected × predicted top1)

Acceptance rules (per case):
  * ``top1_correct``: engine's top1 family ∈ ``acceptable_top1`` of the case.
  * ``top3_correct``: any of top3 ∩ ``acceptable_top3`` non-empty.

Sibling confusion tracking: when ``top1_correct`` is False, record the pair
``(expected_canonical, predicted_top1)`` into the confusion matrix.

Reuses the fastapi shim + module loader pattern from
``run_named_reaction_benchmark_small.py``.
"""
from __future__ import annotations

import argparse
import csv
import importlib.util
import json
import sys
import types
import typing
from collections import Counter, defaultdict
from pathlib import Path


def _install_fastapi_shim_if_needed() -> None:
    try:
        import fastapi  # noqa: F401
        return
    except Exception:
        pass

    fastapi_stub = types.ModuleType("fastapi")

    class HTTPException(Exception):
        def __init__(self, status_code: int = 500, detail=None):
            self.status_code = status_code
            self.detail = detail
            super().__init__(detail or f"HTTPException(status_code={status_code})")

    class APIRouter:
        def get(self, *args, **kwargs):
            return lambda fn: fn

        def post(self, *args, **kwargs):
            return lambda fn: fn

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


def _markdown_report(summary, rows, confusion):
    lines = []
    lines.append(f"# {summary['benchmark_name']}")
    lines.append("")
    lines.append("## Summary")
    lines.append("")
    lines.append(f"- total_cases: {summary['total_cases']}")
    lines.append(f"- top1_correct: {summary['top1_correct']}")
    lines.append(f"- top1_accuracy: {summary['top1_accuracy']:.4f}")
    lines.append(f"- top3_correct: {summary['top3_correct']}")
    lines.append(f"- top3_accuracy: {summary['top3_accuracy']:.4f}")
    lines.append(f"- unique_expected_families: {summary['unique_families']}")
    lines.append(f"- confused_pairs (top1 miss): {summary['confused_pairs']}")
    lines.append("")
    lines.append("## Confusion pairs (top1 ≠ expected)")
    lines.append("")
    lines.append("| expected_canonical | predicted_top1 | count |")
    lines.append("|---|---|---:|")
    for (exp, pred), cnt in sorted(confusion.items(), key=lambda kv: -kv[1]):
        lines.append(f"| {exp} | {pred or '(no-hit)'} | {cnt} |")
    lines.append("")
    lines.append("## Per-family")
    lines.append("")
    per_fam = defaultdict(lambda: {"n": 0, "top1": 0, "top3": 0})
    for row in rows:
        fam = row["expected_family"]
        per_fam[fam]["n"] += 1
        per_fam[fam]["top1"] += int(bool(row["top1_correct"]))
        per_fam[fam]["top3"] += int(bool(row["top3_correct"]))
    lines.append("| family | cases | top1 | top3 |")
    lines.append("|---|---:|---:|---:|")
    for fam in sorted(per_fam):
        fs = per_fam[fam]
        lines.append(f"| {fam} | {fs['n']} | {fs['top1']}/{fs['n']} | {fs['top3']}/{fs['n']} |")
    lines.append("")
    lines.append("## Miss table (top1 miss only)")
    lines.append("")
    lines.append("| case_id | expected | top1 | top3 |")
    lines.append("|---|---|---|---|")
    for row in rows:
        if row["top1_correct"]:
            continue
        lines.append(
            f"| {row['case_id']} | {row['expected_family']} "
            f"| {row['top1_family'] or ''} "
            f"| {row['top3_families'].replace(' | ', ', ')} |"
        )
    return "\n".join(lines) + "\n"


def main() -> int:
    here = Path(__file__).resolve().parent
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=str(here / "app" / "labint.db"))
    ap.add_argument("--benchmark", default=str(here / "benchmark" / "family_admission_benchmark.json"))
    ap.add_argument("--top-k", type=int, default=5)
    ap.add_argument("--min-tanimoto", type=float, default=0.25)
    ap.add_argument("--out-dir", default=str(here / "benchmark"))
    ns = ap.parse_args()

    module = load_module(here / "app" / "evidence_search.py")
    module.DB_PATH = Path(ns.db)

    bench = json.loads(Path(ns.benchmark).read_text(encoding="utf-8"))
    cases = bench.get("cases") or []
    name = bench.get("name") or "family_admission"

    rows = []
    top1_correct = 0
    top3_correct = 0
    confusion = Counter()

    for case in cases:
        expected = case["expected_family"]
        acceptable_top1 = set(case.get("acceptable_top1") or []) | {expected}
        acceptable_top3 = set(case.get("acceptable_top3") or []) | {expected}
        try:
            req = module.StructureEvidenceRequest(
                reaction_smiles=case["reaction_smiles"],
                reagent_text=case.get("reagent_text"),  # C-bucket: forward query-side reagent_text
                top_k=ns.top_k,
                min_tanimoto=ns.min_tanimoto,
            )
            res = module._search_by_reaction(req)
        except Exception as e:
            msg = str(e)
            if "RDKit import failed" in msg:
                print("RDKit import failed — run inside chemlens conda env.")
            rows.append({
                "case_id": case["case_id"],
                "expected_family": expected,
                "top1_family": None,
                "top3_families": "",
                "top3_scores": "",
                "top1_correct": False,
                "top3_correct": False,
                "error": msg,
            })
            confusion[(expected, None)] += 1
            continue

        results = res.get("results") or []
        top3 = [item.get("reaction_family_name") for item in results[:3]]
        scores = [round(float(item.get("match_score") or 0.0), 3) for item in results[:3]]
        top1 = top3[0] if top3 else None

        hit_top1 = top1 in acceptable_top1 if top1 else False
        hit_top3 = bool(acceptable_top3.intersection(x for x in top3 if x))

        top1_correct += int(hit_top1)
        top3_correct += int(hit_top3)

        if not hit_top1:
            confusion[(expected, top1)] += 1

        rows.append({
            "case_id": case["case_id"],
            "expected_family": expected,
            "top1_family": top1,
            "top3_families": " | ".join([x for x in top3 if x]),
            "top3_scores": " | ".join(map(str, scores)),
            "top1_correct": hit_top1,
            "top3_correct": hit_top3,
            "source_extract_id": case.get("source_extract_id"),
            "source_kind": case.get("source_kind"),
            "raw_family_aliases": " | ".join(case.get("raw_family_aliases") or []),
            "error": "",
        })

    total = len(rows)
    unique_families = len(set(r["expected_family"] for r in rows))
    summary = {
        "benchmark_name": name,
        "total_cases": total,
        "top1_correct": top1_correct,
        "top1_accuracy": round(top1_correct / total, 4) if total else 0.0,
        "top3_correct": top3_correct,
        "top3_accuracy": round(top3_correct / total, 4) if total else 0.0,
        "unique_families": unique_families,
        "confused_pairs": sum(confusion.values()),
    }

    out_dir = Path(ns.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    csv_path = out_dir / "family_admission_results.csv"
    with csv_path.open("w", newline="", encoding="utf-8-sig") as f:
        if rows:
            writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
            writer.writeheader()
            writer.writerows(rows)

    json_path = out_dir / "family_admission_results.json"
    json_path.write_text(
        json.dumps({"summary": summary, "rows": rows}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    confusion_path = out_dir / "family_admission_confusion.csv"
    with confusion_path.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        writer.writerow(["expected_canonical", "predicted_top1", "count"])
        for (exp, pred), cnt in sorted(confusion.items(), key=lambda kv: -kv[1]):
            writer.writerow([exp, pred or "", cnt])

    report_path = out_dir / "family_admission_report.md"
    report_path.write_text(_markdown_report(summary, rows, confusion), encoding="utf-8")

    print(name)
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    print(f"CSV:       {csv_path}")
    print(f"JSON:      {json_path}")
    print(f"Confusion: {confusion_path}")
    print(f"MD:        {report_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
