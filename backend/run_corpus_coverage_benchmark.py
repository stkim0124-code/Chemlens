"""run_corpus_coverage_benchmark.py — Phase 2c runner.

Consumes ``benchmark/corpus_coverage_benchmark.json`` (from benchmark_builder)
and measures engine recall over a broader sample of extracts:

  * recall@1, recall@3, recall@5 (expected canonical family ∈ topK)
  * per-family recall breakdown
  * total vs. no_confident_hit cases

Outputs:
  * ``<out-dir>/corpus_coverage_results.csv``
  * ``<out-dir>/corpus_coverage_results.json``
  * ``<out-dir>/corpus_coverage_report.md``

Acceptance uses alias-tolerant matching: if the case carries
``raw_family_aliases`` (or if expected_family is a canonical name), we also
accept any raw alias name predicted by the engine. When that metadata is
missing we fall back to exact match.

The runner supports ``--max-cases`` to cap walltime during first run.
"""
from __future__ import annotations

import argparse
import csv
import importlib.util
import json
import sys
import types
import typing
from collections import defaultdict
from pathlib import Path


def _install_fastapi_shim_if_needed() -> None:
    try:
        import fastapi  # noqa: F401
        return
    except Exception:
        pass
    fastapi_stub = types.ModuleType("fastapi")

    class HTTPException(Exception):
        def __init__(self, status_code=500, detail=None):
            self.status_code = status_code
            self.detail = detail
            super().__init__(detail or f"HTTPException(status_code={status_code})")

    class APIRouter:
        def get(self, *a, **kw):
            return lambda fn: fn

        def post(self, *a, **kw):
            return lambda fn: fn

    def Query(default=None, *a, **kw):
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


def _load_alias_map(db_path: Path):
    """Map canonical → {raw aliases} using final_state_verifier's logic."""
    here = Path(__file__).resolve().parent
    cand = here
    for _ in range(4):
        if (cand / "final_state_verifier.py").exists():
            if str(cand) not in sys.path:
                sys.path.insert(0, str(cand))
            break
        cand = cand.parent
    import final_state_verifier as fsv  # noqa: E402

    conn = fsv.connect_db(db_path)
    raws = fsv.distinct_pattern_names(conn)
    groups, _ = fsv.build_alias_groups(raws)
    conn.close()
    # Also invert: raw → canonical.
    raw_to_canonical = {}
    for canonical, raw_list in groups.items():
        for r in raw_list:
            raw_to_canonical[r] = canonical
    return groups, raw_to_canonical


def _markdown_report(summary, rows, per_fam):
    lines = []
    lines.append(f"# {summary['benchmark_name']}")
    lines.append("")
    lines.append("## Summary")
    lines.append("")
    lines.append(f"- total_cases: {summary['total_cases']}")
    lines.append(f"- recall@1: {summary['recall_at_1']:.4f}")
    lines.append(f"- recall@3: {summary['recall_at_3']:.4f}")
    lines.append(f"- recall@5: {summary['recall_at_5']:.4f}")
    lines.append(f"- no_confident_hit_cases: {summary['no_confident_hit_cases']}")
    lines.append(f"- unique_families: {summary['unique_families']}")
    lines.append("")
    lines.append("## Per-family recall")
    lines.append("")
    lines.append("| family | cases | r@1 | r@3 | r@5 |")
    lines.append("|---|---:|---:|---:|---:|")
    for fam in sorted(per_fam):
        fs = per_fam[fam]
        n = fs["n"]
        lines.append(
            f"| {fam} | {n} | {fs['r1']}/{n} | {fs['r3']}/{n} | {fs['r5']}/{n} |"
        )
    lines.append("")
    lines.append("## Miss table (recall@3 miss)")
    lines.append("")
    lines.append("| case_id | expected | top3 | top3_scores |")
    lines.append("|---|---|---|---|")
    for row in rows:
        if row["hit_at_3"]:
            continue
        lines.append(
            f"| {row['case_id']} | {row['expected_family']} "
            f"| {row['top3_families'].replace(' | ', ', ')} "
            f"| {row['top3_scores']} |"
        )
    return "\n".join(lines) + "\n"


def main() -> int:
    here = Path(__file__).resolve().parent
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=str(here / "app" / "labint.db"))
    ap.add_argument("--benchmark", default=str(here / "benchmark" / "corpus_coverage_benchmark.json"))
    ap.add_argument("--top-k", type=int, default=5)
    ap.add_argument("--min-tanimoto", type=float, default=0.25)
    ap.add_argument("--out-dir", default=str(here / "benchmark"))
    ap.add_argument("--max-cases", type=int, default=0, help="Cap for first run (0 = all)")
    ns = ap.parse_args()

    module = load_module(here / "app" / "evidence_search.py")
    module.DB_PATH = Path(ns.db)

    groups, raw_to_canonical = _load_alias_map(Path(ns.db))

    bench = json.loads(Path(ns.benchmark).read_text(encoding="utf-8"))
    cases = bench.get("cases") or []
    if ns.max_cases and ns.max_cases < len(cases):
        cases = cases[: ns.max_cases]
    name = bench.get("name") or "corpus_coverage"

    rows = []
    r1 = r3 = r5 = 0
    no_hit = 0
    per_fam = defaultdict(lambda: {"n": 0, "r1": 0, "r3": 0, "r5": 0})

    for case in cases:
        expected = case["expected_family"]
        # Expand acceptable set to include every raw-alias under the canonical.
        acceptable = set([expected]) | set(groups.get(expected, []))
        try:
            req = module.StructureEvidenceRequest(
                reaction_smiles=case["reaction_smiles"],
                reagent_text=case.get("reagent_text"),  # C-bucket: forward query-side reagent_text
                top_k=ns.top_k,
                min_tanimoto=ns.min_tanimoto,
            )
            res = module._search_by_reaction(req)
        except Exception as e:
            rows.append({
                "case_id": case["case_id"],
                "expected_family": expected,
                "top1_family": None,
                "top3_families": "",
                "top3_scores": "",
                "hit_at_1": False,
                "hit_at_3": False,
                "hit_at_5": False,
                "no_confident_hit": True,
                "error": str(e),
            })
            per_fam[expected]["n"] += 1
            no_hit += 1
            continue

        results = res.get("results") or []
        names = [item.get("reaction_family_name") for item in results[: ns.top_k]]
        scores = [round(float(item.get("match_score") or 0.0), 3) for item in results[: ns.top_k]]

        # Canonicalise predicted names so raw-alias hits count.
        def canon(n):
            if not n:
                return n
            return raw_to_canonical.get(n, n)

        canon_names = [canon(n) for n in names]

        hit1 = canon_names[:1] and (canon_names[0] in acceptable or canon_names[0] == expected)
        hit3 = any((n in acceptable or n == expected) for n in canon_names[:3] if n)
        hit5 = any((n in acceptable or n == expected) for n in canon_names[:5] if n)

        r1 += int(bool(hit1))
        r3 += int(bool(hit3))
        r5 += int(bool(hit5))
        if res.get("no_confident_hit"):
            no_hit += 1

        per_fam[expected]["n"] += 1
        per_fam[expected]["r1"] += int(bool(hit1))
        per_fam[expected]["r3"] += int(bool(hit3))
        per_fam[expected]["r5"] += int(bool(hit5))

        rows.append({
            "case_id": case["case_id"],
            "expected_family": expected,
            "top1_family": names[0] if names else None,
            "top3_families": " | ".join([n for n in names[:3] if n]),
            "top3_scores": " | ".join(map(str, scores[:3])),
            "top5_families": " | ".join([n for n in names[:5] if n]),
            "hit_at_1": bool(hit1),
            "hit_at_3": bool(hit3),
            "hit_at_5": bool(hit5),
            "no_confident_hit": bool(res.get("no_confident_hit")),
            "source_extract_id": case.get("source_extract_id"),
            "source_kind": case.get("source_kind"),
            "error": "",
        })

    total = len(rows)
    summary = {
        "benchmark_name": name,
        "total_cases": total,
        "recall_at_1": round(r1 / total, 4) if total else 0.0,
        "recall_at_3": round(r3 / total, 4) if total else 0.0,
        "recall_at_5": round(r5 / total, 4) if total else 0.0,
        "hit_at_1": r1,
        "hit_at_3": r3,
        "hit_at_5": r5,
        "no_confident_hit_cases": no_hit,
        "unique_families": len(per_fam),
    }

    out_dir = Path(ns.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    csv_path = out_dir / "corpus_coverage_results.csv"
    with csv_path.open("w", newline="", encoding="utf-8-sig") as f:
        if rows:
            writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
            writer.writeheader()
            writer.writerows(rows)

    json_path = out_dir / "corpus_coverage_results.json"
    json_path.write_text(
        json.dumps({"summary": summary, "rows": rows}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    report_path = out_dir / "corpus_coverage_report.md"
    report_path.write_text(_markdown_report(summary, rows, per_fam), encoding="utf-8")

    print(name)
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    print(f"CSV:  {csv_path}")
    print(f"JSON: {json_path}")
    print(f"MD:   {report_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
