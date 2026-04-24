"""Inspect the 2 admission losers (Sandmeyer 1237, Schmidt 1240) in detail.

Compares 3g-5 baseline and Gate B post-patch row payloads, then reads the
benchmark spec to surface the reaction_smiles for each case.
"""
import json
from pathlib import Path

BASE = Path(r"C:\chemlens\backend")

def main():
    before = json.loads((BASE / "reports/phase3g/apply_3g_5_20260422_000000/admission/family_admission_results.json").read_text(encoding="utf-8"))
    after  = json.loads((BASE / "reports/phase4/gateB/apply_20260424_bench/admission/family_admission_results.json").read_text(encoding="utf-8"))
    lost = {"adm_sandmeyer_reaction_1237", "adm_schmidt_reaction_1240",
            "adm_hofmann_elimination_910"}  # coverage-only, but inspect

    for tag, d in [("BEFORE_3g5", before), ("AFTER_gateB", after)]:
        for r in d["rows"]:
            if r["case_id"] in lost:
                print(f"\n== {tag} {r['case_id']} ==")
                for k in ("expected_family", "top1_family", "top3_families",
                         "top3_scores", "source_kind", "raw_family_aliases"):
                    v = r.get(k)
                    if isinstance(v, list):
                        v = ", ".join(str(x) for x in v)
                    print(f"  {k}: {v}")

    # benchmark spec
    for fn in ("benchmark/family_admission_benchmark.json",
               "benchmark/corpus_coverage_benchmark.json"):
        p = BASE / fn
        if not p.exists():
            continue
        spec = json.loads(p.read_text(encoding="utf-8"))
        cases = spec.get("cases", []) or spec.get("rows", [])
        for c in cases:
            if c.get("case_id") in lost:
                print(f"\n== SPEC from {fn} :: {c.get('case_id')} ==")
                for k in ("expected_family", "reaction_smiles",
                          "source_extract_id", "source_kind",
                          "disallow_top3", "notes"):
                    v = c.get(k)
                    if v is not None and v != "":
                        print(f"  {k}: {v}")

if __name__ == "__main__":
    main()
