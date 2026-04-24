"""Identify greedy predictors in Gate B iterate1 snapshot (task #118)."""
import json
import os
from collections import Counter, defaultdict
from pathlib import Path

_env_base = os.environ.get("CHEMLENS_BACKEND")
BASE = Path(_env_base) if _env_base else Path(r"C:\chemlens\backend")
SNAP = BASE / "reports/phase4/gateB/iterate1_20260424/admission/family_admission_results.json"


def main():
    d = json.loads(SNAP.read_text(encoding="utf-8"))
    greed = Counter()
    pair_cases = defaultdict(list)

    for r in d["rows"]:
        exp = r.get("expected_family", "")
        top1 = r.get("top1_family", "")
        if exp and top1 and not (exp == top1):
            greed[top1] += 1
            pair_cases[(exp, top1)].append(r["case_id"])

    print("=" * 80)
    print("TOP 15 GREEDY PREDICTORS (iterate1 admission)")
    print("=" * 80)
    print(f'{"count":>5}  family')
    for fam, n in greed.most_common(15):
        print(f"{n:>5}  {fam}")
    print()
    print(f"Distinct greedy predictors: {len(greed)}")
    print(f"Total top1 mismatches: {sum(greed.values())}")

    print()
    print("=" * 80)
    print("TOP-12 CONFUSED PAIRS (expected -> predicted)")
    print("=" * 80)
    pair_counts = Counter({k: len(v) for k, v in pair_cases.items()})
    for (exp, pred), n in pair_counts.most_common(12):
        print()
        print(f"  [{n}x]  {exp}")
        print(f"         -> {pred}")
        cases = ", ".join(pair_cases[(exp, pred)][:3])
        print(f"         cases: {cases}")

    print()
    print("=" * 80)
    print("GREEDY CONCENTRATION PROFILE")
    print("=" * 80)
    by_greedy = defaultdict(Counter)
    for (exp, pred), n in pair_counts.items():
        by_greedy[pred][exp] += n
    print(f'{"total":>5}  {"#victims":>8}  predictor -> victim breakdown')
    for pred, total in greed.most_common(15):
        victims = by_greedy[pred]
        parts = [f"{e[:35]}x{n}" for e, n in victims.most_common(3)]
        breakdown = ", ".join(parts)
        print(f"{total:>5}  {len(victims):>8}  {pred}: {breakdown}")


if __name__ == "__main__":
    main()
