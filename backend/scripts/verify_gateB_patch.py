"""Verify Phase 4 Gate B patch integrity.

1. AST parse of app/evidence_search.py.
2. Runtime import to make sure SMARTS compile.
3. Compare feature count vs 3g-5 baseline (should be +5).
4. Spot-check a couple of confused_pairs cases with reaction_delta to verify
   that the new guard branches are reachable.
"""
from __future__ import annotations
import ast
import os
import pathlib
import sys
import traceback


def main() -> int:
    src_path = pathlib.Path("app/evidence_search.py")
    src = src_path.read_text(encoding="utf-8")

    # 1. AST parse
    try:
        ast.parse(src)
        print(f"[1/4] AST parse OK ({len(src.splitlines())} lines, {len(src)} bytes)")
    except SyntaxError as e:
        print(f"[1/4] AST parse FAILED: {e}")
        return 1

    # 2. Runtime import
    sys.path.insert(0, ".")
    os.environ.setdefault("LABINT_DB_PATH", "labint.db")
    try:
        from app import evidence_search as es
    except Exception as e:
        print(f"[2/4] Runtime import FAILED: {type(e).__name__}: {e}")
        traceback.print_exc()
        return 2
    print(f"[2/4] Runtime import OK")

    # 3. Feature count check
    feats = list(es._REACTION_FEATURE_SMARTS.keys())
    new_gateb = {"amine_oxide", "diazonium", "coumarin", "xanthate", "indole"}
    found_new = [k for k in feats if k in new_gateb]
    all_compiled = all(
        es._REACTION_FEATURE_MOLS.get(k) is not None for k in found_new
    )
    print(f"[3/4] Total features: {len(feats)}")
    print(f"     New Gate B features present: {sorted(found_new)} (expected 5)")
    print(f"     All 5 compiled: {all_compiled}")
    if len(found_new) != 5 or not all_compiled:
        return 3

    # 4. Smoke test _family_delta_adjustment on new guard families.
    # Construct minimal reaction_delta stubs that should trigger each new branch.
    test_cases = [
        ("Cope Elimination", {
            "reactants": {"amine_oxide": 1, "alkene": 0},
            "products":  {"alkene": 1},
            "delta":     {"alkene": 1},
        }),
        ("Hofmann Elimination", {
            "reactants": {"amine": 1, "amine_oxide": 0},
            "products":  {"alkene": 1},
            "delta":     {"alkene": 1},
        }),
        ("Sandmeyer Reaction", {
            "reactants": {"diazonium": 1, "aryl_halide": 0},
            "products":  {"aryl_halide": 1},
            "delta":     {"aryl_halide": 1},
        }),
        ("Peterson Olefination", {
            "reactants": {"silicon": 1, "carbonyl": 1},
            "products":  {"alkene": 1},
            "delta":     {"alkene": 1, "carbonyl": -1},
        }),
        ("von Pechmann Reaction", {
            "reactants": {"aromatic_ring": 1, "alcohol": 1, "ester": 1, "carbonyl": 1},
            "products":  {"coumarin": 1, "aromatic_ring": 2},
            "delta":     {"coumarin": 1},
        }),
        ("Ullmann Reaction / Coupling / Biaryl Synthesis", {
            "reactants": {"aryl_halide": 2},
            "products":  {"aromatic_ring": 2},
            "delta":     {"aryl_halide": -2},
        }),
        ("Chugaev Elimination Reaction (Xanthate Ester Pyrolysis)", {
            # Chugaev guard at evidence_search.py:~1148 penalizes when
            #     d.get("alcohol",0) >= 0 and d.get("ether",0) >= 0
            # (the "elimination direction mismatch" check). For a healthy
            # positive smoke test we need at least one of alcohol/ether to go
            # negative so the penalty branch skips and only the xanthate→alkene
            # boost fires. alcohol:-1 is the conventional choice — xanthate is
            # formally the O-C(=S)-S-R ester of the parent alcohol, so the
            # precursor alcohol is "consumed" on the reactant accounting.
            "reactants": {"xanthate": 1, "alcohol": 0, "ether": 0},
            "products":  {"alkene": 1},
            "delta":     {"alkene": 1, "xanthate": -1, "alcohol": -1},
        }),
        ("Biginelli Reaction", {
            "reactants": {"carbonyl": 2, "ester": 1, "amine": 1, "ammonia": 0},
            "products":  {"dihydropyridine": 1, "pyridine": 0},
            "delta":     {},
        }),
        ("Schmidt Reaction", {
            "reactants": {"carboxylic_acid": 1, "azide": 1},
            "products":  {"amide": 1},
            "delta":     {"amide": 1},
        }),
        ("Wolff-Kishner Reduction", {
            "reactants": {"carbonyl": 1, "silicon": 1},
            "products":  {"alkene": 1},
            "delta":     {"alkene": 1, "carbonyl": -1},
        }),
        ("Finkelstein Reaction", {
            "reactants": {"alkyl_halide": 1, "carbonyl": 0, "amine": 0, "amide": 0},
            "products":  {"alkyl_halide": 1, "aryl_halide": 0},
            "delta":     {},
        }),
        ("Acetoacetic Ester Synthesis", {
            "reactants": {"ester": 1, "carbonyl": 2, "amine": 1, "ammonia": 0},
            "products":  {"pyrrole": 1},
            "delta":     {},
        }),
    ]
    print(f"[4/4] Smoke test — 12 new/updated guard branches:")
    for fam, delta in test_cases:
        mult, notes = es._family_delta_adjustment(fam, delta)
        tag = "boost " if mult > 1.0 else ("penal " if mult < 1.0 else "neutr")
        print(f"     [{tag}] {fam!r:65s} mult={mult:.3f} (n_notes={len(notes)})")
        if mult <= 0 or mult > 2.0:
            print(f"          !! mult out of clamp range")
            return 4
    print("ALL OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
