"""Smoke-test the Phase 4 Gate B iter2 Simmons-Smith guard on case 368."""
import json
import os
import sys
from pathlib import Path

BASE = Path(os.environ.get("CHEMLENS_BACKEND", r"C:\chemlens\backend"))
sys.path.insert(0, str(BASE))

from app import evidence_search as ES  # noqa: E402


def find_case_368():
    # Try several plausible locations
    candidates = [
        BASE / "benchmark" / "named_reaction_benchmark_coverage.json",
        BASE / "benchmark" / "coverage_benchmark.json",
    ]
    for p in BASE.glob("benchmark/**/*coverage*.json"):
        candidates.append(p)
    for p in candidates:
        if not p.exists():
            continue
        try:
            d = json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            continue
        cases = d if isinstance(d, list) else d.get("cases", d.get("rows", []))
        for c in cases:
            cid = str(c.get("case_id") or c.get("id") or "")
            if "buchwald" in cid.lower() and "368" in cid:
                return p, c
    return None, None


def main():
    p, case = find_case_368()
    print(f"[case_368] source={p}")
    if not case:
        print("[case_368] NOT FOUND — falling back to manual SMILES stub")
        # Fallback: synthetic SMILES with bystander cyclopropyl that mimics the case
        # Buchwald-Hartwig aryl amination; substrate has cyclopropyl, product has cyclopropyl (delta=0)
        rxn = "BrC1=CC=CC=C1.NC2CC2>>N(C3CC3)C1=CC=CC=C1"
    else:
        rxn = case.get("query_smiles") or case.get("reaction_smiles") or case.get("smiles")
    print(f"[case_368] rxn_smiles={rxn}")

    parsed = ES._parse_reaction_smiles(rxn)
    rd = ES._reaction_delta_from_components(parsed)
    print(f"[case_368] reactant cyclopropane={rd.get('reactants', {}).get('cyclopropane', 'MISSING')}")
    print(f"[case_368] product cyclopropane={rd.get('products', {}).get('cyclopropane', 'MISSING')}")
    print(f"[case_368] delta cyclopropane={rd.get('delta', {}).get('cyclopropane', 'MISSING')}")
    print(f"[case_368] reactant alkene={rd.get('reactants', {}).get('alkene', 'MISSING')}")

    # Exercise the Simmons-Smith guard
    mult, notes = ES._family_delta_adjustment("Simmons-Smith Reaction", rd)
    print(f"[case_368] SIMMONS-SMITH mult={mult:.3f}  notes={notes}")

    # Compare: Buchwald-Hartwig
    mult2, notes2 = ES._family_delta_adjustment("Buchwald-Hartwig Cross-Coupling", rd)
    print(f"[case_368] BUCHWALD-HARTWIG mult={mult2:.3f}  notes={notes2}")

    print()
    # Positive control: genuine Simmons-Smith (alkene → cyclopropane)
    rxn_pos = "C=CCCCCCC>>C1(CCCCCCC)CC1"
    parsed_pos = ES._parse_reaction_smiles(rxn_pos)
    rd_pos = ES._reaction_delta_from_components(parsed_pos)
    print(f"[positive] rxn={rxn_pos}")
    print(f"[positive] reactant alkene={rd_pos.get('reactants', {}).get('alkene', 'MISSING')} "
          f"delta cyclopropane={rd_pos.get('delta', {}).get('cyclopropane', 'MISSING')}")
    mult3, notes3 = ES._family_delta_adjustment("Simmons-Smith Reaction", rd_pos)
    print(f"[positive] SIMMONS-SMITH mult={mult3:.3f}  notes={notes3}")


if __name__ == "__main__":
    main()
