"""benchmark_builder.py — Generate Phase 2 MVP benchmark specs from canonical DB.

Produces three JSON specs in `benchmark/`:

  * ``named_reaction_benchmark_broad.json``      — 1 representative reaction case
                                                   per rich family (264 cases target).
  * ``family_admission_benchmark.json``          — same set but annotated with
                                                   alias/sibling expectations for
                                                   confusion-matrix scoring.
  * ``corpus_coverage_benchmark.json``           — all queryable extracts (or a
                                                   --sample-per-family capped slice)
                                                   covering every rich family.

Selection strategy (per rich family):
  1. Use ``final_state_verifier.build_alias_groups`` to map raw names → canonical.
  2. For each canonical family, gather all ``reaction_extracts`` rows from the
     raw family set whose ``extract_kind`` ∈ {application_example, canonical_overview}.
  3. Prefer extracts that have *both* reactant_smiles and product_smiles populated
     in ``reaction_extracts``. If reaction_extracts side is empty, fall back to
     ``extract_molecules`` queryable pairs.
  4. Score each candidate by ``(#R_frags + #P_frags, pair_key_novelty)`` and
     keep the top candidate for the broad/admission benchmark.
  5. For corpus coverage, emit one case per valid (extract_id, reaction_smiles)
     triple, up to ``--sample-per-family`` per family.

Alias / sibling expectations (admission):
  * ``acceptable_top1``: the canonical family itself.
  * ``acceptable_top3``: canonical family + any raw-name aliases that were
     collapsed into the same canonical group (since the engine may return a raw
     alias).

No RDKit needed — reads SMILES as-is, constructs ``reactant_smiles>>product_smiles``.

Usage::

    python benchmark_builder.py \\
        --db app/labint.db \\
        --out-dir benchmark \\
        --sample-per-family 2

All outputs are deterministic given the same DB snapshot.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

# ---- bootstrap: allow running from phase_queue/processing/ ------------------
_HERE = Path(__file__).resolve().parent
_cand = _HERE
for _ in range(4):
    if (_cand / "final_state_verifier.py").exists():
        if str(_cand) not in sys.path:
            sys.path.insert(0, str(_cand))
        break
    _cand = _cand.parent
for _p in os.environ.get("PYTHONPATH", "").split(os.pathsep):
    if _p and _p not in sys.path:
        sys.path.insert(0, _p)

import final_state_verifier as fsv  # noqa: E402


# ---- helpers ----------------------------------------------------------------


def _split_smi(raw):
    if raw is None:
        return []
    s = str(raw)
    return [x.strip() for x in s.split(" | ") if x.strip()]


def _pair_score(reactants, products):
    """Higher is "richer" — reward fragment count, penalize blanks."""
    if not reactants or not products:
        return -1
    return min(len(reactants), 5) + min(len(products), 5)


def _build_reaction_smiles(reactants, products):
    if not reactants or not products:
        return None
    # Dedupe at both " | " (entry) and "." (fragment) levels. Some DB rows
    # store multi-fragment SMILES (e.g. "A.B") in a single entry, and curation
    # seeds occasionally repeat fragments across entries. Deduping at the
    # fragment level yields cleaner queries without changing RDKit semantics
    # (A.B.A == A.B).
    def _explode_and_uniq(items):
        seen = set()
        out = []
        for it in items:
            if not it:
                continue
            for frag in str(it).split("."):
                frag = frag.strip()
                if frag and frag not in seen:
                    seen.add(frag)
                    out.append(frag)
        return out
    r_frags = _explode_and_uniq(reactants)
    p_frags = _explode_and_uniq(products)
    if not r_frags or not p_frags:
        return None
    return f"{'.'.join(r_frags)}>>{'.'.join(p_frags)}"


# ---- main builder -----------------------------------------------------------


def build_benchmarks(
    db_path: Path,
    out_dir: Path,
    sample_per_family: int = 2,
) -> dict:
    out_dir.mkdir(parents=True, exist_ok=True)

    conn = fsv.connect_db(db_path)
    raw_names = fsv.distinct_pattern_names(conn)
    alias_groups, _collapse = fsv.build_alias_groups(raw_names)
    pair_map = fsv.pair_map_from_extract_molecules(conn)

    # Per rich family: collect candidate extracts.
    broad_cases = []
    admission_cases = []
    coverage_cases = []
    seen_coverage_pair = set()  # dedupe by (family, reaction_smiles)

    rich_family_counts = 0
    total_scanned = 0

    for canonical, raws in sorted(alias_groups.items()):
        summary = fsv.summarize_canonical_family(conn, canonical, raws, pair_map)
        if summary["completion_bucket"] != "rich":
            continue
        rich_family_counts += 1

        rows = fsv.family_rows(conn, raws)
        candidates = []
        for r in rows:
            kind = r["extract_kind"]
            if kind not in ("canonical_overview", "application_example"):
                continue
            total_scanned += 1

            # Prefer reaction_extracts fields; fall back to extract_molecules pair.
            rxs = _split_smi(r["reactant_smiles"])
            pxs = _split_smi(r["product_smiles"])
            if (not rxs or not pxs) and int(r["id"]) in pair_map:
                alt_r, alt_p = pair_map[int(r["id"])]
                rxs = rxs or alt_r
                pxs = pxs or alt_p
            rxn = _build_reaction_smiles(rxs, pxs)
            if rxn is None:
                continue

            score = _pair_score(rxs, pxs)
            candidates.append(
                {
                    "extract_id": int(r["id"]),
                    "raw_family_name": r["reaction_family_name"],
                    "extract_kind": kind,
                    "reactant_smiles": rxs,
                    "product_smiles": pxs,
                    "reaction_smiles": rxn,
                    "score": score,
                    "reagents_text": r["reagents_text"] if "reagents_text" in r.keys() else None,
                }
            )

        # Corpus-coverage cases: up to sample_per_family per canonical family,
        # preferring application_example then canonical_overview, highest score first.
        cov_sorted = sorted(
            candidates,
            key=lambda c: (
                0 if c["extract_kind"] == "application_example" else 1,
                -c["score"],
                c["extract_id"],
            ),
        )
        cov_picked = 0
        for cand in cov_sorted:
            if cov_picked >= sample_per_family:
                break
            key = (canonical, cand["reaction_smiles"])
            if key in seen_coverage_pair:
                continue
            seen_coverage_pair.add(key)
            coverage_cases.append(
                {
                    "case_id": f"cov_{canonical.replace(' ', '_').lower()}_{cand['extract_id']}",
                    "expected_family": canonical,
                    "reaction_smiles": cand["reaction_smiles"],
                    "source_extract_id": cand["extract_id"],
                    "source_kind": cand["extract_kind"],
                    "raw_family_name": cand["raw_family_name"],
                    "reagent_text": cand.get("reagents_text"),  # C-bucket: query-side reagent_text for Phase 5 scoring
                    "notes": "Corpus coverage MVP — sampled from canonical family.",
                    "disallow_top3": [],
                }
            )
            cov_picked += 1

        # Representative broad + admission case: single best candidate.
        if not candidates:
            continue
        best = max(
            candidates,
            key=lambda c: (
                c["score"],
                # Prefer application_example over canonical_overview (has transformation).
                0 if c["extract_kind"] == "application_example" else -1,
                -c["extract_id"],
            ),
        )

        case_id = f"broad_{canonical.replace(' ', '_').lower()}_{best['extract_id']}"
        base = {
            "case_id": case_id,
            "expected_family": canonical,
            "reaction_smiles": best["reaction_smiles"],
            "source_extract_id": best["extract_id"],
            "source_kind": best["extract_kind"],
            "raw_family_name": best["raw_family_name"],
            "reagent_text": best.get("reagents_text"),  # C-bucket: query-side reagent_text
            "notes": "Phase 2 broad benchmark — representative reaction per rich family.",
            "disallow_top3": [],
        }
        broad_cases.append(base)

        # Admission case: also track the set of raw-name aliases.
        raw_set = sorted(set(raws))
        admission = dict(base)
        admission["case_id"] = f"adm_{canonical.replace(' ', '_').lower()}_{best['extract_id']}"
        admission["raw_family_aliases"] = raw_set
        # Engine may return the raw name form; treat all raws as acceptable.
        acceptable = sorted(set([canonical] + raw_set))
        admission["acceptable_top1"] = acceptable
        admission["acceptable_top3"] = acceptable
        admission["notes"] = (
            "Phase 2 family admission benchmark — representative reaction per "
            "rich family. acceptable_{top1,top3} includes canonical + raw "
            "aliases collapsed into same canonical group."
        )
        admission_cases.append(admission)

    conn.close()

    # --- write JSON specs ---
    broad_spec = {
        "name": "CHEMLENS broad benchmark phase2_mvp",
        "notes": (
            "1 representative reaction per rich family (canonical-collapsed). "
            "Built by benchmark_builder.py from app/labint.db."
        ),
        "cases": broad_cases,
    }
    admission_spec = {
        "name": "CHEMLENS family admission benchmark phase2_mvp",
        "notes": (
            "Per-family admission probe. acceptable_{top1,top3} expands to raw-"
            "name aliases under the same canonical family to reflect real alias "
            "tolerance. Use runner `run_family_admission_benchmark.py` to emit "
            "top1/top3 accuracy + confusion matrix."
        ),
        "cases": admission_cases,
    }
    coverage_spec = {
        "name": "CHEMLENS corpus coverage benchmark phase2_mvp",
        "notes": (
            f"Up to {sample_per_family} reactions per rich family, prioritising "
            "application_example over canonical_overview. Use runner "
            "`run_corpus_coverage_benchmark.py` for recall@1/@3/@5."
        ),
        "sample_per_family": sample_per_family,
        "cases": coverage_cases,
    }

    broad_path = out_dir / "named_reaction_benchmark_broad.json"
    admission_path = out_dir / "family_admission_benchmark.json"
    coverage_path = out_dir / "corpus_coverage_benchmark.json"

    broad_path.write_text(
        json.dumps(broad_spec, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    admission_path.write_text(
        json.dumps(admission_spec, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    coverage_path.write_text(
        json.dumps(coverage_spec, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    stats = {
        "rich_families": rich_family_counts,
        "broad_cases": len(broad_cases),
        "admission_cases": len(admission_cases),
        "coverage_cases": len(coverage_cases),
        "extracts_scanned": total_scanned,
        "broad_path": str(broad_path),
        "admission_path": str(admission_path),
        "coverage_path": str(coverage_path),
    }
    return stats


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True)
    ap.add_argument("--out-dir", default="benchmark")
    ap.add_argument("--sample-per-family", type=int, default=2)
    ns = ap.parse_args()

    stats = build_benchmarks(
        Path(ns.db).resolve(),
        Path(ns.out_dir).resolve(),
        sample_per_family=ns.sample_per_family,
    )
    print("[benchmark_builder]", json.dumps(stats, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())
