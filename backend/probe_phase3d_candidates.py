"""Phase 3d candidate probe.

Identifies "rich but thin" families — ones that pass the rich completion
threshold (ov>=1, ap>=2, queryable_R>=3, queryable_P>=3, pair>=3) but still
lose admission top1 cases in the 3c-b benchmark. These are the depth-sprint
candidates: adding more seed variants should widen their fingerprint coverage
and reduce the admission bleed.

Usage:
  python probe_phase3d_candidates.py \
    --db C:\\chemlens\\backend\\app\\labint.db \
    --confusion C:\\chemlens\\backend\\reports\\phase3c\\apply_3c_b_20260421_094500\\admission\\family_admission_confusion.csv \
    --out-dir C:\\chemlens\\backend\\reports\\phase3d\\probe_20260421
"""
from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path
from typing import Any, Dict, List

# Add backend to sys.path so we can import final_state_verifier
sys.path.insert(0, str(Path(__file__).resolve().parent))

import final_state_verifier as fsv  # type: ignore


def load_confusion(path: Path) -> Dict[str, int]:
    """Return map: expected_canonical -> admission-loss-count."""
    loss: Dict[str, int] = {}
    with path.open("r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            exp = (row.get("expected_canonical") or "").strip()
            try:
                cnt = int(row.get("count") or "1")
            except ValueError:
                cnt = 1
            if exp:
                loss[exp] = loss.get(exp, 0) + cnt
    return loss


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--db", required=True)
    p.add_argument("--confusion", required=True)
    p.add_argument("--out-dir", required=True)
    p.add_argument("--top", type=int, default=30)
    args = p.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    conf_path = Path(args.confusion)
    if not conf_path.exists():
        print(f"confusion not found: {conf_path}", file=sys.stderr)
        return 2
    losses = load_confusion(conf_path)
    print(f"loaded {len(losses)} victim families from confusion")

    conn = fsv.connect_db(Path(args.db))

    raw_names = fsv.distinct_pattern_names(conn)
    alias_groups, _events = fsv.build_alias_groups(raw_names)
    pair_map = fsv.pair_map_from_extract_molecules(conn)

    candidates: List[Dict[str, Any]] = []
    missing_in_db: List[str] = []

    for canonical, raws in alias_groups.items():
        if canonical not in losses:
            continue
        summary = fsv.summarize_canonical_family(conn, canonical, raws, pair_map)
        candidates.append({
            "canonical": canonical,
            "admission_loss_count": losses[canonical],
            "extract_count": summary.get("extract_count", 0),
            "overview_count": summary.get("overview_count", 0),
            "application_count": summary.get("application_count", 0),
            "mechanism_count": summary.get("mechanism_count", 0),
            "queryable_R": summary.get("queryable_reactants", 0),
            "queryable_P": summary.get("queryable_products", 0),
            "unique_pair_count": summary.get("unique_queryable_pair_count", 0),
            "molecule_rows": summary.get("molecule_rows", 0),
            "completion_bucket": summary.get("completion_bucket", "?"),
            "collision_prone": summary.get("collision_prone_candidate", False),
            "rich_pass": summary.get("rich_completion_pass", False),
            "raw_names": "|".join(raws),
        })

    # Find victims that are in confusion but NOT in any alias group — means their
    # canonical spelling in confusion disagrees with the DB canonical.
    covered = {c["canonical"] for c in candidates}
    for v in losses.keys():
        if v not in covered:
            missing_in_db.append(v)

    conn.close()

    # Rank: primary = admission_loss_count desc; tiebreak = rich-but-thin signal
    # (lower unique_pair_count and lower extract_count = more room to grow).
    def score(c: Dict[str, Any]) -> tuple:
        return (
            -int(c["admission_loss_count"]),                  # more losses first
            +int(c["unique_pair_count"]),                     # fewer pairs = more room
            +int(c["extract_count"]),                         # fewer extracts = higher leverage
            -int(c["queryable_R"] + c["queryable_P"]),        # break ties stably
        )

    candidates.sort(key=score)

    # CSV output
    csv_path = out_dir / "phase3d_candidates.csv"
    with csv_path.open("w", encoding="utf-8", newline="") as f:
        fieldnames = [
            "canonical", "admission_loss_count", "completion_bucket", "rich_pass",
            "extract_count", "overview_count", "application_count", "mechanism_count",
            "queryable_R", "queryable_P", "unique_pair_count", "molecule_rows",
            "collision_prone", "raw_names",
        ]
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for c in candidates:
            w.writerow({k: c.get(k, "") for k in fieldnames})

    # Top-N markdown
    md_path = out_dir / "phase3d_candidates_top.md"
    with md_path.open("w", encoding="utf-8") as f:
        f.write("# Phase 3d candidates — rich-but-thin admission victims\n\n")
        f.write(f"Source confusion: `{args.confusion}`\n")
        f.write(f"Total distinct victims in confusion: **{len(losses)}**\n")
        f.write(f"Mapped to canonical in DB: **{len(candidates)}**\n")
        if missing_in_db:
            f.write(f"Unmapped (confusion canonical doesn't match DB): **{len(missing_in_db)}**\n\n")
            f.write("  Unmapped list (first 10): " + ", ".join(f"`{n}`" for n in missing_in_db[:10]) + "\n\n")
        f.write(f"\nTop {args.top} sorted by loss count desc, then pair/extract count asc:\n\n")
        f.write("| rank | canonical | losses | bucket | rich | ov | ap | mech | qR | qP | pairs | mol_rows | collision |\n")
        f.write("|------|-----------|--------|--------|------|----|----|------|----|----|-------|----------|----------|\n")
        for i, c in enumerate(candidates[:args.top], start=1):
            f.write(
                f"| {i} | {c['canonical']} | {c['admission_loss_count']} | "
                f"{c['completion_bucket']} | {'Y' if c['rich_pass'] else 'N'} | "
                f"{c['overview_count']} | {c['application_count']} | {c['mechanism_count']} | "
                f"{c['queryable_R']} | {c['queryable_P']} | {c['unique_pair_count']} | "
                f"{c['molecule_rows']} | {'Y' if c['collision_prone'] else ''} |\n"
            )

    # JSON output (full)
    json_path = out_dir / "phase3d_candidates.json"
    with json_path.open("w", encoding="utf-8") as f:
        json.dump({
            "db": args.db,
            "confusion": args.confusion,
            "total_victims": len(losses),
            "mapped": len(candidates),
            "unmapped": missing_in_db,
            "candidates": candidates,
        }, f, indent=2, ensure_ascii=False)

    print(f"wrote {csv_path}")
    print(f"wrote {md_path}")
    print(f"wrote {json_path}")
    print(f"mapped {len(candidates)}/{len(losses)} victims; {len(missing_in_db)} unmapped")

    # Also print quick summary so we can see it in console
    rich_count = sum(1 for c in candidates if c.get("rich_pass"))
    shallow_count = sum(1 for c in candidates if c.get("completion_bucket") == "shallow")
    print(f"  rich_pass victims: {rich_count}")
    print(f"  shallow victims:   {shallow_count}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
