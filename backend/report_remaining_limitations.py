from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Dict, List

BACKEND_DIR = Path(__file__).resolve().parent
APP_DIR = BACKEND_DIR / "app"
CANONICAL_DB = APP_DIR / "labint.db"
BRIDGE_WORK_DB = APP_DIR / "labint_round9_bridge_work.db"
REPORT_JSON = BACKEND_DIR / "CANONICAL_DB_GAP_REPORT.json"
REPORT_MD = BACKEND_DIR / "CANONICAL_DB_GAP_REPORT.md"
BRIDGE_JSON = BACKEND_DIR / "BRIDGE_WORK_STATUS.json"


def fetch_all(conn: sqlite3.Connection, sql: str, params=()):
    conn.row_factory = sqlite3.Row
    return conn.execute(sql, params).fetchall()


def fetch_one_dict(conn: sqlite3.Connection, sql: str, params=()):
    conn.row_factory = sqlite3.Row
    row = conn.execute(sql, params).fetchone()
    return dict(row) if row else {}


def build_report() -> Dict:
    if not CANONICAL_DB.exists():
        raise SystemExit(f"Canonical DB not found: {CANONICAL_DB}")

    conn = sqlite3.connect(str(CANONICAL_DB))
    try:
        manual_families = fetch_all(
            conn,
            """
            WITH manual AS (
              SELECT reference_family_name AS family, COUNT(*) AS page_count
              FROM manual_page_knowledge
              WHERE reference_family_name IS NOT NULL AND trim(reference_family_name)<>''
              GROUP BY reference_family_name
            ), queryable AS (
              SELECT reaction_family_name AS family,
                     SUM(CASE WHEN queryable=1 THEN 1 ELSE 0 END) AS queryable_count,
                     SUM(CASE WHEN quality_tier=1 THEN 1 ELSE 0 END) AS tier1_count,
                     SUM(CASE WHEN quality_tier=2 THEN 1 ELSE 0 END) AS tier2_count
              FROM extract_molecules
              GROUP BY reaction_family_name
            )
            SELECT manual.family AS family,
                   manual.page_count AS page_count,
                   COALESCE(queryable.queryable_count,0) AS queryable_count,
                   COALESCE(queryable.tier1_count,0) AS tier1_count,
                   COALESCE(queryable.tier2_count,0) AS tier2_count
            FROM manual LEFT JOIN queryable ON manual.family=queryable.family
            ORDER BY manual.page_count DESC, manual.family ASC
            """,
        )
        covered = [dict(r) for r in manual_families if r["queryable_count"] > 0]
        uncovered = [dict(r) for r in manual_families if r["queryable_count"] == 0]

        reaction_cards = fetch_one_dict(
            conn,
            """
            SELECT COUNT(*) AS total,
                   SUM(CASE WHEN substrate_smiles IS NOT NULL AND trim(substrate_smiles)<>'' THEN 1 ELSE 0 END) AS substrate_nonempty,
                   SUM(CASE WHEN product_smiles IS NOT NULL AND trim(product_smiles)<>'' THEN 1 ELSE 0 END) AS product_nonempty,
                   SUM(CASE WHEN (substrate_smiles IS NOT NULL AND trim(substrate_smiles)<>'') OR (product_smiles IS NOT NULL AND trim(product_smiles)<>'') THEN 1 ELSE 0 END) AS any_nonempty
            FROM reaction_cards
            """,
        )

        structure_source_counts = [
            list(r)
            for r in conn.execute(
                "SELECT COALESCE(structure_source,'NULL'), COUNT(*) FROM extract_molecules GROUP BY COALESCE(structure_source,'NULL') ORDER BY 2 DESC"
            ).fetchall()
        ]

        current_state = fetch_one_dict(
            conn,
            """
            SELECT
              (SELECT COUNT(*) FROM manual_page_knowledge) AS manual_page_knowledge,
              (SELECT COUNT(*) FROM manual_page_entities) AS manual_page_entities,
              (SELECT COUNT(*) FROM reaction_extracts) AS reaction_extracts,
              (SELECT COUNT(*) FROM extract_molecules) AS extract_molecules_total,
              (SELECT COUNT(*) FROM extract_molecules WHERE queryable=1) AS queryable,
              (SELECT COUNT(*) FROM extract_molecules WHERE quality_tier=1) AS tier1,
              (SELECT COUNT(*) FROM extract_molecules WHERE quality_tier=2) AS tier2,
              (SELECT COUNT(*) FROM extract_molecules WHERE quality_tier=3) AS tier3
            """,
        )
    finally:
        conn.close()

    bridge_status = {
        "exists": BRIDGE_WORK_DB.exists(),
        "should_use_as_baseline": False,
        "reason": "app/labint.db is the canonical baseline. Any bridge/work DB is disposable.",
    }
    if BRIDGE_WORK_DB.exists():
        conn2 = sqlite3.connect(str(BRIDGE_WORK_DB))
        try:
            bridge_status["queryable"] = conn2.execute(
                "SELECT COUNT(*) FROM extract_molecules WHERE queryable=1"
            ).fetchone()[0]
        finally:
            conn2.close()

    benchmark_path = BACKEND_DIR / "benchmark" / "named_reaction_benchmark_small_results.json"
    benchmark_summary = None
    if benchmark_path.exists():
        benchmark_summary = json.load(open(benchmark_path, encoding="utf-8")).get("summary")

    return {
        "canonical_db": str(CANONICAL_DB),
        "current_state": current_state,
        "structure_source_counts": structure_source_counts,
        "manual_family_total": len(manual_families),
        "manual_family_covered": len(covered),
        "manual_family_uncovered": len(uncovered),
        "top_uncovered_families": uncovered[:30],
        "top_covered_families": sorted(
            covered, key=lambda d: (-d["queryable_count"], -d["page_count"], d["family"])
        )[:30],
        "reaction_cards": reaction_cards,
        "benchmark_summary": benchmark_summary,
        "bridge_work_status": bridge_status,
        "recommended_next_order": [
            "1) Treat app/labint.db as the only canonical baseline",
            "2) Expand queryable family coverage beyond the current level with structure-bearing pages first",
            "3) Expand benchmark from small to medium after each dataization batch",
            "4) Only then consider reaction_cards direct SMILES backfill with a reviewed mapping strategy",
        ],
    }


def write_md(report: Dict) -> str:
    lines: List[str] = []
    lines.append("# CHEMLENS Canonical DB Gap Report\n\n")
    lines.append(f"- Canonical DB: `{report['canonical_db']}`\n")
    cs = report["current_state"]
    lines.append(
        f"- queryable / tier1 / tier2 / tier3: **{cs['queryable']} / {cs['tier1']} / {cs['tier2']} / {cs['tier3']}**\n"
    )
    lines.append(
        f"- Manual families covered by queryable evidence: **{report['manual_family_covered']} / {report['manual_family_total']}**\n"
    )
    rc = report["reaction_cards"]
    lines.append(
        f"- reaction_cards with any direct SMILES: **{rc['any_nonempty']} / {rc['total']}**\n"
    )
    if report.get("benchmark_summary"):
        bs = report["benchmark_summary"]
        lines.append(
            f"- Benchmark top1/top3: **{bs['top1_correct']}/{bs['reaction_cases']}**, **{bs['top3_correct']}/{bs['reaction_cases']}**\n"
        )
    lines.append("\n## Highest-priority uncovered families\n\n")
    for row in report["top_uncovered_families"][:20]:
        lines.append(f"- {row['family']} — pages {row['page_count']}\n")
    lines.append("\n## Strongly-covered families\n\n")
    for row in report["top_covered_families"][:20]:
        lines.append(
            f"- {row['family']} — queryable {row['queryable_count']} (tier1 {row['tier1_count']}, tier2 {row['tier2_count']})\n"
        )
    lines.append("\n## One-source-of-truth policy\n\n")
    lines.append("- `app/labint.db` is the only canonical baseline.\n")
    if report["bridge_work_status"]["exists"]:
        lines.append(
            f"- `app/labint_round9_bridge_work.db` exists, but must remain disposable (queryable={report['bridge_work_status'].get('queryable', '?')}).\n"
        )
    else:
        lines.append("- No legacy bridge work DB was found.\n")
    lines.append("\n## Recommended next order\n\n")
    for item in report["recommended_next_order"]:
        lines.append(f"- {item}\n")
    return "".join(lines)


def main() -> None:
    report = build_report()
    REPORT_JSON.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    REPORT_MD.write_text(write_md(report), encoding="utf-8")
    BRIDGE_JSON.write_text(json.dumps(report["bridge_work_status"], ensure_ascii=False, indent=2), encoding="utf-8")
    print(
        json.dumps(
            {
                "report_json": str(REPORT_JSON),
                "report_md": str(REPORT_MD),
                "bridge_work_status": report["bridge_work_status"],
                "manual_family_covered": report["manual_family_covered"],
                "manual_family_total": report["manual_family_total"],
                "reaction_cards_with_any_smiles": report["reaction_cards"]["any_nonempty"],
                "reaction_cards_total": report["reaction_cards"]["total"],
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
