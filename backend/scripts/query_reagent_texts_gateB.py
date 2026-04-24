"""Query labint.db for reagent_text of the 8 Category A (reagent-channel) cases.

Reads the 8 case_ids from reports/phase4/gateB/deferred_reagent_channel.json
and prints one JSON line per case with record_id, reaction_name, reagent text
from whatever column(s) exist on the relevant table.
"""
from __future__ import annotations
import json
import sqlite3
import sys
from pathlib import Path


CASE_IDS = [
    "adm_swern_oxidation_1327",
    "adm_corey-kim_oxidation_843",
    "adm_pfitzner-moffatt_oxidation_1150",
    "adm_dess-martin_oxidation_871",
    "adm_jacobsen-katsuki_epoxidation_934",
    "adm_prilezhaev_reaction_1171",
    "adm_wittig_reaction_-_schlosser_modification_1381",
    "adm_bamford-stevens-shapiro_olefination_1531",
]


def case_id_to_record_id(case_id: str) -> int:
    # convention: adm_<family_slug>_<record_id>
    return int(case_id.rsplit("_", 1)[-1])


def find_reagent_columns(conn: sqlite3.Connection) -> dict:
    """Return {table: [columns-that-look-like-reagent]} across main tables."""
    cur = conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
    tables = [r[0] for r in cur.fetchall()]
    hits = {}
    for t in tables:
        cur.execute(f"PRAGMA table_info({t})")
        cols = [r[1] for r in cur.fetchall()]
        reagent_cols = [c for c in cols if "reagent" in c.lower() or "condition" in c.lower() or "note" in c.lower()]
        if reagent_cols:
            hits[t] = {"all_cols": cols, "reagent_cols": reagent_cols}
    return hits


def main() -> int:
    # Main DB lives at backend/app/labint.db (backend/labint.db is a stale 0-byte stub)
    db = Path("app/labint.db").resolve()
    print(f"# DB: {db} size={db.stat().st_size}", file=sys.stderr)
    conn = sqlite3.connect(str(db))
    cur = conn.cursor()

    hits = find_reagent_columns(conn)
    print("# reagent-like columns:", file=sys.stderr)
    for t, info in hits.items():
        print(f"#   {t}: {info['reagent_cols']}  (all: {info['all_cols']})", file=sys.stderr)

    record_ids = [case_id_to_record_id(c) for c in CASE_IDS]
    q_marks = ",".join(["?"] * len(record_ids))

    # Query A: reaction_cards via reactions.staging_card_id chain
    print("\n### reaction_cards via reactions.staging_card_id", file=sys.stderr)
    cur.execute(
        f"""
        SELECT r.id AS reaction_id,
               r.title,
               r.source_summary,
               rc.reagents,
               rc.solvent,
               rc.conditions,
               rc.notes,
               rc.yield_pct
        FROM reactions r
        LEFT JOIN reaction_cards rc ON rc.id = r.staging_card_id
        WHERE r.id IN ({q_marks})
        ORDER BY r.id
        """,
        record_ids,
    )
    cols = [d[0] for d in cur.description]
    for row in cur.fetchall():
        print(json.dumps(dict(zip(cols, row)), ensure_ascii=False, default=str))

    # Query B: reaction_conditions directly on reaction_id
    print("\n### reaction_conditions on reaction_id", file=sys.stderr)
    cur.execute(
        f"""
        SELECT reaction_id, reagents, solvent, conditions, yield_pct, notes
        FROM reaction_conditions
        WHERE reaction_id IN ({q_marks})
        ORDER BY reaction_id
        """,
        record_ids,
    )
    cols = [d[0] for d in cur.description]
    for row in cur.fetchall():
        print(json.dumps(dict(zip(cols, row)), ensure_ascii=False, default=str))

    # Query C: reaction_extracts via scheme_candidate_id and reactions relation
    # reactions.staging_card_id may be a scheme_candidate_id if that's the linking convention
    print("\n### reaction_extracts via staging_card_id as scheme_candidate_id", file=sys.stderr)
    cur.execute(
        f"""
        SELECT re.scheme_candidate_id,
               re.reaction_family_name,
               re.reactants_text,
               re.products_text,
               re.reagents_text,
               re.catalysts_text,
               re.solvents_text,
               re.conditions_text,
               re.notes_text
        FROM reaction_extracts re
        WHERE re.scheme_candidate_id IN ({q_marks})
        ORDER BY re.scheme_candidate_id
        """,
        record_ids,
    )
    cols = [d[0] for d in cur.description]
    for row in cur.fetchall():
        print(json.dumps(dict(zip(cols, row)), ensure_ascii=False, default=str))

    # Query D: manual_page_entities keyed indirectly; try matching source pages
    # (low priority — pages carry free-text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
