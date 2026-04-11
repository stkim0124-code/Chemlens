from __future__ import annotations

import argparse
import csv
import re
import sqlite3
from pathlib import Path
from typing import Dict, Iterable

BACKEND_DIR = Path(__file__).resolve().parent
DEFAULT_DB = BACKEND_DIR / "app" / "labint_round9_bridge_work.db"
DEFAULT_CSV = BACKEND_DIR / "reaction_family_patterns_seed.csv"


def norm_name(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", (name or "").lower()).strip()


def ensure_schema(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS reaction_family_patterns (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            family_name TEXT NOT NULL,
            family_name_norm TEXT NOT NULL,
            transformation_type TEXT,
            reactant_fg_smarts TEXT,
            product_fg_smarts TEXT,
            key_reagents_clue TEXT,
            hierarchy_parent TEXT,
            priority INTEGER NOT NULL DEFAULT 100,
            active INTEGER NOT NULL DEFAULT 1,
            notes TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(family_name)
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_reaction_family_patterns_norm ON reaction_family_patterns(family_name_norm)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_reaction_family_patterns_parent ON reaction_family_patterns(hierarchy_parent)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_reaction_family_patterns_priority ON reaction_family_patterns(priority, active)")


def load_rows(csv_path: Path) -> Iterable[Dict[str, str]]:
    with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            cleaned = {k: (v or "").strip() for k, v in row.items()}
            cleaned["family_name_norm"] = norm_name(cleaned.get("family_name", ""))
            yield cleaned


def upsert_rows(conn: sqlite3.Connection, rows: Iterable[Dict[str, str]]) -> int:
    n = 0
    for row in rows:
        conn.execute(
            """
            INSERT INTO reaction_family_patterns (
                family_name, family_name_norm, transformation_type,
                reactant_fg_smarts, product_fg_smarts, key_reagents_clue,
                hierarchy_parent, priority, active, notes, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(family_name) DO UPDATE SET
                family_name_norm=excluded.family_name_norm,
                transformation_type=excluded.transformation_type,
                reactant_fg_smarts=excluded.reactant_fg_smarts,
                product_fg_smarts=excluded.product_fg_smarts,
                key_reagents_clue=excluded.key_reagents_clue,
                hierarchy_parent=excluded.hierarchy_parent,
                priority=excluded.priority,
                active=excluded.active,
                notes=excluded.notes,
                updated_at=CURRENT_TIMESTAMP
            """,
            (
                row.get("family_name", ""),
                row.get("family_name_norm", ""),
                row.get("transformation_type", ""),
                row.get("reactant_fg_smarts", ""),
                row.get("product_fg_smarts", ""),
                row.get("key_reagents_clue", ""),
                row.get("hierarchy_parent", ""),
                int(row.get("priority") or 100),
                int(row.get("active") or 1),
                row.get("notes", ""),
            ),
        )
        n += 1
    return n


def main() -> None:
    parser = argparse.ArgumentParser(description="Install reaction_family_patterns seed into a target SQLite DB")
    parser.add_argument("--db", default=str(DEFAULT_DB))
    parser.add_argument("--csv", default=str(DEFAULT_CSV))
    args = parser.parse_args()

    db_path = Path(args.db)
    csv_path = Path(args.csv)
    if not db_path.exists():
        raise SystemExit(f"Target DB not found: {db_path}")
    if not csv_path.exists():
        raise SystemExit(f"Seed CSV not found: {csv_path}")

    conn = sqlite3.connect(str(db_path))
    try:
        ensure_schema(conn)
        n = upsert_rows(conn, load_rows(csv_path))
        conn.commit()
        total = conn.execute("SELECT COUNT(*) FROM reaction_family_patterns").fetchone()[0]
        parents = conn.execute("SELECT hierarchy_parent, COUNT(*) FROM reaction_family_patterns GROUP BY hierarchy_parent ORDER BY COUNT(*) DESC, hierarchy_parent").fetchall()
        print({"upserted": n, "reaction_family_patterns": total})
        for parent, count in parents:
            print(f"  - {parent}: {count}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
