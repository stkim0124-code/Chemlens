
import json
import os
import shutil
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Tuple

TARGETS = [
    {
        "long_name": "Fries-, Photo-Fries, and Anionic Ortho-Fries Rearrangement",
        "short_name": "Fries Rearrangement",
        "extra_synonyms": [
            "Photo-Fries rearrangement",
            "Anionic ortho-Fries rearrangement",
            "Ortho-Fries rearrangement",
        ],
        "notes": "Long-form ontology shell mapped to canonical short-form family.",
    },
    {
        "long_name": "Hofmann-Löffler-Freytag Reaction (Remote Functionalization)",
        "short_name": "Hofmann-Loffler-Freytag Reaction",
        "extra_synonyms": [
            "Hofmann-Loeffler-Freytag Reaction",
            "Hofmann-Löffler-Freytag Reaction",
            "HLF Reaction",
            "Remote Functionalization",
        ],
        "notes": "Long-form ontology shell mapped to canonical short-form family.",
    },
    {
        "long_name": "Houben-Hoesch Reaction/Synthesis",
        "short_name": "Houben-Hoesch Reaction",
        "extra_synonyms": [
            "Houben-Hoesch Synthesis",
            "Hoesch Reaction",
            "Reaction/Synthesis",
        ],
        "notes": "Long-form ontology shell mapped to canonical short-form family.",
    },
]

DB_CANDIDATES = [
    ("canonical", Path("app") / "labint.db"),
    ("work", Path("app") / "labint_round9_bridge_work.db"),
    ("stage", Path("app") / "labint_v5_stage.db"),
]

def normalize(text: str) -> str:
    text = text or ""
    text = text.strip().lower()
    # Preserve unicode but simplify punctuation/whitespace
    import re
    text = re.sub(r"[–—−]", "-", text)
    text = re.sub(r"[^0-9a-zA-Z가-힣\u00C0-\u024F\u1E00-\u1EFF\s-]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text

def split_synonyms(raw: str) -> List[str]:
    if not raw:
        return []
    parts = [p.strip() for p in raw.split("|")]
    return [p for p in parts if p]

def merge_synonyms(existing: str, additions: List[str]) -> str:
    items = split_synonyms(existing)
    seen = {normalize(x): x for x in items}
    for item in additions:
        n = normalize(item)
        if n and n not in seen:
            items.append(item)
            seen[n] = item
    return "|".join(items)

def ensure_alias_entry(conn: sqlite3.Connection, alias: str, canonical: str, notes: str) -> bool:
    alias_norm = normalize(alias)
    canonical_norm = normalize(canonical)
    row = conn.execute(
        "SELECT id, canonical_name, canonical_name_norm, notes FROM abbreviation_aliases WHERE alias_norm=? AND entity_type='reaction_family'",
        (alias_norm,),
    ).fetchone()
    if row:
        updates = []
        params = []
        if row[1] != canonical:
            updates.append("canonical_name=?")
            params.append(canonical)
        if row[2] != canonical_norm:
            updates.append("canonical_name_norm=?")
            params.append(canonical_norm)
        new_notes = row[3] or ""
        if notes and notes not in new_notes:
            new_notes = (new_notes + " | " + notes).strip(" |")
            updates.append("notes=?")
            params.append(new_notes)
        if updates:
            params.extend([row[0]])
            conn.execute(f"UPDATE abbreviation_aliases SET {', '.join(updates)}, updated_at=datetime('now') WHERE id=?", params)
            return True
        return False

    conn.execute(
        """
        INSERT INTO abbreviation_aliases
        (alias, alias_norm, canonical_name, canonical_name_norm, entity_type, notes, source_label, confidence)
        VALUES (?, ?, ?, ?, 'reaction_family', ?, 'alias_cleanup_patch', 0.99)
        """,
        (alias, alias_norm, canonical, canonical_norm, notes),
    )
    return True

def update_family_pattern(conn: sqlite3.Connection, long_name: str, short_name: str, extra_synonyms: List[str], notes: str) -> Dict:
    result = {
        "short_found": False,
        "long_found": False,
        "short_updated": False,
        "long_updated": False,
        "alias_rows_changed": 0,
    }
    short_row = conn.execute(
        "SELECT id, synonym_names, description_short, family_name_norm FROM reaction_family_patterns WHERE family_name=?",
        (short_name,),
    ).fetchone()
    long_row = conn.execute(
        "SELECT id, synonym_names, description_short, family_name_norm FROM reaction_family_patterns WHERE family_name=?",
        (long_name,),
    ).fetchone()

    if short_row:
        result["short_found"] = True
        new_syn = merge_synonyms(short_row[1], [long_name] + extra_synonyms)
        new_desc = short_row[2] or ""
        marker = f"Alias includes: {long_name}"
        if marker not in new_desc:
            new_desc = (new_desc + ("; " if new_desc else "") + marker).strip()
        if new_syn != (short_row[1] or "") or new_desc != (short_row[2] or ""):
            conn.execute(
                "UPDATE reaction_family_patterns SET synonym_names=?, description_short=?, updated_at=datetime('now') WHERE id=?",
                (new_syn, new_desc, short_row[0]),
            )
            result["short_updated"] = True

    if long_row:
        result["long_found"] = True
        new_syn = merge_synonyms(long_row[1], [short_name] + extra_synonyms)
        new_desc = f"Alias shell. Canonical family: {short_name}."
        if new_syn != (long_row[1] or "") or new_desc != (long_row[2] or ""):
            conn.execute(
                "UPDATE reaction_family_patterns SET synonym_names=?, description_short=?, updated_at=datetime('now') WHERE id=?",
                (new_syn, new_desc, long_row[0]),
            )
            result["long_updated"] = True

    # alias rows in both directions so lookup can normalize either way
    changed = 0
    for alias in [long_name] + extra_synonyms:
        if ensure_alias_entry(conn, alias, short_name, notes):
            changed += 1
    # also map the short form to itself if missing as a reaction_family alias, harmless but improves lookup
    if ensure_alias_entry(conn, short_name, short_name, f"Canonical reaction family self-alias. {notes}"):
        changed += 1
    result["alias_rows_changed"] = changed
    return result

def db_counts(conn: sqlite3.Connection, family: str) -> Dict[str, int]:
    return {
        "reaction_extracts": conn.execute("SELECT COUNT(*) FROM reaction_extracts WHERE reaction_family_name=?", (family,)).fetchone()[0],
        "extract_molecules": conn.execute("SELECT COUNT(*) FROM extract_molecules WHERE reaction_family_name=?", (family,)).fetchone()[0],
    }

def create_report_dir(base_dir: Path) -> Path:
    base_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_dir = base_dir / stamp
    idx = 1
    while report_dir.exists():
        report_dir = base_dir / f"{stamp}_{idx:02d}"
        idx += 1
    report_dir.mkdir(parents=True, exist_ok=True)
    return report_dir

def main() -> int:
    backend_root = Path(__file__).resolve().parent
    report_dir = create_report_dir(backend_root / "reports" / "family_alias_cleanup")
    summary = {
        "backend_root": str(backend_root),
        "report_dir": str(report_dir),
        "db_results": [],
        "targets": TARGETS,
        "timestamp": datetime.now().isoformat(timespec="seconds"),
    }

    print("=" * 72)
    print("FAMILY ALIAS / ONTOLOGY CLEANUP")
    print("=" * 72)
    print(f"backend_root: {backend_root}")
    print(f"report_dir:    {report_dir}")

    for label, relpath in DB_CANDIDATES:
        db_path = backend_root / relpath
        if not db_path.exists():
            print(f"[SKIP] {label}: {db_path} (missing)")
            summary["db_results"].append({"label": label, "db_path": str(db_path), "status": "missing"})
            continue

        backup_path = report_dir / f"{label}_{db_path.name}.backup"
        shutil.copy2(db_path, backup_path)

        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        try:
            per_target = []
            for tgt in TARGETS:
                before_long = db_counts(conn, tgt["long_name"])
                before_short = db_counts(conn, tgt["short_name"])
                changes = update_family_pattern(conn, tgt["long_name"], tgt["short_name"], tgt["extra_synonyms"], tgt["notes"])
                after_long = db_counts(conn, tgt["long_name"])
                after_short = db_counts(conn, tgt["short_name"])
                per_target.append({
                    "long_name": tgt["long_name"],
                    "short_name": tgt["short_name"],
                    "before": {"long": before_long, "short": before_short},
                    "after": {"long": after_long, "short": after_short},
                    "changes": changes,
                })
            conn.commit()
            summary["db_results"].append({
                "label": label,
                "db_path": str(db_path),
                "status": "updated",
                "backup_path": str(backup_path),
                "results": per_target,
            })
            print(f"[OK] {label}: {db_path}")
            for item in per_target:
                print(f"  - {item['long_name']} -> {item['short_name']} | short_updated={item['changes']['short_updated']} long_updated={item['changes']['long_updated']} alias_rows_changed={item['changes']['alias_rows_changed']}")
        finally:
            conn.close()

    json_path = report_dir / "family_alias_cleanup_summary.json"
    md_path = report_dir / "family_alias_cleanup_summary.md"
    json_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    md_lines = [
        "# Family Alias / Ontology Cleanup Summary",
        "",
        f"- Generated at: {summary['timestamp']}",
        f"- Backend root: `{summary['backend_root']}`",
        "",
    ]
    for db in summary["db_results"]:
        md_lines.append(f"## {db['label']}")
        md_lines.append("")
        md_lines.append(f"- DB: `{db['db_path']}`")
        md_lines.append(f"- Status: **{db['status']}**")
        if db.get("backup_path"):
            md_lines.append(f"- Backup: `{db['backup_path']}`")
        md_lines.append("")
        for item in db.get("results", []):
            md_lines.append(f"### {item['long_name']} -> {item['short_name']}")
            md_lines.append("")
            md_lines.append(f"- short_updated: `{item['changes']['short_updated']}`")
            md_lines.append(f"- long_updated: `{item['changes']['long_updated']}`")
            md_lines.append(f"- alias_rows_changed: `{item['changes']['alias_rows_changed']}`")
            md_lines.append(f"- long extracts/molecules: `{item['after']['long']['reaction_extracts']}` / `{item['after']['long']['extract_molecules']}`")
            md_lines.append(f"- short extracts/molecules: `{item['after']['short']['reaction_extracts']}` / `{item['after']['short']['extract_molecules']}`")
            md_lines.append("")
    md_path.write_text("\n".join(md_lines), encoding="utf-8")

    print(f"summary json: {json_path}")
    print(f"summary md:   {md_path}")
    print("=" * 72)
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
