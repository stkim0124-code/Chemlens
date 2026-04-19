
import json, sqlite3, shutil, hashlib
from pathlib import Path
from datetime import datetime

ROOT = Path(__file__).resolve().parent
APP = ROOT / "app"
DBS = [APP / "labint.db", APP / "labint_round9_bridge_work.db"]

KNOW_PATH = ROOT / "batch59_page_knowledge_502_553.json"
ENT_PATH = ROOT / "batch59_page_entities_502_553.json"
REPORT_PATH = ROOT / "BASELINE_SEAL_AFTER_BATCH59_RESTORE.json"

def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open('rb') as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b''):
            h.update(chunk)
    return h.hexdigest()

def load_json(path):
    with path.open('r', encoding='utf-8') as f:
        return json.load(f)

def maybe_backup(db: Path) -> Path:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup = db.with_name(f"{db.stem}.backup_before_batch59_restore_{ts}{db.suffix}")
    shutil.copy2(db, backup)
    return backup

def ensure_rows(db: Path, knowledge_rows, entity_rows):
    conn = sqlite3.connect(str(db))
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    existing = cur.execute(
        "SELECT COUNT(*) FROM manual_page_knowledge WHERE page_no BETWEEN 502 AND 553"
    ).fetchone()[0]

    result = {
        "db": str(db),
        "backup": None,
        "before": {},
        "after": {},
        "inserted_knowledge": 0,
        "inserted_entities": 0,
        "skipped": False,
    }

    result["before"]["manual_page_knowledge"] = cur.execute("SELECT COUNT(*) FROM manual_page_knowledge").fetchone()[0]
    result["before"]["manual_page_entities"] = cur.execute("SELECT COUNT(*) FROM manual_page_entities").fetchone()[0]
    result["before"]["p502_553_records"] = existing

    if existing > 0:
        result["skipped"] = True
    else:
        backup = maybe_backup(db)
        result["backup"] = str(backup)

        old_to_new = {}
        for row in knowledge_rows:
            cols = [
                "source_label", "page_label", "page_no", "title", "section_name", "page_kind",
                "summary", "family_names", "reference_family_name", "notes", "image_filename",
                "created_at", "updated_at"
            ]
            vals = [row.get(c) for c in cols]
            cur.execute(
                f"INSERT INTO manual_page_knowledge ({','.join(cols)}) VALUES ({','.join(['?']*len(cols))})",
                vals
            )
            old_to_new[row["id"]] = cur.lastrowid
            result["inserted_knowledge"] += 1

        for row in entity_rows:
            cols = [
                "page_knowledge_id", "entity_text", "entity_text_norm", "canonical_name",
                "entity_type", "alias_id", "family_name", "notes", "confidence",
                "created_at", "updated_at"
            ]
            vals = [
                old_to_new[row["page_knowledge_id"]],
                row.get("entity_text"),
                row.get("entity_text_norm"),
                row.get("canonical_name"),
                row.get("entity_type"),
                row.get("alias_id"),
                row.get("family_name"),
                row.get("notes"),
                row.get("confidence"),
                row.get("created_at"),
                row.get("updated_at"),
            ]
            cur.execute(
                f"INSERT INTO manual_page_entities ({','.join(cols)}) VALUES ({','.join(['?']*len(cols))})",
                vals
            )
            result["inserted_entities"] += 1

        conn.commit()

    result["after"]["manual_page_knowledge"] = cur.execute("SELECT COUNT(*) FROM manual_page_knowledge").fetchone()[0]
    result["after"]["manual_page_entities"] = cur.execute("SELECT COUNT(*) FROM manual_page_entities").fetchone()[0]
    result["after"]["p502_553_records"] = cur.execute(
        "SELECT COUNT(*) FROM manual_page_knowledge WHERE page_no BETWEEN 502 AND 553"
    ).fetchone()[0]
    result["after"]["distinct_pages"] = cur.execute("SELECT COUNT(DISTINCT page_no) FROM manual_page_knowledge").fetchone()[0]
    result["after"]["page_range"] = list(cur.execute("SELECT MIN(page_no), MAX(page_no) FROM manual_page_knowledge").fetchone())
    result["after"]["queryable"] = cur.execute("SELECT COUNT(*) FROM extract_molecules WHERE queryable=1").fetchone()[0]
    result["after"]["tier1"] = cur.execute("SELECT COUNT(*) FROM extract_molecules WHERE queryable=1 AND quality_tier=1").fetchone()[0]
    result["after"]["tier2"] = cur.execute("SELECT COUNT(*) FROM extract_molecules WHERE queryable=1 AND quality_tier=2").fetchone()[0]
    result["after"]["tier3"] = cur.execute("SELECT COUNT(*) FROM extract_molecules WHERE quality_tier=3").fetchone()[0]
    result["after"]["queryable_family_coverage"] = cur.execute(
        "SELECT COUNT(DISTINCT reaction_family_name) FROM extract_molecules WHERE queryable=1"
    ).fetchone()[0]
    result["after"]["structure_source_counts"] = [list(r) for r in cur.execute(
        "SELECT COALESCE(structure_source,'NULL'), COUNT(*) FROM extract_molecules GROUP BY COALESCE(structure_source,'NULL') ORDER BY 2 DESC"
    ).fetchall()]
    conn.close()
    return result

def load_benchmark():
    p = ROOT / "benchmark" / "named_reaction_benchmark_small_results.json"
    if not p.exists():
        return None
    with p.open('r', encoding='utf-8') as f:
        data = json.load(f)
    summary = {
        "path": str(p),
        "sha256": sha256_file(p),
    }
    if isinstance(data, dict):
        for k in [
            "benchmark_name", "total_cases", "reaction_cases", "top1_correct",
            "top1_accuracy", "top3_correct", "top3_accuracy",
            "disallow_top3_violations", "disallow_violation_rate", "unique_families"
        ]:
            if k in data:
                summary[k] = data[k]
        if "rows" in data:
            summary["bad_top1_cases"] = [
                {
                    "case_id": r.get("case_id"),
                    "top1_family": r.get("top1_family"),
                    "top3_families": r.get("top3_families"),
                    "top3_scores": r.get("top3_scores"),
                }
                for r in data["rows"]
                if (not r.get("adversarial_type")) and (not r.get("top1_correct", True))
            ]
    return summary

def main():
    knowledge_rows = load_json(KNOW_PATH)
    entity_rows = load_json(ENT_PATH)
    report = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "knowledge_rows_in_patch": len(knowledge_rows),
        "entity_rows_in_patch": len(entity_rows),
        "db_results": [],
        "benchmark": load_benchmark(),
        "evidence_search_sha256": sha256_file(APP / "evidence_search.py") if (APP / "evidence_search.py").exists() else None,
    }

    for db in DBS:
        if db.exists():
            report["db_results"].append(ensure_rows(db, knowledge_rows, entity_rows))
        else:
            report["db_results"].append({"db": str(db), "missing": True})

    with REPORT_PATH.open("w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    print(json.dumps(report, ensure_ascii=False, indent=2))

if __name__ == "__main__":
    main()
