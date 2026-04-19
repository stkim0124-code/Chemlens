import sqlite3, json, sys
from pathlib import Path

def find_root(start: Path) -> Path:
    for cand in [start, start.parent]:
        if (cand / 'app' / 'labint.db').exists():
            return cand
    return start

def scalar(cur, sql):
    row = cur.execute(sql).fetchone()
    return row[0] if row else None

def main():
    root = find_root(Path(__file__).resolve().parent)
    db = root / 'app' / 'labint.db'
    if not db.exists():
        print(f'ERROR: DB not found: {db}')
        sys.exit(1)
    conn = sqlite3.connect(str(db))
    cur = conn.cursor()
    p502 = cur.execute('select title from manual_page_knowledge where page_no=502 limit 1').fetchone()
    out = {
        'root': str(root),
        'db': str(db),
        'manual_page_knowledge': scalar(cur, 'select count(*) from manual_page_knowledge'),
        'manual_page_entities': scalar(cur, 'select count(*) from manual_page_entities'),
        'distinct_pages': scalar(cur, 'select count(distinct page_no) from manual_page_knowledge'),
        'page_range': cur.execute('select min(page_no), max(page_no) from manual_page_knowledge').fetchone(),
        'p502_553_records': scalar(cur, 'select count(*) from manual_page_knowledge where page_no between 502 and 553'),
        'p502_title': p502[0] if p502 else None,
        'reaction_family_patterns': scalar(cur, 'select count(distinct family_name) from reaction_family_patterns'),
        'abbreviation_aliases': scalar(cur, 'select count(*) from abbreviation_aliases'),
        'reaction_extracts': scalar(cur, 'select count(*) from reaction_extracts'),
        'extract_molecules_total': scalar(cur, 'select count(*) from extract_molecules'),
        'queryable': scalar(cur, 'select count(*) from extract_molecules where queryable=1'),
        'tier1': scalar(cur, 'select count(*) from extract_molecules where queryable=1 and quality_tier=1'),
        'tier2': scalar(cur, 'select count(*) from extract_molecules where queryable=1 and quality_tier=2'),
        'tier3': scalar(cur, 'select count(*) from extract_molecules where quality_tier=3'),
        'queryable_family_coverage': scalar(cur, 'select count(distinct reaction_family_name) from extract_molecules where queryable=1'),
        'structure_source_counts': cur.execute("select coalesce(structure_source,'NULL'), count(*) from extract_molecules group by 1 order by 2 desc").fetchall(),
    }
    print(json.dumps(out, ensure_ascii=False, indent=2))

if __name__ == '__main__':
    main()
