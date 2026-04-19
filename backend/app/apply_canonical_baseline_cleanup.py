#!/usr/bin/env python3
import argparse, sqlite3, os, shutil, json
PATCH_LABELS_REMOVE = [
    'named_reactions_frontmatter_batch1',
    'named_reactions_frontmatter_batch2',
    'named_reactions_frontmatter_batch3',
    'named_reactions_frontmatter_batch4',
    'named_reactions_frontmatter_batch59',
]

def snapshot(cur):
    out = {}
    for t in ['manual_page_knowledge','manual_page_entities','extract_molecules']:
        try:
            out[t] = cur.execute(f'SELECT COUNT(*) FROM {t}').fetchone()[0]
        except Exception as e:
            out[t] = f'ERR:{e}'
    out['mpk_distinct_pages'] = cur.execute('SELECT COUNT(DISTINCT page_no) FROM manual_page_knowledge').fetchone()[0]
    out['mpk_duplicate_pages'] = cur.execute('SELECT COUNT(*) FROM (SELECT page_no, COUNT(*) c FROM manual_page_knowledge GROUP BY page_no HAVING c>1)').fetchone()[0]
    out['batch57_58_59'] = cur.execute("SELECT source_label, COUNT(*) FROM manual_page_knowledge WHERE source_label IN ('named_reactions_frontmatter_batch57','named_reactions_frontmatter_batch58','named_reactions_frontmatter_batch59') GROUP BY source_label ORDER BY source_label").fetchall()
    out['queryable'] = cur.execute('SELECT COUNT(*) FROM extract_molecules WHERE queryable=1').fetchone()[0]
    out['tier1'] = cur.execute('SELECT COUNT(*) FROM extract_molecules WHERE queryable=1 AND quality_tier=1').fetchone()[0]
    return out

def apply_cleanup(db_path):
    con = sqlite3.connect(db_path)
    cur = con.cursor()
    before = snapshot(cur)
    ids = [r[0] for r in cur.execute(
        f"SELECT id FROM manual_page_knowledge WHERE source_label IN ({','.join('?'*len(PATCH_LABELS_REMOVE))})",
        PATCH_LABELS_REMOVE,
    ).fetchall()]
    removed_entities = 0
    if ids:
        q = ','.join('?'*len(ids))
        removed_entities = cur.execute(f'DELETE FROM manual_page_entities WHERE page_knowledge_id IN ({q})', ids).rowcount
        removed_knowledge = cur.execute(f'DELETE FROM manual_page_knowledge WHERE id IN ({q})', ids).rowcount
    else:
        removed_knowledge = 0
    con.commit()
    after = snapshot(cur)
    report = {
        'db_path': db_path,
        'removed_source_labels': PATCH_LABELS_REMOVE,
        'removed_manual_page_knowledge': removed_knowledge,
        'removed_manual_page_entities': removed_entities,
        'before': before,
        'after': after,
    }
    con.close()
    return report

if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument('--db', required=True)
    ap.add_argument('--backup', action='store_true')
    args = ap.parse_args()
    if args.backup:
        bak = args.db + '.before_canonical_cleanup.bak'
        shutil.copy2(args.db, bak)
        print(f'backup: {bak}')
    report = apply_cleanup(args.db)
    print(json.dumps(report, ensure_ascii=False, indent=2))
