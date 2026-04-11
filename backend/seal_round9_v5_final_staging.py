import os, csv, shutil, hashlib, datetime, json
from pathlib import Path

ROOT = Path(__file__).resolve().parent
stamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
seal_dir = ROOT / 'sealed_baselines' / f'round9_v5_final_staging_{stamp}'
seal_dir.mkdir(parents=True, exist_ok=True)

candidates = [
    'labint_round9_v5.db',
    'manifest_full_v5.csv',
    'raw_json_dump_full_v5.json',
    'fail_queue_full_v5.csv',
    'manifest_retry404_v5.csv',
    'raw_json_dump_retry404_v5.json',
    'manifest_schema_backfill_retry404_v5.csv',
    'raw_json_dump_schema_backfill_retry404_v5.json',
    'manifest_retry13_remaining_v5.csv',
    'raw_json_dump_retry13_remaining_v5.json',
    'manifest_retry8_remaining_v5.csv',
    'raw_json_dump_retry8_remaining_v5.json',
    'manifest_manual_retry6_v5.csv',
    'raw_json_dump_manual_retry6_v5.json',
]

def sha256_of(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, 'rb') as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b''):
            h.update(chunk)
    return h.hexdigest()

inventory = []
for name in candidates:
    src = ROOT / name
    if src.exists() and src.is_file():
        dst = seal_dir / name
        shutil.copy2(src, dst)
        inventory.append({
            'filename': name,
            'status': 'copied',
            'size_bytes': src.stat().st_size,
            'sha256': sha256_of(src),
        })
    else:
        inventory.append({
            'filename': name,
            'status': 'missing',
            'size_bytes': '',
            'sha256': '',
        })

src_db = ROOT / 'labint_round9_v5.db'
fixed_db = ROOT / 'labint_round9_v5_final_staging.db'
if src_db.exists():
    shutil.copy2(src_db, fixed_db)
    shutil.copy2(src_db, seal_dir / 'labint_round9_v5_final_staging.db')
    fixed_db_status = 'created'
    fixed_db_sha = sha256_of(src_db)
else:
    fixed_db_status = 'missing'
    fixed_db_sha = ''

inv_path = seal_dir / 'seal_inventory.csv'
with open(inv_path, 'w', newline='', encoding='utf-8-sig') as f:
    writer = csv.DictWriter(f, fieldnames=['filename','status','size_bytes','sha256'])
    writer.writeheader()
    writer.writerows(inventory)

copied = sum(1 for x in inventory if x['status'] == 'copied')
missing = sum(1 for x in inventory if x['status'] == 'missing')
summary = (
    '# ROUND9 V5 FINAL STAGING BASELINE\n\n'
    f'- sealed_at: {stamp}\n'
    f'- backend_root: {ROOT}\n'
    f'- seal_dir: {seal_dir}\n'
    f'- copied_artifacts: {copied}\n'
    f'- missing_artifacts: {missing}\n'
    f'- fixed_db: {fixed_db_status}\n'
    f'- fixed_db_sha256: {fixed_db_sha}\n\n'
    '## Official baseline names\n'
    '- DB: `labint_round9_v5_final_staging.db`\n'
    f'- Seal folder: `sealed_baselines/round9_v5_final_staging_{stamp}/`\n'
    f'- Inventory: `sealed_baselines/round9_v5_final_staging_{stamp}/seal_inventory.csv`\n\n'
    '## Policy note\n'
    '- This baseline is **staging only**.\n'
    '- `reaction_cards` promote is still forbidden.\n'
    '- Future work should branch from this sealed baseline rather than from scattered intermediate outputs.\n'
)

(ROOT / 'SEAL_SUMMARY_round9_v5_final.md').write_text(summary, encoding='utf-8')
(seal_dir / 'SEAL_SUMMARY_round9_v5_final.md').write_text(summary, encoding='utf-8')

meta = {
    'sealed_at': stamp,
    'backend_root': str(ROOT),
    'seal_dir': str(seal_dir),
    'copied_artifacts': copied,
    'missing_artifacts': missing,
    'fixed_db': fixed_db_status,
    'fixed_db_sha256': fixed_db_sha,
    'official_db_name': 'labint_round9_v5_final_staging.db',
    'policy': {
        'staging_only': True,
        'promote_reaction_cards_forbidden': True,
    },
}
(ROOT / 'SEAL_METADATA_round9_v5_final.json').write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding='utf-8')
(seal_dir / 'SEAL_METADATA_round9_v5_final.json').write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding='utf-8')

print('=== seal_round9_v5_final_staging ===')
print(f'ROOT={ROOT}')
print(f'SEAL_DIR={seal_dir}')
print(f'copied_artifacts={copied} | missing_artifacts={missing}')
print(f'fixed_db={fixed_db_status}')
print('Summary: SEAL_SUMMARY_round9_v5_final.md')
print('Metadata: SEAL_METADATA_round9_v5_final.json')
print('Inventory:', inv_path)
