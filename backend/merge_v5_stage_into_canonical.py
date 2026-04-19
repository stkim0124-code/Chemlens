from __future__ import annotations

import argparse
import shutil
import sqlite3
import sys
from datetime import datetime
from pathlib import Path
from typing import Iterable

ALLOWED_STRUCTURE_SOURCES = (
    'gemini_auto_seed',
    'deterministic_gemini_seed',
    'deterministic_seed_from_existing',
)


def connect(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    return conn


def table_columns(conn: sqlite3.Connection, table: str) -> list[str]:
    return [r[1] for r in conn.execute(f"PRAGMA table_info({table})")]


def get_state(conn: sqlite3.Connection) -> dict:
    cur = conn.cursor()
    return {
        'queryable': cur.execute("SELECT COUNT(*) FROM extract_molecules WHERE queryable=1").fetchone()[0],
        'tier1': cur.execute("SELECT COUNT(*) FROM extract_molecules WHERE queryable=1 AND quality_tier=1").fetchone()[0],
        'tier2': cur.execute("SELECT COUNT(*) FROM extract_molecules WHERE queryable=1 AND quality_tier=2").fetchone()[0],
        'family_coverage': cur.execute("SELECT COUNT(DISTINCT reaction_family_name) FROM extract_molecules WHERE queryable=1").fetchone()[0],
        'reaction_extracts': cur.execute("SELECT COUNT(*) FROM reaction_extracts").fetchone()[0],
        'extract_molecules_total': cur.execute("SELECT COUNT(*) FROM extract_molecules").fetchone()[0],
        'reaction_family_patterns': cur.execute("SELECT COUNT(*) FROM reaction_family_patterns").fetchone()[0],
        'gemini_like_rows': cur.execute(
            "SELECT COUNT(*) FROM extract_molecules WHERE structure_source IN ({})".format(','.join('?'*len(ALLOWED_STRUCTURE_SOURCES))),
            ALLOWED_STRUCTURE_SOURCES,
        ).fetchone()[0],
    }


def print_state(label: str, state: dict) -> None:
    print(f"[{label}]")
    for k, v in state.items():
        print(f"  {k:28s} = {v}")


def print_diff(before: dict, after: dict) -> None:
    print('[DIFF]')
    for k in before:
        delta = after[k] - before[k]
        sign = '+' if delta > 0 else ''
        print(f"  {k:28s} = {before[k]} -> {after[k]} ({sign}{delta})")


def fetch_new_gemini_molecule_ids(canonical: sqlite3.Connection, source: sqlite3.Connection) -> list[int]:
    placeholders = ','.join('?'*len(ALLOWED_STRUCTURE_SOURCES))
    source_ids = [
        r[0] for r in source.execute(
            f"SELECT id FROM extract_molecules WHERE structure_source IN ({placeholders}) ORDER BY id",
            ALLOWED_STRUCTURE_SOURCES,
        )
    ]
    canonical_ids = {r[0] for r in canonical.execute("SELECT id FROM extract_molecules")}
    return [mid for mid in source_ids if mid not in canonical_ids]


def fetch_related_extract_ids(source: sqlite3.Connection, molecule_ids: list[int]) -> list[int]:
    if not molecule_ids:
        return []
    placeholders = ','.join('?'*len(molecule_ids))
    return sorted({
        r[0] for r in source.execute(
            f"SELECT DISTINCT extract_id FROM extract_molecules WHERE id IN ({placeholders})",
            molecule_ids,
        )
    })


def get_new_pattern_ids(canonical: sqlite3.Connection, source: sqlite3.Connection) -> list[int]:
    c_ids = {r[0] for r in canonical.execute("SELECT id FROM reaction_family_patterns")}
    s_ids = {r[0] for r in source.execute("SELECT id FROM reaction_family_patterns")}
    return sorted(s_ids - c_ids)


def check_id_conflicts(canonical: sqlite3.Connection, table: str, ids: Iterable[int]) -> list[int]:
    ids = list(ids)
    if not ids:
        return []
    conflicts: list[int] = []
    for i in range(0, len(ids), 500):
        batch = ids[i:i+500]
        placeholders = ','.join('?'*len(batch))
        rows = canonical.execute(f"SELECT id FROM {table} WHERE id IN ({placeholders})", batch).fetchall()
        conflicts.extend(r[0] for r in rows)
    return conflicts


def copy_rows(canonical: sqlite3.Connection, source: sqlite3.Connection, table: str, ids: list[int]) -> int:
    if not ids:
        return 0
    c_cols = table_columns(canonical, table)
    s_cols = table_columns(source, table)
    cols = [c for c in c_cols if c in s_cols]
    col_list = ','.join(cols)
    placeholders = ','.join('?'*len(cols))
    inserted = 0
    for i in range(0, len(ids), 500):
        batch = ids[i:i+500]
        ph = ','.join('?'*len(batch))
        rows = source.execute(f"SELECT {col_list} FROM {table} WHERE id IN ({ph})", batch).fetchall()
        canonical.executemany(f"INSERT INTO {table} ({col_list}) VALUES ({placeholders})", rows)
        inserted += len(rows)
    return inserted


def new_queryable_families(canonical: sqlite3.Connection, source: sqlite3.Connection, new_mol_ids: list[int]) -> list[str]:
    if not new_mol_ids:
        return []
    c_fams = {
        r[0] for r in canonical.execute(
            "SELECT DISTINCT reaction_family_name FROM extract_molecules WHERE queryable=1"
        )
    }
    ph = ','.join('?'*len(new_mol_ids))
    s_fams = {
        r[0] for r in source.execute(
            f"SELECT DISTINCT reaction_family_name FROM extract_molecules WHERE queryable=1 AND id IN ({ph})",
            new_mol_ids,
        )
    }
    return sorted(s_fams - c_fams)


def main() -> int:
    ap = argparse.ArgumentParser(description='Safely merge successful v5 stage DB results into canonical labint.db')
    ap.add_argument('--canonical', default='app/labint.db')
    ap.add_argument('--source', default='app/labint_v5_stage.db')
    ap.add_argument('--dry-run', action='store_true')
    ap.add_argument('--apply', action='store_true')
    ap.add_argument('--no-backup', action='store_true')
    args = ap.parse_args()

    canonical_path = Path(args.canonical).resolve()
    source_path = Path(args.source).resolve()
    if not canonical_path.exists():
        print(f'[ERROR] canonical not found: {canonical_path}')
        return 1
    if not source_path.exists():
        print(f'[ERROR] source not found: {source_path}')
        return 1
    if canonical_path == source_path:
        print('[ERROR] canonical and source paths are identical')
        return 1

    is_dry_run = not args.apply
    print('='*72)
    print('MERGE V5 STAGE INTO CANONICAL')
    print('='*72)
    print(f'canonical: {canonical_path}')
    print(f'source:    {source_path}')
    print(f'mode:      {"DRY-RUN" if is_dry_run else "APPLY"}')
    print(f'allowed structure_source: {", ".join(ALLOWED_STRUCTURE_SOURCES)}')
    print()

    backup_path = None
    if args.apply and not args.no_backup:
        ts = datetime.now().strftime('%Y%m%d_%H%M%S')
        backup_path = canonical_path.parent / f'{canonical_path.stem}.backup_before_v5_merge_{ts}.db'
        print('[STEP 1] backup canonical')
        shutil.copy2(canonical_path, backup_path)
        print(f'  backup: {backup_path.name} ({backup_path.stat().st_size:,} bytes)')
        print()
    else:
        print('[STEP 1] backup skipped')
        print()

    canonical = connect(canonical_path)
    source = connect(source_path)
    try:
        before = get_state(canonical)
        print('[STEP 2] identify merge targets')
        new_mol_ids = fetch_new_gemini_molecule_ids(canonical, source)
        new_re_ids = fetch_related_extract_ids(source, new_mol_ids)
        new_fp_ids = get_new_pattern_ids(canonical, source)
        print(f'  new extract_molecules:      {len(new_mol_ids)}')
        print(f'  related reaction_extracts:  {len(new_re_ids)}')
        print(f'  new family_patterns:        {len(new_fp_ids)}')
        print()

        print('[STEP 3] integrity checks')
        conflicts_mol = check_id_conflicts(canonical, 'extract_molecules', new_mol_ids)
        conflicts_re = check_id_conflicts(canonical, 'reaction_extracts', new_re_ids)
        conflicts_fp = check_id_conflicts(canonical, 'reaction_family_patterns', new_fp_ids)
        print(f'  conflicts extract_molecules:     {len(conflicts_mol)}')
        print(f'  conflicts reaction_extracts:     {len(conflicts_re)}')
        print(f'  conflicts reaction_family_patterns: {len(conflicts_fp)}')
        if conflicts_mol or conflicts_re or conflicts_fp:
            print('[ERROR] id conflicts found; aborting')
            return 2
        if new_mol_ids:
            ph = ','.join('?'*len(new_mol_ids))
            fks = {
                r[0] for r in source.execute(
                    f"SELECT DISTINCT extract_id FROM extract_molecules WHERE id IN ({ph})",
                    new_mol_ids,
                )
            }
            existing_re = {r[0] for r in canonical.execute('SELECT id FROM reaction_extracts')}
            orphans = fks - (existing_re | set(new_re_ids))
            print(f'  orphan FK extract_ids: {len(orphans)}')
            if orphans:
                print(f'[ERROR] orphan extract_ids: {sorted(list(orphans))[:10]}')
                return 3
        print('  OK')
        print()

        print('[STEP 4] canonical state before merge')
        print_state('BEFORE', before)
        print()

        fams = new_queryable_families(canonical, source, new_mol_ids)
        print(f'[STEP 5] newly queryable families if merged: {len(fams)}')
        for fam in fams[:25]:
            print(f'  + {fam}')
        if len(fams) > 25:
            print(f'  ... ({len(fams)-25} more)')
        print()

        if is_dry_run:
            after = before.copy()
            after['extract_molecules_total'] += len(new_mol_ids)
            after['reaction_extracts'] += len(new_re_ids)
            after['reaction_family_patterns'] += len(new_fp_ids)
            # precise deltas from source
            if new_mol_ids:
                ph = ','.join('?'*len(new_mol_ids))
                q = source.execute(
                    f'''SELECT 
                         SUM(CASE WHEN queryable=1 THEN 1 ELSE 0 END),
                         SUM(CASE WHEN queryable=1 AND quality_tier=1 THEN 1 ELSE 0 END),
                         SUM(CASE WHEN queryable=1 AND quality_tier=2 THEN 1 ELSE 0 END)
                         FROM extract_molecules WHERE id IN ({ph})''',
                    new_mol_ids,
                ).fetchone()
                after['queryable'] += q[0] or 0
                after['tier1'] += q[1] or 0
                after['tier2'] += q[2] or 0
                after['family_coverage'] += len(fams)
                after['gemini_like_rows'] += len(new_mol_ids)
            print('[STEP 6] DRY-RUN projected state')
            print_state('PROJECTED AFTER', after)
            print()
            print_diff(before, after)
            print()
            print('='*72)
            print('[DONE] dry-run only; use --apply to merge')
            print('='*72)
            return 0

        print('[STEP 6] apply merge')
        canonical.execute('BEGIN')
        try:
            n_fp = copy_rows(canonical, source, 'reaction_family_patterns', new_fp_ids)
            n_re = copy_rows(canonical, source, 'reaction_extracts', new_re_ids)
            n_mol = copy_rows(canonical, source, 'extract_molecules', new_mol_ids)
            canonical.commit()
            print(f'  inserted family_patterns:   {n_fp}')
            print(f'  inserted reaction_extracts: {n_re}')
            print(f'  inserted extract_molecules: {n_mol}')
        except Exception as e:
            canonical.rollback()
            print(f'[ERROR] merge failed, rollback: {e}')
            return 4
        print()

        after = get_state(canonical)
        print('[STEP 7] canonical state after merge')
        print_state('AFTER', after)
        print()
        print_diff(before, after)
        print()
        print('='*72)
        print('[DONE] merge successful')
        if backup_path:
            print(f'backup: {backup_path}')
        print('='*72)
        return 0
    finally:
        canonical.close()
        source.close()


if __name__ == '__main__':
    raise SystemExit(main())
