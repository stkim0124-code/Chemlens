#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import shutil
import sqlite3
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Sequence, Set

ALLOWED_STRUCTURE_SOURCES = (
    'gemini_auto_seed',
    'deterministic_gemini_seed',
    'deterministic_seed_from_existing',
)

ROLE_PRIORITY = {
    'reactant': 0,
    'substrate': 0,
    'starting_material': 0,
    'product': 1,
    'intermediate': 2,
    'reagent': 3,
    'catalyst': 4,
    'solvent': 5,
}


@dataclass
class FamilyPacket:
    family: str
    reaction_extract_id: int
    molecules: List[sqlite3.Row]


def now_stamp() -> str:
    return datetime.now().strftime('%Y%m%d_%H%M%S')


def connect(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    return conn


def table_columns(conn: sqlite3.Connection, table: str) -> List[str]:
    return [r[1] for r in conn.execute(f'PRAGMA table_info({table})')]


def get_state(conn: sqlite3.Connection) -> Dict[str, int]:
    cur = conn.cursor()
    return {
        'queryable': cur.execute("SELECT COUNT(*) FROM extract_molecules WHERE queryable=1").fetchone()[0],
        'tier1': cur.execute("SELECT COUNT(*) FROM extract_molecules WHERE queryable=1 AND quality_tier=1").fetchone()[0],
        'tier2': cur.execute("SELECT COUNT(*) FROM extract_molecules WHERE queryable=1 AND quality_tier=2").fetchone()[0],
        'family_coverage': cur.execute("SELECT COUNT(DISTINCT reaction_family_name) FROM extract_molecules WHERE queryable=1 AND reaction_family_name IS NOT NULL AND TRIM(reaction_family_name)<>''").fetchone()[0],
        'reaction_extracts': cur.execute("SELECT COUNT(*) FROM reaction_extracts").fetchone()[0],
        'extract_molecules_total': cur.execute("SELECT COUNT(*) FROM extract_molecules").fetchone()[0],
        'reaction_family_patterns': cur.execute("SELECT COUNT(*) FROM reaction_family_patterns").fetchone()[0],
        'gemini_like_rows': cur.execute(
            "SELECT COUNT(*) FROM extract_molecules WHERE structure_source IN ({})".format(','.join('?'*len(ALLOWED_STRUCTURE_SOURCES))),
            ALLOWED_STRUCTURE_SOURCES,
        ).fetchone()[0],
    }


def print_state(label: str, state: Dict[str, int]) -> None:
    print(f'[{label}]')
    for k, v in state.items():
        print(f'  {k:28s} = {v}')


def safe_name(s: str) -> str:
    out = ''.join(ch if ch.isalnum() else '_' for ch in s)
    while '__' in out:
        out = out.replace('__', '_')
    return out.strip('_')[:80] or 'item'


def safe_read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding='utf-8'))


def collect_rejected_names(obj: Any, out: Set[str]) -> None:
    if isinstance(obj, dict):
        status = str(obj.get('status', obj.get('decision', obj.get('result', '')))).strip().lower()
        if 'reject' in status:
            for k in ('family_name', 'family', 'name'):
                v = obj.get(k)
                if isinstance(v, str) and v.strip():
                    out.add(v.strip())
        for k, v in obj.items():
            lk = str(k).lower()
            if lk in {'rejected', 'still_rejected', 'rejected_families'}:
                if isinstance(v, list):
                    for item in v:
                        if isinstance(item, str) and item.strip():
                            out.add(item.strip())
                        else:
                            collect_rejected_names(item, out)
                elif isinstance(v, dict):
                    collect_rejected_names(v, out)
            else:
                collect_rejected_names(v, out)
    elif isinstance(obj, list):
        for item in obj:
            collect_rejected_names(item, out)


def find_latest_summary(root: Path) -> Path | None:
    if not root.exists():
        return None
    cands = sorted(root.glob('*/selective_merge_summary.json'))
    return cands[-1] if cands else None


def load_rejected_families(summary_path: Path) -> List[str]:
    payload = safe_read_json(summary_path)
    names: Set[str] = set()
    collect_rejected_names(payload, names)
    return sorted(names)


def run_benchmark(script_dir: Path, db_path: Path, benchmark_path: Path, out_dir: Path, label: str) -> dict:
    out_dir.mkdir(parents=True, exist_ok=True)
    json_out = out_dir / f'benchmark_{label}.json'
    csv_out = out_dir / f'benchmark_{label}.csv'
    md_out = out_dir / f'benchmark_{label}.md'
    cmd = [
        sys.executable,
        str(script_dir / 'run_named_reaction_benchmark_small.py'),
        '--db', str(db_path),
        '--benchmark', str(benchmark_path),
        '--json-out', str(json_out),
        '--csv-out', str(csv_out),
        '--report-md', str(md_out),
    ]
    env = os.environ.copy()
    env['PYTHONUTF8'] = '1'
    env['LABINT_DB_PATH'] = str(db_path)
    proc = subprocess.run(
        cmd,
        cwd=str(script_dir),
        capture_output=True,
        text=True,
        encoding='utf-8',
        errors='replace',
        env=env,
    )
    if proc.returncode != 0:
        raise RuntimeError(f'benchmark failed: rc={proc.returncode}\nSTDOUT:\n{proc.stdout}\nSTDERR:\n{proc.stderr}')
    data = json.loads(json_out.read_text(encoding='utf-8'))
    payload = data.get('summary') if isinstance(data, dict) else None
    if not isinstance(payload, dict):
        payload = data if isinstance(data, dict) else {}
    summary = {
        'db_used': str(db_path),
        'benchmark_path': str(benchmark_path),
        'top1_accuracy': float(payload.get('top1_accuracy', 0.0)),
        'top3_accuracy': float(payload.get('top3_accuracy', 0.0)),
        'disallow_top3_violations': int(payload.get('disallow_top3_violations', 0)),
        'json_out': str(json_out),
        'csv_out': str(csv_out),
        'md_out': str(md_out),
    }
    return summary


def role_sort_key(row: sqlite3.Row) -> tuple:
    role = str(row['role'] or '').strip().lower()
    return (
        ROLE_PRIORITY.get(role, 9),
        0 if int(row['queryable'] or 0) == 1 else 1,
        int(row['quality_tier'] or 999),
        int(row['id']),
    )


def get_rejected_packets(source_conn: sqlite3.Connection, family_names: Sequence[str], allowed_sources: Sequence[str]) -> List[FamilyPacket]:
    if not family_names:
        return []
    packets: List[FamilyPacket] = []
    src_place = ','.join('?' * len(allowed_sources))
    for fam in family_names:
        rows = source_conn.execute(
            f"""
            SELECT em.*, re.id AS reaction_extract_id
            FROM extract_molecules em
            JOIN reaction_extracts re ON re.id = em.extract_id
            WHERE re.reaction_family_name=?
              AND em.structure_source IN ({src_place})
            ORDER BY em.id
            """,
            [fam, *allowed_sources],
        ).fetchall()
        if not rows:
            # fallback on extract_molecules.reaction_family_name only
            rows = source_conn.execute(
                f"""
                SELECT em.*, em.extract_id AS reaction_extract_id
                FROM extract_molecules em
                WHERE em.reaction_family_name=?
                  AND em.structure_source IN ({src_place})
                ORDER BY em.id
                """,
                [fam, *allowed_sources],
            ).fetchall()
        if not rows:
            continue
        rows = sorted(rows, key=role_sort_key)
        packets.append(FamilyPacket(family=fam, reaction_extract_id=int(rows[0]['reaction_extract_id']), molecules=rows))
    return packets


def choose_subset(rows: List[sqlite3.Row], variant: str) -> List[sqlite3.Row]:
    reactants = [r for r in rows if str(r['role'] or '').lower() in {'reactant', 'substrate', 'starting_material'}]
    products = [r for r in rows if str(r['role'] or '').lower() == 'product']
    inter = [r for r in rows if str(r['role'] or '').lower() == 'intermediate']
    reag = [r for r in rows if str(r['role'] or '').lower() in {'reagent', 'catalyst'}]
    queryable = [r for r in rows if int(r['queryable'] or 0) == 1]

    variants = {
        'minimal_pair': (reactants[:1] + products[:1]) or rows[:min(2, len(rows))],
        'queryable_pair': ([r for r in queryable if str(r['role'] or '').lower() in {'reactant','substrate','starting_material'}][:1] +
                           [r for r in queryable if str(r['role'] or '').lower() == 'product'][:1]) or [],
        'core_pair_set': (reactants[:2] + products[:2]) or rows[:min(4, len(rows))],
        'core_plus_intermediate': (reactants[:2] + products[:2] + inter[:1]) or rows[:min(5, len(rows))],
        'core_plus_reagent': (reactants[:2] + products[:2] + reag[:1]) or rows[:min(5, len(rows))],
        'queryable_only': queryable or rows[:min(3, len(rows))],
        'all_original': rows,
    }
    chosen = variants.get(variant, rows)
    out: List[sqlite3.Row] = []
    seen: Set[int] = set()
    for r in chosen:
        rid = int(r['id'])
        if rid not in seen:
            out.append(r)
            seen.add(rid)
    return out


def insert_subset(canonical_conn: sqlite3.Connection, source_conn: sqlite3.Connection, packet: FamilyPacket, subset_rows: List[sqlite3.Row]) -> tuple[int, List[int], bool]:
    fam = packet.family
    existing = canonical_conn.execute(
        'SELECT id FROM reaction_extracts WHERE reaction_family_name=? ORDER BY id LIMIT 1', (fam,)
    ).fetchone()
    extract_created = False
    if existing:
        target_extract_id = int(existing['id'])
    else:
        src = source_conn.execute('SELECT * FROM reaction_extracts WHERE id=?', (packet.reaction_extract_id,)).fetchone()
        if src is None:
            raise RuntimeError(f'missing source reaction_extract for {fam}')
        cols = table_columns(canonical_conn, 'reaction_extracts')
        insert_cols = [c for c in cols if c != 'id']
        vals = [src[c] if c in src.keys() else None for c in insert_cols]
        canonical_conn.execute(
            f"INSERT INTO reaction_extracts ({','.join(insert_cols)}) VALUES ({','.join('?'*len(insert_cols))})",
            vals,
        )
        target_extract_id = int(canonical_conn.execute('SELECT last_insert_rowid()').fetchone()[0])
        extract_created = True

    canonical_cols = table_columns(canonical_conn, 'extract_molecules')
    insert_cols = [c for c in canonical_cols if c != 'id']
    existing_keys = {
        (str(r['role'] or ''), str(r['smiles'] or ''))
        for r in canonical_conn.execute('SELECT role, smiles FROM extract_molecules WHERE extract_id=?', (target_extract_id,)).fetchall()
    }
    inserted_mol_ids: List[int] = []
    for src_row in subset_rows:
        key = (str(src_row['role'] or ''), str(src_row['smiles'] or ''))
        if key in existing_keys:
            continue
        vals = []
        for c in insert_cols:
            if c == 'extract_id':
                vals.append(target_extract_id)
            else:
                vals.append(src_row[c] if c in src_row.keys() else None)
        canonical_conn.execute(
            f"INSERT INTO extract_molecules ({','.join(insert_cols)}) VALUES ({','.join('?'*len(insert_cols))})",
            vals,
        )
        inserted_mol_ids.append(int(canonical_conn.execute('SELECT last_insert_rowid()').fetchone()[0]))
    return target_extract_id, inserted_mol_ids, extract_created


def rollback_subset(canonical_conn: sqlite3.Connection, extract_id: int, mol_ids: Sequence[int], extract_created: bool) -> None:
    if mol_ids:
        canonical_conn.execute(
            f"DELETE FROM extract_molecules WHERE id IN ({','.join('?'*len(mol_ids))})",
            list(mol_ids),
        )
    remaining = canonical_conn.execute('SELECT COUNT(*) FROM extract_molecules WHERE extract_id=?', (extract_id,)).fetchone()[0]
    if extract_created and remaining == 0:
        canonical_conn.execute('DELETE FROM reaction_extracts WHERE id=?', (extract_id,))


def main() -> int:
    ap = argparse.ArgumentParser(description='Retry only the rejected families from v5 selective merge using narrower evidence subsets.')
    ap.add_argument('--db', default='app\\labint.db')
    ap.add_argument('--stage-db', default='app\\labint_v5_stage.db')
    ap.add_argument('--benchmark-file', default='benchmark\\named_reaction_benchmark_small.json')
    ap.add_argument('--selective-summary', default='')
    ap.add_argument('--selective-root', default='reports\\v5_selective_merge')
    ap.add_argument('--report-dir', default='reports\\v5_rejected_retry')
    ap.add_argument('--allowed-sources', nargs='*', default=list(ALLOWED_STRUCTURE_SOURCES))
    ap.add_argument('--dry-run', action='store_true')
    ap.add_argument('--apply', action='store_true')
    ap.add_argument('--no-backup', action='store_true')
    ap.add_argument('--allow-top1-drop', type=float, default=0.0)
    ap.add_argument('--allow-top3-drop', type=float, default=0.0)
    ap.add_argument('--allow-extra-violations', type=int, default=0)
    args = ap.parse_args()

    if args.apply and args.dry_run:
        raise SystemExit('Choose one of --apply or --dry-run')
    if not args.apply and not args.dry_run:
        args.dry_run = True

    script_dir = Path(__file__).resolve().parent
    canonical = (script_dir / args.db).resolve() if not Path(args.db).is_absolute() else Path(args.db)
    source = (script_dir / args.stage_db).resolve() if not Path(args.stage_db).is_absolute() else Path(args.stage_db)
    benchmark = (script_dir / args.benchmark_file).resolve() if not Path(args.benchmark_file).is_absolute() else Path(args.benchmark_file)
    report_root = (script_dir / args.report_dir).resolve() if not Path(args.report_dir).is_absolute() else Path(args.report_dir)
    report_dir = report_root / now_stamp()
    report_dir.mkdir(parents=True, exist_ok=True)
    if args.selective_summary:
        selective_summary = (script_dir / args.selective_summary).resolve() if not Path(args.selective_summary).is_absolute() else Path(args.selective_summary)
    else:
        root = (script_dir / args.selective_root).resolve() if not Path(args.selective_root).is_absolute() else Path(args.selective_root)
        selective_summary = find_latest_summary(root)
        if selective_summary is None:
            raise SystemExit(f'No selective merge summary found under: {root}')

    print('=' * 76)
    print('RETRY ONLY REJECTED FAMILIES FROM V5 SELECTIVE MERGE (SUBSET FIX)')
    print('=' * 76)
    print(f'canonical:         {canonical.resolve()}')
    print(f'source:            {source.resolve()}')
    print(f'benchmark:         {benchmark.resolve()}')
    print(f'selective_summary: {selective_summary.resolve()}')
    mode_label = 'APPLY' if args.apply else 'DRY-RUN'
    print(f"mode:              {mode_label}")
    print(f'report dir:        {report_dir.resolve()}')
    print()

    rejected_names = load_rejected_families(selective_summary)
    print('[STEP 1] rejected families from previous selective merge')
    print(f'  total rejected candidates: {len(rejected_names)}')
    for fam in rejected_names:
        print(f'  - {fam}')
    print()

    variants = ['minimal_pair', 'queryable_pair', 'core_pair_set', 'core_plus_intermediate', 'core_plus_reagent', 'queryable_only', 'all_original']

    with connect(canonical) as cconn, connect(source) as sconn:
        before = get_state(cconn)
        print('[STEP 2] canonical state before retry merge')
        print_state('BEFORE', before)
        print()

        baseline = run_benchmark(script_dir, canonical, benchmark, report_dir, 'baseline')
        print('[STEP 3] baseline benchmark')
        print(json.dumps(baseline, ensure_ascii=False, indent=2))
        print()
        if baseline['top1_accuracy'] == 0.0 and baseline['top3_accuracy'] == 0.0:
            raise SystemExit('Baseline benchmark parsed as 0.0/0.0; aborting.')

        packets = get_rejected_packets(sconn, rejected_names, args.allowed_sources)
        packet_map = {p.family: p for p in packets}
        print('[STEP 4] retry variants ({})'.format('apply' if args.apply else 'dry-run preview'))
        for fam in rejected_names:
            if fam not in packet_map:
                print(f'  - {fam}: NO PACKET FOUND IN STAGE')
            else:
                print(f'  - {fam}: would try {", ".join(variants)}')
        print()

        accepted: List[Dict[str, Any]] = []
        still_rejected: List[Dict[str, Any]] = []
        missing_in_stage: List[str] = []
        backup_path = None

        if args.apply:
            if not args.no_backup:
                backup_path = canonical.parent / f'{canonical.stem}.backup_before_v5_rejected_subset_retry_{now_stamp()}.db'
                print('[STEP 5] backup canonical')
                shutil.copy2(canonical, backup_path)
                print(f'  backup: {backup_path.name} ({backup_path.stat().st_size:,} bytes)')
                print()

            bench_counter = 0
            for fam in rejected_names:
                packet = packet_map.get(fam)
                if packet is None:
                    missing_in_stage.append(fam)
                    print(f'[RETRY] {fam}\n  -> SKIPPED (not found in stage)')
                    continue
                print(f'[RETRY] {fam}')
                accepted_variant = None
                last_reason = 'all variants failed'
                for variant in variants:
                    subset = choose_subset(packet.molecules, variant)
                    if not subset:
                        print(f'  - {variant}: SKIPPED (empty subset)')
                        continue
                    cconn.execute('BEGIN')
                    extract_id, mol_ids, extract_created = insert_subset(cconn, sconn, packet, subset)
                    cconn.commit()

                    bench_counter += 1
                    bench = run_benchmark(script_dir, canonical, benchmark, report_dir, f'retry_{bench_counter:03d}_{safe_name(fam)}_{variant}')
                    is_bad = False
                    if bench['top1_accuracy'] < baseline['top1_accuracy'] - args.allow_top1_drop:
                        is_bad = True
                        last_reason = f"top1 regression: baseline={baseline['top1_accuracy']:.4f} current={bench['top1_accuracy']:.4f}"
                    elif bench['top3_accuracy'] < baseline['top3_accuracy'] - args.allow_top3_drop:
                        is_bad = True
                        last_reason = f"top3 regression: baseline={baseline['top3_accuracy']:.4f} current={bench['top3_accuracy']:.4f}"
                    elif int(bench['disallow_top3_violations']) > int(baseline['disallow_top3_violations']) + args.allow_extra_violations:
                        is_bad = True
                        last_reason = f"violation regression: baseline={baseline['disallow_top3_violations']} current={bench['disallow_top3_violations']}"
                    if is_bad:
                        cconn.execute('BEGIN')
                        rollback_subset(cconn, extract_id, mol_ids, extract_created)
                        cconn.commit()
                        print(f'  - {variant}: REJECTED ({last_reason})')
                        continue

                    accepted_variant = variant
                    accepted.append({
                        'family_name': fam,
                        'accepted_variant': variant,
                        'inserted_extract_id': extract_id,
                        'inserted_molecule_count': len(mol_ids),
                        'benchmark': bench,
                    })
                    print(f"  - {variant}: ACCEPTED (top1={bench['top1_accuracy']:.4f}, top3={bench['top3_accuracy']:.4f}, violations={bench['disallow_top3_violations']})")
                    break

                if accepted_variant is None:
                    still_rejected.append({'family_name': fam, 'reason': last_reason})
            print()
        after = get_state(cconn)
        print('[STEP 6] canonical state after rejected retry')
        print_state('AFTER', after)
        print()
        print('[DIFF]')
        for k in before:
            dv = after[k] - before[k]
            print(f'  {k:28s} = {before[k]} -> {after[k]} ({dv:+d})')

    summary = {
        'mode': 'apply' if args.apply else 'dry-run',
        'selective_summary': str(selective_summary),
        'rejected_candidates_loaded': rejected_names,
        'accepted': accepted,
        'still_rejected': still_rejected,
        'missing_in_stage': missing_in_stage,
        'before': before,
        'after': after,
        'backup': str(backup_path) if backup_path else None,
    }
    summary_path = report_dir / 'rejected_retry_summary.json'
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding='utf-8')

    print('\n' + '=' * 76)
    print('[DONE] rejected-family subset retry finished')
    print(f'accepted: {len(accepted)} | still_rejected: {len(still_rejected)} | missing_in_stage: {len(missing_in_stage)}')
    print(f'summary:  {summary_path}')
    if backup_path:
        print(f'backup:   {backup_path}')
    print('=' * 76)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
