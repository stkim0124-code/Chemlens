from __future__ import annotations

import argparse
import json
import os
import shutil
import sqlite3
import subprocess
import sys
from collections import OrderedDict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable

ALLOWED_STRUCTURE_SOURCES = (
    'gemini_auto_seed',
    'deterministic_gemini_seed',
    'deterministic_seed_from_existing',
)

PSEUDO_FAMILY_TERMS = (
    'rules', 'guidelines', 'principle', 'principles', 'classification', 'glossary', 'index'
)


@dataclass
class FamilyPacket:
    family: str
    reaction_extract_ids: list[int]
    extract_molecule_ids: list[int]
    queryable_molecules: int
    run_order: int


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
        delta = after[k] - after.get(k, 0) + 0  # keep shape; overwritten below
        delta = after[k] - before[k]
        sign = '+' if delta > 0 else ''
        print(f"  {k:28s} = {before[k]} -> {after[k]} ({sign}{delta})")


def safe_name(text: str) -> str:
    out = ''.join(ch if ch.isalnum() or ch in ('-', '_') else '_' for ch in text)
    while '__' in out:
        out = out.replace('__', '_')
    return out.strip('_')[:80] or 'family'


def is_pseudo_family(name: str) -> bool:
    lower = name.lower()
    return any(term in lower for term in PSEUDO_FAMILY_TERMS)


def load_run_order(run_items_path: Path | None) -> dict[str, int]:
    if not run_items_path or not run_items_path.exists():
        return {}
    order: OrderedDict[str, int] = OrderedDict()
    with run_items_path.open('r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                item = json.loads(line)
            except Exception:
                continue
            fam = str(item.get('family') or '').strip()
            if not fam:
                continue
            if item.get('status') != 'inserted':
                continue
            if fam not in order:
                order[fam] = len(order)
    return dict(order)


def candidate_family_packets(canonical: sqlite3.Connection, source: sqlite3.Connection, run_order: dict[str, int], include_existing: bool) -> list[FamilyPacket]:
    placeholders = ','.join('?' * len(ALLOWED_STRUCTURE_SOURCES))
    canonical_mol_ids = {r[0] for r in canonical.execute('SELECT id FROM extract_molecules')}
    canonical_queryable_families = {
        r[0] for r in canonical.execute('SELECT DISTINCT reaction_family_name FROM extract_molecules WHERE queryable=1')
    }

    rows = source.execute(
        f'''
        SELECT id, extract_id, reaction_family_name, queryable
        FROM extract_molecules
        WHERE structure_source IN ({placeholders})
        ORDER BY id
        ''',
        ALLOWED_STRUCTURE_SOURCES,
    ).fetchall()

    buckets: dict[str, dict[str, object]] = {}
    for row in rows:
        mid = int(row['id'])
        if mid in canonical_mol_ids:
            continue
        family = str(row['reaction_family_name'] or '').strip()
        if not family:
            continue
        if is_pseudo_family(family):
            continue
        if not include_existing and family in canonical_queryable_families:
            continue
        bucket = buckets.setdefault(family, {
            'mol_ids': [],
            'extract_ids': set(),
            'queryable_molecules': 0,
        })
        bucket['mol_ids'].append(mid)
        bucket['extract_ids'].add(int(row['extract_id']))
        bucket['queryable_molecules'] += int(row['queryable'] or 0)

    packets: list[FamilyPacket] = []
    for family, bucket in buckets.items():
        packets.append(FamilyPacket(
            family=family,
            reaction_extract_ids=sorted(bucket['extract_ids']),
            extract_molecule_ids=sorted(bucket['mol_ids']),
            queryable_molecules=int(bucket['queryable_molecules']),
            run_order=run_order.get(family, 10**9),
        ))

    packets.sort(key=lambda p: (p.run_order, -p.queryable_molecules, p.family.lower()))
    return packets


def copy_rows(conn_dst: sqlite3.Connection, conn_src: sqlite3.Connection, table: str, ids: Iterable[int]) -> int:
    ids = list(ids)
    if not ids:
        return 0
    dst_cols = table_columns(conn_dst, table)
    src_cols = table_columns(conn_src, table)
    cols = [c for c in dst_cols if c in src_cols]
    col_list = ','.join(cols)
    placeholders = ','.join('?' * len(cols))
    inserted = 0
    for i in range(0, len(ids), 500):
        batch = ids[i:i+500]
        ph = ','.join('?' * len(batch))
        rows = conn_src.execute(f"SELECT {col_list} FROM {table} WHERE id IN ({ph})", batch).fetchall()
        conn_dst.executemany(f"INSERT INTO {table} ({col_list}) VALUES ({placeholders})", rows)
        inserted += len(rows)
    return inserted


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
    bench = data.get('summary', data) if isinstance(data, dict) else {}
    summary = {
        'db_used': str(db_path),
        'benchmark_path': str(benchmark_path),
        'top1_accuracy': float(bench.get('top1_accuracy', 0.0)),
        'top3_accuracy': float(bench.get('top3_accuracy', 0.0)),
        'disallow_top3_violations': int(bench.get('disallow_top3_violations', 0)),
        'json_out': str(json_out),
        'csv_out': str(csv_out),
        'md_out': str(md_out),
    }
    if summary['top1_accuracy'] == 0.0 and summary['top3_accuracy'] == 0.0:
        raise RuntimeError(
            'benchmark parser sanity check failed: top1/top3 are both 0.0. '            'Check benchmark JSON structure and DB path before merging.\n'             f'JSON: {json_out}'
        )
    return summary


def regression(baseline: dict, current: dict, allow_top1_drop: float, allow_top3_drop: float, allow_extra_violations: int) -> tuple[bool, str]:
    if current['top1_accuracy'] < baseline['top1_accuracy'] - allow_top1_drop:
        return True, f"top1 regression: baseline={baseline['top1_accuracy']:.4f} current={current['top1_accuracy']:.4f}"
    if current['top3_accuracy'] < baseline['top3_accuracy'] - allow_top3_drop:
        return True, f"top3 regression: baseline={baseline['top3_accuracy']:.4f} current={current['top3_accuracy']:.4f}"
    if current['disallow_top3_violations'] > baseline['disallow_top3_violations'] + allow_extra_violations:
        return True, f"disallow_top3_violations regression: baseline={baseline['disallow_top3_violations']} current={current['disallow_top3_violations']}"
    return False, ''


def main() -> int:
    ap = argparse.ArgumentParser(description='Selectively merge v5 stage families into canonical, guarded by benchmark after each family.')
    ap.add_argument('--canonical', default='app/labint.db')
    ap.add_argument('--source', default='app/labint_v5_stage.db')
    ap.add_argument('--benchmark-script', default='run_named_reaction_benchmark_small.py')
    ap.add_argument('--benchmark', default='benchmark/named_reaction_benchmark_small.json')
    ap.add_argument('--report-dir', default='reports/v5_selective_merge')
    ap.add_argument('--run-items', default='reports/v5/20260418_142426/run_items.jsonl')
    ap.add_argument('--family-limit', type=int, default=999999)
    ap.add_argument('--start-index', type=int, default=0)
    ap.add_argument('--allow-top1-drop', type=float, default=0.0)
    ap.add_argument('--allow-top3-drop', type=float, default=0.0)
    ap.add_argument('--allow-extra-violations', type=int, default=0)
    ap.add_argument('--include-existing-families', action='store_true')
    ap.add_argument('--apply', action='store_true')
    ap.add_argument('--dry-run', action='store_true')
    ap.add_argument('--no-backup', action='store_true')
    args = ap.parse_args()

    script_dir = Path(__file__).resolve().parent
    canonical_path = (script_dir / args.canonical).resolve() if not Path(args.canonical).is_absolute() else Path(args.canonical)
    source_path = (script_dir / args.source).resolve() if not Path(args.source).is_absolute() else Path(args.source)
    benchmark_script = (script_dir / args.benchmark_script).resolve() if not Path(args.benchmark_script).is_absolute() else Path(args.benchmark_script)
    benchmark_path = (script_dir / args.benchmark).resolve() if not Path(args.benchmark).is_absolute() else Path(args.benchmark)
    report_root = (script_dir / args.report_dir).resolve() if not Path(args.report_dir).is_absolute() else Path(args.report_dir)
    run_items_path = (script_dir / args.run_items).resolve() if not Path(args.run_items).is_absolute() else Path(args.run_items)

    if not canonical_path.exists():
        print(f'[ERROR] canonical not found: {canonical_path}')
        return 1
    if not source_path.exists():
        print(f'[ERROR] source not found: {source_path}')
        return 1
    if not benchmark_script.exists():
        print(f'[ERROR] benchmark script not found: {benchmark_script}')
        return 1
    if not benchmark_path.exists():
        print(f'[ERROR] benchmark file not found: {benchmark_path}')
        return 1

    is_apply = args.apply and not args.dry_run
    ts = datetime.now().strftime('%Y%m%d_%H%M%S')
    report_dir = report_root / ts
    report_dir.mkdir(parents=True, exist_ok=True)

    print('=' * 76)
    print('SELECTIVE MERGE V5 STAGE INTO CANONICAL')
    print('=' * 76)
    print(f'canonical: {canonical_path}')
    print(f'source:    {source_path}')
    print(f'benchmark: {benchmark_path}')
    print(f'run_items: {run_items_path if run_items_path.exists() else "(not found; will use extract id order)"}')
    print(f'mode:      {"APPLY" if is_apply else "DRY-RUN"}')
    print(f'report dir: {report_dir}')
    print()

    canonical = connect(canonical_path)
    source = connect(source_path)
    backup_path = None
    try:
        run_order = load_run_order(run_items_path if run_items_path.exists() else None)
        packets = candidate_family_packets(canonical, source, run_order, include_existing=args.include_existing_families)
        packets = packets[args.start_index:args.start_index + args.family_limit]

        print('[STEP 1] candidate families')
        print(f'  total candidates: {len(packets)}')
        for p in packets[:25]:
            print(f'  - {p.family} | extracts={len(p.reaction_extract_ids)} | mols={len(p.extract_molecule_ids)} | queryable_mols={p.queryable_molecules} | order={p.run_order}')
        if len(packets) > 25:
            print(f'  ... ({len(packets)-25} more)')
        print()

        before = get_state(canonical)
        print('[STEP 2] canonical state before merge')
        print_state('BEFORE', before)
        print()

        baseline = run_benchmark(script_dir, canonical_path, benchmark_path, report_dir, 'baseline')
        print('[STEP 3] baseline benchmark')
        print(json.dumps(baseline, ensure_ascii=False, indent=2))
        print()

        if not is_apply:
            summary = {
                'canonical': str(canonical_path),
                'source': str(source_path),
                'benchmark': str(benchmark_path),
                'baseline': baseline,
                'candidate_count': len(packets),
                'families': [p.family for p in packets],
                'before_state': before,
                'mode': 'dry-run',
            }
            (report_dir / 'selective_merge_plan.json').write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding='utf-8')
            print('=' * 76)
            print('[DONE] dry-run only; use --apply to perform selective merge')
            print('=' * 76)
            return 0

        if not args.no_backup:
            backup_path = canonical_path.parent / f'{canonical_path.stem}.backup_before_v5_selective_merge_{ts}.db'
            print('[STEP 4] backup canonical')
            shutil.copy2(canonical_path, backup_path)
            print(f'  backup: {backup_path.name} ({backup_path.stat().st_size:,} bytes)')
            print()

        accepted: list[dict] = []
        rejected: list[dict] = []
        for idx, packet in enumerate(packets, start=1 + args.start_index):
            print(f'[MERGE] {idx:03d} {packet.family}')
            canonical.execute('BEGIN')
            try:
                n_re = copy_rows(canonical, source, 'reaction_extracts', packet.reaction_extract_ids)
                n_mol = copy_rows(canonical, source, 'extract_molecules', packet.extract_molecule_ids)
                canonical.commit()
            except Exception as e:
                canonical.rollback()
                rejected.append({
                    'family': packet.family,
                    'status': 'insert_failed',
                    'reason': str(e),
                    'reaction_extract_ids': packet.reaction_extract_ids,
                    'extract_molecule_ids': packet.extract_molecule_ids,
                })
                print(f'  -> insert_failed: {e}')
                continue

            current = run_benchmark(script_dir, canonical_path, benchmark_path, report_dir, f'after_{idx:03d}_{safe_name(packet.family)}')
            is_reg, reason = regression(
                baseline,
                current,
                allow_top1_drop=args.allow_top1_drop,
                allow_top3_drop=args.allow_top3_drop,
                allow_extra_violations=args.allow_extra_violations,
            )
            if is_reg:
                canonical.execute('BEGIN')
                canonical.execute(
                    f"DELETE FROM extract_molecules WHERE id IN ({','.join('?' * len(packet.extract_molecule_ids))})",
                    packet.extract_molecule_ids,
                )
                canonical.execute(
                    f"DELETE FROM reaction_extracts WHERE id IN ({','.join('?' * len(packet.reaction_extract_ids))})",
                    packet.reaction_extract_ids,
                )
                canonical.commit()
                rejected.append({
                    'family': packet.family,
                    'status': 'rejected_regression',
                    'reason': reason,
                    'benchmark': current,
                    'reaction_extract_ids': packet.reaction_extract_ids,
                    'extract_molecule_ids': packet.extract_molecule_ids,
                })
                print(f'  -> REJECTED ({reason})')
            else:
                accepted.append({
                    'family': packet.family,
                    'status': 'accepted',
                    'benchmark': current,
                    'reaction_extract_ids': packet.reaction_extract_ids,
                    'extract_molecule_ids': packet.extract_molecule_ids,
                    'inserted_reaction_extracts': n_re,
                    'inserted_extract_molecules': n_mol,
                })
                print(f"  -> ACCEPTED (top1={current['top1_accuracy']:.4f}, top3={current['top3_accuracy']:.4f}, violations={current['disallow_top3_violations']})")
            print()

        after = get_state(canonical)
        summary = {
            'canonical': str(canonical_path),
            'source': str(source_path),
            'benchmark': str(benchmark_path),
            'baseline': baseline,
            'accepted_count': len(accepted),
            'rejected_count': len(rejected),
            'accepted_families': [x['family'] for x in accepted],
            'rejected_families': [x['family'] for x in rejected],
            'before_state': before,
            'after_state': after,
            'backup_path': str(backup_path) if backup_path else None,
            'mode': 'apply',
        }
        (report_dir / 'selective_merge_summary.json').write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding='utf-8')
        (report_dir / 'accepted_families.json').write_text(json.dumps(accepted, ensure_ascii=False, indent=2), encoding='utf-8')
        (report_dir / 'rejected_families.json').write_text(json.dumps(rejected, ensure_ascii=False, indent=2), encoding='utf-8')

        print('[STEP 5] canonical state after selective merge')
        print_state('AFTER', after)
        print()
        print_diff(before, after)
        print()
        print('=' * 76)
        print('[DONE] selective merge finished')
        print(f'accepted: {len(accepted)} | rejected: {len(rejected)}')
        print(f'summary:  {report_dir / "selective_merge_summary.json"}')
        if backup_path:
            print(f'backup:   {backup_path}')
        print('=' * 76)
        return 0
    finally:
        canonical.close()
        source.close()


if __name__ == '__main__':
    raise SystemExit(main())
