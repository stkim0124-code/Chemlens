#!/usr/bin/env python
from __future__ import annotations
import argparse, json, shutil, sqlite3
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent
APP = ROOT / 'app'
DEFAULT_DB = APP / 'labint.db'
DEFAULT_STAGE = APP / 'labint_v5_stage.db'
DEFAULT_BENCHMARK = ROOT / 'benchmark' / 'named_reaction_benchmark_small.json'
DEFAULT_BACKUP_DIR = ROOT / 'backups' / 'stable_freeze'
DEFAULT_REPORT_DIR = ROOT / 'reports' / 'stable_freeze'


def ts() -> str:
    return datetime.now().strftime('%Y%m%d_%H%M%S')


def ensure_dir(p: Path) -> Path:
    p.mkdir(parents=True, exist_ok=True)
    return p


def get_cols(conn: sqlite3.Connection, table: str) -> set[str]:
    return {r[1] for r in conn.execute(f'PRAGMA table_info({table})').fetchall()}


def pick(cols: set[str], *names: str) -> str | None:
    for n in names:
        if n in cols:
            return n
    return None


def scalar(conn: sqlite3.Connection, sql: str, params: tuple = ()):
    return conn.execute(sql, params).fetchone()[0]


def run_benchmark(root: Path, db: Path, benchmark_file: Path, report_dir: Path) -> dict:
    import subprocess, sys
    ensure_dir(report_dir)
    json_out = report_dir / 'benchmark_snapshot.json'
    csv_out = report_dir / 'benchmark_snapshot.csv'
    md_out = report_dir / 'benchmark_snapshot.md'
    cmd = [
        sys.executable, str(root / 'run_named_reaction_benchmark_small.py'),
        '--db', str(db), '--benchmark', str(benchmark_file),
        '--json-out', str(json_out), '--csv-out', str(csv_out), '--report-md', str(md_out),
    ]
    completed = subprocess.run(cmd, cwd=str(root), capture_output=True, text=True, encoding='utf-8', errors='replace')
    if completed.returncode != 0:
        raise RuntimeError(f'benchmark failed\nSTDOUT:\n{completed.stdout}\nSTDERR:\n{completed.stderr}')
    payload = json.loads(json_out.read_text(encoding='utf-8'))
    return payload.get('summary', payload)


def get_state(db: Path) -> dict:
    conn = sqlite3.connect(str(db))
    try:
        em_cols = get_cols(conn, 'extract_molecules')
        re_cols = get_cols(conn, 'reaction_extracts')
        fam_col = pick(re_cols, 'reaction_family_name', 'family_name', 'family')
        query_col = pick(em_cols, 'queryable', 'is_queryable')
        tier_col = pick(em_cols, 'quality_tier', 'structure_tier')
        source_col = pick(em_cols, 'structure_source')
        state = {
            'queryable': scalar(conn, f'SELECT COUNT(*) FROM extract_molecules WHERE {query_col}=1') if query_col else None,
            'tier1': scalar(conn, f"SELECT COUNT(*) FROM extract_molecules WHERE {tier_col} IN (1, '1', 'tier1')") if tier_col else None,
            'tier2': scalar(conn, f"SELECT COUNT(*) FROM extract_molecules WHERE {tier_col} IN (2, '2', 'tier2')") if tier_col else None,
            'tier3': scalar(conn, f"SELECT COUNT(*) FROM extract_molecules WHERE {tier_col} IN (3, '3', 'tier3')") if tier_col else None,
            'reaction_extracts': scalar(conn, 'SELECT COUNT(*) FROM reaction_extracts'),
            'extract_molecules_total': scalar(conn, 'SELECT COUNT(*) FROM extract_molecules'),
            'queryable_family_coverage': scalar(conn, f"SELECT COUNT(DISTINCT re.{fam_col}) FROM reaction_extracts re WHERE EXISTS (SELECT 1 FROM extract_molecules em WHERE em.extract_id=re.id AND em.{query_col}=1)") if fam_col and query_col else None,
            'total_registry_families': scalar(conn, 'SELECT COUNT(*) FROM reaction_family_patterns') if scalar(conn, "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='reaction_family_patterns'") else None,
        }
        if source_col:
            state['structure_source_counts'] = conn.execute(
                f"SELECT COALESCE({source_col},'NULL'), COUNT(*) FROM extract_molecules GROUP BY COALESCE({source_col},'NULL') ORDER BY COUNT(*) DESC"
            ).fetchall()
        return state
    finally:
        conn.close()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument('--db', default=str(DEFAULT_DB))
    ap.add_argument('--stage-db', default=str(DEFAULT_STAGE))
    ap.add_argument('--benchmark-file', default=str(DEFAULT_BENCHMARK))
    ap.add_argument('--backup-root', default=str(DEFAULT_BACKUP_DIR))
    ap.add_argument('--report-root', default=str(DEFAULT_REPORT_DIR))
    args = ap.parse_args()

    db = Path(args.db)
    stage_db = Path(args.stage_db)
    benchmark_file = Path(args.benchmark_file)
    stamp = ts()
    backup_dir = ensure_dir(Path(args.backup_root) / stamp)
    report_dir = ensure_dir(Path(args.report_root) / stamp)

    print('=' * 72)
    print('FREEZE CURRENT CANONICAL STATE')
    print('=' * 72)
    print(f'canonical:   {db}')
    print(f'stage_db:    {stage_db}')
    print(f'benchmark:   {benchmark_file}')
    print(f'backup_dir:  {backup_dir}')
    print(f'report_dir:  {report_dir}')

    canon_backup = backup_dir / f'labint.stable_{stamp}.db'
    stage_backup = backup_dir / f'labint_v5_stage.stable_{stamp}.db'
    shutil.copy2(db, canon_backup)
    print(f'[backup] canonical copied -> {canon_backup}')
    if stage_db.exists():
        shutil.copy2(stage_db, stage_backup)
        print(f'[backup] stage copied     -> {stage_backup}')

    state = get_state(db)
    bench = run_benchmark(ROOT, db, benchmark_file, report_dir)

    payload = {
        'timestamp': stamp,
        'canonical_db': str(db),
        'stage_db': str(stage_db),
        'canonical_backup': str(canon_backup),
        'stage_backup': str(stage_backup) if stage_db.exists() else None,
        'state': state,
        'benchmark': bench,
    }
    (report_dir / 'stable_freeze_summary.json').write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
    md = [
        '# CHEMLENS stable freeze summary',
        '',
        f'- timestamp: `{stamp}`',
        f'- canonical backup: `{canon_backup}`',
        f'- stage backup: `{stage_backup if stage_db.exists() else "(missing)"}`',
        '',
        '## State',
        '',
        f"- total_registry_families: {state.get('total_registry_families')}",
        f"- queryable_family_coverage: {state.get('queryable_family_coverage')}",
        f"- reaction_extracts: {state.get('reaction_extracts')}",
        f"- extract_molecules_total: {state.get('extract_molecules_total')}",
        f"- queryable: {state.get('queryable')}",
        f"- tier1: {state.get('tier1')}",
        f"- tier2: {state.get('tier2')}",
        f"- tier3: {state.get('tier3')}",
        '',
        '## Benchmark',
        '',
        f"- top1_accuracy: {bench.get('top1_accuracy')}",
        f"- top3_accuracy: {bench.get('top3_accuracy')}",
        f"- disallow_top3_violations: {bench.get('disallow_top3_violations')}",
    ]
    (report_dir / 'stable_freeze_summary.md').write_text('\n'.join(md) + '\n', encoding='utf-8')

    print('[state]')
    print(json.dumps(state, ensure_ascii=False, indent=2))
    print('[benchmark]')
    print(json.dumps(bench, ensure_ascii=False, indent=2))
    print(f'[summary] {report_dir / "stable_freeze_summary.json"}')
    print('=' * 72)
    return 0

if __name__ == '__main__':
    raise SystemExit(main())
