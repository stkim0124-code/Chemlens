import argparse, json, sqlite3, re
from collections import Counter
from datetime import datetime
from pathlib import Path


def build_argparser():
    p = argparse.ArgumentParser(description='Inspect latest PDF example automation extraction errors from stage DB')
    p.add_argument('--backend-root', default='.', help='Backend root path')
    p.add_argument('--stage-db', default=None, help='Override stage DB path')
    p.add_argument('--report-dir', default=None, help='Override output report dir')
    p.add_argument('--limit', type=int, default=20, help='Max sample rows to export')
    return p


def table_columns(conn, table_name: str):
    rows = conn.execute(f'PRAGMA table_info({table_name})').fetchall()
    return [r[1] for r in rows]


def choose_table(conn):
    tables = [r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name").fetchall()]
    cand = []
    for t in tables:
        cols = set(table_columns(conn, t))
        score = 0
        if 'extract' in t.lower(): score += 5
        if 'status' in cols: score += 3
        if any('json' in c.lower() for c in cols): score += 2
        if score: cand.append((score, t))
    cand.sort(reverse=True)
    return cand[0][1] if cand else None


def find_first(cols, names):
    m = {c.lower(): c for c in cols}
    for n in names:
        if n.lower() in m:
            return m[n.lower()]
    return None


def safe_json_loads(value):
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return json.loads(text)
    except Exception:
        return None


def latest_run_value(conn, table_name, run_col):
    if not run_col:
        return None
    vals = [str(r[0]) for r in conn.execute(f'SELECT {run_col} FROM {table_name}').fetchall() if r and r[0]]
    return max(vals) if vals else None


def main():
    args = build_argparser().parse_args()
    backend_root = Path(args.backend_root).resolve()
    stage_db = Path(args.stage_db).resolve() if args.stage_db else backend_root / 'app' / 'labint_pdf_examples_stage.db'
    report_dir = Path(args.report_dir).resolve() if args.report_dir else backend_root / 'reports' / 'pdf_example_error_inspection' / datetime.now().strftime('%Y%m%d_%H%M%S')
    report_dir.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(str(stage_db))
    conn.row_factory = sqlite3.Row
    table = choose_table(conn)
    cols = table_columns(conn, table)
    run_col = find_first(cols, ['report_run', 'run_id', 'run_tag', 'batch_run', 'created_run'])
    status_col = find_first(cols, ['status'])
    family_col = find_first(cols, ['family_name', 'source_family_name', 'requested_family_name', 'reaction_family_name'])
    json_cols = [c for c in cols if 'json' in c.lower() or c.lower() in {'payload', 'response', 'result'}]
    err_cols = [c for c in cols if 'error' in c.lower() or c.lower() in {'message', 'notes_text'}]
    latest = latest_run_value(conn, table, run_col)
    rows = conn.execute(f'SELECT * FROM {table} WHERE {run_col} = ?', (latest,)).fetchall() if latest and run_col else conn.execute(f'SELECT * FROM {table}').fetchall()

    top_errors = Counter()
    samples = []
    for row in rows:
        d = dict(row)
        status = str(d.get(status_col, '<null>')).strip().lower() if status_col else '<null>'
        if status != 'error':
            continue
        msg = None
        for c in err_cols:
            if d.get(c):
                msg = str(d.get(c)).strip()
                break
        if not msg:
            for c in json_cols:
                obj = safe_json_loads(d.get(c))
                if isinstance(obj, dict):
                    msg = obj.get('error') or obj.get('error_message') or obj.get('message')
                    if msg:
                        break
        if not msg:
            msg = '<no_error_message_captured>'
        msg = re.sub(r'\s+', ' ', str(msg)).strip()
        top_errors[msg[:240]] += 1
        if len(samples) < args.limit:
            samples.append({
                'family_name': d.get(family_col),
                'status': d.get(status_col),
                'error_message': msg,
            })

    out = {
        'stage_db': str(stage_db),
        'extraction_table': table,
        'latest_extract_run': latest,
        'top_error_messages': top_errors.most_common(20),
        'samples': samples,
    }
    out_json = report_dir / 'pdf_example_error_inspection_summary.json'
    out_md = report_dir / 'pdf_example_error_inspection_summary.md'
    out_json.write_text(json.dumps(out, indent=2, ensure_ascii=False), encoding='utf-8')
    lines = ['# PDF Example Error Inspection','', f'- stage_db: `{stage_db}`', f'- extraction_table: `{table}`', f'- latest_extract_run: `{latest}`', '', '## Top error messages', '']
    for msg, n in out['top_error_messages']:
        lines.append(f'- {n}x {msg}')
    lines.append('')
    lines.append('## Samples')
    lines.append('')
    for s in samples:
        lines.append(f"- {s['family_name']}: {s['error_message']}")
    out_md.write_text('\n'.join(lines), encoding='utf-8')
    print('='*72)
    print('INSPECT PDF EXAMPLE AUTOMATION ERRORS')
    print('='*72)
    print(f'backend_root: {backend_root}')
    print(f'stage_db:     {stage_db}')
    print(f'report_dir:   {report_dir}')
    print(f'[ERROR INSPECT] run={latest} total_error_types={len(top_errors)}')
    for msg, n in top_errors.most_common(10):
        print(f'  {n}x {msg}')
    print(f'summary json: {out_json}')
    print(f'summary md:   {out_md}')
    print('='*72)

if __name__ == '__main__':
    main()
