import argparse, json, re, sqlite3, unicodedata
from collections import Counter
from datetime import datetime
from pathlib import Path

try:
    from rdkit import Chem  # type: ignore
    RDKit_AVAILABLE = True
except Exception:
    Chem = None
    RDKit_AVAILABLE = False


def build_argparser():
    p = argparse.ArgumentParser(description='Verify PDF example Gemini batch extraction quality from stage DB (nested-JSON aware)')
    p.add_argument('--backend-root', default='.', help='Backend root path')
    p.add_argument('--stage-db', default=None, help='Override stage DB path')
    p.add_argument('--report-dir', default=None, help='Override output report dir')
    return p


def normalize_family_name(value: str) -> str:
    value = unicodedata.normalize('NFKD', value)
    value = value.encode('ascii', 'ignore').decode('ascii')
    value = value.lower().strip()
    value = value.replace('&', ' and ')
    value = re.sub(r'\(.*?\)', ' ', value)
    value = re.sub(r'[^a-z0-9]+', ' ', value)
    value = re.sub(r'\s+', ' ', value).strip()
    return value


def is_nonempty(value) -> bool:
    if value is None:
        return False
    if isinstance(value, (list, tuple, set, dict)):
        return len(value) > 0
    return str(value).strip() != ''


def is_parse_safe(smiles: str) -> bool:
    if not RDKit_AVAILABLE or not is_nonempty(smiles):
        return False
    try:
        mol = Chem.MolFromSmiles(str(smiles).strip())
        return mol is not None
    except Exception:
        return False


def table_columns(conn, table_name: str):
    rows = conn.execute(f'PRAGMA table_info({table_name})').fetchall()
    return [r[1] for r in rows]


def choose_table(conn, preferred_kind: str):
    tables = [r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name").fetchall()]
    candidates = []
    for t in tables:
        cols = set(table_columns(conn, t))
        score = 0
        lname = t.lower()
        if preferred_kind == 'extraction':
            if 'extract' in lname:
                score += 5
            if 'status' in cols:
                score += 3
            for c in ['target_name', 'example_target_name', 'reactant_smiles', 'product_smiles', 'report_run']:
                if c in cols:
                    score += 2
            if any('json' in c.lower() for c in cols):
                score += 2
        elif preferred_kind == 'page':
            if 'page' in lname:
                score += 5
            for c in ['page_no', 'family_name', 'report_run']:
                if c in cols:
                    score += 2
        elif preferred_kind == 'region':
            if 'region' in lname:
                score += 5
            for c in ['page_id', 'family_name', 'report_run']:
                if c in cols:
                    score += 2
        if score > 0:
            candidates.append((score, t))
    candidates.sort(reverse=True)
    return candidates[0][1] if candidates else None


def find_first(cols, names):
    colset = {c.lower(): c for c in cols}
    for name in names:
        if name.lower() in colset:
            return colset[name.lower()]
    return None


def latest_run_value(conn, table_name: str, run_col: str):
    if not run_col:
        return None
    rows = conn.execute(
        f'SELECT {run_col} FROM {table_name} WHERE {run_col} IS NOT NULL AND TRIM(CAST({run_col} AS TEXT)) <> \"\"'
    ).fetchall()
    values = [str(r[0]) for r in rows if r and r[0] is not None and str(r[0]).strip()]
    if not values:
        return None
    return max(values)


def safe_json_loads(value):
    if value is None:
        return None
    if isinstance(value, (dict, list)):
        return value
    text = str(value).strip()
    if not text:
        return None
    try:
        return json.loads(text)
    except Exception:
        return None


def get_nested(obj, *keys):
    cur = obj
    for k in keys:
        if not isinstance(cur, dict) or k not in cur:
            return None
        cur = cur[k]
    return cur


def first_nonempty(*values):
    for v in values:
        if is_nonempty(v):
            return v
    return None


def extract_nested_payload(row_dict, json_cols):
    payloads = []
    for c in json_cols:
        obj = safe_json_loads(row_dict.get(c))
        if obj is not None:
            payloads.append((c, obj))
    return payloads


def derive_metrics_from_row(row_dict, json_payloads):
    target = first_nonempty(
        row_dict.get('target_name'),
        row_dict.get('example_target_name'),
        row_dict.get('target_molecule_name'),
        row_dict.get('target_compound_name'),
    )
    reactant = first_nonempty(row_dict.get('reactant_smiles'), row_dict.get('reactants_smiles'))
    product = first_nonempty(row_dict.get('product_smiles'), row_dict.get('products_smiles'))
    source_family = first_nonempty(
        row_dict.get('family_name'), row_dict.get('source_family_name'), row_dict.get('requested_family_name'), row_dict.get('reaction_family_name')
    )
    extracted_family = first_nonempty(
        row_dict.get('extracted_family_name'), row_dict.get('predicted_family_name'), row_dict.get('matched_family_name')
    )
    conditions = first_nonempty(row_dict.get('conditions_text'), row_dict.get('condition_text'), row_dict.get('conditions'))
    yield_text = first_nonempty(row_dict.get('yield_text'), row_dict.get('yield_percent'), row_dict.get('yield'))
    reagents = first_nonempty(row_dict.get('reagents_text'), row_dict.get('reagent_text'), row_dict.get('reagents'))
    error_text = first_nonempty(row_dict.get('error_message'), row_dict.get('error_text'), row_dict.get('message'), row_dict.get('notes_text'))

    for _, obj in json_payloads:
        if isinstance(obj, dict):
            target = first_nonempty(target, obj.get('example_target_name'), obj.get('target_name'), obj.get('target'))
            source_family = first_nonempty(source_family, obj.get('family_name'), obj.get('reaction_family_name'))
            extracted_family = first_nonempty(extracted_family, obj.get('extracted_family_name'), obj.get('predicted_family_name'))
            error_text = first_nonempty(error_text, obj.get('error'), obj.get('error_message'), obj.get('message'))
            extracts = obj.get('extracts') if isinstance(obj.get('extracts'), list) else []
            for ex in extracts:
                if not isinstance(ex, dict):
                    continue
                reactant = first_nonempty(reactant, ex.get('reactant_smiles'), ex.get('reactants_smiles'))
                product = first_nonempty(product, ex.get('product_smiles'), ex.get('products_smiles'))
                source_family = first_nonempty(source_family, ex.get('reaction_family_name'), ex.get('family_name'))
                conditions = first_nonempty(conditions, ex.get('conditions_text'), ex.get('condition_text'))
                yield_text = first_nonempty(yield_text, ex.get('yield_text'), ex.get('yield'))
                reagents = first_nonempty(reagents, ex.get('reagents_text'), ex.get('reagents'))
                extracted_family = first_nonempty(extracted_family, ex.get('extracted_family_name'), ex.get('predicted_family_name'))
    return {
        'target': target,
        'reactant': reactant,
        'product': product,
        'source_family': source_family,
        'extracted_family': extracted_family,
        'conditions': conditions,
        'yield_text': yield_text,
        'reagents': reagents,
        'error_text': error_text,
    }


def main():
    args = build_argparser().parse_args()
    backend_root = Path(args.backend_root).resolve()
    stage_db = Path(args.stage_db).resolve() if args.stage_db else (backend_root / 'app' / 'labint_pdf_examples_stage.db')
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    report_dir = Path(args.report_dir).resolve() if args.report_dir else (backend_root / 'reports' / 'pdf_example_batch_verification' / timestamp)
    report_dir.mkdir(parents=True, exist_ok=True)

    print('=' * 72)
    print('VERIFY PDF EXAMPLE GEMINI BATCH')
    print('=' * 72)
    print(f'backend_root: {backend_root}')
    print(f'stage_db:     {stage_db}')
    print(f'report_dir:   {report_dir}')

    if not stage_db.exists():
        raise SystemExit(f'stage db not found: {stage_db}')

    conn = sqlite3.connect(str(stage_db))
    conn.row_factory = sqlite3.Row

    extraction_table = choose_table(conn, 'extraction')
    page_table = choose_table(conn, 'page')
    region_table = choose_table(conn, 'region')
    if extraction_table is None:
        raise SystemExit('could not detect extraction table in stage DB')

    extraction_cols = table_columns(conn, extraction_table)
    run_col = find_first(extraction_cols, ['report_run', 'run_id', 'run_tag', 'batch_run', 'created_run'])
    status_col = find_first(extraction_cols, ['status'])
    json_cols = [c for c in extraction_cols if 'json' in c.lower() or c.lower() in {'payload', 'response', 'result'}]

    latest_extract_run = latest_run_value(conn, extraction_table, run_col)
    total_extractions = conn.execute(f'SELECT COUNT(*) FROM {extraction_table}').fetchone()[0]
    total_pages = conn.execute(f'SELECT COUNT(*) FROM {page_table}').fetchone()[0] if page_table else None
    total_regions = conn.execute(f'SELECT COUNT(*) FROM {region_table}').fetchone()[0] if region_table else None

    if latest_extract_run is not None and run_col:
        rows = conn.execute(f'SELECT * FROM {extraction_table} WHERE {run_col} = ?', (latest_extract_run,)).fetchall()
    else:
        rows = conn.execute(f'SELECT * FROM {extraction_table}').fetchall()

    status_counts = Counter()
    target_nonnull = reactant_nonnull = product_nonnull = either_nonnull = both_nonnull = 0
    reactant_parse_safe = product_parse_safe = both_parse_safe = 0
    mismatch_pairs = mismatch_count = text_only_count = 0
    family_row_counts = Counter()
    error_top = Counter()
    ok_with_json_only = 0

    for row in rows:
        row_dict = dict(row)
        status_val = str(row_dict.get(status_col)).strip() if status_col and row_dict.get(status_col) is not None else '<null>'
        status_counts[status_val] += 1

        payloads = extract_nested_payload(row_dict, json_cols)
        derived = derive_metrics_from_row(row_dict, payloads)
        target_ok = is_nonempty(derived['target'])
        reactant_ok = is_nonempty(derived['reactant'])
        product_ok = is_nonempty(derived['product'])
        cond_ok = is_nonempty(derived['conditions'])
        yield_ok = is_nonempty(derived['yield_text'])
        reag_ok = is_nonempty(derived['reagents'])

        if target_ok:
            target_nonnull += 1
        if reactant_ok:
            reactant_nonnull += 1
        if product_ok:
            product_nonnull += 1
        if reactant_ok or product_ok:
            either_nonnull += 1
        if reactant_ok and product_ok:
            both_nonnull += 1
        if not is_nonempty(row_dict.get('target_name')) and target_ok and payloads:
            ok_with_json_only += 1

        r_safe = is_parse_safe(derived['reactant']) if reactant_ok else False
        p_safe = is_parse_safe(derived['product']) if product_ok else False
        if r_safe:
            reactant_parse_safe += 1
        if p_safe:
            product_parse_safe += 1
        if r_safe and p_safe:
            both_parse_safe += 1

        if (not reactant_ok) and (not product_ok) and (target_ok or cond_ok or yield_ok or reag_ok):
            text_only_count += 1

        source_family = derived['source_family']
        extracted_family = derived['extracted_family']
        if is_nonempty(source_family):
            family_row_counts[str(source_family).strip()] += 1
        elif is_nonempty(extracted_family):
            family_row_counts[str(extracted_family).strip()] += 1

        if is_nonempty(source_family) and is_nonempty(extracted_family):
            mismatch_pairs += 1
            if normalize_family_name(str(source_family)) != normalize_family_name(str(extracted_family)):
                mismatch_count += 1

        if status_val.lower() == 'error':
            msg = derived['error_text']
            if is_nonempty(msg):
                msg = re.sub(r'\s+', ' ', str(msg)).strip()
                error_top[msg[:240]] += 1
            else:
                error_top['<no_error_message_captured>'] += 1

    row_count = len(rows)
    mismatch_ratio = (mismatch_count / mismatch_pairs) if mismatch_pairs else None
    summary = {
        'stage_db': str(stage_db),
        'page_table': page_table,
        'region_table': region_table,
        'extraction_table': extraction_table,
        'latest_extract_run': latest_extract_run,
        'rdkit_available': RDKit_AVAILABLE,
        'json_columns_considered': json_cols,
        'totals': {
            'page_rows': total_pages,
            'region_rows': total_regions,
            'extraction_rows': total_extractions,
        },
        'latest_extraction': {
            'rows': row_count,
            'status_counts': dict(status_counts),
            'target_name_nonnull': target_nonnull,
            'reactant_smiles_nonnull': reactant_nonnull,
            'product_smiles_nonnull': product_nonnull,
            'either_smiles_nonnull': either_nonnull,
            'both_smiles_nonnull': both_nonnull,
            'reactant_parse_safe': reactant_parse_safe,
            'product_parse_safe': product_parse_safe,
            'both_parse_safe': both_parse_safe,
            'text_only_count': text_only_count,
            'family_mismatch_count': mismatch_count,
            'family_mismatch_pairs': mismatch_pairs,
            'family_mismatch_ratio': mismatch_ratio,
            'ok_with_json_only_recovery': ok_with_json_only,
            'top_error_messages': error_top.most_common(10),
            'family_row_counts': dict(family_row_counts.most_common()),
        },
    }

    out_json = report_dir / 'pdf_example_batch_verification_summary.json'
    out_md = report_dir / 'pdf_example_batch_verification_summary.md'
    out_json.write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding='utf-8')

    lines = []
    lines.append('# PDF Example Gemini Batch Verification')
    lines.append('')
    lines.append(f'- stage_db: `{stage_db}`')
    lines.append(f'- extraction_table: `{extraction_table}`')
    lines.append(f'- latest_extract_run: `{latest_extract_run}`')
    lines.append(f'- json_columns_considered: `{json_cols}`')
    lines.append('')
    lines.append('## Totals')
    lines.append('')
    lines.append(f'- page_rows: {total_pages}')
    lines.append(f'- region_rows: {total_regions}')
    lines.append(f'- extraction_rows: {total_extractions}')
    lines.append('')
    le = summary['latest_extraction']
    lines.append('## Latest extraction')
    lines.append('')
    for k in ['rows','status_counts','target_name_nonnull','reactant_smiles_nonnull','product_smiles_nonnull','either_smiles_nonnull','both_smiles_nonnull','reactant_parse_safe','product_parse_safe','both_parse_safe','text_only_count','family_mismatch_count','family_mismatch_pairs','family_mismatch_ratio','ok_with_json_only_recovery']:
        lines.append(f'- {k}: {le[k]}')
    lines.append('')
    lines.append('## Top error messages')
    lines.append('')
    for msg, n in le['top_error_messages']:
        lines.append(f'- {n}x {msg}')
    out_md.write_text('\n'.join(lines), encoding='utf-8')

    print(f"[EXTRACT] run={latest_extract_run} rows={row_count} status_counts={dict(status_counts)} target_name_nonnull={target_nonnull} reactant_smiles_nonnull={reactant_nonnull} product_smiles_nonnull={product_nonnull} both_smiles_nonnull={both_nonnull}")
    print(f"[PARSE] reactant_parse_safe={reactant_parse_safe} product_parse_safe={product_parse_safe} both_parse_safe={both_parse_safe}")
    print(f"[RECOVER] ok_with_json_only_recovery={ok_with_json_only} json_columns={json_cols}")
    if error_top:
        print('[ERROR TOP]')
        for msg, n in error_top.most_common(5):
            print(f'  {n}x {msg}')
    print(f'summary json: {out_json}')
    print(f'summary md:   {out_md}')
    print('=' * 72)


if __name__ == '__main__':
    main()
