from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

try:
    from rdkit import Chem
except Exception:
    Chem = None


def find_latest(base: Path, pattern: str) -> Optional[Path]:
    matches = sorted(base.glob(pattern), key=lambda p: p.stat().st_mtime)
    return matches[-1] if matches else None


def resolve_path(base: Path, value: Optional[str], fallback_glob: str) -> Optional[Path]:
    if value:
        p = Path(value)
        if not p.is_absolute():
            p = base / p
        if p.exists():
            return p
    return find_latest(base, fallback_glob)


def create_report_dir(base: Path) -> Path:
    base.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    path = base / stamp
    i = 1
    while path.exists():
        path = base / f"{stamp}_{i:02d}"
        i += 1
    path.mkdir(parents=True, exist_ok=False)
    return path


def load_json(path: Path) -> Any:
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)


def q(conn: sqlite3.Connection, sql: str, params: Tuple[Any, ...] = ()) -> List[sqlite3.Row]:
    conn.row_factory = sqlite3.Row
    return conn.execute(sql, params).fetchall()


def rdkit_check(smiles: Optional[str]) -> Dict[str, Any]:
    if not smiles:
        return {'rdkit_available': Chem is not None, 'parse_ok': False, 'sanitize_ok': False, 'reason': 'empty_smiles'}
    if Chem is None:
        return {'rdkit_available': False}
    result: Dict[str, Any] = {'rdkit_available': True, 'parse_ok': False, 'sanitize_ok': False}
    try:
        mol = Chem.MolFromSmiles(smiles, sanitize=False)
    except Exception as e:
        result['parse_error'] = f'{type(e).__name__}: {e}'
        return result
    if mol is None:
        result['parse_error'] = 'MolFromSmiles returned None'
        return result
    result['parse_ok'] = True
    try:
        Chem.SanitizeMol(mol)
        result['sanitize_ok'] = True
        result['canonical_smiles'] = Chem.MolToSmiles(mol)
    except Exception as e:
        result['sanitize_error'] = f'{type(e).__name__}: {e}'
        try:
            result['canonical_smiles_partial'] = Chem.MolToSmiles(mol)
        except Exception:
            pass
    return result


def infer_cluster(first_case: str) -> str:
    x = (first_case or '').lower()
    if 'barton' in x:
        return 'barton'
    if 'buchner' in x:
        return 'buchner'
    return ''


def extract_applied_families_from_apply_summary(apply_summary_path: Optional[Path]) -> List[str]:
    if not apply_summary_path or not apply_summary_path.exists():
        return []
    try:
        data = load_json(apply_summary_path)
    except Exception:
        return []
    if str(data.get('mode', '')).lower() != 'apply':
        return []
    if not data.get('guard_pass'):
        return []
    if data.get('final_guard_pass') is False:
        return []
    out: List[str] = []
    for row in data.get('apply_results', []):
        if row.get('status') == 'inserted' and row.get('family_name'):
            out.append(row['family_name'])
    return out


def load_rejected_map(rejected_json_path: Path) -> Dict[str, Dict[str, Any]]:
    data = load_json(rejected_json_path)
    out: Dict[str, Dict[str, Any]] = {}
    for item in data:
        fam = item.get('family') or item.get('family_name')
        if fam:
            out[fam] = item
    return out


def load_diagnose(diagnose_summary_path: Path) -> Dict[str, Any]:
    data = load_json(diagnose_summary_path)
    fam_map: Dict[str, Dict[str, Any]] = {}
    for fam in data.get('families', []):
        family_name = fam.get('family_name')
        if not family_name:
            continue
        chosen = None
        variants = fam.get('variants', [])
        for v in variants:
            if v.get('variant') == 'minimal_pair':
                chosen = v
                break
        if chosen is None and variants:
            chosen = variants[0]
        fam_map[family_name] = chosen or {}
    return {
        'raw': data,
        'families': fam_map,
        'applied_family_exclusions': data.get('applied_family_exclusions', []) or [],
        'effective_rejected_count': data.get('effective_rejected_count', len(fam_map)),
        'summary_rejected_count': data.get('summary_rejected_count', len(fam_map)),
    }


def analyze_family(canon: sqlite3.Connection, stage: sqlite3.Connection, family: str, rejected: Dict[str, Any], diagnose: Dict[str, Any]) -> Dict[str, Any]:
    canon_extracts = q(canon, 'select * from reaction_extracts where reaction_family_name=? order by id', (family,))
    stage_extracts = q(stage, 'select * from reaction_extracts where reaction_family_name=? order by id', (family,))
    canon_mols = q(canon, 'select * from extract_molecules where reaction_family_name=? order by id', (family,))
    stage_mols = q(stage, 'select * from extract_molecules where reaction_family_name=? order by id', (family,))

    canon_extract_ids = {r['id'] for r in canon_extracts}
    stage_only_extract_ids = [r['id'] for r in stage_extracts if r['id'] not in canon_extract_ids]

    seed_extracts = [
        r for r in stage_extracts
        if (r['parse_status'] or '') == 'gemini_auto_seed' or (r['extractor_model'] or '') == 'gemini_auto_seed'
    ]
    seed_extract_ids = {r['id'] for r in seed_extracts}
    seed_molecules = [r for r in stage_mols if (r['structure_source'] or '') == 'gemini_auto_seed' or r['extract_id'] in seed_extract_ids]

    molecule_dump: List[Dict[str, Any]] = []
    core_queryable_count = 0
    core_roles = []
    for m in seed_molecules:
        role = m['role']
        if m['queryable'] and role in ('reactant', 'product'):
            core_queryable_count += 1
            core_roles.append(role)
        molecule_dump.append({
            'id': m['id'],
            'extract_id': m['extract_id'],
            'role': role,
            'smiles': m['smiles'],
            'quality_tier': m['quality_tier'],
            'queryable': m['queryable'],
            'source_field': m['source_field'],
            'structure_source': m['structure_source'],
            'normalized_text': m['normalized_text'],
            'rdkit': rdkit_check(m['smiles']),
        })

    extract_dump: List[Dict[str, Any]] = []
    for r in seed_extracts:
        extract_dump.append({
            'id': r['id'],
            'scheme_candidate_id': r['scheme_candidate_id'],
            'extract_kind': r['extract_kind'],
            'reactant_smiles': r['reactant_smiles'],
            'product_smiles': r['product_smiles'],
            'reactants_text': r['reactants_text'],
            'products_text': r['products_text'],
            'intermediates_text': r['intermediates_text'],
            'reagents_text': r['reagents_text'],
            'parse_status': r['parse_status'],
            'promote_decision': r['promote_decision'],
            'rejection_reason': r['rejection_reason'],
            'extractor_model': r['extractor_model'],
            'smiles_confidence': r['smiles_confidence'],
        })

    first_case = diagnose.get('first_changed_case', '')
    cluster = infer_cluster(first_case)
    changed = diagnose.get('changed_case_count', 0)
    bench = rejected.get('benchmark', {}) if rejected else {}

    suspicion_flags: List[str] = []
    if any(m['role'] == 'reagent' and ('[Ru]' in (m['smiles'] or '') or '[Rh]' in (m['smiles'] or '') or '[Pd]' in (m['smiles'] or '')) for m in seed_molecules):
        suspicion_flags.append('organometallic_reagent_present')
    if any(m['role'] == 'intermediate' and m['queryable'] for m in seed_molecules):
        suspicion_flags.append('queryable_intermediate_present')
    if core_queryable_count >= 3:
        suspicion_flags.append('three_or_more_queryable_core_molecules')
    if cluster == 'barton' and any(m['role'] in ('reactant', 'product') and m['queryable'] for m in seed_molecules):
        suspicion_flags.append('single_smiles_exposure_risk')
    if cluster == 'buchner' and any(m['role'] == 'reactant' for m in seed_molecules if m['queryable']):
        suspicion_flags.append('core_pair_similarity_risk')

    recommendation = 'inspect_seed'
    if cluster == 'barton':
        recommendation = 'review_single_smiles_exposure_then_consider_seed_replacement'
    elif cluster == 'buchner':
        recommendation = 'review_core_pair_simplification_then_consider_seed_replacement'

    return {
        'family_name': family,
        'changed_case_count': changed,
        'first_changed_case': first_case,
        'first_changed_top1': diagnose.get('first_changed_top1', ''),
        'cluster': cluster,
        'rejected_reason': rejected.get('reason', ''),
        'rejected_benchmark_top1': bench.get('top1_accuracy'),
        'rejected_benchmark_top3': bench.get('top3_accuracy'),
        'rejected_reaction_extract_ids': rejected.get('reaction_extract_ids', []),
        'rejected_extract_molecule_ids': rejected.get('extract_molecule_ids', []),
        'canonical_extract_ids': [r['id'] for r in canon_extracts],
        'stage_extract_ids': [r['id'] for r in stage_extracts],
        'stage_only_extract_ids': stage_only_extract_ids,
        'seed_extract_ids': sorted(seed_extract_ids),
        'core_queryable_count': core_queryable_count,
        'core_queryable_roles': core_roles,
        'suspicion_flags': suspicion_flags,
        'recommendation': recommendation,
        'stage_seed_extracts': extract_dump,
        'stage_seed_molecules': molecule_dump,
    }


def render_md(summary: Dict[str, Any]) -> str:
    lines: List[str] = []
    lines.append('# Rejected stage seed analysis v2')
    lines.append('')
    lines.append(f"Generated: {summary['generated_at']}")
    lines.append('')
    lines.append(f"- Canonical DB: `{summary['canonical_db']}`")
    lines.append(f"- Stage DB: `{summary['stage_db']}`")
    lines.append(f"- Rejected JSON: `{summary['rejected_json']}`")
    lines.append(f"- Diagnose summary: `{summary['diagnose_summary']}`")
    if summary.get('apply_summary'):
        lines.append(f"- Apply summary: `{summary['apply_summary']}`")
    lines.append('')
    lines.append(f"- Base rejected families in selective merge report: **{summary['base_rejected_count']}**")
    lines.append(f"- Applied salvage families: **{len(summary['applied_families'])}**")
    lines.append(f"- Active rejected families from latest post-apply diagnose: **{summary['active_rejected_count']}**")
    lines.append('')
    if summary['applied_families']:
        lines.append('## Applied salvage families')
        lines.append('')
        for fam in summary['applied_families']:
            lines.append(f'- {fam}')
        lines.append('')
    if summary['inactive_base_families']:
        lines.append('## Base rejected families no longer active')
        lines.append('')
        for fam in summary['inactive_base_families']:
            lines.append(f'- {fam}')
        lines.append('')
    lines.append('## Active rejected families')
    lines.append('')
    for fam in summary['families']:
        lines.append(f"### {fam['family_name']}")
        lines.append('')
        lines.append(f"- changed_cases: **{fam['changed_case_count']}**")
        lines.append(f"- first_case: `{fam['first_changed_case']}`")
        lines.append(f"- first_top1: `{fam['first_changed_top1']}`")
        lines.append(f"- cluster: `{fam['cluster']}`")
        lines.append(f"- stage_only_extract_ids: `{fam['stage_only_extract_ids']}`")
        lines.append(f"- seed_extract_ids: `{fam['seed_extract_ids']}`")
        lines.append(f"- core_queryable_count: `{fam['core_queryable_count']}` roles=`{fam['core_queryable_roles']}`")
        lines.append(f"- recommendation: `{fam['recommendation']}`")
        if fam['suspicion_flags']:
            lines.append(f"- suspicion_flags: `{', '.join(fam['suspicion_flags'])}`")
        lines.append('')
        lines.append('#### stage_seed_extracts')
        lines.append('')
        for ex in fam['stage_seed_extracts']:
            lines.append(f"- extract_id={ex['id']} kind={ex['extract_kind']} reactant_smiles=`{ex['reactant_smiles']}` product_smiles=`{ex['product_smiles']}`")
        lines.append('')
    return '\n'.join(lines) + '\n'


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument('--canonical-db', default='app/labint.db')
    ap.add_argument('--stage-db', default='app/labint_v5_stage.db')
    ap.add_argument('--rejected-json', default='reports/v5_selective_merge/20260418_160546/rejected_families.json')
    ap.add_argument('--diagnose-summary', default='reports/v5_rejected_diagnose/20260418_201257/rejected_diagnosis_summary.json')
    ap.add_argument('--apply-summary', default='reports/gemini_salvage_apply/20260418_201125/gemini_salvage_apply_summary.json')
    args = ap.parse_args()

    root = Path.cwd()
    canonical_db = resolve_path(root, args.canonical_db, 'app/labint.db')
    stage_db = resolve_path(root, args.stage_db, 'app/labint_v5_stage.db')
    rejected_json = resolve_path(root, args.rejected_json, 'reports/v5_selective_merge/*/rejected_families.json')
    diagnose_summary = resolve_path(root, args.diagnose_summary, 'reports/v5_rejected_diagnose/*/rejected_diagnosis_summary.json')
    apply_summary = resolve_path(root, args.apply_summary, 'reports/gemini_salvage_apply/*/gemini_salvage_apply_summary.json')

    if not canonical_db or not canonical_db.exists():
        raise SystemExit('canonical db not found')
    if not stage_db or not stage_db.exists():
        raise SystemExit('stage db not found')
    if not rejected_json or not rejected_json.exists():
        raise SystemExit('rejected_families.json not found')
    if not diagnose_summary or not diagnose_summary.exists():
        raise SystemExit('diagnose summary not found')

    report_dir = create_report_dir(root / 'reports' / 'rejected_stage_seed_analysis')

    rejected_map = load_rejected_map(rejected_json)
    diagnose = load_diagnose(diagnose_summary)
    active_families = list(diagnose['families'].keys())
    applied_from_apply = extract_applied_families_from_apply_summary(apply_summary)
    applied_from_diag = list(diagnose.get('applied_family_exclusions', []))
    applied_families = sorted(set(applied_from_apply) | set(applied_from_diag))
    inactive_base_families = sorted(set(rejected_map.keys()) - set(active_families))

    canon = sqlite3.connect(str(canonical_db))
    stage = sqlite3.connect(str(stage_db))

    family_rows: List[Dict[str, Any]] = []
    try:
        for fam in active_families:
            family_rows.append(analyze_family(canon, stage, fam, rejected_map.get(fam, {}), diagnose['families'].get(fam, {})))
    finally:
        canon.close()
        stage.close()

    family_rows.sort(key=lambda r: (-int(r['changed_case_count'] or 0), r['family_name']))

    summary = {
        'generated_at': datetime.now().isoformat(timespec='seconds'),
        'canonical_db': str(canonical_db),
        'stage_db': str(stage_db),
        'rejected_json': str(rejected_json),
        'diagnose_summary': str(diagnose_summary),
        'apply_summary': str(apply_summary) if apply_summary else '',
        'base_rejected_count': len(rejected_map),
        'active_rejected_count': len(active_families),
        'applied_families': applied_families,
        'inactive_base_families': inactive_base_families,
        'families': family_rows,
    }

    out_json = report_dir / 'rejected_stage_seed_analysis_summary.json'
    out_md = report_dir / 'rejected_stage_seed_analysis_summary.md'
    out_json.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding='utf-8')
    out_md.write_text(render_md(summary), encoding='utf-8')

    print('=' * 76)
    print('ANALYZE REJECTED STAGE SEEDS v2')
    print('=' * 76)
    print(f'canonical_db:   {canonical_db}')
    print(f'stage_db:       {stage_db}')
    print(f'rejected_json:  {rejected_json}')
    print(f'diagnose:       {diagnose_summary}')
    print(f'apply_summary:  {apply_summary}')
    print(f'report_dir:     {report_dir}')
    print('')
    print(f'base rejected families:   {len(rejected_map)}')
    print(f'applied families:         {len(applied_families)}')
    print(f'active rejected families: {len(active_families)}')
    print(f'inactive base families:   {len(inactive_base_families)}')
    for row in family_rows:
        print(f"  - {row['family_name']} | changed_cases={row['changed_case_count']} | cluster={row['cluster']} | stage_only_extract_ids={row['stage_only_extract_ids']} | recommendation={row['recommendation']}")
    print('')
    print(f'summary json: {out_json}')
    print(f'summary md:   {out_md}')
    print('=' * 76)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
