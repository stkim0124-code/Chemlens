import argparse
import datetime as dt
import json
from html import escape
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple

REPORT_SUBDIR = 'family_completion_dashboard'
VERIFIER_SUBDIR = 'final_state_verifier'

MANUAL_ALIAS_OVERRIDES = {

    'Alkene (olefin) Metathesis': 'Alkene (Olefin) Metathesis',
    'Barton-Mccombie Radical Deoxygenation Reaction': 'Barton-McCombie Radical Deoxygenation Reaction',
    'Fries-, Photo-Fries, and Anionic Ortho-Fries Rearrangement': 'Fries Rearrangement',
    'Hofmann-L\u00f6ffler-Freytag Reaction (Remote Functionalization)': 'Hofmann-Loffler-Freytag Reaction',
    'Houben-Hoesch Reaction/Synthesis': 'Houben-Hoesch Reaction',
    'Krapcho Dealkoxycarbonylation': 'Krapcho Dealkoxycarbonylation (Krapcho Reaction)',
    'Krapcho Reaction': 'Krapcho Dealkoxycarbonylation (Krapcho Reaction)',
    'Alder Ene Reaction': 'Alder (Ene) Reaction (Hydro-Allyl Addition)',
    'Alder (ene) Reaction': 'Alder (Ene) Reaction (Hydro-Allyl Addition)',
    'Amadori Rearrangement': 'Amadori Reaction / Rearrangement',
    'Arbuzov Reaction': 'Arbuzov Reaction (Michaelis-Arbuzov Reaction)',
    'Aza-Claisen Rearrangement': 'Aza-Claisen Rearrangement (3-Aza-Cope Rearrangement)',
    'Balz-Schiemann Reaction': 'Balz-Schiemann Reaction (Schiemann Reaction)',
    'Buchner Method of Ring Expansion': 'Buchner Method of Ring Expansion (Buchner Reaction)',
    'Buchner Reaction': 'Buchner Method of Ring Expansion (Buchner Reaction)',
    'Carroll Rearrangement': 'Carroll Rearrangement (Kimel-Cope Rearrangement)',
    'Kimel-Cope Rearrangement': 'Carroll Rearrangement (Kimel-Cope Rearrangement)',
    'Chichibabin Amination Reaction': 'Chichibabin Amination Reaction (Chichibabin Reaction)',
    'Chichibabin Reaction': 'Chichibabin Amination Reaction (Chichibabin Reaction)',
    'Claisen Condensation': 'Claisen Condensation / Claisen Reaction',
    'Claisen Reaction': 'Claisen Condensation / Claisen Reaction',
    'Charette Cyclopropanation': 'Charette Asymmetric Cyclopropanation',
    'Chugaev Elimination': 'Chugaev Elimination Reaction (Xanthate Ester Pyrolysis)',
    'Chugaev Elimination Reaction': 'Chugaev Elimination Reaction (Xanthate Ester Pyrolysis)',
    'Combes Reaction': 'Combes Quinoline Synthesis',
    'Dakin Oxidation / Dakin Reaction': 'Dakin Oxidation',
    'Cope Elimination': 'Cope Elimination / Cope Reaction',
    'CBS Reduction': 'Corey-Bakshi-Shibata Reduction (CBS Reduction)',
    'Corey-Bakshi-Shibata Reduction': 'Corey-Bakshi-Shibata Reduction (CBS Reduction)',
    'Corey-Chaykovsky Reaction': 'Corey-Chaykovsky Epoxidation and Cyclopropanation',
    'Corey-Chaykovsky Epoxidation': 'Corey-Chaykovsky Epoxidation and Cyclopropanation',
    'Corey-Fuchs Reaction': 'Corey-Fuchs Alkyne Synthesis',
    'Corey-Kim Reaction': 'Corey-Kim Oxidation',
    'Cope Reaction': 'Cope Rearrangement',
    'Corey-Nicolaou Reaction': 'Corey-Nicolaou Macrolactonization',
    'Corey-Winter Reaction': 'Corey-Winter Olefination',
    'Dakin-West': 'Dakin-West Reaction',
    "Davis Oxaziridine Oxidations": "Davis' Oxaziridine Oxidations",
    "Davis Oxaziridine Oxidation": "Davis' Oxaziridine Oxidations",
    'Darzens Reaction': 'Darzens Glycidic Ester Condensation',
    'Danheiser Annulation': 'Danheiser Benzannulation',
    'Criegee Reaction': 'Criegee Oxidation',
    'Danishefsky Diene Cycloaddition': "Danishefsky's Diene Cycloaddition",
    'De Mayo Cycloaddition (Enone-Alkene [2+2] Photocycloaddition)': 'De Mayo Cycloaddition',
    'Demjanov and Tiffeneau-Demjanov Rearrangement': 'Demjanov Rearrangement and Tiffeneau-Demjanov Rearrangement',
    'Dess-Martin Reaction': 'Dess-Martin Oxidation',
    'Enyne Ring-Closing Metathesis': 'Enyne Metathesis',
    'Eschenmoser Claisen Rearrangement': 'Eschenmoser-Claisen Rearrangement',
    'Eschweiler-Clarke Methylation (Reductive Alkylation)': 'Eschweiler-Clarke Methylation',
    'Feist-Benary Furan Synthesis': 'Feist-Bénary Furan Synthesis',
    'Fischer Indole Reaction': 'Fischer Indole Synthesis',
    'Fleming Tamao Oxidation': 'Fleming-Tamao Oxidation',
    'Friedel Crafts Acylation': 'Friedel-Crafts Acylation',
    'Friedel Crafts Alkylation': 'Friedel-Crafts Alkylation',
    'Gabriel Reaction': 'Gabriel Synthesis',
    'Glaser Reaction': 'Glaser Coupling',
    'Hell Volhard Zelinsky Reaction': 'Hell-Volhard-Zelinsky Reaction',
    'HVZ Reaction': 'Hell-Volhard-Zelinsky Reaction',
    'Hantzsch Synthesis': 'Hantzsch Dihydropyridine Synthesis',
    'Hetero Diels-Alder Cycloaddition': 'Hetero Diels-Alder Cycloaddition (HDA)',
    'Hetero Diels Alder Cycloaddition': 'Hetero Diels-Alder Cycloaddition (HDA)',
    'Hofmann Elimination Reaction': 'Hofmann Elimination',
    'Hofmann Rearrangement Reaction': 'Hofmann Rearrangement',
    'Horner-Wadsworth-Emmons': 'Horner-Wadsworth-Emmons Olefination',
    'Horner-Wadsworth-Emmons Reaction': 'Horner-Wadsworth-Emmons Olefination',
    'Favorskii Reaction': 'Favorskii Rearrangement',
    'Ferrier Reaction/Rearrangement': 'Ferrier Reaction',
    'Evans Aldol': 'Evans Aldol Reaction',
    'Furukawa Simmons-Smith Modification': 'Furukawa Modification',
    'Gattermann Formylation': 'Gattermann and Gattermann-Koch Formylation',
    'Gattermann-Koch Formylation': 'Gattermann and Gattermann-Koch Formylation',
    'Hunsdiecker-Borodin Reaction': 'Hunsdiecker Reaction',
    'Katsuki-Jacobsen Epoxidation': 'Jacobsen-Katsuki Epoxidation',
    'Jacobsen-Katsuki Reaction': 'Jacobsen-Katsuki Epoxidation',
    'Jones Oxidation/Oxidation of Alcohols by Chromium Reagents': 'Jones Oxidation',
    'Jacobsen HKR': 'Jacobsen Hydrolytic Kinetic Resolution',
    'Hydrolytic Kinetic Resolution': 'Jacobsen Hydrolytic Kinetic Resolution',
    'Johnson Claisen Rearrangement': 'Johnson-Claisen Rearrangement',
    'Japp Klingemann Reaction': 'Japp-Klingemann Reaction',
    'Still-Gennari Modification': 'Horner-Wadsworth-Emmons Olefination – Still-Gennari Modification',
    'Still-Gennari Olefination': 'Horner-Wadsworth-Emmons Olefination – Still-Gennari Modification',
    'Julia-Lythgoe Reaction': 'Julia-Lythgoe Olefination',
}


def now_stamp() -> str:
    return dt.datetime.now().strftime('%Y%m%d_%H%M%S')


def load_json(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding='utf-8'))


def write_json(path: Path, obj: Any) -> None:
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding='utf-8')


def find_latest_verifier_json(backend_root: Path) -> Path:
    report_root = backend_root / 'reports' / VERIFIER_SUBDIR
    candidates = sorted(report_root.glob('*/final_state_verifier.json'))
    if not candidates:
        raise FileNotFoundError(f'No final_state_verifier.json found under: {report_root}')
    return candidates[-1]


def collect_known_canonical_names(verifier: Dict[str, Any]) -> List[str]:
    names: List[str] = []
    for item in verifier.get('focus_families', []):
        if item.get('family'):
            names.append(item['family'])
    for name in verifier.get('completion_overview', {}).get('recent_completed_families', []):
        if name:
            names.append(name)
    for item in verifier.get('duplicate_pattern_name_sample', []):
        if item.get('family'):
            names.append(item['family'])
    for item in verifier.get('top_shallow_families', []):
        if item.get('family'):
            names.append(item['family'])
    for item in verifier.get('canonical_alias_groups_sample', []):
        if item.get('canonical_name'):
            names.append(item['canonical_name'])
    out, seen = [], set()
    for n in names:
        if n not in seen:
            seen.add(n)
            out.append(n)
    return out


def _preferred_casefold_map(names: Iterable[str]) -> Dict[str, str]:
    grouped = {}
    preferred_targets = set(MANUAL_ALIAS_OVERRIDES.values())
    for n in names:
        grouped.setdefault(n.lower(), []).append(n)
    out = {}
    for lower, candidates in grouped.items():
        def score(c: str):
            return (1 if c in preferred_targets else 0, sum(ch.isupper() for ch in c), -sum(ch.islower() for ch in c), c)
        out[lower] = sorted(set(candidates), key=score, reverse=True)[0]
    return out


def canonicalize_name(name: str, known_names: Iterable[str]) -> str:
    if not name:
        return name
    if name in MANUAL_ALIAS_OVERRIDES:
        return MANUAL_ALIAS_OVERRIDES[name]
    lower_map = _preferred_casefold_map(known_names)
    return lower_map.get(name.lower(), name)


def canonicalize_name_list(names: List[str], known_names: Iterable[str]) -> Tuple[List[str], List[Dict[str, str]]]:
    out, alias_events, seen = [], [], set()
    for name in names:
        canon = canonicalize_name(name, known_names)
        if canon != name:
            alias_events.append({'raw_name': name, 'canonical_name': canon})
        if canon not in seen:
            seen.add(canon)
            out.append(canon)
    return out, alias_events


def build_dashboard(verifier: Dict[str, Any], verifier_json_path: Path) -> Dict[str, Any]:
    known_names = collect_known_canonical_names(verifier)
    overview = verifier.get('completion_overview', {})
    overview_raw = verifier.get('completion_overview_raw', {})

    raw_missing = verifier.get('missing_family_sample', [])
    canonical_missing, alias_events = canonicalize_name_list(raw_missing, known_names)
    raw_missing_uncollapsed = verifier.get('missing_family_sample_raw', [])

    canonical_recent_completed, completed_alias_events = canonicalize_name_list(
        overview.get('recent_completed_families', []), known_names
    )
    alias_events.extend(completed_alias_events)

    focus_families = verifier.get('focus_families', [])
    focus_family_name_set = {item.get('family') for item in focus_families if item.get('family')}
    canonical_missing = [
        n for n in canonical_missing
        if n not in focus_family_name_set and n not in set(canonical_recent_completed)
    ]

    duplicate_rows = verifier.get('duplicate_pattern_name_sample', [])
    alias_groups = verifier.get('canonical_alias_groups_sample', [])

    display_notes = [
        'This dashboard is derived from final_state_verifier JSON and is intended as a display/triage layer, not a replacement for canonical DB truth.',
        'Verifier v2 already collapses registry aliases before missing/shallow/rich classification; dashboard mainly renders canonical family view.',
        'If raw/un-collapsed counts are present, they are shown as diagnostics only.',
    ]

    return {
        'generated_at': dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'source_verifier_json': str(verifier_json_path),
        'canonical_totals': verifier.get('canonical_totals', {}),
        'completion_bucket_counts': {
            'missing_count_canonicalized': int(overview.get('missing_count', 0) or 0),
            'shallow_count_canonicalized': int(overview.get('shallow_count', 0) or 0),
            'rich_count_canonicalized': int(overview.get('rich_count', 0) or 0),
            'collision_prone_candidate_count_canonicalized': int(overview.get('collision_prone_candidate_count', 0) or 0),
            'recent_completed_family_count_canonicalized': int(overview.get('recent_completed_family_count', 0) or 0),
            'missing_count_raw_uncollapsed': int(overview_raw.get('missing_count', 0) or 0),
            'missing_sample_display_count': len(canonical_missing),
        },
        'focus_families': focus_families,
        'recent_completed_families_canonicalized': canonical_recent_completed,
        'missing_family_sample_canonicalized': canonical_missing,
        'missing_family_sample_raw_uncollapsed': raw_missing_uncollapsed,
        'top_shallow_families_canonicalized': verifier.get('top_shallow_families', []),
        'duplicate_pattern_name_sample': duplicate_rows,
        'canonical_alias_groups_sample': alias_groups,
        'display_notes': display_notes,
    }


def render_markdown(dash: Dict[str, Any]) -> str:
    lines: List[str] = []
    lines.append('# FAMILY COMPLETION DASHBOARD V2')
    lines.append('')
    lines.append(f"generated_at: {dash['generated_at']}")
    lines.append(f"source_verifier_json: {dash['source_verifier_json']}")
    lines.append('')
    lines.append('## Summary')
    for key, value in dash['canonical_totals'].items():
        lines.append(f'- {key}: {value}')
    lines.append('')
    lines.append('## Completion bucket counts')
    for key, value in dash['completion_bucket_counts'].items():
        lines.append(f'- {key}: {value}')
    lines.append('')
    lines.append('## Recent completed families (canonicalized)')
    for name in dash['recent_completed_families_canonicalized']:
        lines.append(f'- {name}')
    lines.append('')
    lines.append('## Missing family sample (canonicalized)')
    for name in dash['missing_family_sample_canonicalized']:
        lines.append(f'- {name}')
    lines.append('')
    if dash.get('missing_family_sample_raw_uncollapsed'):
        lines.append('## Missing family sample (raw/un-collapsed)')
        for name in dash['missing_family_sample_raw_uncollapsed']:
            lines.append(f'- {name}')
        lines.append('')
    lines.append('## Focus families')
    for item in dash['focus_families']:
        lines.append('')
        lines.append(f"### {item['family']}")
        for key in [
            'extract_count', 'overview_count', 'application_count', 'extract_with_both',
            'queryable_reactants', 'queryable_products', 'unique_queryable_pair_count',
            'completion_minimum_pass', 'rich_completion_pass', 'completion_bucket',
            'collision_prone_candidate'
        ]:
            lines.append(f'- {key}: {item.get(key)}')
    lines.append('')
    lines.append('## Top shallow families (canonicalized)')
    for item in dash['top_shallow_families_canonicalized'][:25]:
        lines.append(
            f"- {item['family']} :: extract_count={item.get('extract_count')}, overview={item.get('overview_count')}, application={item.get('application_count')}, pairs={item.get('unique_queryable_pair_count')}"
        )
    lines.append('')
    if dash.get('canonical_alias_groups_sample'):
        lines.append('## Canonical alias groups sample')
        for item in dash['canonical_alias_groups_sample']:
            lines.append(f"- {item['canonical_name']} :: raw_names={' ; '.join(item['raw_names'])}")
        lines.append('')
    if dash.get('duplicate_pattern_name_sample'):
        lines.append('## Duplicate family names in reaction_family_patterns (raw rows)')
        for item in dash['duplicate_pattern_name_sample']:
            lines.append(f"- {item['family']} :: row_count={item['row_count']}")
        lines.append('')
    return '\n'.join(lines) + '\n'


def render_html(dash: Dict[str, Any]) -> str:
    def li(items: List[str]) -> str:
        if not items:
            return '<li><em>none</em></li>'
        return ''.join(f'<li>{escape(str(i))}</li>' for i in items)
    def kv(d: Dict[str, Any]) -> str:
        return ''.join(f'<tr><td>{escape(str(k))}</td><td>{escape(str(v))}</td></tr>' for k, v in d.items())
    rows = []
    for item in dash['focus_families']:
        rows.append(
            '<tr>' + ''.join([
                f"<td>{escape(str(item.get('family')))}</td>",
                f"<td>{escape(str(item.get('extract_count')))}</td>",
                f"<td>{escape(str(item.get('overview_count')))}</td>",
                f"<td>{escape(str(item.get('application_count')))}</td>",
                f"<td>{escape(str(item.get('unique_queryable_pair_count')))}</td>",
                f"<td>{escape(str(item.get('completion_bucket')))}</td>",
                f"<td>{escape(str(item.get('rich_completion_pass')))}</td>",
            ]) + '</tr>'
        )
    shallow_rows = []
    for item in dash['top_shallow_families_canonicalized'][:25]:
        shallow_rows.append(
            '<tr>' + ''.join([
                f"<td>{escape(str(item.get('family')))}</td>",
                f"<td>{escape(str(item.get('extract_count')))}</td>",
                f"<td>{escape(str(item.get('overview_count')))}</td>",
                f"<td>{escape(str(item.get('application_count')))}</td>",
                f"<td>{escape(str(item.get('unique_queryable_pair_count')))}</td>",
            ]) + '</tr>'
        )
    return f'''<!DOCTYPE html>
<html lang="en"><head><meta charset="utf-8"><title>Family Completion Dashboard V2</title>
<style>
body {{ font-family: Arial, sans-serif; margin: 24px; color:#222; }}
.card {{ border:1px solid #ddd; border-radius:10px; padding:16px; margin-bottom:18px; }}
h1,h2,h3 {{ margin:0 0 12px 0; }}
table {{ border-collapse: collapse; width:100%; }}
th,td {{ border:1px solid #ddd; padding:8px; text-align:left; vertical-align:top; }}
th {{ background:#f5f5f5; }}
.small {{ color:#666; font-size:0.92rem; }}
.grid {{ display:grid; grid-template-columns: repeat(2, minmax(0,1fr)); gap:18px; }}
</style></head><body>
<h1>Family Completion Dashboard V2</h1>
<p class="small">generated_at: {escape(dash['generated_at'])}<br>source_verifier_json: {escape(dash['source_verifier_json'])}</p>
<div class="grid">
<div class="card"><h2>Summary</h2><table>{kv(dash['canonical_totals'])}</table></div>
<div class="card"><h2>Completion bucket counts</h2><table>{kv(dash['completion_bucket_counts'])}</table></div>
</div>
<div class="card"><h2>Recent completed families (canonicalized)</h2><ul>{li(dash['recent_completed_families_canonicalized'])}</ul></div>
<div class="card"><h2>Missing family sample (canonicalized)</h2><ul>{li(dash['missing_family_sample_canonicalized'])}</ul></div>
<div class="card"><h2>Focus families</h2><table><tr><th>Family</th><th>Extracts</th><th>Overview</th><th>Application</th><th>Pairs</th><th>Bucket</th><th>Rich</th></tr>{''.join(rows)}</table></div>
<div class="card"><h2>Top shallow families (canonicalized)</h2><table><tr><th>Family</th><th>Extracts</th><th>Overview</th><th>Application</th><th>Pairs</th></tr>{''.join(shallow_rows)}</table></div>
</body></html>'''


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument('--backend-root', default='.', help='Backend root directory')
    ap.add_argument('--verifier-json', default='', help='Explicit final_state_verifier.json path')
    args = ap.parse_args()

    backend_root = Path(args.backend_root)
    verifier_json = Path(args.verifier_json) if args.verifier_json else find_latest_verifier_json(backend_root)
    verifier = load_json(verifier_json)
    dash = build_dashboard(verifier, verifier_json)

    stamp = now_stamp()
    out_dir = backend_root / 'reports' / REPORT_SUBDIR / stamp
    out_dir.mkdir(parents=True, exist_ok=True)
    json_path = out_dir / 'family_completion_dashboard.json'
    md_path = out_dir / 'family_completion_dashboard.md'
    html_path = out_dir / 'family_completion_dashboard.html'

    write_json(json_path, dash)
    md_path.write_text(render_markdown(dash), encoding='utf-8')
    html_path.write_text(render_html(dash), encoding='utf-8')

    print('dashboard json:', str(json_path))
    print('dashboard md:  ', str(md_path))
    print('dashboard html:', str(html_path))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
