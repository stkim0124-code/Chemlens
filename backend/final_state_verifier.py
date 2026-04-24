import argparse
import datetime as dt
import json
import sqlite3
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

REPORT_SUBDIR = 'final_state_verifier'
RECENT_COMPLETION_NOTE_TAGS = [
    'phase3_family_completion_',
    'phase4_missing_trio_',
    'phase5_shallow_top5_completion_',
    'phase6_shallow_top5_completion_',
    'phase7_shallow_top5_completion_',
    'phase8_shallow_top5_completion_',
    'phase9_shallow_top5_completion_',
    'phase10_shallow_top5_completion_',
    'phase11_shallow_top10_completion_',
    'phase12_shallow_top10_completion_',
    'phase13_shallow_top10_completion_',
    'phase14_shallow_top10_completion_',
    'phase15_shallow_top10_completion_',
]
DEFAULT_FOCUS_FAMILIES = [
    'Alkene (Olefin) Metathesis',
    'Barton-McCombie Radical Deoxygenation Reaction',
    'Fries Rearrangement',
    'Hofmann-Loffler-Freytag Reaction',
    'Houben-Hoesch Reaction',
]
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
    # --- Phase 3a-bis v2 additions (20260421_064001) ---
    'Baeyer-Villiger Oxidation/rearrangement': 'Baeyer-Villiger Oxidation/Rearrangement',  # id=13 -> id=291 (case variant on /rearrangement tail) [3a-bis-v2]
    'Barton Radical Decarboxylation': 'Barton Radical Decarboxylation Reaction',  # id=44 -> id=43 (missing Reaction suffix) [3a-bis-v2]
    'Simmons-Smith Reaction': 'Simmons-Smith Cyclopropanation',  # id=151 -> id=199 (generic -> mechanism-specific) [3a-bis-v2]
    'Suzuki Cross-Coupling': 'Suzuki Cross-Coupling (Suzuki-Miyaura Cross-Coupling)',  # id=148 -> id=217 (full DB canonical with trailing Cross-Coupling) [3a-bis-v2]
    'Baeyer-Villiger Oxidation': 'Baeyer-Villiger Oxidation/Rearrangement',  # id=42 -> id=291 (bare 'Oxidation' → combined Oxidation/Rearrangement) [3b]
    'Favorskii and Homo-Favorskii Rearrangement': 'Favorskii Rearrangement',  # id=359 -> id=811 (umbrella → plain Favorskii; consistent with line 99 mapping 'Favorskii Reaction' → 'Favorskii Rearrangement') [3b]
    # --- end Phase 3a-bis v2 additions ---
    # --- Phase 4 Gate C additions (20260424, task #126) ---
    'Keck Macrolactonization': 'Corey-Nicolaou Macrolactonization',  # macrolactonization triad merge: Keck DCC/DMAP → umbrella CN; same ω-hydroxy-acid→lactone transform, reagent diff invisible in SMILES [gateC]
    'Yamaguchi Macrolactonization': 'Corey-Nicolaou Macrolactonization',  # macrolactonization triad merge: Yamaguchi 2,4,6-trichlorobenzoyl chloride → umbrella CN [gateC]
    'Ullmann Biaryl Ether and Biaryl Amine Synthesis / Condensation': 'Ullmann Reaction / Coupling / Biaryl Synthesis',  # Ullmann biaryl-ether/amine merge; same Cu-mediated mechanism, heteroatom nucleophile is substrate difference [gateC]
    # --- end Phase 4 Gate C additions ---
    # --- Option F-A Alias Round 2 (20260424, T5 residual resolver) ---
    'Stille Carbonylative Cross-Coupling': 'Stille Cross-Coupling (Migita-Kosugi-Stille Coupling)',  # Stille with CO insertion — Sn reagent + halide identical, only CO differentiates; invisible in SMILES alone [optF-A]
    'Wittig Reaction - Schlosser Modification': 'Wittig Reaction',  # Schlosser Z-selective Wittig uses LiBr/RLi but same P=CR2 → C=C transform; SMILES doesn't encode stereoselectivity-inducing additives [optF-A]
    'Wittig Reaction – Schlosser Modification': 'Wittig Reaction',  # en-dash variant [optF-A]
    'Horner-Wadsworth-Emmons Olefination – Still-Gennari Modification': 'Horner-Wadsworth-Emmons Olefination',  # Still-Gennari uses (CF3CH2O)2P(O)CH2CO2R for Z-selectivity — phosphonate substitution invisible in SMILES [optF-A]
    'Horner-Wadsworth-Emmons Olefination - Still-Gennari Modification': 'Horner-Wadsworth-Emmons Olefination',  # hyphen variant [optF-A]
    # --- end Option F-A additions ---
}


def now_stamp() -> str:
    return dt.datetime.now().strftime('%Y%m%d_%H%M%S')


def connect_db(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    return conn


def safe_scalar(conn: sqlite3.Connection, sql: str, params: Tuple[Any, ...] = ()) -> Any:
    try:
        row = conn.execute(sql, params).fetchone()
        if row is None:
            return None
        return row[0]
    except sqlite3.DatabaseError:
        return None


def run_integrity_checks(conn: sqlite3.Connection) -> Dict[str, Optional[str]]:
    out: Dict[str, Optional[str]] = {'quick_check': None, 'integrity_check_head': None}
    for pragma_name, key in [('quick_check', 'quick_check'), ('integrity_check', 'integrity_check_head')]:
        try:
            row = conn.execute(f'PRAGMA {pragma_name}').fetchone()
            if row and row[0] is not None:
                text = str(row[0])
                out[key] = text if len(text) < 500 else text[:500] + ' ...'
        except sqlite3.DatabaseError as e:
            out[key] = f'ERROR: {e}'
    return out


def distinct_pattern_names(conn: sqlite3.Connection) -> List[str]:
    rows = conn.execute(
        'SELECT DISTINCT family_name FROM reaction_family_patterns WHERE COALESCE(family_name,\'\')<>\'\' ORDER BY family_name'
    ).fetchall()
    return [r['family_name'] for r in rows]


def _preferred_casefold_map(names: Iterable[str]) -> Dict[str, str]:
    grouped: Dict[str, List[str]] = defaultdict(list)
    for n in names:
        grouped[n.lower()].append(n)
    preferred_targets = set(MANUAL_ALIAS_OVERRIDES.values())
    out: Dict[str, str] = {}
    for lower, candidates in grouped.items():
        def score(c: str) -> Tuple[int, int, int, str]:
            return (1 if c in preferred_targets else 0, sum(ch.isupper() for ch in c), -sum(ch.islower() for ch in c), c)
        out[lower] = sorted(set(candidates), key=score, reverse=True)[0]
    return out


def canonicalize_name(name: str, preferred_names: Iterable[str]) -> str:
    if not name:
        return name
    if name in MANUAL_ALIAS_OVERRIDES:
        return MANUAL_ALIAS_OVERRIDES[name]
    preferred = list(preferred_names)
    if name in preferred:
        lower_map = _preferred_casefold_map(preferred)
        return lower_map.get(name.lower(), name)
    lower_map = _preferred_casefold_map(preferred)
    return lower_map.get(name.lower(), name)


def build_alias_groups(raw_names: Sequence[str]) -> Tuple[Dict[str, List[str]], List[Dict[str, Any]]]:
    preferred_names = list(raw_names)
    grouped: Dict[str, List[str]] = defaultdict(list)
    alias_events: List[Dict[str, Any]] = []
    seen_pairs = set()
    for raw in raw_names:
        canon = canonicalize_name(raw, preferred_names)
        grouped[canon].append(raw)
        if canon != raw:
            pair = (raw, canon)
            if pair not in seen_pairs:
                seen_pairs.add(pair)
                alias_events.append({'raw_name': raw, 'canonical_name': canon})
    for canon, raws in grouped.items():
        grouped[canon] = sorted(set(raws))
    alias_events.sort(key=lambda x: (x['canonical_name'], x['raw_name']))
    return dict(sorted(grouped.items(), key=lambda kv: kv[0])), alias_events


def sql_in_clause(items: Sequence[str]) -> Tuple[str, Tuple[str, ...]]:
    placeholders = ','.join(['?'] * len(items))
    return placeholders, tuple(items)


def pair_map_from_extract_molecules(conn: sqlite3.Connection) -> Dict[int, Tuple[List[str], List[str]]]:
    out: Dict[int, Tuple[List[str], List[str]]] = {}
    try:
        rows = conn.execute(
            '''
            SELECT extract_id, role, smiles
            FROM extract_molecules
            WHERE queryable=1
              AND smiles IS NOT NULL AND smiles<>''
              AND role IN ('reactant','product')
            ORDER BY extract_id, id
            '''
        ).fetchall()
    except sqlite3.DatabaseError:
        return out
    tmp: Dict[int, Dict[str, List[str]]] = defaultdict(lambda: {'reactant': [], 'product': []})
    for r in rows:
        tmp[int(r['extract_id'])][r['role']].append(r['smiles'])
    for extract_id, bucket in tmp.items():
        out[extract_id] = (sorted(set(bucket['reactant'])), sorted(set(bucket['product'])))
    return out


def family_rows(conn: sqlite3.Connection, raw_family_names: Sequence[str]) -> List[sqlite3.Row]:
    if not raw_family_names:
        return []
    try:
        placeholders, params = sql_in_clause(raw_family_names)
        return conn.execute(
            f'''
            SELECT id, reaction_family_name, extract_kind, reactant_smiles, product_smiles,
                   notes_text, transformation_text, reactants_text, products_text, reagents_text
            FROM reaction_extracts
            WHERE reaction_family_name IN ({placeholders})
            ORDER BY id
            ''',
            params,
        ).fetchall()
    except sqlite3.DatabaseError:
        return []


def recent_completion_families(conn: sqlite3.Connection) -> List[str]:
    clauses = ' OR '.join(['notes_text LIKE ?' for _ in RECENT_COMPLETION_NOTE_TAGS])
    params = tuple(f'%{tag}%' for tag in RECENT_COMPLETION_NOTE_TAGS)
    try:
        rows = conn.execute(
            f'''
            SELECT DISTINCT reaction_family_name
            FROM reaction_extracts
            WHERE reaction_family_name IS NOT NULL
              AND ({clauses})
            ORDER BY reaction_family_name
            ''',
            params,
        ).fetchall()
        return [r['reaction_family_name'] for r in rows if r['reaction_family_name']]
    except sqlite3.DatabaseError:
        return []


def summarize_canonical_family(
    conn: sqlite3.Connection,
    canonical_family: str,
    raw_family_names: Sequence[str],
    mol_pair_map: Dict[int, Tuple[List[str], List[str]]],
) -> Dict[str, Any]:
    rows = family_rows(conn, raw_family_names)
    extract_count = len(rows)
    overview_count = sum(1 for r in rows if r['extract_kind'] == 'canonical_overview')
    application_count = sum(1 for r in rows if r['extract_kind'] == 'application_example')
    mechanism_count = sum(1 for r in rows if r['extract_kind'] == 'mechanism_example')

    extract_with_reactant = 0
    extract_with_product = 0
    extract_with_both = 0
    pair_keys = set()
    curated_summaries = []

    for r in rows:
        reactants: List[str] = []
        products: List[str] = []
        if r['reactant_smiles']:
            reactants = [s for s in str(r['reactant_smiles']).split(' | ') if s]
        elif int(r['id']) in mol_pair_map:
            reactants = mol_pair_map[int(r['id'])][0]
        if r['product_smiles']:
            products = [s for s in str(r['product_smiles']).split(' | ') if s]
        elif int(r['id']) in mol_pair_map:
            products = mol_pair_map[int(r['id'])][1]

        if reactants:
            extract_with_reactant += 1
        if products:
            extract_with_product += 1
        if reactants and products:
            extract_with_both += 1
            pair_keys.add(' || '.join([' . '.join(sorted(set(reactants))), ' . '.join(sorted(set(products)))]))

        notes_text = r['notes_text'] or ''
        if any(tag in notes_text for tag in RECENT_COMPLETION_NOTE_TAGS):
            curated_summaries.append(
                {
                    'id': int(r['id']),
                    'raw_family_name': r['reaction_family_name'],
                    'extract_kind': r['extract_kind'],
                    'transformation_text': r['transformation_text'],
                    'reactants_text': r['reactants_text'],
                    'products_text': r['products_text'],
                }
            )

    queryable_reactants = 0
    queryable_products = 0
    molecule_rows = 0
    if raw_family_names:
        placeholders, params = sql_in_clause(raw_family_names)
        mol = conn.execute(
            f'''
            SELECT
              SUM(CASE WHEN role='reactant' AND queryable=1 AND smiles IS NOT NULL AND smiles<>'' THEN 1 ELSE 0 END) AS queryable_reactants,
              SUM(CASE WHEN role='product' AND queryable=1 AND smiles IS NOT NULL AND smiles<>'' THEN 1 ELSE 0 END) AS queryable_products,
              COUNT(*) AS molecule_rows
            FROM extract_molecules
            WHERE reaction_family_name IN ({placeholders})
            ''',
            params,
        ).fetchone()
        queryable_reactants = int(mol['queryable_reactants'] or 0)
        queryable_products = int(mol['queryable_products'] or 0)
        molecule_rows = int(mol['molecule_rows'] or 0)
    unique_queryable_pair_count = len(pair_keys)

    completion_minimum_pass = (
        overview_count >= 1
        and application_count >= 2
        and unique_queryable_pair_count >= 1
        and queryable_reactants >= 1
        and queryable_products >= 1
    )
    rich_completion_pass = (
        overview_count >= 1
        and application_count >= 2
        and unique_queryable_pair_count >= 3
        and queryable_reactants >= 3
        and queryable_products >= 3
    )
    collision_prone_candidate = (extract_count >= 3 and unique_queryable_pair_count <= 1)
    if extract_count == 0 and molecule_rows == 0:
        completion_bucket = 'missing'
    elif rich_completion_pass:
        completion_bucket = 'rich'
    else:
        completion_bucket = 'shallow'

    return {
        'family': canonical_family,
        'raw_family_names_collapsed': list(raw_family_names),
        'extract_count': extract_count,
        'overview_count': overview_count,
        'application_count': application_count,
        'mechanism_count': mechanism_count,
        'extract_with_reactant': extract_with_reactant,
        'extract_with_product': extract_with_product,
        'extract_with_both': extract_with_both,
        'queryable_reactants': queryable_reactants,
        'queryable_products': queryable_products,
        'molecule_rows': molecule_rows,
        'unique_queryable_pair_count': unique_queryable_pair_count,
        'completion_minimum_pass': completion_minimum_pass,
        'rich_completion_pass': rich_completion_pass,
        'completion_bucket': completion_bucket,
        'collision_prone_candidate': collision_prone_candidate,
        'recent_curated_extract_summaries': curated_summaries,
    }


def build_overview(summary_rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    bucket_counts = Counter(row['completion_bucket'] for row in summary_rows)
    collision = [r['family'] for r in summary_rows if r['collision_prone_candidate']]
    recent = [r['family'] for r in summary_rows if r['recent_curated_extract_summaries']]
    return {
        'missing_count': bucket_counts.get('missing', 0),
        'shallow_count': bucket_counts.get('shallow', 0),
        'rich_count': bucket_counts.get('rich', 0),
        'collision_prone_candidate_count': len(collision),
        'recent_completed_family_count': len(recent),
        'recent_completed_families': recent,
        'collision_prone_candidates': collision,
    }


def gather_alias_residue(conn: sqlite3.Connection, canonical_pattern_names: Iterable[str]) -> Dict[str, Any]:
    canon_set = set(canonical_pattern_names)
    lower_map = {n.lower(): n for n in canonical_pattern_names}
    residue: Dict[str, Any] = {}
    for table in ('reaction_extracts', 'extract_molecules'):
        case_aliases: List[Dict[str, Any]] = []
        unknowns: List[Dict[str, Any]] = []
        try:
            rows = conn.execute(
                f'''
                SELECT reaction_family_name, COUNT(*) AS row_count
                FROM {table}
                WHERE COALESCE(reaction_family_name,'')<>''
                GROUP BY reaction_family_name
                ORDER BY row_count DESC, reaction_family_name
                '''
            ).fetchall()
        except sqlite3.DatabaseError:
            rows = []
        for r in rows:
            name = r['reaction_family_name']
            n = int(r['row_count'])
            if name in canon_set:
                continue
            exact = lower_map.get(name.lower())
            if exact:
                case_aliases.append({'name': name, 'row_count': n, 'canonical_target': exact})
            else:
                unknowns.append({'name': name, 'row_count': n})
        residue[table] = {'case_aliases': case_aliases, 'unknown_family_names': unknowns}
    return residue


def render_md(report: Dict[str, Any]) -> str:
    lines: List[str] = []
    lines.append('# FINAL STATE VERIFIER V2')
    lines.append('')
    lines.append(f"generated_at: {report['generated_at']}")
    lines.append(f"db_path: {report['db_path']}")
    lines.append('')
    lines.append('## Canonical totals')
    totals = report['canonical_totals']
    for key in [
        'registry_family_total_raw',
        'registry_family_distinct_total_raw',
        'registry_duplicate_name_rows_raw',
        'registry_family_distinct_total_canonicalized',
        'families_with_extracts_canonicalized',
        'queryable_family_count_canonicalized',
        'reaction_extract_count',
        'extract_molecule_count',
        'queryable_molecule_count',
        'extracts_with_both_smiles',
    ]:
        lines.append(f'- {key}: {totals.get(key)}')
    lines.append('')
    lines.append('## Integrity')
    for k, v in report['integrity'].items():
        lines.append(f'- {k}: {v}')
    lines.append('')
    lines.append('## Completion buckets (canonicalized)')
    ov = report['completion_overview']
    for key in ['missing_count', 'shallow_count', 'rich_count', 'collision_prone_candidate_count', 'recent_completed_family_count']:
        lines.append(f'- {key}: {ov.get(key)}')
    if ov.get('recent_completed_families'):
        lines.append(f"- recent_completed_families: {'; '.join(ov['recent_completed_families'])}")
    lines.append('')
    if report.get('completion_overview_raw'):
        lines.append('## Completion buckets (raw/un-collapsed registry view)')
        ov_raw = report['completion_overview_raw']
        for key in ['missing_count', 'shallow_count', 'rich_count', 'collision_prone_candidate_count', 'recent_completed_family_count']:
            lines.append(f'- {key}: {ov_raw.get(key)}')
        lines.append('')
    lines.append('## Focus families')
    lines.append('')
    for row in report['focus_families']:
        lines.append(f"### {row['family']}")
        if row.get('raw_family_names_collapsed'):
            lines.append(f"- raw_family_names_collapsed: {'; '.join(row['raw_family_names_collapsed'])}")
        for key in [
            'extract_count', 'overview_count', 'application_count', 'extract_with_both',
            'queryable_reactants', 'queryable_products', 'unique_queryable_pair_count',
            'completion_minimum_pass', 'rich_completion_pass', 'completion_bucket', 'collision_prone_candidate'
        ]:
            lines.append(f'- {key}: {row.get(key)}')
        curated = row.get('recent_curated_extract_summaries') or []
        if curated:
            lines.append('- recent_curated_extract_summaries:')
            for item in curated:
                lines.append(f"  - [{item['id']}] {item['extract_kind']} :: {item['transformation_text']}")
        lines.append('')
    if report.get('canonical_alias_groups_sample'):
        lines.append('## Canonical alias groups sample')
        for item in report['canonical_alias_groups_sample']:
            lines.append(f"- {item['canonical_name']} :: raw_names={' ; '.join(item['raw_names'])}")
        lines.append('')
    if report.get('duplicate_pattern_name_sample'):
        lines.append('## Duplicate family names in reaction_family_patterns (raw rows)')
        for item in report['duplicate_pattern_name_sample']:
            lines.append(f"- {item['family']} :: row_count={item['row_count']}")
        lines.append('')
    lines.append('## Alias residue')
    for table, payload in report['alias_residue'].items():
        case_n = len(payload.get('case_aliases', []))
        unk_n = len(payload.get('unknown_family_names', []))
        lines.append(f'- {table}: case_aliases={case_n}, unknown_family_names={unk_n}')
    lines.append('')
    lines.append('## Top shallow families (canonicalized)')
    for row in report['top_shallow_families']:
        lines.append(
            f"- {row['family']} :: extract_count={row['extract_count']}, overview={row['overview_count']}, application={row['application_count']}, pairs={row['unique_queryable_pair_count']}"
        )
    lines.append('')
    lines.append('## Missing family sample (canonicalized)')
    for fam in report['missing_family_sample']:
        lines.append(f'- {fam}')
    if report.get('missing_family_sample_raw'):
        lines.append('')
        lines.append('## Missing family sample (raw/un-collapsed)')
        for fam in report['missing_family_sample_raw']:
            lines.append(f'- {fam}')
    return '\n'.join(lines) + '\n'


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument('--db', required=True, help='Path to canonical SQLite DB')
    ap.add_argument('--reports-root', default='reports', help='Reports root directory')
    ap.add_argument('--focus-families', default=';'.join(DEFAULT_FOCUS_FAMILIES), help='Semicolon-separated family names')
    args = ap.parse_args()

    db_path = Path(args.db)
    reports_root = Path(args.reports_root)
    stamp = now_stamp()
    out_dir = reports_root / REPORT_SUBDIR / stamp
    out_dir.mkdir(parents=True, exist_ok=True)

    conn = connect_db(db_path)
    raw_pattern_names = distinct_pattern_names(conn)
    alias_groups, alias_collapse_events = build_alias_groups(raw_pattern_names)
    canonical_pattern_names = list(alias_groups.keys())
    mol_pair_map = pair_map_from_extract_molecules(conn)
    integrity = run_integrity_checks(conn)

    duplicate_pattern_name_sample = []
    try:
        dup_rows = conn.execute(
            "SELECT family_name, COUNT(*) AS row_count FROM reaction_family_patterns GROUP BY family_name HAVING COUNT(*)>1 ORDER BY row_count DESC, family_name LIMIT 25"
        ).fetchall()
        duplicate_pattern_name_sample = [{'family': r['family_name'], 'row_count': int(r['row_count'])} for r in dup_rows]
    except sqlite3.DatabaseError:
        duplicate_pattern_name_sample = []

    summaries = [
        summarize_canonical_family(conn, canon, raws, mol_pair_map)
        for canon, raws in alias_groups.items()
    ]
    summaries_by_family = {s['family']: s for s in summaries}

    # raw/un-collapsed view retained for diagnostics
    raw_summaries = [summarize_canonical_family(conn, raw, [raw], mol_pair_map) for raw in raw_pattern_names]

    focus_requested = [f.strip() for f in args.focus_families.split(';') if f.strip()]
    focus_canonical = []
    for fam in focus_requested:
        canon = canonicalize_name(fam, canonical_pattern_names)
        if canon in summaries_by_family and canon not in focus_canonical:
            focus_canonical.append(canon)
    focus = [summaries_by_family[f] for f in focus_canonical]

    alias_residue = gather_alias_residue(conn, canonical_pattern_names)
    completion_overview = build_overview(summaries)
    completion_overview_raw = build_overview(raw_summaries)

    shallow_sorted = sorted(
        [r for r in summaries if r['completion_bucket'] == 'shallow'],
        key=lambda r: (r['completion_minimum_pass'], r['extract_count'], r['unique_queryable_pair_count'], r['family'])
    )
    missing = [r['family'] for r in summaries if r['completion_bucket'] == 'missing']
    missing_raw = [r['family'] for r in raw_summaries if r['completion_bucket'] == 'missing']

    families_with_extracts_canon = sum(1 for r in summaries if r['extract_count'] > 0)
    queryable_family_count_canon = sum(1 for r in summaries if r['queryable_reactants'] > 0 and r['queryable_products'] > 0)

    canonical_alias_groups_sample = [
        {'canonical_name': canon, 'raw_names': raws}
        for canon, raws in alias_groups.items() if len(raws) > 1
    ][:25]

    totals = {
        'registry_family_total_raw': safe_scalar(conn, 'SELECT COUNT(*) FROM reaction_family_patterns'),
        'registry_family_distinct_total_raw': safe_scalar(conn, 'SELECT COUNT(DISTINCT family_name) FROM reaction_family_patterns'),
        'registry_duplicate_name_rows_raw': safe_scalar(conn, "SELECT COUNT(*) FROM (SELECT family_name FROM reaction_family_patterns GROUP BY family_name HAVING COUNT(*)>1)"),
        'registry_family_distinct_total_canonicalized': len(canonical_pattern_names),
        'families_with_extracts_canonicalized': families_with_extracts_canon,
        'queryable_family_count_canonicalized': queryable_family_count_canon,
        'reaction_extract_count': safe_scalar(conn, 'SELECT COUNT(*) FROM reaction_extracts'),
        'extract_molecule_count': safe_scalar(conn, 'SELECT COUNT(*) FROM extract_molecules'),
        'queryable_molecule_count': safe_scalar(conn, "SELECT COUNT(*) FROM extract_molecules WHERE queryable=1 AND smiles IS NOT NULL AND smiles<>'' AND role IN ('reactant','product')"),
        'extracts_with_both_smiles': safe_scalar(conn, "SELECT COUNT(*) FROM reaction_extracts WHERE COALESCE(reactant_smiles,'')<>'' AND COALESCE(product_smiles,'')<>''"),
    }

    report = {
        'generated_at': dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'db_path': str(db_path),
        'integrity': integrity,
        'canonical_totals': totals,
        'completion_overview': completion_overview,
        'completion_overview_raw': completion_overview_raw,
        'focus_families': focus,
        'alias_residue': alias_residue,
        'duplicate_pattern_name_sample': duplicate_pattern_name_sample,
        'canonical_alias_groups_sample': canonical_alias_groups_sample,
        'alias_collapse_events': alias_collapse_events,
        'top_shallow_families': shallow_sorted[:25],
        'missing_family_sample': missing[:50],
        'missing_family_sample_raw': missing_raw[:50],
    }

    json_path = out_dir / 'final_state_verifier.json'
    md_path = out_dir / 'final_state_verifier.md'
    json_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf-8')
    md_path.write_text(render_md(report), encoding='utf-8')

    print('verify json:', json_path.as_posix())
    print('verify md:  ', md_path.as_posix())
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
