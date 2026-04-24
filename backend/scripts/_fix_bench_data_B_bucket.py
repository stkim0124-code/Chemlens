"""B-bucket benchmark data fixes.

Three broken test cases in the three bench JSONs are corrected in-place:

1. Beckmann Rearrangement (id 1435 in adm/brd/cov):
   Old reactant "O=NC=C1CCCCC1" is nitroso-cyclohexene, not an oxime. Fixed
   to "ON=C1CCCCC1" (cyclohexanone oxime → caprolactam).

2. Stille Cross-Coupling (id 1303 in adm/brd/cov):
   Old reactant "CC(C)(C)C" (tBu) had no Sn. Fixed to methylstannane
   "C[Sn](CCCC)(CCCC)CCCC". Product acetophenone is the Stille-Carbonylative
   variant; since Option F-A merged carbonylative into main Stille, this
   represents the merged canonical correctly.

3. Stille Cross-Coupling carbonylative variant (id 1304 in cov):
   Same fix as #2 — replace tBu with methylstannane.

Output: benchmark JSONs updated in-place. Backups written as
  <original>.bak_B_pre_<ts>.json

"""
from __future__ import annotations
import json, time
from pathlib import Path

BENCH_DIR = Path(r'C:\chemlens\backend\benchmark')
FIXES = [
    # (case_id_substring, old_smiles, new_smiles, note)
    ('_beckmann_rearrangement_1435',
     'O=NC=C1CCCCC1>>O=C1CCCCCN1',
     'ON=C1CCCCC1>>O=C1CCCCCN1',
     'Fix nitroso→oxime: cyclohexanone oxime → caprolactam'),
    ('_stille_cross-coupling_(migita-kosugi-stille_coupling)_1303',
     'CC(C)(C)C.Ic1ccccc1>>CC(=O)c1ccccc1',
     'C[Sn](CCCC)(CCCC)CCCC.Ic1ccccc1>>CC(=O)c1ccccc1',
     'Restore Sn: methylstannane + iodobenzene → acetophenone (Carbonylative, merged alias)'),
    ('_stille_cross-coupling_(migita-kosugi-stille_coupling)_1304',
     'CC(C)(C)C.COc1ccc(Br)cc1>>COc1ccc(C(C)=O)cc1',
     'C[Sn](CCCC)(CCCC)CCCC.COc1ccc(Br)cc1>>COc1ccc(C(C)=O)cc1',
     'Restore Sn: methylstannane + p-anisyl bromide → p-methoxyacetophenone'),
]

FILES = [
    BENCH_DIR / 'family_admission_benchmark.json',
    BENCH_DIR / 'named_reaction_benchmark_broad.json',
    BENCH_DIR / 'corpus_coverage_benchmark.json',
]


def main():
    ts = time.strftime('%Y%m%d_%H%M%S')
    for f in FILES:
        if not f.exists():
            print(f'SKIP (missing): {f}')
            continue
        # backup
        bak = f.with_suffix(f'.bak_B_pre_{ts}.json')
        bak.write_text(f.read_text(encoding='utf-8'), encoding='utf-8')
        d = json.loads(f.read_text(encoding='utf-8'))
        cases = d.get('cases', [])
        for c in cases:
            for cid_sub, old, new, note in FIXES:
                if cid_sub in c.get('case_id', '') and c.get('reaction_smiles') == old:
                    c['reaction_smiles'] = new
                    c.setdefault('data_fix_notes', []).append(f'B-bucket {ts}: {note}')
                    print(f'  patched {c["case_id"]}')
        f.write_text(json.dumps(d, ensure_ascii=False, indent=2), encoding='utf-8')
        print(f'wrote {f.name}')


if __name__ == '__main__':
    main()
