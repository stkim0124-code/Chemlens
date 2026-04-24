"""Direct probe of Stille guard multipliers after B-bucket fix."""
import sys
from pathlib import Path
ROOT = Path(r'C:\chemlens\backend')
sys.path.insert(0, str(ROOT / 'app'))
import evidence_search as es

smi = 'C[Sn](CCCC)(CCCC)CCCC.Ic1ccccc1>>CC(=O)c1ccccc1'
r, p = smi.split('>>')
rc = es._count_reaction_features(r); pc = es._count_reaction_features(p)
delta = {k: pc.get(k, 0) - rc.get(k, 0) for k in rc}
print(f'r: Sn={rc.get("tin")} arX={rc.get("aryl_halide")} vnX={rc.get("vinyl_halide")} akX={rc.get("alkyl_halide")} carb={rc.get("carbonyl")}')
print(f'p: Sn={pc.get("tin")} arX={pc.get("aryl_halide")} carb={pc.get("carbonyl")} ester={pc.get("ester")}')
rd = {'reactants': rc, 'products': pc, 'delta': delta}
for fam in ['Stille Cross-Coupling (Migita-Kosugi-Stille Coupling)', 'Friedel-Crafts Acylation',
            'Stille Cross-Coupling', 'Stille Carbonylative Cross-Coupling']:
    mult, notes = es._family_delta_adjustment(fam, rd)
    print(f'[{fam}] mult={mult:.3f} notes={notes}')
