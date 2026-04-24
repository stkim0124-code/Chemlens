"""Direct probe of Mitsunobu vs Aldol guard multipliers."""
import sys
from pathlib import Path
ROOT = Path(r'C:\chemlens\backend')
sys.path.insert(0, str(ROOT / 'app'))
import evidence_search as es

cases = [
    ('Mitsunobu', 'CCCCCC[C@H](C)O.O=C(O)c1ccccc1>>CCCCCC[C@@H](C)OC(=O)c1ccccc1',
     ['Mitsunobu Reaction', 'Aldol Reaction', 'Regitz Diazo Transfer']),
    ('Biginelli', 'CCOC(=O)CC(C)=O.NC(N)=O.O=Cc1ccccc1>>CCOC(=O)C1=C(C)NC(=O)NC1c1ccccc1',
     ['Biginelli Reaction', 'Darzens Glycidic Ester Condensation', 'Knoevenagel Condensation']),
    ('McMurry', 'CC(=O)c1ccccc1>>CC(=C(C)c1ccccc1)c1ccccc1',
     ['McMurry Coupling', 'Wolff-Kishner Reduction']),
    ('Passerini', 'CC(=O)O.O=Cc1ccccc1.[C-]#[N+]C(C)(C)C>>CC(=O)OC(C(=O)NC(C)(C)C)c1ccccc1',
     ['Passerini Multicomponent Reaction', 'Aldol Reaction']),
    ('NHK', 'IC=Cc1ccccc1.O=Cc1ccccc1>>OC(C=Cc1ccccc1)c1ccccc1',
     ['Nozaki-Hiyama-Kishi Reaction', 'Sharpless Asymmetric Dihydroxylation']),
    ('Chugaev', 'CCSC(=S)OC1CCCCC1>>C1=CCCCC1',
     ['Chugaev Elimination Reaction (Xanthate Ester Pyrolysis)', 'Ring-Closing Metathesis']),
]
for label, smi, fams in cases:
    print(f'\n=== {label}: {smi}')
    r, p = smi.split('>>')
    rc = es._count_reaction_features(r); pc = es._count_reaction_features(p)
    delta = {k: pc.get(k, 0) - rc.get(k, 0) for k in rc}
    print(f"  r alc={rc.get('alcohol')} cooh={rc.get('carboxylic_acid')} iso={rc.get('isocyanide')} urea={rc.get('urea')} xan={rc.get('xanthate')} cbn={rc.get('carbonyl')} vhl={rc.get('vinyl_halide')} arx={rc.get('aryl_halide')}")
    print(f"  p alc={pc.get('alcohol')} ester={pc.get('ester')} alkene={pc.get('alkene')} alkyne={pc.get('alkyne')}")
    print(f"  d alc={delta.get('alcohol')} ester={delta.get('ester')} cbn={delta.get('carbonyl')} alkene={delta.get('alkene')}")
    rd = {'reactants': rc, 'products': pc, 'delta': delta}
    for fam in fams:
        mult, notes = es._family_delta_adjustment(fam, rd)
        print(f"  [{fam}] mult={mult:.3f}  notes={notes}")
