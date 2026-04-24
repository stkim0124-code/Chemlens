"""Validate Phase 4 Gate B SMARTS patterns with RDKit.

5 new feature SMARTS must parse. Also test positive+negative hits on
representative molecules so we know the pattern matches what it claims to.
"""
from rdkit import Chem

PATTERNS = {
    "amine_oxide": "[NX4+]([#6])([#6])([#6])[O-]",
    "diazonium":   "[NX2+]#[NX1]",
    "coumarin":    "O=c1ccc2ccccc2o1",
    "xanthate":    "[#16X1]=[#6X3]([OX2][#6])[SX2][#6]",
    "indole":      "c1ccc2[nH]ccc2c1",
}

# {name: ([should-match SMILES], [should-NOT-match SMILES])}
TESTS = {
    "amine_oxide": (
        ["C[N+](C)(C)[O-]", "C[N+](C)(C-c1ccccc1)[O-]"],
        ["CN(C)C", "c1ccccc1"],
    ),
    "diazonium": (
        ["c1ccccc1[N+]#N", "CC[N+]#N"],
        ["c1ccc(N)cc1", "N#N"],
    ),
    "coumarin": (
        ["O=c1ccc2ccccc2o1", "O=C1Oc2ccccc2C=C1"],
        ["c1ccccc1O", "O=C1CCc2ccccc2O1"],
    ),
    "xanthate": (
        ["CCOC(=S)SCC", "COC(=S)SC"],
        ["CCOC(=O)OCC", "CC(=S)C"],
    ),
    "indole": (
        ["c1ccc2[nH]ccc2c1", "c1ccc2[nH]c(C)cc2c1"],
        ["c1ccc2[nH]nc2c1", "c1ccccc1"],
    ),
}


def main() -> int:
    print(f"{'name':15s}  {'parse':6s}  pos-hits  neg-hits  verdict")
    print("-" * 65)
    all_ok = True
    for name, smarts in PATTERNS.items():
        patt = Chem.MolFromSmarts(smarts)
        parse_ok = patt is not None
        pos_hits = 0
        neg_hits = 0
        if parse_ok:
            pos_smi, neg_smi = TESTS[name]
            for smi in pos_smi:
                m = Chem.MolFromSmiles(smi)
                if m is not None and m.HasSubstructMatch(patt):
                    pos_hits += 1
            for smi in neg_smi:
                m = Chem.MolFromSmiles(smi)
                if m is not None and m.HasSubstructMatch(patt):
                    neg_hits += 1
        verdict = "OK" if (parse_ok and pos_hits == len(TESTS[name][0]) and neg_hits == 0) else "FAIL"
        if verdict == "FAIL":
            all_ok = False
        print(f"{name:15s}  {'yes' if parse_ok else 'no':6s}  "
              f"{pos_hits}/{len(TESTS[name][0])}       "
              f"{neg_hits}/{len(TESTS[name][1])}       {verdict}")
        print(f"                  smarts: {smarts}")
    print("-" * 65)
    print("ALL PASS" if all_ok else "SOME FAIL")
    return 0 if all_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
