# Gemini salvage summary

- model: `gemini-2.5-pro`
- diag_summary: `C:\chemlens\backend\reports\v5_rejected_diagnose\20260418_201257\rejected_diagnosis_summary.json`
- total_families: 2
- generated_at: `2026-04-18T20:50:33`

## Enyne Metathesis

- changed_cases: 2
- first_case: `barton_radical_rxn_353_exact_extract`
- first_top1: `Enyne Metathesis`
- cluster: `barton`

### C1

- substrate_smiles: `["C=CCCCCCC", "C#CCCCC"]`
- product_smiles: `["CCCC/C=C/C=C/CCCCCC"]`
- optional_reagent_smiles: `["[Ru]"]`
- rationale: Classic intermolecular cross-enyne metathesis between a terminal alkene and alkyne.
- collision_avoidance_note: Avoids radical motifs (Barton-like) and halogenated substrates. Simple hydrocarbons.

### C2

- substrate_smiles: `["C=CCCCCCC", "C#CCCCC"]`
- product_smiles: `["CCCC/C=C/C=C/CCCCCC"]`
- optional_reagent_smiles: `["[Ru]"]`
- rationale: Classic intermolecular cross-enyne metathesis between a terminal alkene and alkyne.
- collision_avoidance_note: Avoids radical conditions (Barton-like) and halogenated substrates.

### C3

- substrate_smiles: `["C=CCCCCCC", "C#CCCCC"]`
- product_smiles: `["CCCC/C=C/C=C/CCCCCC"]`
- optional_reagent_smiles: `["[Ru]"]`
- rationale: Classic intermolecular cross-enyne metathesis between a terminal alkene and alkyne.
- collision_avoidance_note: Avoids radical-like substrates (e.g., Barton esters) and halogen-rich molecules.

## Hofmann-Loffler-Freytag Reaction

- changed_cases: 2
- first_case: `barton_radical_rxn_353_exact_extract`
- first_top1: `Hofmann-Loffler-Freytag Reaction`
- cluster: `barton`

### C1

- substrate_smiles: `["CCCCN(C)Cl"]`
- product_smiles: `["CN1CCCC1"]`
- optional_reagent_smiles: `["O=S(=O)(O)O"]`
- rationale: Canonical HLF reaction. N-chloroamine cyclizes to a pyrrolidine via 1,5-HAT.
- collision_avoidance_note: Avoids Barton-like motifs. Simple N-haloamine substrate, acid-catalyzed cyclization.

### C2

- substrate_smiles: `["CCCCN(C)Cl"]`
- product_smiles: `["CN1CCCC1"]`
- optional_reagent_smiles: `["O=S(=O)(O)O"]`
- rationale: Canonical HLF reaction. N-chloroamine undergoes acid-catalyzed intramolecular C-H amination via a nitrogen radical to form a pyrrolidine ring.
- collision_avoidance_note: Avoids Barton-like motifs. Focuses on the core N-haloamine to cyclic amine transformation under acidic conditions, distinct from radical halogenation.

### C3

- substrate_smiles: `["CCCCN(C)Cl"]`
- product_smiles: `["CN1CCCC1"]`
- optional_reagent_smiles: `["O=S(=O)(O)O"]`
- rationale: Canonical HLF reaction. N-chloroamine cyclizes to a pyrrolidine via intramolecular C-H amination under acidic conditions.
- collision_avoidance_note: Avoids Barton-like motifs (e.g., thiohydroxamates, peroxides, hv) and decarboxylative halogenation. Simple N-haloamine substrate.
