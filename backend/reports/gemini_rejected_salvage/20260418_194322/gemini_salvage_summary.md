# Gemini salvage summary

- model: `gemini-2.5-pro`
- diag_summary: `C:\chemlens\backend\reports\v5_rejected_diagnose\20260418_185710\rejected_diagnosis_summary.json`
- total_families: 2
- generated_at: `2026-04-18T19:44:06`

## Finkelstein Reaction

- changed_cases: 3
- first_case: `barton_radical_rxn_353_exact_extract`
- first_top1: `Finkelstein Reaction`
- cluster: `barton`

### C1

- substrate_smiles: `["CCCCBr"]`
- product_smiles: `["CCCCI"]`
- optional_reagent_smiles: `["[Na+].[I-]", "CC(=O)C"]`
- rationale: This is a quintessential Finkelstein reaction: the conversion of a primary alkyl bromide to a primary alkyl iodide using sodium iodide in acetone. The reaction proceeds via an SN2 mechanism, and the equilibrium is driven by the precipitation of the less soluble sodium bromide in acetone.
- collision_avoidance_note: This example is a simple SN2 halide exchange on a saturated primary alkyl chain. It contains no radical initiators (e.g., AIBN, peroxides), photolytic conditions (hv), or functional groups like thiohydroxamate esters that could be misidentified as a Barton-type radical reaction. The transformation is a clean substitution, not a decarboxylative halogenation.

### C2

- substrate_smiles: `["ClCCC1CCCCC1"]`
- product_smiles: `["ICCC1CCCCC1"]`
- optional_reagent_smiles: `["[Na+].[I-]"]`
- rationale: This example demonstrates the Finkelstein reaction on a primary alkyl chloride containing a non-participating cycloalkane ring. The conversion of an alkyl chloride to an alkyl iodide is a classic application of this reaction, highlighting the nucleophilic substitution of a halide by another.
- collision_avoidance_note: The reaction is a straightforward halide exchange on a primary carbon, characteristic of an SN2 mechanism. The substrate lacks any features that could trigger a Barton radical pathway, such as carboxylic acid derivatives, thioesters, or proximity to radical-stabilizing groups under radical conditions. The simple exchange of Cl for I is diagnostic for the Finkelstein reaction.

### C3

- substrate_smiles: `["c1ccc(CBr)cc1"]`
- product_smiles: `["c1ccc(CI)cc1"]`
- optional_reagent_smiles: `["[Na+].[I-]", "CC(=O)C"]`
- rationale: This example showcases the Finkelstein reaction on a benzylic bromide. Benzylic halides are excellent substrates for SN2 reactions due to the stabilization of the transition state by the adjacent aromatic ring. The clean conversion to the corresponding iodide is a textbook outcome.
- collision_avoidance_note: While benzylic positions can undergo radical reactions under specific conditions (e.g., with NBS and light), the given transformation (R-Br to R-I with NaI) is unambiguously a nucleophilic substitution. There are no radical initiators or Barton-specific functional groups present, preventing confusion with radical mechanisms.

## Hunsdiecker Reaction

- changed_cases: 3
- first_case: `barton_radical_rxn_353_exact_extract`
- first_top1: `Hunsdiecker Reaction`
- cluster: `barton`

### C1

- substrate_smiles: `["CCCCC(=O)[O-].[Ag+]"]`
- product_smiles: `["CCCCCBr"]`
- optional_reagent_smiles: `["BrBr"]`
- rationale: This is a classic, textbook example of the Hunsdiecker reaction using a simple linear aliphatic silver carboxylate. The reaction clearly shows the decarboxylative bromination, resulting in an alkyl bromide that is one carbon shorter than the starting material. The transformation is unambiguous and highlights the core features of the reaction family.
- collision_avoidance_note: This example uses a silver carboxylate, the defining precursor for the Hunsdiecker reaction, which is structurally distinct from the thiohydroxamate (Barton) esters used in the Barton radical reaction. The absence of peroxides, UV light, or complex radical initiators further distinguishes it from generic radical halogenations that might be confused with Barton-type processes.

### C2

- substrate_smiles: `["O=C([O-])C1CCCCC1.[Ag+]"]`
- product_smiles: `["BrC1CCCCC1"]`
- optional_reagent_smiles: `["BrBr"]`
- rationale: This example demonstrates the Hunsdiecker reaction on an alicyclic substrate, converting silver cyclohexanecarboxylate to bromocyclohexane. The loss of the carboxylate group and its replacement with a bromine atom on the saturated ring is a clear and unmistakable topological change, reinforcing the decarboxylative halogenation pattern.
- collision_avoidance_note: The substrate is a simple silver salt of a cycloalkanecarboxylic acid, which is a canonical Hunsdiecker precursor. It lacks the N-O-C(=S) moiety of a Barton ester, preventing misclassification. The simple saturated ring system avoids any potential for competing reactions that could create ambiguity.

### C3

- substrate_smiles: `["CC(C)(C)CC(=O)[O-].[Ag+]"]`
- product_smiles: `["CC(C)(C)CI"]`
- optional_reagent_smiles: `["II"]`
- rationale: This example uses a sterically hindered substrate (silver 3,3-dimethylbutanoate) and iodine as the halogen source to produce a neopentyl iodide. It showcases the reaction's applicability to branched systems and with different halogens. The one-carbon shortening and installation of the halide are very obvious in the neopentyl framework.
- collision_avoidance_note: By strictly adhering to the silver carboxylate precursor, this example avoids the Barton ester pattern. The use of a simple halogen (I2) without other radical promoters ensures the reaction identity is tied to the Hunsdiecker mechanism, not a more general radical decarboxylation pathway that could be confused with the Barton reaction.
