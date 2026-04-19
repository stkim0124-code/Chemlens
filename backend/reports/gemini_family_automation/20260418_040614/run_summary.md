# Gemini family automation run

- run_id: 20260418_040614
- canonical_db: C:\chemlens\backend\app\labint.db
- stage_db: C:\chemlens\backend\app\labint_gemini_autorun_exp.db
- family_target: 120
- inserted_count: 5
- created_pattern_count: 0
- final_family_coverage: 46

## Decisions

| family | status | decision | target_family | model | created_pattern | note |
|---|---|---|---|---|---:|---|
| Aldol Reaction | inserted | merge_existing | Aldol Reaction | gemini-2.5-pro | False | The candidate page details several advanced synthetic applications of asymmetric aldol reactions in total synthesis. These examples, including auxiliary-controlled, reagent-controlled, and catalytic variants, are all specific instances of the general Aldol Reaction. Therefore, this page should be me |
| Barbier Coupling Reaction | inserted | merge_existing | Barbier Coupling Reaction | gemini-2.5-pro | False | This is a canonical overview page for the Barbier reaction, which already exists as a family. The decision is to merge. |
| Baylis-Hillman Reaction | validation_failed | merge_existing |  | gemini-2.5-pro |  | molecule[3] invalid smiles: RDKit failed to parse SMILES: N1CCN(CC1)CC1 |
| Birch Reduction | inserted | merge_existing | Birch Reduction | gemini-2.5-pro | False | The candidate page is a canonical overview of the Birch Reduction, which perfectly matches an existing family. The chosen example with anisole illustrates the characteristic regioselectivity for arenes bearing an electron-donating group. |
| Carroll Rearrangement | inserted | merge_existing | Carroll Rearrangement | gemini-2.5-pro | False | The candidate page is a canonical overview of the Carroll Rearrangement, which already exists as a family. The chemistry, name, and scope are identical, making a merge appropriate. |
| Alder (Ene) Reaction (Hydro-Allyl Addition) | inserted | merge_existing | Alder (Ene) Reaction (Hydro-Allyl Addition) | gemini-2.5-pro | False | This is a canonical overview page for the Alder (Ene) reaction. The decision is to merge with the existing family 'Alder (Ene) Reaction (Hydro-Allyl Addition)' due to the perfect name match and content alignment. The page covers the general pericyclic mechanism and mentions several variants like aza |
