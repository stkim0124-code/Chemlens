# PDF Example Gemini Batch Verification

- stage_db: `C:\chemlens\backend\app\labint_pdf_examples_stage.db`
- extraction_table: `pdf_example_extractions`
- latest_extract_run: `20260419_104311`
- json_columns_considered: `['raw_json']`

## Totals

- page_rows: 770
- region_rows: 2795
- extraction_rows: 65

## Latest extraction

- rows: 35
- status_counts: {'error': 34, 'ok': 1}
- target_name_nonnull: 1
- reactant_smiles_nonnull: 1
- product_smiles_nonnull: 1
- either_smiles_nonnull: 1
- both_smiles_nonnull: 1
- reactant_parse_safe: 1
- product_parse_safe: 1
- both_parse_safe: 1
- text_only_count: 0
- family_mismatch_count: 0
- family_mismatch_pairs: 0
- family_mismatch_ratio: None
- ok_with_json_only_recovery: 1

## Top error messages

- 34x 429 Client Error: Too Many Requests for url: https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-pro:generateContent?key=AIzaSyAVXJGOZPympLvIO9fmUhxsuBL9dl6NDxc