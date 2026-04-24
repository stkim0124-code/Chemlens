# PDF Example Gemini Batch Verification

- stage_db: `C:\chemlens\backend\app\labint_pdf_examples_stage.db`
- extraction_table: `pdf_example_extractions`
- latest_extract_run: `20260421_022758`
- json_columns_considered: `['raw_json']`

## Totals

- page_rows: 793
- region_rows: 2847
- extraction_rows: 117

## Latest extraction

- rows: 2
- status_counts: {'error': 2}
- target_name_nonnull: 0
- reactant_smiles_nonnull: 0
- product_smiles_nonnull: 0
- either_smiles_nonnull: 0
- both_smiles_nonnull: 0
- reactant_parse_safe: 0
- product_parse_safe: 0
- both_parse_safe: 0
- text_only_count: 0
- family_mismatch_count: 0
- family_mismatch_pairs: 0
- family_mismatch_ratio: None
- ok_with_json_only_recovery: 0

## Top error messages

- 2x 403 Client Error: Forbidden for url: https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key=<REDACTED>