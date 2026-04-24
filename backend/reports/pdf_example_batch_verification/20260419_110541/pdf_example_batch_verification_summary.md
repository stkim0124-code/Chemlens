# PDF Example Gemini Batch Verification

- stage_db: `C:\chemlens\backend\app\labint_pdf_examples_stage.db`
- extraction_table: `pdf_example_extractions`
- latest_extract_run: `20260419_110537`
- json_columns_considered: `['raw_json']`

## Totals

- page_rows: 772
- region_rows: 2801
- extraction_rows: 71

## Latest extraction

- rows: 6
- status_counts: {'error': 6}
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

- 6x 429 Client Error: Too Many Requests for url: https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-pro:generateContent?key=<REDACTED>