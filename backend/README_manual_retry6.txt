CHEMLENS manual retry6 patch

Purpose
- No more Gemini API calls for the remaining 6 failed pages.
- These 6 pages were manually structured from the page images using GPT vision.
- p85 (Baldwin's Rules) is intentionally treated as meta_explanatory, not a named reaction page.

Files
- backfill_manual_retry6.py
- raw_json_dump_manual_retry6_v5.json
- retry6_manual_pages.csv
- run_backfill_manual_retry6.bat

How to use
1. Copy these files into C:\chemlens\backend and overwrite if prompted.
2. Run run_backfill_manual_retry6.bat
   or:
   conda activate chemlens
   cd /d C:\chemlens\backend
   python backfill_manual_retry6.py --db labint_round9_v5.db --batch full_batch1 --raw-json raw_json_dump_manual_retry6_v5.json --only-pages-csv retry6_manual_pages.csv

Expected output
- manifest_manual_retry6_v5.csv
- raw_json_dump_manual_retry6_v5.json (re-saved in backend)

Covered pages
- p72  Aza-Claisen Rearrangement
- p85  Baldwin's Rules / Guidelines for Ring-Closing Reactions  (meta_explanatory)
- p103 Beckmann Rearrangement
- p125 Burgess Dehydration Reaction
- p143 Claisen-Ireland Rearrangement
- p145 Clemmensen Reduction
