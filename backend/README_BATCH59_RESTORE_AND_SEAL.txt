BATCH59 RESTORE + SEARCH BASELINE SEAL

What this patch does
1. Restores manual_page_knowledge page_no 502..553 from donor batch59 data.
2. Restores the linked manual_page_entities rows for those pages.
3. Leaves extract_molecules / benchmark / evidence_search intact.
4. Writes BASELINE_SEAL_AFTER_BATCH59_RESTORE.json with:
   - DB before/after counts
   - queryable / tier counts
   - structure_source counts
   - benchmark summary (if benchmark result file exists)
   - evidence_search.py sha256

How to use
1. Extract this zip directly into C:\chemlens\backend
2. Open Anaconda Prompt
3. Run:

conda activate chemlens
cd /d C:\chemlens\backend
python apply_batch59_restore_and_seal.py

Recommended follow-up
python VERIFY_CURRENT_BACKEND_STATE.py
python run_named_reaction_benchmark_small.py --benchmark benchmark\named_reaction_benchmark_small.json

Expected result
- p502_553_records should become 52
- queryable / tier / family coverage should stay unchanged
- benchmark should remain 27/27 top1 and 27/27 top3
