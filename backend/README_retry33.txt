CHEMLENS retry33 patch

What changed
- retry_fail_queue_404.py can now read fail targets directly from fail_queue_full_v5.csv
- It retries only the 404-model failed pages from that CSV
- It skips pages that were already successfully parsed in the same batch, so accidental reruns do not burn extra tokens
- It replaces old fail_queue entries instead of stacking duplicate rows on repeat failure

Files included
- retry_fail_queue_404.py
- fail_queue_full_v5.csv
- run_retry_33_only.bat

Recommended location
- Extract these files into your backend root folder (same place as labint_round9_v5.db and pipeline_named_reactions_v5.py)

Recommended command (Anaconda Prompt)
conda activate chemlens
cd /d C:\chemlens\backend
python retry_fail_queue_404.py --db labint_round9_v5.db --batch full_batch1 --fail-csv fail_queue_full_v5.csv --zip-dir "C:\chemlens\backend\app\data\images\named reactions"

What this does NOT do
- It does not rerun the already successful pages
- It does not rebuild the full batch
- It does not touch reaction_cards

Outputs
- raw_json_dump_retry404_v5.json
- manifest_retry404_v5.csv
