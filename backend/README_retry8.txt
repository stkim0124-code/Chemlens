CHEMLENS retry8 only patch

Purpose
- Retry only the 8 pages that still failed after retry13_remaining.
- Already successful pages are not retried.

Files
- retry_fail_queue_404.py
- retry8_remaining_from_manifest.csv
- run_retry_8_only.bat

How to use
1) Extract all files in this zip directly into C:\chemlens\backend
2) Overwrite if prompted
3) Run run_retry_8_only.bat

Equivalent command
python retry_fail_queue_404.py --db labint_round9_v5.db --batch full_batch1 --fail-csv retry8_remaining_from_manifest.csv --zip-dir "C:\chemlens\backend\app\data\images\named reactions" --tag retry8_remaining --skip-existing

Expected target pages
- p72, p85, p93, p103, p117, p125, p143, p145
