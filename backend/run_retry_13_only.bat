@echo off
call conda activate chemlens
cd /d C:\chemlens\backend
python retry_fail_queue_404.py --db labint_round9_v5.db --batch full_batch1 --fail-csv retry13_remaining_from_manifest.csv --zip-dir "C:\chemlens\backend\app\data\images\named reactions" --tag retry13_remaining --skip-existing
pause
