@echo off
call conda activate chemlens
cd /d C:\chemlens\backend
python backfill_schema_normalized_retry404.py --db labint_round9_v5.db --batch full_batch1 --raw-json raw_json_dump_retry404_v5.json --only-pages-csv manifest_retry404_v5.csv --zip-dir "C:\chemlens\backend\app\data\images\named reactions" --tag schema_backfill_retry404
pause
