@echo off
call conda activate chemlens
cd /d C:\chemlens\backend
python backfill_manual_retry6.py --db labint_round9_v5.db --batch full_batch1 --raw-json raw_json_dump_manual_retry6_v5.json --only-pages-csv retry6_manual_pages.csv
pause
