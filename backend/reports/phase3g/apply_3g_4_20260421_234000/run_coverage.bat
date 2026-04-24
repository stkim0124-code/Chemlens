@echo off
set PYTHONUTF8=1
cd /d C:\chemlens\backend
"C:\Users\tmdxo\miniconda3\Scripts\conda.exe" run --no-capture-output -n chemlens python run_corpus_coverage_benchmark.py --out-dir "C:\chemlens\backend\reports\phase3g\apply_3g_4_20260421_234000\coverage" > "C:\chemlens\backend\reports\phase3g\apply_3g_4_20260421_234000\coverage_run.log" 2>&1
