@echo off
set PYTHONUTF8=1
cd /d C:\chemlens\backend
"C:\Users\tmdxo\miniconda3\Scripts\conda.exe" run --no-capture-output -n chemlens python run_corpus_coverage_benchmark.py --out-dir "C:\chemlens\backend\reports\phase3g\apply_3g_2a_20260421_190000\coverage" > "C:\chemlens\backend\reports\phase3g\apply_3g_2a_20260421_190000\coverage_run.log" 2>&1
