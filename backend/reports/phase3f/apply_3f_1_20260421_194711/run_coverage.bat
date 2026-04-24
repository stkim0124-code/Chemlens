@echo off
set PYTHONUTF8=1
cd /d C:\chemlens\backend
"C:\Users\tmdxo\miniconda3\Scripts\conda.exe" run --no-capture-output -n chemlens python run_corpus_coverage_benchmark.py --out-dir "C:\chemlens\backend\reports\phase3f\apply_3f_1_20260421_194711\coverage" > "C:\chemlens\backend\reports\phase3f\apply_3f_1_20260421_194711\coverage_run.log" 2>&1
