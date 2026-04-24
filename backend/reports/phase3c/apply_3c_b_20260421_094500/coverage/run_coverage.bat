@echo off
cd /d C:\chemlens\backend
set "PATH=C:\Users\tmdxo\miniconda3\envs\chemlens\Library\bin;C:\Users\tmdxo\miniconda3\envs\chemlens;%PATH%"
set "PYTHONIOENCODING=utf-8"
set "PYTHONUTF8=1"
C:\Users\tmdxo\miniconda3\envs\chemlens\python.exe run_corpus_coverage_benchmark.py --out-dir "C:\chemlens\backend\reports\phase3c\apply_3c_b_20260421_094500\coverage"
