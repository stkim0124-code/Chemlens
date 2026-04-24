@echo off
set PYTHONUTF8=1
cd /d C:\chemlens\backend
"C:\Users\tmdxo\miniconda3\Scripts\conda.exe" run --no-capture-output -n chemlens python run_family_admission_benchmark.py --out-dir "C:\chemlens\backend\reports\phase3g\apply_3g_4_20260421_234000\admission" > "C:\chemlens\backend\reports\phase3g\apply_3g_4_20260421_234000\admission_run.log" 2>&1
