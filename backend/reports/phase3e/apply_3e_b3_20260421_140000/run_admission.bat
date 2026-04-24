@echo off
set PYTHONUTF8=1
cd /d C:\chemlens\backend
"C:\Users\tmdxo\miniconda3\Scripts\conda.exe" run --no-capture-output -n chemlens python run_family_admission_benchmark.py --out-dir "C:\chemlens\backend\reports\phase3e\apply_3e_b3_20260421_140000\admission" > "C:\chemlens\backend\reports\phase3e\apply_3e_b3_20260421_140000\admission_run.log" 2>&1
