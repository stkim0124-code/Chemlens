@echo off
set PYTHONUTF8=1
cd /d C:\chemlens\backend
"C:\Users\tmdxo\miniconda3\Scripts\conda.exe" run --no-capture-output -n chemlens python run_family_admission_benchmark.py --out-dir "C:\chemlens\backend\reports\phase3d\apply_3d_tier1_20260421_104107\admission" > "C:\chemlens\backend\reports\phase3d\apply_3d_tier1_20260421_104107\admission_run.log" 2>&1
