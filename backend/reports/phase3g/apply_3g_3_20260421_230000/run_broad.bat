@echo off
set PYTHONUTF8=1
cd /d C:\chemlens\backend
"C:\Users\tmdxo\miniconda3\Scripts\conda.exe" run --no-capture-output -n chemlens python run_named_reaction_benchmark_small.py --benchmark "C:\chemlens\backend\benchmark\named_reaction_benchmark_broad.json" --csv-out "C:\chemlens\backend\reports\phase3g\apply_3g_3_20260421_230000\broad\broad_results.csv" --json-out "C:\chemlens\backend\reports\phase3g\apply_3g_3_20260421_230000\broad\broad_results.json" --report-md "C:\chemlens\backend\reports\phase3g\apply_3g_3_20260421_230000\broad\broad_report.md" > "C:\chemlens\backend\reports\phase3g\apply_3g_3_20260421_230000\broad_run.log" 2>&1
