# Iter2 benchmark runner v2: fix admission (killed last time) + broad (uses 259-case JSON)
$ErrorActionPreference = 'Continue'
$env:PYTHONIOENCODING = 'utf-8'
$snap = 'C:\chemlens\backend\reports\phase4\gateB\iter2_20260424_203319'

cd C:\chemlens\backend

Write-Host "[1/3] admission (259 cases) — retry"
& conda run -n chemlens python run_family_admission_benchmark.py `
    --out-dir "$snap\admission" `
    *> "$snap\logs\admission.log"
Write-Host "admission rc=$LASTEXITCODE"

Write-Host "[2/3] broad — 259-case run"
& conda run -n chemlens python run_named_reaction_benchmark_small.py `
    --benchmark C:\chemlens\backend\benchmark\named_reaction_benchmark_broad.json `
    --csv-out   "$snap\broad\broad_results.csv" `
    --json-out  "$snap\broad\broad_results.json" `
    --report-md "$snap\broad\broad_report.md" `
    *> "$snap\logs\broad.log"
Write-Host "broad rc=$LASTEXITCODE"

Write-Host "[3/3] diff vs iterate1"
$baseline = 'C:\chemlens\backend\reports\phase4\gateB\iterate1_20260424'
& conda run -n chemlens python scripts\gateB_eval_harness.py diff `
    --before $baseline `
    --after  $snap `
    --out    "$snap\diff" `
    --defects-registry C:\chemlens\backend\benchmark\defects\benchmark_defects_registry.json `
    --guard-registry   C:\chemlens\backend\scripts\gateB_guard_registry.json `
    *> "$snap\logs\diff.log"
Write-Host "diff rc=$LASTEXITCODE"

Write-Host "ALL_DONE"
