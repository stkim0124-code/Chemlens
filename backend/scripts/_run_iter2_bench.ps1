$ErrorActionPreference = 'Continue'
$env:PYTHONIOENCODING = 'utf-8'
$ts = Get-Date -Format 'yyyyMMdd_HHmmss'
$snap = "C:\chemlens\backend\reports\phase4\gateB\iter2_$ts"
New-Item -ItemType Directory -Force -Path "$snap\admission" | Out-Null
New-Item -ItemType Directory -Force -Path "$snap\broad"     | Out-Null
New-Item -ItemType Directory -Force -Path "$snap\coverage"  | Out-Null
New-Item -ItemType Directory -Force -Path "$snap\diff"      | Out-Null
New-Item -ItemType Directory -Force -Path "$snap\logs"      | Out-Null

Write-Host "SNAP=$snap"

cd C:\chemlens\backend

# --- 1) Admission ---
$t = Get-Date
Write-Host "[1/4] admission start $t"
& conda run -n chemlens python run_family_admission_benchmark.py `
    --out-dir "$snap\admission" `
    *> "$snap\logs\admission.log"
Write-Host "[1/4] admission done. rc=$LASTEXITCODE"

# --- 2) Broad ---
$t = Get-Date
Write-Host "[2/4] broad start $t"
& conda run -n chemlens python run_named_reaction_benchmark_small.py `
    --csv-out   "$snap\broad\broad_results.csv" `
    --json-out  "$snap\broad\broad_results.json" `
    --report-md "$snap\broad\broad_report.md" `
    *> "$snap\logs\broad.log"
Write-Host "[2/4] broad done. rc=$LASTEXITCODE"

# --- 3) Coverage ---
$t = Get-Date
Write-Host "[3/4] coverage start $t"
& conda run -n chemlens python run_corpus_coverage_benchmark.py `
    --out-dir "$snap\coverage" `
    *> "$snap\logs\coverage.log"
Write-Host "[3/4] coverage done. rc=$LASTEXITCODE"

# --- 4) Diff vs iterate1 baseline ---
$t = Get-Date
Write-Host "[4/4] diff start $t"
$baseline = "C:\chemlens\backend\reports\phase4\gateB\iterate1_20260424"
& conda run -n chemlens python scripts\gateB_eval_harness.py diff `
    --before $baseline `
    --after  $snap `
    --out    "$snap\diff" `
    --defects-registry C:\chemlens\backend\benchmark\defects\benchmark_defects_registry.json `
    --guard-registry   C:\chemlens\backend\scripts\gateB_guard_registry.json `
    *> "$snap\logs\diff.log"
Write-Host "[4/4] diff done. rc=$LASTEXITCODE"

Write-Host "ALL_DONE snap=$snap"
