# Gate C benchmark runner: admission + broad + coverage
$ErrorActionPreference = 'Continue'
$env:PYTHONIOENCODING = 'utf-8'
$snap = 'C:\chemlens\backend\reports\phase4\gateC\apply_20260424_204941'

cd C:\chemlens\backend

# Save new benchmark JSONs (post-rebuild) into snapshot
Copy-Item C:\chemlens\backend\benchmark\family_admission_benchmark.json "$snap\benchmark_backup\family_admission_benchmark.post.json" -Force
Copy-Item C:\chemlens\backend\benchmark\named_reaction_benchmark_broad.json "$snap\benchmark_backup\named_reaction_benchmark_broad.post.json" -Force
Copy-Item C:\chemlens\backend\benchmark\corpus_coverage_benchmark.json "$snap\benchmark_backup\corpus_coverage_benchmark.post.json" -Force

Write-Host "[1/3] admission"
& conda run -n chemlens python run_family_admission_benchmark.py `
    --out-dir "$snap\admission" `
    *> "$snap\logs\admission.log"
Write-Host "admission rc=$LASTEXITCODE"

Write-Host "[2/3] broad"
& conda run -n chemlens python run_named_reaction_benchmark_small.py `
    --benchmark C:\chemlens\backend\benchmark\named_reaction_benchmark_broad.json `
    --csv-out   "$snap\broad\broad_results.csv" `
    --json-out  "$snap\broad\broad_results.json" `
    --report-md "$snap\broad\broad_report.md" `
    *> "$snap\logs\broad.log"
Write-Host "broad rc=$LASTEXITCODE"

Write-Host "[3/3] coverage"
& conda run -n chemlens python run_corpus_coverage_benchmark.py `
    --out-dir "$snap\coverage" `
    *> "$snap\logs\coverage.log"
Write-Host "coverage rc=$LASTEXITCODE"

Write-Host "ALL_DONE"
