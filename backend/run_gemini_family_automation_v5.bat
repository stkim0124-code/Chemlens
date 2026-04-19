@echo off
setlocal
REM ============================================================
REM ChemLens v5 runner (ASCII-only, Windows-safe)
REM v5 = v3 discovery power + v4.5 frontier control.
REM ============================================================

set "DB=app\labint.db"
set "STAGE=app\labint_v5_stage.db"
set "GATE_BENCH=named_reaction_benchmark_gate.json"
set "DIAG_BENCH=named_reaction_benchmark_v4.json"
set "REPORTS=reports/v5"
set "MODELS=gemini-2.5-pro"

echo.
echo [STEP 0] plan-only mode: preview candidates and auto-selected mode
echo.
python gemini_family_automation_v5.py --plan-only --reset-stage --db "%DB%" --stage-db "%STAGE%" --benchmark-file "%GATE_BENCH%" --diagnostic-benchmark-file "%DIAG_BENCH%" --report-dir "%REPORTS%" --candidate-limit 30 --discovery-candidate-limit 3 --frontier-max-candidate-limit 6 --frontier-grow-after 2 --gemini-models "%MODELS%"

echo.
echo ============================================================
echo [CHECK] Review candidate list above.
echo         Press any key to start REAL automation.
echo         Press Ctrl+C to cancel.
echo ============================================================
pause

echo.
echo [STEP 1] Starting v5 automation
echo.
python gemini_family_automation_v5.py --reset-stage --db "%DB%" --stage-db "%STAGE%" --benchmark-file "%GATE_BENCH%" --diagnostic-benchmark-file "%DIAG_BENCH%" --report-dir "%REPORTS%" --family-target 250 --batch-size 3 --candidate-limit 30 --max-rounds 20 --max-empty-rounds 2 --discovery-candidate-limit 3 --frontier-max-candidate-limit 6 --frontier-grow-after 2 --zero-progress-stop-rounds 1 --bulk-zero-progress-stop-rounds 2 --disk-budget-gb 10 --max-snapshots 12 --snapshot-every 25 --gemini-fallback yes --min-confidence 0.75 --min-baseline-top1 0.99 --min-baseline-top3 0.99 --diagnostic-every-rounds 3 --gemini-models "%MODELS%"

echo.
echo ============================================================
echo [DONE] Check %REPORTS%/[run_id]/run_summary.json
echo ============================================================
endlocal
