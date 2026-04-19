@echo off
setlocal
REM ============================================================
REM run_v4_automation.bat - ChemLens v4.5 discovery-safe runner
REM Current frontier is discovery-heavy (seed=0 / extracts=0).
REM Keep runs short, schema-safe, and stop on zero progress.
REM ============================================================

set "DB=app\labint.db"
set "STAGE=app\labint_v4_stage.db"
set "GATE_BENCH=named_reaction_benchmark_gate.json"
set "DIAG_BENCH=named_reaction_benchmark_v4.json"
set "REPORTS=reports/v4"
set "MODELS=gemini-2.5-pro"

echo.
echo [STEP 0] plan-only mode: preview candidates
echo.
python gemini_family_automation_v4.py --plan-only --reset-stage --db "%DB%" --stage-db "%STAGE%" --benchmark-file "%GATE_BENCH%" --diagnostic-benchmark-file "%DIAG_BENCH%" --report-dir "%REPORTS%" --candidate-limit 30 --discovery-candidate-limit 3 --zero-progress-stop-rounds 1 --gemini-models "%MODELS%"

echo.
echo ============================================================
echo [CHECK] Review candidate list above.
echo         Press any key to start REAL automation.
echo         Press Ctrl+C to cancel.
echo ============================================================
pause

echo.
echo [STEP 1] Starting v4 automation
echo.
python gemini_family_automation_v4.py --reset-stage --db "%DB%" --stage-db "%STAGE%" --benchmark-file "%GATE_BENCH%" --diagnostic-benchmark-file "%DIAG_BENCH%" --report-dir "%REPORTS%" --family-target 250 --batch-size 1 --candidate-limit 30 --discovery-candidate-limit 3 --max-rounds 6 --max-empty-rounds 1 --zero-progress-stop-rounds 1 --disk-budget-gb 10 --max-snapshots 10 --snapshot-every 25 --gemini-fallback yes --min-confidence 0.75 --min-baseline-top1 0.99 --min-baseline-top3 0.99 --gemini-models "%MODELS%"

echo.
echo ============================================================
echo [DONE] Check %REPORTS%/[run_id]/run_summary.json
echo ============================================================
endlocal
