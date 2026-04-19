@echo off
setlocal

REM Continuous bounded run:
REM - fresh stage DB
REM - benchmark after every accepted insert so bad inserts can be rolled back immediately
REM - do NOT stop on first bad candidate; quarantine and continue
REM - widen candidate search automatically after empty rounds

python gemini_family_automation.py ^
  --reset-stage ^
  --run-through yes ^
  --db app\labint.db ^
  --stage-db app\labint_gemini_autorun_continuous.db ^
  --family-target 120 ^
  --candidate-limit 20 ^
  --candidate-limit-step 10 ^
  --candidate-limit-max 80 ^
  --max-rounds 40 ^
  --allow-generic yes ^
  --benchmark yes ^
  --benchmark-every 1 ^
  --stop-on-regression yes ^
  --rollback-on-regression yes ^
  --continue-on-empty-rounds yes ^
  --max-consecutive-empty-rounds 8 ^
  --quarantine-failures yes ^
  --candidate-request-retries 2 ^
  --candidate-retry-sleep 2 ^
  --chaos-mode yes ^
  --report-dir reports\gemini_family_automation_continuous

endlocal
