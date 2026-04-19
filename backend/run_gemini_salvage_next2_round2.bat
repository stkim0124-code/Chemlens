@echo off
setlocal
cd /d %~dp0
call run_gemini_salvage_rejected_families.bat reports\v5_rejected_diagnose\20260418_201257\rejected_diagnosis_summary.json 2
endlocal
