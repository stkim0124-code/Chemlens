@echo off
setlocal
cd /d %~dp0

echo [1/4] 현재 상태 점검
python VERIFY_CURRENT_BACKEND_STATE.py
if errorlevel 1 goto :fail

echo.
echo [2/4] tier3 PubChem backfill 실행
python tier3_pubchem_backfill.py --db app\labint.db
if errorlevel 1 goto :fail

echo.
echo [3/4] benchmark 실행
python run_named_reaction_benchmark_small.py --benchmark benchmark\named_reaction_benchmark_small.json
if errorlevel 1 goto :fail

echo.
echo [4/4] 실행 후 상태 재점검
python VERIFY_CURRENT_BACKEND_STATE.py
if errorlevel 1 goto :fail

echo.
echo DONE
exit /b 0

:fail
echo.
echo FAILED
exit /b 1
