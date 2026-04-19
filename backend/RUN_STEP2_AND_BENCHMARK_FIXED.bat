@echo off
setlocal EnableExtensions EnableDelayedExpansion

REM Keep window open and write a log next to this file.
set "SCRIPT_DIR=%~dp0"
set "ROOT=%SCRIPT_DIR%"

REM Support both cases:
REM 1) files copied directly into backend root
REM 2) files kept inside a subfolder under backend root
if not exist "%ROOT%app\labint.db" (
  if exist "%SCRIPT_DIR%..\app\labint.db" set "ROOT=%SCRIPT_DIR%..\"
)

cd /d "%ROOT%"
set "LOG=%ROOT%step2_and_benchmark_log.txt"

echo ================================================== > "%LOG%"
echo RUN_STEP2_AND_BENCHMARK_FIXED started >> "%LOG%"
echo Working directory: %ROOT% >> "%LOG%"
echo ================================================== >> "%LOG%"

echo [INFO] Working directory: %ROOT%
echo [INFO] Log file: %LOG%

after_where:
where python >nul 2>nul
if errorlevel 1 (
  echo [ERROR] python 명령을 찾지 못했습니다. >> "%LOG%"
  echo [ERROR] python 명령을 찾지 못했습니다.
  echo miniconda/conda 환경에서 backend 폴더를 열고 다시 실행해주세요.
  goto :end_fail
)

if not exist "VERIFY_CURRENT_BACKEND_STATE.py" (
  echo [ERROR] VERIFY_CURRENT_BACKEND_STATE.py not found >> "%LOG%"
  echo [ERROR] VERIFY_CURRENT_BACKEND_STATE.py 파일이 없습니다.
  goto :end_fail
)
if not exist "tier3_pubchem_backfill.py" (
  echo [ERROR] tier3_pubchem_backfill.py not found >> "%LOG%"
  echo [ERROR] tier3_pubchem_backfill.py 파일이 없습니다.
  goto :end_fail
)
if not exist "run_named_reaction_benchmark_small.py" (
  echo [ERROR] run_named_reaction_benchmark_small.py not found >> "%LOG%"
  echo [ERROR] run_named_reaction_benchmark_small.py 파일이 없습니다.
  goto :end_fail
)
if not exist "benchmark\named_reaction_benchmark_small.json" (
  echo [ERROR] benchmark\named_reaction_benchmark_small.json not found >> "%LOG%"
  echo [ERROR] benchmark\named_reaction_benchmark_small.json 파일이 없습니다.
  goto :end_fail
)
if not exist "app\labint.db" (
  echo [ERROR] app\labint.db not found >> "%LOG%"
  echo [ERROR] app\labint.db 파일이 없습니다.
  goto :end_fail
)

echo.
echo [1/4] 현재 상태 점검
echo [1/4] VERIFY_CURRENT_BACKEND_STATE.py >> "%LOG%"
python VERIFY_CURRENT_BACKEND_STATE.py 1>> "%LOG%" 2>&1
if errorlevel 1 goto :step_fail

echo.
echo [2/4] tier3 PubChem backfill 실행
echo [2/4] tier3_pubchem_backfill.py --db app\labint.db >> "%LOG%"
python tier3_pubchem_backfill.py --db app\labint.db 1>> "%LOG%" 2>&1
if errorlevel 1 goto :step_fail

echo.
echo [3/4] benchmark 실행
echo [3/4] run_named_reaction_benchmark_small.py --benchmark benchmark\named_reaction_benchmark_small.json >> "%LOG%"
python run_named_reaction_benchmark_small.py --benchmark benchmark\named_reaction_benchmark_small.json 1>> "%LOG%" 2>&1
if errorlevel 1 goto :step_fail

echo.
echo [4/4] 실행 후 상태 재점검
echo [4/4] VERIFY_CURRENT_BACKEND_STATE.py >> "%LOG%"
python VERIFY_CURRENT_BACKEND_STATE.py 1>> "%LOG%" 2>&1
if errorlevel 1 goto :step_fail

echo.
echo [DONE] 정상 완료
echo [DONE] 정상 완료 >> "%LOG%"
echo 로그 파일을 열어 결과를 확인하세요:
echo %LOG%
goto :end_ok

:step_fail
echo.
echo [FAILED] 중간 단계에서 오류가 났습니다.
echo [FAILED] 중간 단계에서 오류가 났습니다. >> "%LOG%"
echo 아래 로그 파일을 열어 마지막 20~30줄을 저에게 보내주세요.
echo %LOG%
goto :end_fail

:end_ok
echo.
pause
exit /b 0

:end_fail
echo.
pause
exit /b 1
