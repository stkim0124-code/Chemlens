@echo off
setlocal EnableExtensions

set "SCRIPT_DIR=%~dp0"
set "ROOT=%SCRIPT_DIR%"
if not exist "%ROOT%app\labint.db" (
  if exist "%SCRIPT_DIR%..\app\labint.db" set "ROOT=%SCRIPT_DIR%..\"
)
cd /d "%ROOT%"
set "LOG=%ROOT%step2_and_benchmark_log.txt"

echo ================================================== > "%LOG%"
echo STEP2_AND_BENCHMARK started >> "%LOG%"
echo Working directory: %ROOT% >> "%LOG%"
echo ================================================== >> "%LOG%"

echo [INFO] Working directory: %ROOT%
echo [INFO] Log file: %LOG%

REM Try to activate conda env named chemlens.
set "CONDA_OK="
if defined CONDA_EXE (
  call "%CONDA_EXE%" activate chemlens >nul 2>nul
  if not errorlevel 1 set "CONDA_OK=1"
)
if not defined CONDA_OK (
  for %%I in (
    "%USERPROFILE%\miniconda3\Scripts\activate.bat"
    "%USERPROFILE%\anaconda3\Scripts\activate.bat"
    "C:\miniconda3\Scripts\activate.bat"
    "C:\anaconda3\Scripts\activate.bat"
  ) do (
    if exist %%~I (
      call %%~I chemlens >nul 2>nul
      if not errorlevel 1 set "CONDA_OK=1"
    )
  )
)

where python >nul 2>nul
if errorlevel 1 (
  echo [ERROR] python not found >> "%LOG%"
  echo [ERROR] python not found
  goto fail
)

python -c "import sys; print(sys.executable)" >> "%LOG%" 2>&1
python -c "import rdkit, requests; print('PY_DEPS_OK')" >> "%LOG%" 2>&1
if errorlevel 1 (
  echo [ERROR] current python does not have rdkit/requests >> "%LOG%"
  echo [ERROR] current python does not have rdkit/requests
  echo Open this log file and send me the last 30 lines:
  echo %LOG%
  goto fail
)

if not exist "VERIFY_CURRENT_BACKEND_STATE.py" (
  echo [ERROR] VERIFY_CURRENT_BACKEND_STATE.py not found >> "%LOG%"
  echo [ERROR] VERIFY_CURRENT_BACKEND_STATE.py not found
  goto fail
)
if not exist "tier3_pubchem_backfill.py" (
  echo [ERROR] tier3_pubchem_backfill.py not found >> "%LOG%"
  echo [ERROR] tier3_pubchem_backfill.py not found
  goto fail
)
if not exist "run_named_reaction_benchmark_small.py" (
  echo [ERROR] run_named_reaction_benchmark_small.py not found >> "%LOG%"
  echo [ERROR] run_named_reaction_benchmark_small.py not found
  goto fail
)
if not exist "benchmark\named_reaction_benchmark_small.json" (
  echo [ERROR] benchmark\named_reaction_benchmark_small.json not found >> "%LOG%"
  echo [ERROR] benchmark\named_reaction_benchmark_small.json not found
  goto fail
)
if not exist "app\labint.db" (
  echo [ERROR] app\labint.db not found >> "%LOG%"
  echo [ERROR] app\labint.db not found
  goto fail
)

echo.
echo [1/4] verify current state
echo [1/4] verify current state >> "%LOG%"
python VERIFY_CURRENT_BACKEND_STATE.py >> "%LOG%" 2>&1
if errorlevel 1 goto fail

echo.
echo [2/4] run tier3 backfill
echo [2/4] run tier3 backfill >> "%LOG%"
python tier3_pubchem_backfill.py --db app\labint.db >> "%LOG%" 2>&1
if errorlevel 1 goto fail

echo.
echo [3/4] run benchmark
echo [3/4] run benchmark >> "%LOG%"
python run_named_reaction_benchmark_small.py --benchmark benchmark\named_reaction_benchmark_small.json >> "%LOG%" 2>&1
if errorlevel 1 goto fail

echo.
echo [4/4] verify state again
echo [4/4] verify state again >> "%LOG%"
python VERIFY_CURRENT_BACKEND_STATE.py >> "%LOG%" 2>&1
if errorlevel 1 goto fail

echo.
echo [DONE] success
echo [DONE] success >> "%LOG%"
echo Open this log file:
echo %LOG%
goto end

:fail
echo.
echo [FAILED] check this log file:
echo %LOG%

:end
pause
exit /b 0
