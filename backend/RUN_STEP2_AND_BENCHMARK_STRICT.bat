@echo off
setlocal EnableExtensions
cd /d "%~dp0"
set "LOG=%CD%\step2_and_benchmark_log.txt"

> "%LOG%" echo ==================================================
>>"%LOG%" echo STEP2_AND_BENCHMARK_STRICT started
>>"%LOG%" echo Working directory: %CD%
>>"%LOG%" echo ==================================================

echo [INFO] Working directory: %CD%
echo [INFO] Log file: %LOG%

set "CONDA_BAT=%USERPROFILE%\miniconda3\condabin\conda.bat"
if not exist "%CONDA_BAT%" set "CONDA_BAT=%USERPROFILE%\anaconda3\condabin\conda.bat"
if not exist "%CONDA_BAT%" (
  echo [FAILED] conda.bat not found
  >>"%LOG%" echo [FAILED] conda.bat not found
  pause
  exit /b 1
)

call "%CONDA_BAT%" activate chemlens >> "%LOG%" 2>&1
if errorlevel 1 (
  echo [FAILED] conda activate chemlens
  >>"%LOG%" echo [FAILED] conda activate chemlens
  pause
  exit /b 1
)

where python >> "%LOG%" 2>&1
python -c "import sys; print(sys.executable)" >> "%LOG%" 2>&1
if errorlevel 1 (
  echo [FAILED] python not available after conda activate
  >>"%LOG%" echo [FAILED] python not available after conda activate
  pause
  exit /b 1
)

python -c "import rdkit, requests; print('IMPORT_OK')" >> "%LOG%" 2>&1
if errorlevel 1 (
  echo [FAILED] rdkit/requests import check failed
  >>"%LOG%" echo [FAILED] rdkit/requests import check failed
  pause
  exit /b 1
)

echo.
echo [1/4] verify current state
python VERIFY_CURRENT_BACKEND_STATE.py >> "%LOG%" 2>&1
if errorlevel 1 (
  echo [FAILED] verify current state
  >>"%LOG%" echo [FAILED] verify current state
  pause
  exit /b 1
)

echo.
echo [2/4] run tier3 backfill
python tier3_pubchem_backfill.py >> "%LOG%" 2>&1
if errorlevel 1 (
  echo [FAILED] tier3 backfill
  >>"%LOG%" echo [FAILED] tier3 backfill
  pause
  exit /b 1
)

echo.
echo [3/4] run benchmark
python run_named_reaction_benchmark_small.py --benchmark benchmark/named_reaction_benchmark_small.json >> "%LOG%" 2>&1
if errorlevel 1 (
  echo [FAILED] benchmark
  >>"%LOG%" echo [FAILED] benchmark
  pause
  exit /b 1
)

echo.
echo [4/4] verify state again
python VERIFY_CURRENT_BACKEND_STATE.py >> "%LOG%" 2>&1
if errorlevel 1 (
  echo [FAILED] verify state again
  >>"%LOG%" echo [FAILED] verify state again
  pause
  exit /b 1
)

echo.
echo [DONE] real success
>>"%LOG%" echo [DONE] real success

echo Open this log file:
echo %LOG%
pause
exit /b 0
