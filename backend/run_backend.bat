@echo off
setlocal EnableExtensions EnableDelayedExpansion

chcp 65001 >nul
set PYTHONUTF8=1

set "ENV_NAME=chemlens"
set "HOST=127.0.0.1"
set "PORT=8000"
set "EXTRA_ARGS=%*"

cd /d "%~dp0"

if not exist "app\main.py" (
  echo [ERR] Missing app\main.py
  pause
  exit /b 2
)

call :ACTIVATE_CONDA "%ENV_NAME%"

echo [backend] ROOT=%CD%
if defined LABINT_DB_PATH echo [backend] LABINT_DB_PATH=%LABINT_DB_PATH%
echo [backend] HOST=%HOST% PORT=%PORT%

python -m uvicorn app.main:app --reload --host %HOST% --port %PORT% %EXTRA_ARGS%
set "EXITCODE=%ERRORLEVEL%"
if not "%EXITCODE%"=="0" (
  echo.
  echo [ERR] Backend exited with code %EXITCODE%.
  echo       If RDKit/FastAPI deps are missing, open Anaconda Prompt and run this file again.
  pause
)
exit /b %EXITCODE%

:ACTIVATE_CONDA
set "ENV=%~1"
set "ACTIVATED=0"

where conda >nul 2>nul
if %errorlevel%==0 (
  call conda activate %ENV% >nul 2>nul
  if %errorlevel%==0 set "ACTIVATED=1"
)

if "%ACTIVATED%"=="0" (
  if exist "%USERPROFILE%\miniconda3\Scripts\activate.bat" (
    call "%USERPROFILE%\miniconda3\Scripts\activate.bat" %ENV% >nul 2>nul
    if %errorlevel%==0 set "ACTIVATED=1"
  )
)

if "%ACTIVATED%"=="0" (
  if exist "%USERPROFILE%\anaconda3\Scripts\activate.bat" (
    call "%USERPROFILE%\anaconda3\Scripts\activate.bat" %ENV% >nul 2>nul
    if %errorlevel%==0 set "ACTIVATED=1"
  )
)

if "%ACTIVATED%"=="0" (
  echo [WARN] Could not auto-activate conda env "%ENV%".
  echo        If Python/RDKit/Deps are missing, open Anaconda Prompt and run this .bat again.
)

exit /b 0
