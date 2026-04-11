@echo off
setlocal EnableExtensions EnableDelayedExpansion

REM -----------------------------------------------------------------------------
REM run_backend.bat (CHEMLENS)
REM - Portable backend launcher (run from anywhere)
REM - Best-effort conda env activation + diagnostics
REM - Optional: --no-install to skip pip install
REM - UTF-8 safe for Korean filenames
REM -----------------------------------------------------------------------------

REM UTF-8 console + Python
chcp 65001 >nul
set PYTHONUTF8=1

set "ENV_NAME=chemlens"
set "NO_INSTALL=0"

if /I "%~1"=="--no-install" set "NO_INSTALL=1"

REM Go to backend root (where this .bat lives)
cd /d "%~dp0"

REM Sanity checks
if not exist "requirements.txt" (
  echo [ERR] Missing requirements.txt in backend root.
  pause
  exit /b 2
)
if not exist "app\main.py" (
  echo [ERR] Missing app\main.py. Are you running this inside the backend folder?
  pause
  exit /b 2
)

call :ACTIVATE_CONDA "%ENV_NAME%"

echo.
echo [DIAG] Working dir: %CD%
where python >nul 2>nul
if %errorlevel%==0 (
  for /f "delims=" %%P in ('where python') do echo [DIAG] python: %%P
) else (
  echo [WARN] python not found on PATH.
)
python -V 2>nul

if "%NO_INSTALL%"=="0" (
  echo.
  echo [1/2] Install/upgrade pip deps ^(RDKit should be installed via conda separately^)...
  python -m pip install -r requirements.txt
  if errorlevel 1 (
    echo [ERR] pip install failed. Check the output above.
    pause
    exit /b 1
  )
) else (
  echo.
  echo [1/2] Skipping pip install ^(--no-install^)
)

echo.
echo [2/2] Start FastAPI ^(Uvicorn^)...
echo     URL: http://127.0.0.1:8000
echo.
python -m uvicorn app.main:app --host 127.0.0.1 --port 8000 --reload

pause
exit /b 0

:ACTIVATE_CONDA
set "ENV=%~1"
set "ACTIVATED=0"

REM Try if conda is already available
where conda >nul 2>nul
if %errorlevel%==0 (
  call conda activate %ENV% >nul 2>nul
  if %errorlevel%==0 set "ACTIVATED=1"
)

REM Try common activate.bat locations
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
  echo        If Python/RDKit/Deps are missing, open "Anaconda Prompt" and run this .bat again.
)

exit /b 0
