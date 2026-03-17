@echo off
setlocal EnableExtensions EnableDelayedExpansion

REM -----------------------------------------------------------------------------
REM run_fix_docs_db.bat
REM Purpose:
REM   1) Remove UNIQUE constraint on documents.file_path (safe, schema-independent)
REM   2) Sync documents.file_path to match PDFs under app\data\pdfs (apply)
REM
REM Notes:
REM - This script is portable: run from anywhere; it resolves paths relative to itself.
REM - It tries hard to activate conda env, but will still run if you're already in the env.
REM -----------------------------------------------------------------------------

REM Ensure UTF-8 for Korean filenames / console output
chcp 65001 >nul
set PYTHONUTF8=1

REM Config
set "ENV_NAME=chemlens"

REM Go to backend root (where this .bat lives)
cd /d "%~dp0"

REM Sanity checks
if not exist "tools\migrate_docs_remove_unique.py" (
  echo [ERR] Missing tools\migrate_docs_remove_unique.py
  echo       Make sure backend\tools\ is present.
  pause
  exit /b 2
)
if not exist "tools\sync_docs_db.py" (
  echo [ERR] Missing tools\sync_docs_db.py
  echo       Make sure backend\tools\ is present.
  pause
  exit /b 2
)
if not exist "app\data\labint_docs.db" (
  echo [ERR] Missing app\data\labint_docs.db
  echo       If you haven't ingested docs yet, create/init it first.
  pause
  exit /b 2
)
if not exist "app\data\pdfs" (
  echo [ERR] Missing app\data\pdfs directory
  echo       Put your PDFs under: backend\app\data\pdfs\*.pdf
  pause
  exit /b 2
)

REM Try to activate conda env (best effort).
REM Works if conda is on PATH and initialized; otherwise we try common install paths.
set "ACTIVATED=0"

where conda >nul 2>nul
if %errorlevel%==0 (
  call conda activate %ENV_NAME% >nul 2>nul
  if %errorlevel%==0 set "ACTIVATED=1"
)

if "%ACTIVATED%"=="0" (
  if exist "%USERPROFILE%\miniconda3\Scripts\activate.bat" (
    call "%USERPROFILE%\miniconda3\Scripts\activate.bat" %ENV_NAME% >nul 2>nul
    if %errorlevel%==0 set "ACTIVATED=1"
  )
)

if "%ACTIVATED%"=="0" (
  if exist "%USERPROFILE%\anaconda3\Scripts\activate.bat" (
    call "%USERPROFILE%\anaconda3\Scripts\activate.bat" %ENV_NAME% >nul 2>nul
    if %errorlevel%==0 set "ACTIVATED=1"
  )
)

if "%ACTIVATED%"=="0" (
  echo [WARN] Could not auto-activate conda env "%ENV_NAME%".
  echo        If Python/RDKit/Deps are missing, open "Anaconda Prompt" and run this .bat again.
  echo.
)

echo [1/2] Remove UNIQUE(file_path) with backup...
python tools\migrate_docs_remove_unique.py --db "app\data\labint_docs.db"
if errorlevel 1 (
  echo [ERR] Migration failed. Check output above.
  pause
  exit /b 1
)

echo.
echo [2/2] Apply PDF path sync to DB...
python tools\sync_docs_db.py --db "app\data\labint_docs.db" --pdfs "app\data\pdfs" --apply
if errorlevel 1 (
  echo [ERR] Sync apply failed. Check output above.
  pause
  exit /b 1
)

echo.
echo DONE.
pause
