@echo off
setlocal EnableExtensions EnableDelayedExpansion

REM -----------------------------------------------------------------------------
REM CLEAN_BACKEND_RESIDUE.bat
REM - Removes non-source residues that frequently cause confusion or accidental commits
REM - Safe: does NOT touch app\data\pdfs, app\data\images, or app\labint.db content
REM -----------------------------------------------------------------------------

chcp 65001 >nul
set PYTHONUTF8=1

cd /d "%~dp0"

echo [CLEAN] Backend root: %CD%

REM 1) Remove accidental duplicate root script (One Source of Truth is tools\sync_docs_db.py)
if exist "sync_docs_db.py" (
  echo [CLEAN] Deleting duplicate root sync_docs_db.py
  del /f /q "sync_docs_db.py" >nul 2>nul
)

REM 2) Remove Python caches
for /d /r %%D in (__pycache__) do (
  echo [CLEAN] Removing %%D
  rmdir /s /q "%%D" >nul 2>nul
)

REM 3) Remove docs DB backups produced by migrate script (optional; keep if you want)
if exist "app\data\labint_docs.db.bak_*" (
  echo [CLEAN] Removing labint_docs.db.bak_* backups under app\data
  del /f /q "app\data\labint_docs.db.bak_*" >nul 2>nul
)

REM 4) Remove sync reports (optional; regenerated)
if exist "sync_report.csv" (
  echo [CLEAN] Removing sync_report.csv (root)
  del /f /q "sync_report.csv" >nul 2>nul
)
if exist "app\data\sync_report.csv" (
  echo [CLEAN] Removing app\data\sync_report.csv
  del /f /q "app\data\sync_report.csv" >nul 2>nul
)

echo.
echo DONE.
pause
