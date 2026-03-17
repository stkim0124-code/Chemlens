@echo off
setlocal EnableExtensions EnableDelayedExpansion

REM -----------------------------------------------------------------------------
REM run_docs_sync.bat
REM - Dry-run sync of documents.file_path to PDFs folder
REM - Portable + UTF-8 safe
REM -----------------------------------------------------------------------------

chcp 65001 >nul
set PYTHONUTF8=1

set "ENV_NAME=chemlens"

cd /d "%~dp0"

REM Sanity checks
if not exist "tools\sync_docs_db.py" (
  echo [ERR] Missing tools\sync_docs_db.py
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

call :ACTIVATE_CONDA "%ENV_NAME%"

set "DB=%~dp0app\data\labint_docs.db"
set "PDFS=%~dp0app\data\pdfs"

echo.
echo [DRY-RUN] Sync documents.file_path to match PDFs folder
echo   DB  : %DB%
echo   PDF : %PDFS%
echo.
python tools\sync_docs_db.py --db "%DB%" --pdfs "%PDFS%"
if errorlevel 1 (
  echo [ERR] Dry-run failed. Check the output above.
  pause
  exit /b 1
)

echo.
echo If the DRY-RUN looks good, run APPLY:
echo   python tools\sync_docs_db.py --db "%DB%" --pdfs "%PDFS%" --apply
echo.
pause
exit /b 0

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
  echo        If Python/RDKit/Deps are missing, open "Anaconda Prompt" and run this .bat again.
)

exit /b 0
