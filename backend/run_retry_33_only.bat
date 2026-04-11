@echo off
setlocal
cd /d %~dp0

echo [CHEMLENS] retry 33 failed pages only (CSV-based, primary-only)
echo.

echo Current folder: %CD%
echo Using fail CSV  : fail_queue_full_v5.csv
echo Using DB        : labint_round9_v5.db
echo.

echo If conda is not already active, activate your chemlens env first.
echo Example:
echo   conda activate chemlens
echo.

python retry_fail_queue_404.py --db labint_round9_v5.db --batch full_batch1 --fail-csv fail_queue_full_v5.csv --zip-dir "%CD%\app\data\images\named reactions"

echo.
echo Done. Press any key to close.
pause >nul
endlocal
