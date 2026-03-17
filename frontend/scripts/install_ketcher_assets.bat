@echo off
setlocal
cd /d %~dp0\..

echo [CHEMLENS] Installing OFFICIAL Ketcher standalone assets into public\ketcher ...
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0install_ketcher_assets.ps1"
if errorlevel 1 (
  echo.
  echo [CHEMLENS] Install failed.
  exit /b 1
)

echo.
echo [CHEMLENS] Install complete.
echo - If dev server is running, stop and restart it.
endlocal
