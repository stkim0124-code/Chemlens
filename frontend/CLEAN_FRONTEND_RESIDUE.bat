@echo off
setlocal EnableExtensions EnableDelayedExpansion

chcp 65001 >nul
set PYTHONUTF8=1

cd /d "%~dp0"

echo [CLEAN] Frontend root: %CD%

REM Remove build artifacts
if exist "dist" (
  echo [CLEAN] Removing dist
  rmdir /s /q "dist" >nul 2>nul
)
if exist "dist-ssr" (
  echo [CLEAN] Removing dist-ssr
  rmdir /s /q "dist-ssr" >nul 2>nul
)

REM Remove accidental caches
for /d /r %%D in (node_modules) do (
  REM do not delete node_modules automatically; too heavy
  echo [INFO] node_modules present at %%D (not deleted)
)

echo.
echo DONE.
pause
