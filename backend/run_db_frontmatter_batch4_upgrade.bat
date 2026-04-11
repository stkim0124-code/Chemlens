@echo off
setlocal
cd /d %~dp0
python upgrade_labint_frontmatter_batch4.py
if errorlevel 1 (
  echo.
  echo [ERROR] frontmatter batch4 upgrade failed.
  exit /b 1
)
echo.
echo [OK] frontmatter batch4 upgrade completed.
endlocal
