@echo off
setlocal EnableExtensions
chcp 65001 >nul
cd /d "%~dp0"
python repair_labint_frontmatter_state.py
if errorlevel 1 (
  echo [ERR] frontmatter repair failed
  pause
  exit /b 1
)
echo [ok] frontmatter repair completed
