@echo off
setlocal
cd /d %~dp0
python upgrade_labint_frontmatter_batch1.py --db app\labint.db --db app\labint_round9_bridge_work.db --export-templates-dir seed_templates
if errorlevel 1 (
  echo.
  echo [ERROR] Frontmatter batch1 upgrade failed.
  exit /b 1
)
echo.
echo [OK] Frontmatter batch1 upgrade completed.
endlocal
