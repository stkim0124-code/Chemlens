@echo off
setlocal
cd /d %~dp0
if not exist app\labint.db (
  echo [ERROR] app\labint.db not found
  exit /b 1
)
python upgrade_labint_frontmatter_batch3.py --db app\labint.db --db app\labint_round9_bridge_work.db --export-templates-dir seed_templates
if errorlevel 1 exit /b %errorlevel%
echo [OK] frontmatter batch3 upgrade applied.
endlocal
