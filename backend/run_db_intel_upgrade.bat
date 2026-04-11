@echo off
cd /d %~dp0
python upgrade_labint_intel_schema.py --db app\labint.db --db app\labint_round9_bridge_work.db --export-templates-dir seed_templates
pause
