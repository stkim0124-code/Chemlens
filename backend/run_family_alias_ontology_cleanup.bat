@echo off
setlocal
cd /d "%~dp0"
echo ========================================================================
echo FAMILY ALIAS / ONTOLOGY CLEANUP
echo ========================================================================
python family_alias_ontology_cleanup.py
if errorlevel 1 (
  echo [ERROR] alias cleanup failed.
  exit /b 1
)
