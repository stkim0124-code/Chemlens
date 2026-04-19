@echo off
setlocal ENABLEDELAYEDEXPANSION
cd /d %~dp0

echo ========================================================================
echo GEMINI SINGLE-FAMILY REBUILD - SCHWARTZ HYDROZIRCONATION ROUND1
echo ========================================================================
python gemini_rebuild_single_family.py --config family_prompts\schwartz_hydrozirconation_round1.json
if errorlevel 1 (
  echo [ERROR] rebuild failed.
  exit /b 1
)
