@echo off
setlocal ENABLEDELAYEDEXPANSION
cd /d %~dp0

echo ========================================================================
echo GEMINI SINGLE-FAMILY REBUILD - PINNER REACTION ROUND2
echo ========================================================================
python gemini_rebuild_single_family.py --config family_prompts\pinner_reaction_round2.json
if errorlevel 1 (
  echo [ERROR] rebuild failed.
  exit /b 1
)
