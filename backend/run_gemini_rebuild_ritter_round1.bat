@echo off
setlocal
cd /d %~dp0

call conda activate chemlens
if errorlevel 1 goto :err

echo ========================================================================
echo GEMINI SINGLE-FAMILY REBUILD - RITTER REACTION ROUND1
echo ========================================================================
python gemini_rebuild_single_family.py ^
  --config family_prompts/ritter_reaction_round1.json ^
  --model gemini-2.5-pro ^
  --attempts 3
if errorlevel 1 goto :err
exit /b 0

:err
echo [ERROR] rebuild failed.
exit /b 1
