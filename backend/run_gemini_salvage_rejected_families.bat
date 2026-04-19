@echo off
setlocal
cd /d %~dp0
python gemini_salvage_rejected_families.py %*
endlocal
