@echo off
setlocal
cd /d %~dp0
python install_reaction_family_patterns.py --db app\labint_round9_bridge_work.db --csv reaction_family_patterns_seed.csv
if errorlevel 1 pause
endlocal
