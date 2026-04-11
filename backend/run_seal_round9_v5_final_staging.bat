@echo off
call conda activate chemlens
cd /d %~dp0
python seal_round9_v5_final_staging.py
pause
