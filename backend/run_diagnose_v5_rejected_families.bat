@echo off
setlocal
cd /d C:\chemlens\backend

echo [STEP 0] diagnose rejected families (case-level)
python diagnose_v5_rejected_families.py
