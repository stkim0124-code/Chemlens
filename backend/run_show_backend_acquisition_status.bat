@echo off
setlocal
cd /d %~dp0
echo =============================================================
echo SHOW BACKEND ACQUISITION STATUS
echo =============================================================
python show_backend_acquisition_status.py
echo =============================================================
endlocal
