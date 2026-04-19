@echo off
setlocal EnableExtensions
chcp 65001 >nul
cd /d "%~dp0"
if exist backend_clean_release.zip del /q backend_clean_release.zip
powershell -NoProfile -Command "Compress-Archive -Path * -DestinationPath backend_clean_release.zip -Force -CompressionLevel Optimal -Exclude '.env','__pycache__','*.pyc','*.pyo','node_modules'"
echo [ok] backend_clean_release.zip created (without .env / __pycache__)
