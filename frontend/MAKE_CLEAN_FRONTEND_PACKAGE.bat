@echo off
setlocal EnableExtensions
chcp 65001 >nul
cd /d "%~dp0"
if exist frontend_clean_release.zip del /q frontend_clean_release.zip
powershell -NoProfile -Command "Compress-Archive -Path * -DestinationPath frontend_clean_release.zip -Force -CompressionLevel Optimal -Exclude '.env','node_modules','.vite','dist'"
echo [ok] frontend_clean_release.zip created (without .env / node_modules / dist)
