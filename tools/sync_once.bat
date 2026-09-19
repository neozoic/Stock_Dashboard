@echo off
REM Run one immediate sync (no debounce wait)
setlocal
chcp 65001 >nul
set "SCRIPT=%~dp0auto_sync.py"

python "%SCRIPT%" --once
pause
