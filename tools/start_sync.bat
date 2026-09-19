@echo off
REM Start Stock_Dashboard auto_sync in background (this session only)
setlocal
chcp 65001 >nul

set "SCRIPT=%~dp0auto_sync.py"
set "LOG_DIR=%~dp0..\logs"

if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"

echo Starting auto_sync in background, log: %LOG_DIR%\auto_sync.log
echo Stop: powershell "Get-CimInstance Win32_Process -Filter ""Name='pythonw.exe'"" | ? {$_.CommandLine -match 'auto_sync'} | %% {Stop-Process -Id $_.ProcessId -Force}"
echo Note: for logon autostart use install_task.bat (Task Scheduler)
echo.
start "auto_sync" /min pythonw "%SCRIPT%"
ping -n 3 127.0.0.1 >nul
type "%LOG_DIR%\auto_sync.log" 2>nul
pause
