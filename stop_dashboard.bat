@echo off
REM A-share fund-flow dashboard - one-click stop
REM Verifies the process is actually stock_dashboard.py before killing,
REM so an unrelated process that happens to hold the port is never killed.
setlocal
chcp 65001 >nul
if not defined DASHBOARD_PORT set DASHBOARD_PORT=8765

set PID=
for /f "tokens=5" %%P in ('netstat -ano ^| findstr "LISTENING" ^| findstr /c:":%DASHBOARD_PORT% "') do set PID=%%P

if not defined PID (
    echo Dashboard is not running ^(nothing listening on port %DASHBOARD_PORT%^).
    exit /b 0
)

REM verify the PID belongs to stock_dashboard.py before killing
powershell -NoProfile -Command "$c=(Get-CimInstance Win32_Process -Filter \"ProcessId=%PID%\").CommandLine; if ($c -match 'stock_dashboard\.py') { exit 0 } else { exit 1 }"
if errorlevel 1 (
    echo [REFUSED] PID %PID% listens on %DASHBOARD_PORT% but is not stock_dashboard.py - not killing it.
    exit /b 1
)

echo Stopping dashboard process %PID% on port %DASHBOARD_PORT% ...
taskkill /f /pid %PID% >nul 2>&1
if errorlevel 1 (
    echo [FAIL] Could not kill PID %PID% - it may have exited already.
) else (
    echo Dashboard stopped.
)
endlocal
