@echo off
REM A-share fund-flow dashboard - one-click stop
setlocal
chcp 65001 >nul
if not defined DASHBOARD_PORT set DASHBOARD_PORT=8765

set PID=
for /f "tokens=5" %%P in ('netstat -ano ^| findstr "LISTENING" ^| findstr /c:":%DASHBOARD_PORT% "') do set PID=%%P

if not defined PID (
    echo Dashboard is not running ^(nothing listening on port %DASHBOARD_PORT%^).
    exit /b 0
)

echo Stopping process %PID% on port %DASHBOARD_PORT% ...
taskkill /f /pid %PID% >nul 2>&1
if errorlevel 1 (
    echo [FAIL] Could not kill PID %PID% - it may have exited already.
) else (
    echo Dashboard stopped.
)
endlocal
