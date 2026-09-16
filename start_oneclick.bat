@echo off
REM A-share fund-flow dashboard - one-click start
REM Start backend in background -> wait for health check -> open browser
setlocal
chcp 65001 >nul
set PYTHONIOENCODING=utf-8
set PYTHONUTF8=1

cd /d "%~dp0"

REM ---- config (overridable via env) ----
if not defined TDX_HOME set TDX_HOME=C:\new_tdx64
if not defined DASHBOARD_PORT set DASHBOARD_PORT=8765
if not defined DASHBOARD_BIND set DASHBOARD_BIND=0.0.0.0
if not defined TDX_CALL_TIMEOUT set TDX_CALL_TIMEOUT=8

set URL=http://localhost:%DASHBOARD_PORT%

echo === A-share fund-flow dashboard ===
echo TDX_HOME       = %TDX_HOME%
echo DASHBOARD_PORT = %DASHBOARD_PORT%
echo.

REM ---- python check ----
python --version >nul 2>&1
if errorlevel 1 (
    echo [ERROR] python not found. Install Python 3.11+ and add it to PATH.
    pause
    exit /b 1
)

REM ---- already running? just open the browser ----
curl -sf -m 5 "%URL%/api/health" >nul 2>&1
if not errorlevel 1 (
    echo Dashboard already running, opening browser...
    start "" "%URL%/"
    exit /b 0
)

REM ---- port occupied by something else? ----
set PID=
for /f "tokens=5" %%P in ('netstat -ano ^| findstr "LISTENING" ^| findstr /c:":%DASHBOARD_PORT% "') do set PID=%%P
if defined PID (
    echo [ERROR] Port %DASHBOARD_PORT% is used by PID %PID% and it is not this dashboard.
    echo Stop that process first, or run: set DASHBOARD_PORT=8766
    pause
    exit /b 1
)

REM ---- start backend in background ----
if not exist "logs" mkdir logs
echo Starting backend in background ^(logs: logs\dashboard.stdout.log / .stderr.log^)...
REM cmd /c wrapper is required so the log redirection reaches the child process
start "" /min cmd /c "python stock_dashboard.py 1>>logs\dashboard.stdout.log 2>>logs\dashboard.stderr.log"

REM ---- wait for health check (up to ~90 s; first health request itself can take 3-5 s) ----
set N=0
:wait_health
curl -sf -m 5 "%URL%/api/health" >nul 2>&1
if not errorlevel 1 goto opened
set /a N+=1
if %N% geq 30 (
    echo.
    echo [WARN] Health check failed after ~90s. See logs\dashboard.stderr.log
    pause
    exit /b 1
)
REM sleep 1s: use ping, because Git's GNU timeout shadows Windows timeout when Git\bin is in PATH
ping -n 2 127.0.0.1 >nul
goto wait_health

:opened
echo Backend is ready, opening browser %URL%/
start "" "%URL%/"
echo.
echo You may close this window. To stop the dashboard run stop_dashboard.bat
endlocal
