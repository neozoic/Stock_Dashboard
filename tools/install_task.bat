@echo off
REM Register Stock_Dashboard auto_sync with Windows Task Scheduler (start on user logon)
setlocal
chcp 65001 >nul

set "TASK_NAME=Stock_Dashboard_AutoSync"
set "SCRIPT=%~dp0auto_sync.py"
set "WORK_DIR=%~dp0.."

echo ============================================
echo   Stock_Dashboard auto-sync installer
echo ============================================

REM locate pythonw (no console window on logon)
set "PYTHONW_EXE="
for /f "delims=" %%P in ('where pythonw 2^>nul') do (
    set "PYTHONW_EXE=%%P"
    goto found_pythonw
)
echo [ERROR] pythonw not found in PATH.
pause
exit /b 1
:found_pythonw

REM remove old task if present
schtasks /delete /tn "%TASK_NAME%" /f >nul 2>&1

REM create task: run as current user at logon (no admin / no UAC needed)
schtasks /create ^
    /tn "%TASK_NAME%" ^
    /tr "\"%PYTHONW_EXE%\" \"%SCRIPT%\"" ^
    /sc onlogon ^
    /f >nul 2>&1

if errorlevel 1 (
    echo [WARN] schtasks failed - start manually instead: tools\start_sync.bat
    pause
    exit /b 1
)

echo [OK] Task "%TASK_NAME%" registered
echo     Trigger : user logon
echo     Script  : %SCRIPT%
echo     Log     : %WORK_DIR%\logs\auto_sync.log
echo.
echo Start now: schtasks /run /tn "%TASK_NAME%"
pause
