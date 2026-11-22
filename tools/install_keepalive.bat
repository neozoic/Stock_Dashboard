@echo off
REM Register Stock_Dashboard keepalive tasks (wrapper -> install_keepalive.py).
REM   1) Stock_Dashboard_KeepAlive - every N minutes (schtasks, no elevation needed)
REM   2) login autostart          - HKCU Run key (schtasks /sc onlogon needs admin)
REM ASCII-only content on purpose (cmd.exe code page safety).
setlocal
chcp 65001 >nul

cd /d "%~dp0.."

python "%~dp0install_keepalive.py" %*
echo.
pause
