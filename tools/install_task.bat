@echo off
REM Register Stock_Dashboard scheduled tasks.
REM Kept for backward compatibility -- now delegates to install_keepalive.py,
REM which registers BOTH:
REM   Stock_Dashboard_KeepAlive - keepalive watchdog (every N minutes)
REM   Stock_Dashboard_AutoSync  - auto commit/push watcher (every N minutes)
REM plus HKCU Run entries so both start at user logon.
REM ASCII-only content on purpose (cmd.exe code page safety).
setlocal
chcp 65001 >nul

cd /d "%~dp0.."

python "%~dp0install_keepalive.py" %*
echo.
pause
