@echo off
REM 手动启动 Stock_Dashboard auto_sync 后台服务
setlocal
set "PYTHON_EXE=C:\Users\openclaw\AppData\Local\Programs\Python\Python312\python.exe"
set "SCRIPT=C:\Mac\Home\Desktop\win_WorkSpace_ClaudeCode\Stock_Dashboard\tools\auto_sync.py"
set "LOG_DIR=C:\Mac\Home\Desktop\win_WorkSpace_ClaudeCode\Stock_Dashboard\logs"

if not exist "%PYTHON_EXE%" (
    echo [错误] 找不到 python: %PYTHON_EXE%
    pause
    exit /b 1
)

if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"

echo 启动 auto_sync(后台),日志: %LOG_DIR%\auto_sync.log
echo 停止: schtasks /end /tn Stock_Dashboard_AutoSync
echo  或:   taskkill /FI "WINDOWTITLE eq auto_sync*"
echo.
start /B "auto_sync" "C:\Users\openclaw\AppData\Local\Programs\Python\Python312\pythonw.exe" "%SCRIPT%" > "%LOG_DIR%\auto_sync_startup.log" 2>&1
echo 启动完成。1 秒后查看日志:
timeout /T 2 /nobreak >nul
type "%LOG_DIR%\auto_sync.log" 2>nul
pause
