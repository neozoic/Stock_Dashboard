@echo off
REM 注册 Stock_Dashboard auto_sync 到 Windows Task Scheduler(开机自启)
setlocal

set "TASK_NAME=Stock_Dashboard_AutoSync"
set "PYTHON_EXE=C:\Users\openclaw\AppData\Local\Programs\Python\Python312\python.exe"
set "SCRIPT=C:\Mac\Home\Desktop\win_WorkSpace_ClaudeCode\Stock_Dashboard\tools\auto_sync.py"
set "WORK_DIR=C:\Mac\Home\Desktop\win_WorkSpace_ClaudeCode\Stock_Dashboard"

echo ============================================
echo   Stock_Dashboard auto-sync 安装
echo ============================================

REM 检查 python
if not exist "%PYTHON_EXE%" (
    echo [错误] 找不到 python: %PYTHON_EXE%
    pause
    exit /b 1
)

REM 删除旧任务(如有)
schtasks /delete /tn "%TASK_NAME%" /f >nul 2>&1

REM 创建新任务:开机时以当前用户身份启动(避免 UAC),无窗口
schtasks /create ^
    /tn "%TASK_NAME%" ^
    /tr "\"%PYTHON_EXE%\" \"%SCRIPT%\"" ^
    /sc onlogon ^
    /rl highest ^
    /f >nul 2>&1

if errorlevel 1 (
    echo [警告] schtasks 创建失败(可能需要管理员权限)
    echo         改用手动启动: tools\start_sync.bat
    pause
    exit /b 1
)

echo [成功] 已注册任务 "%TASK_NAME%"
echo        触发条件: 用户登录时(开机)
echo        监控脚本: %SCRIPT%
echo        日志位置: %WORK_DIR%\logs\auto_sync.log
echo.
echo 立即启动: schtasks /run /tn "%TASK_NAME%"
pause
