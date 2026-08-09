@echo off
REM A 股资金流实时看板 - Windows 启动脚本
REM UTF-8 编码 + 设置 TDX_HOME + Python 版本自检

setlocal
chcp 65001 >nul
set PYTHONIOENCODING=utf-8
set PYTHONUTF8=1

REM 通达信客户端安装路径（按实际修改；默认 C:\new_tdx64）
if not defined TDX_HOME set TDX_HOME=C:\new_tdx64

REM 端口与绑定地址
if not defined DASHBOARD_PORT set DASHBOARD_PORT=8765
if not defined DASHBOARD_BIND set DASHBOARD_BIND=0.0.0.0

REM TdxQuant 单次调用超时（秒）；盘中可改 5，盘后建议 12
if not defined TDX_CALL_TIMEOUT set TDX_CALL_TIMEOUT=8

cd /d "%~dp0"

echo === A 股资金流看板启动 ===
echo TDX_HOME      = %TDX_HOME%
echo DASHBOARD_PORT = %DASHBOARD_PORT%
echo DASHBOARD_BIND = %DASHBOARD_BIND%
echo.

REM Python 版本自检（仅提示，不阻断）
python --version

REM 启动后端（日志写到 logs\ 子目录）
if not exist "logs" mkdir logs
python stock_dashboard.py 1>logs\dashboard.stdout.log 2>logs\dashboard.stderr.log

endlocal