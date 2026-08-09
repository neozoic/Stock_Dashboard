@echo off
REM 立即同步一次(不等待 debounce)
setlocal
set "PYTHON_EXE=C:\Users\openclaw\AppData\Local\Programs\Python\Python312\python.exe"
set "SCRIPT=C:\Mac\Home\Desktop\win_WorkSpace_ClaudeCode\Stock_Dashboard\tools\auto_sync.py"

"%PYTHON_EXE%" "%SCRIPT%" --once
pause
