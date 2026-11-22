#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
注册 Stock_Dashboard 的计划任务 / 登录自启项。

装两套东西:
  1) keepalive —— tools/dashboard_watchdog.py
       - 计划任务 Stock_Dashboard_KeepAlive: 每 N 分钟跑一次
         (看板死了自动拉起, 通达信没了自动拉起+自动点登录)
       - 登录自启: 计划任务 onlogon 需要管理员权限, 普通用户会
         "Access is denied", 所以额外写一个 HKCU\\...\\Run 注册项兜底
  2) auto_sync —— tools/auto_sync.py
       - 计划任务 Stock_Dashboard_AutoSync: 每 N 分钟触发 (常驻循环,
         自带单实例锁, 重复触发会立即退出)
       - 登录自启: 同样用 HKCU\\...\\Run 注册项

为什么用 schtasks.exe 而不是 Register-ScheduledTask:
  Register-ScheduledTask 走 WMI/CIM, 普通用户注册会 "Access is denied";
  schtasks.exe 能注册用户级任务。/sc onlogon 例外, 必须提权。

用法:
    python tools/install_keepalive.py [--interval 5] [--no-autosync]
    python tools/install_keepalive.py --uninstall
"""
import argparse
import os
import subprocess
import sys
import winreg
from pathlib import Path

# abspath 保留映射盘路径 (C:\Mac\Home\...), 不展开成 UNC
TOOLS_DIR = Path(os.path.abspath(__file__)).parent
PROJECT_DIR = TOOLS_DIR.parent

TASK_KEEPALIVE = "Stock_Dashboard_KeepAlive"
TASK_STARTUP = "Stock_Dashboard_Startup"
TASK_AUTOSYNC = "Stock_Dashboard_AutoSync"
RUN_KEY = r"Software\Microsoft\Windows\CurrentVersion\Run"
RUN_VALUE_STARTUP = "Stock_Dashboard_Startup"
RUN_VALUE_AUTOSYNC = "Stock_Dashboard_AutoSync"

SCRIPTS = {
    "keepalive": TOOLS_DIR / "dashboard_watchdog.py",
    "autosync": TOOLS_DIR / "auto_sync.py",
}


def find_pythonw():
    """找 pythonw.exe（登录自启不弹黑框）"""
    found = subprocess.run(["where", "pythonw.exe"], capture_output=True, text=True,
                           encoding="utf-8", errors="replace")
    for line in (found.stdout or "").splitlines():
        line = line.strip()
        if line and Path(line).exists():
            return line
    import shutil
    py = shutil.which("python.exe")
    if py:
        cand = Path(py).with_name("pythonw.exe")
        if cand.exists():
            return str(cand)
    return None


def run_schtasks(args):
    r = subprocess.run(["schtasks", "/create", *args],
                       capture_output=True, text=True,
                       encoding="utf-8", errors="replace")
    return r.returncode == 0, ((r.stdout or "") + (r.stderr or "")).strip()


def delete_task(name):
    subprocess.run(["schtasks", "/delete", "/tn", name, "/f"],
                   capture_output=True, text=True,
                   encoding="utf-8", errors="replace")


def set_run_value(name, command):
    with winreg.CreateKeyEx(winreg.HKEY_CURRENT_USER, RUN_KEY, 0,
                            winreg.KEY_SET_VALUE) as key:
        winreg.SetValueEx(key, name, 0, winreg.REG_SZ, command)


def del_run_value(name):
    try:
        with winreg.OpenKey(winreg.HKEY_CURRENT_USER, RUN_KEY, 0,
                            winreg.KEY_SET_VALUE) as key:
            winreg.DeleteValue(key, name)
    except FileNotFoundError:
        pass


def register_script(pythonw, script, task_name, interval, run_value_name):
    """注册一个脚本: 周期任务 + 登录自启注册项"""
    action = f'"{pythonw}" "{script}"'

    delete_task(task_name)
    del_run_value(run_value_name)

    ok, out = run_schtasks([
        "/tn", task_name,
        "/tr", action,
        "/sc", "minute", "/mo", str(interval),
        "/it",
        "/f",
    ])
    if ok:
        print(f"[OK] 计划任务 '{task_name}' 已注册 (每 {interval} 分钟)")
    else:
        print(f"[ERROR] 创建 '{task_name}' 失败: {out}")
        return False

    try:
        set_run_value(run_value_name, action)
        print(f"[OK] 登录自启项 '{run_value_name}' 已写入")
    except Exception as e:
        print(f"[WARN] 注册表自启项写入失败: {e}")
    return True


def uninstall():
    for name in (TASK_KEEPALIVE, TASK_STARTUP, TASK_AUTOSYNC):
        delete_task(name)
    for name in (RUN_VALUE_STARTUP, RUN_VALUE_AUTOSYNC):
        del_run_value(name)
    print("[OK] 已移除计划任务与登录自启项")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--interval", type=int, default=5,
                    help="keepalive 触发间隔（分钟）")
    ap.add_argument("--sync-interval", type=int, default=5,
                    help="auto_sync 触发间隔（分钟）")
    ap.add_argument("--no-autosync", action="store_true",
                    help="只装 keepalive，不装 auto_sync")
    ap.add_argument("--uninstall", action="store_true")
    args = ap.parse_args()

    if args.uninstall:
        uninstall()
        return 0

    for key, path in SCRIPTS.items():
        if not path.exists():
            print(f"[ERROR] 找不到 {key} 脚本: {path}")
            return 1

    pythonw = find_pythonw()
    if not pythonw:
        print("[ERROR] 找不到 pythonw.exe")
        return 1

    print("=" * 44)
    print("  Stock_Dashboard keepalive installer")
    print("=" * 44)
    print(f"pythonw : {pythonw}")
    print(f"项目目录 : {PROJECT_DIR}")
    print()

    ok = register_script(pythonw, SCRIPTS["keepalive"], TASK_KEEPALIVE,
                         args.interval, RUN_VALUE_STARTUP)
    if not ok:
        return 1

    if not args.no_autosync:
        ok = register_script(pythonw, SCRIPTS["autosync"], TASK_AUTOSYNC,
                             args.sync_interval, RUN_VALUE_AUTOSYNC)
        if not ok:
            return 1

    print()
    print(f"验证 : schtasks /query /tn \"{TASK_KEEPALIVE}\"")
    print(f"立即跑: schtasks /run  /tn \"{TASK_KEEPALIVE}\"")
    print(f"日志 : {PROJECT_DIR / 'logs' / 'keepalive.log'}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
