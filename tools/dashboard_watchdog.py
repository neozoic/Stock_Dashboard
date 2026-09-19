#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
看板 watchdog - 每 5 分钟检查一次 DASHBOARD_PORT（默认 8765）

行为矩阵（no_agent cron 模式下, 空 stdout = 静默不发消息）：
  +-----------------+----------------+----------------------------------+
  | 看板(端口)      | 通达信(TdxW)   | 动作                             |
  +-----------------+----------------+----------------------------------+
  | 活着            | -              | 静默退出 (空 stdout)             |
  | 死              | 活着           | 自动 Popen 重启 → 轮询健康检查   |
  |                 |                | ~30s → 发"已自动恢复"消息       |
  | 死              | 死             | 发"看板+通达信双死, 等手动"      |
  +-----------------+----------------+----------------------------------+

设计原则:
- 看板死 + 通达信活 = 静默修复 (auto-recoverable, 不打扰用户)
- 看板死 + 通达信也死 = 报警 (通达信需要用户手动登录, agent 不假装能修)
- 路径全部自定位: 本脚本位于 <项目>/tools/ 下, 主程序在上一级目录

2026-09-05: 从 .hermes 两份硬编码旧路径的副本重写, 作为唯一权威版本;
             .hermes 下的副本已改为调用本脚本的 thin wrapper。
"""
import os
import subprocess
import sys
import time
from pathlib import Path

PROJECT_DIR = Path(__file__).resolve().parent.parent
DASHBOARD_SCRIPT = PROJECT_DIR / "stock_dashboard.py"
LOG_DIR = PROJECT_DIR / "logs"
DASHBOARD_PORT = int(os.environ.get("DASHBOARD_PORT", "8765"))
TDX_PROCESS = os.environ.get("TDX_EXE_NAME", "TdxW.exe")


def dashboard_alive(port: int, host: str = "127.0.0.1", timeout: float = 5.0) -> bool:
    """/api/health 可应答 = 看板活着（TCP connect 会漏判'进程僵死但 socket 残留'）"""
    import urllib.request
    try:
        with urllib.request.urlopen(f"http://{host}:{port}/api/health", timeout=timeout) as r:
            return r.status == 200
    except Exception:
        return False


def tdx_alive() -> bool:
    """Windows tasklist 探测通达信进程"""
    try:
        r = subprocess.run(
            ["tasklist", "/FI", f"IMAGENAME eq {TDX_PROCESS}", "/NH"],
            capture_output=True, text=True, timeout=5,
        )
        return TDX_PROCESS.lower() in r.stdout.lower()
    except Exception:
        return False


def start_dashboard() -> None:
    """detach 模式拉起看板 - Windows 必须用 DETACHED_PROCESS + CREATE_NEW_PROCESS_GROUP
    否则父进程退出子进程跟着死。env 对齐 start_dashboard.bat。"""
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    DETACHED_PROCESS = 0x00000008
    CREATE_NEW_PROCESS_GROUP = 0x00000200

    env = os.environ.copy()
    env.setdefault("PYTHONIOENCODING", "utf-8")
    env.setdefault("PYTHONUTF8", "1")
    env.setdefault("TDX_HOME", r"C:\new_tdx64")
    env["DASHBOARD_PORT"] = str(DASHBOARD_PORT)
    env.setdefault("DASHBOARD_BIND", "0.0.0.0")
    env.setdefault("TDX_CALL_TIMEOUT", "8")

    log_fh = open(LOG_DIR / "dashboard.stdout.log", "ab", buffering=0)
    subprocess.Popen(
        [sys.executable, str(DASHBOARD_SCRIPT)],
        cwd=str(PROJECT_DIR),
        env=env,
        stdout=log_fh, stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL,
        creationflags=DETACHED_PROCESS | CREATE_NEW_PROCESS_GROUP,
        close_fds=True,
    )


def main() -> int:
    if dashboard_alive(DASHBOARD_PORT):
        return 0  # 一切正常, 静默

    # 看板死了 - 看通达信
    if not tdx_alive():
        # 双死 -> 报警 (通达信需要手动启动)
        print(f"[WARN] 看板({DASHBOARD_PORT}) + 通达信({TDX_PROCESS}) 都挂了")
        print("   启动通达信后, watchdog 会自动拉起看板")
        return 0

    # 通达信活 -> 自动拉起看板（健康检查轮询 ~30s：冷启动 + TDX 初始化可能较慢）
    try:
        start_dashboard()
        for _ in range(15):
            time.sleep(2)
            if dashboard_alive(DASHBOARD_PORT):
                print(f"[OK] 看板已自动恢复 (port {DASHBOARD_PORT}, tdx=ok)")
                return 0
        print(f"[WARN] 看板已尝试拉起, 30s 后健康检查仍未通过 - 查日志 {LOG_DIR}")
    except Exception as e:
        print(f"[FAIL] 拉起看板失败: {type(e).__name__}: {e}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
