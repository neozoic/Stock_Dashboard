#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
看板 keepalive —— Windows 计划任务每 5 分钟调用一次（登录时也调一次）

职责:
  1. 看板进程死了 -> 拉起（通达信不在则先拉通达信，通达信停在登录框则自动点登录）
  2. 看板活着但 TQ 断了 (tdx_health != ok) 连续 N 次 -> 重启看板进程
     （进程内 TQ 状态中毒后无法自愈，重启是唯一解）
  3. 拉起前滚动日志，避免 dashboard.stdout.log / dashboard.stderr.log 无上限膨胀

行为矩阵:
  +------------------+-----------------+------------------------------------------+
  | 看板(端口)       | 通达信(TdxW)    | 动作                                     |
  +------------------+-----------------+------------------------------------------+
  | 活 + tdx ok      | -               | 静默（只写 keepalive.log 心跳）          |
  | 活 + tdx fail    | 未登录          | 不重启看板（重启无用），提示手动登录      |
  | 活 + tdx fail    | 已登录          | 连续 N 次 -> 重启看板（进程内状态中毒）  |
  | 死               | 死              | 拉通达信 -> 自动登录 -> 拉看板            |
  | 死               | 活              | 自动登录(若需要) -> 拉看板               |
  +------------------+-----------------+------------------------------------------+

限流: 1 小时内最多重启 3 次，防止异常时反复重启打爆 CPU。
路径全部自定位: 本脚本位于 <项目>/tools/ 下, 主程序在上一级目录。

2026-09-05: 从 .hermes 两份硬编码旧路径的副本重写, 作为唯一权威版本;
             .hermes 下的副本已改为调用本脚本的 thin wrapper。
2026-11-22: 升级为计划任务用的 keepalive —— 新增通达信自动拉起+自动登录、
             TQ 假死自愈、端口残留清理、日志滚动、限流。
"""
import json
import os
import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import tdx_login  # noqa: E402

# 用 abspath 而不是 resolve(): resolve() 会把映射盘 C:\Mac\Home 展开成 UNC
# (\psf\Home\...), 后续 CreateProcess 用 UNC 当 cwd 更容易出问题
PROJECT_DIR = Path(os.path.abspath(__file__)).parent.parent
DASHBOARD_SCRIPT = PROJECT_DIR / "stock_dashboard.py"
LOG_DIR = PROJECT_DIR / "logs"
DASHBOARD_PORT = int(os.environ.get("DASHBOARD_PORT", "8765"))
TDX_PROCESS = os.environ.get("TDX_EXE_NAME", "TdxW.exe")
TDX_HOME = Path(os.environ.get("TDX_HOME", r"C:\new_tdx64"))

KEEPALIVE_LOG = LOG_DIR / "keepalive.log"
STATE_FILE = LOG_DIR / "keepalive_state.json"
LOCK_FILE = LOG_DIR / "keepalive.lock"
LOCK_STALE_SECONDS = 900  # 锁文件超过 15 分钟视为陈旧（上次进程崩了）

# 日志滚动阈值
LOG_MAX_BYTES = int(os.environ.get("DASHBOARD_LOG_MAX_BYTES", 8 * 1024 * 1024))
LOG_KEEP = int(os.environ.get("DASHBOARD_LOG_KEEP", 3))

# TQ 假死连续几次才重启看板（每 5 分钟一次 -> 15 分钟容忍窗）
TDX_FAIL_THRESHOLD = int(os.environ.get("DASHBOARD_TDX_FAIL_THRESHOLD", 3))
# 1 小时内最多重启几次
MAX_RESTARTS_PER_HOUR = int(os.environ.get("DASHBOARD_MAX_RESTARTS_PER_HOUR", 3))
# 拉起通达信后等 TQ 接口就绪的最长时间（秒）
TDX_READY_TIMEOUT = int(os.environ.get("DASHBOARD_TDX_READY_TIMEOUT", 120))

DETACHED_PROCESS = 0x00000008
CREATE_NEW_PROCESS_GROUP = 0x00000200


def log(msg: str, quiet: bool = False) -> None:
    """写 keepalive 自身日志（计划任务下 stdout 会丢，必须落盘）

    quiet=True 只落盘不输出 stdout —— 正常心跳走这条，保持 stdout 干净。
    """
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    line = f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {msg}\n"
    try:
        with open(KEEPALIVE_LOG, "a", encoding="utf-8") as f:
            f.write(line)
    except Exception:
        pass
    if quiet:
        return
    try:
        sys.stdout.write(line)
        sys.stdout.flush()
    except Exception:
        pass


# ============================================================
# 互斥锁（登录自启 + 定时任务可能撞在一起）
# ============================================================
def acquire_lock() -> bool:
    """同一时刻只允许一个 watchdog 实例在跑"""
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    for _ in range(2):
        try:
            fd = os.open(str(LOCK_FILE), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                f.write(f"{os.getpid()} {time.time()}")
            return True
        except FileExistsError:
            stale = False
            try:
                content = LOCK_FILE.read_text(encoding="utf-8").strip().split()
                pid = int(content[0])
                stamp = float(content[1])
                if time.time() - stamp > LOCK_STALE_SECONDS:
                    stale = True
                else:
                    try:
                        os.kill(pid, 0)  # 探活：进程还在说明真有实例在跑
                    except OSError:
                        stale = True
            except Exception:
                stale = True
            if not stale:
                log("[SKIP] 另一个 watchdog 实例正在运行，本次跳过")
                return False
            try:
                LOCK_FILE.unlink()
            except OSError:
                return False
    return False


def release_lock() -> None:
    try:
        LOCK_FILE.unlink()
    except OSError:
        pass


# ============================================================
# 状态（限流 + 连续失败计数）
# ============================================================
def load_state() -> dict:
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            st = json.load(f)
        if isinstance(st, dict):
            return st
    except Exception:
        pass
    return {"tdx_fail_count": 0, "restarts": []}


def save_state(st: dict) -> None:
    try:
        with open(STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(st, f, ensure_ascii=False)
    except Exception:
        pass


def restart_budget_ok(st: dict) -> bool:
    """1 小时内重启次数未超限"""
    now = time.time()
    recent = [t for t in st.get("restarts", []) if now - float(t) < 3600]
    st["restarts"] = recent
    return len(recent) < MAX_RESTARTS_PER_HOUR


def record_restart(st: dict) -> None:
    st.setdefault("restarts", []).append(time.time())


# ============================================================
# 探活
# ============================================================
def dashboard_alive(port: int, host: str = "127.0.0.1", timeout: float = 5.0) -> bool:
    """/api/health 可应答 = 看板活着（TCP connect 会漏判'进程僵死但 socket 残留'）"""
    import urllib.request
    try:
        with urllib.request.urlopen(f"http://{host}:{port}/api/health", timeout=timeout) as r:
            return r.status == 200
    except Exception:
        return False


def dashboard_health(port: int = DASHBOARD_PORT, timeout: float = 8.0) -> dict:
    """拿 /api/health 的 JSON；拿不到返回 {}"""
    import urllib.request
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/api/health", timeout=timeout) as r:
            if r.status != 200:
                return {}
            return json.loads(r.read().decode("utf-8", "replace"))
    except Exception:
        return {}


def tdx_pids() -> list:
    """通达信进程 PID 列表"""
    try:
        r = subprocess.run(
            ["tasklist", "/FI", f"IMAGENAME eq {TDX_PROCESS}", "/NH"],
            capture_output=True, text=True, encoding="utf-8", errors="replace", timeout=5,
        )
    except Exception:
        return []
    out = r.stdout or ""
    pids = []
    for tok in out.replace("\r", "").split():
        if tok.isdigit() and int(tok) > 4:
            pids.append(int(tok))
    return pids


def tdx_alive() -> bool:
    """Windows tasklist 探测通达信进程"""
    return bool(tdx_pids())


def tdx_logged_in() -> bool:
    """通达信是否已进入行情界面（不是停在登录框）"""
    for pid in tdx_pids():
        try:
            if tdx_login.is_logged_in(pid):
                return True
        except Exception:
            continue
    return False


# ============================================================
# 端口残留清理
# ============================================================
def port_listener_pids(port: int) -> set:
    """返回占用该端口 LISTENING 的 PID 集合"""
    pids = set()
    try:
        r = subprocess.run(
            ["netstat", "-ano", "-p", "tcp"], capture_output=True, text=True, encoding="utf-8", errors="replace", timeout=10
        )
    except Exception:
        return pids
    for line in (r.stdout or "").splitlines():
        parts = line.split()
        if len(parts) < 5 or parts[0].upper() != "TCP":
            continue
        if "LISTENING" not in parts[3].upper():
            continue
        if parts[1].rsplit(":", 1)[-1] != str(port):
            continue
        if parts[-1].isdigit():
            pids.add(int(parts[-1]))
    return pids


def kill_port_listeners(port: int) -> int:
    """干掉占用端口的残留进程（僵尸实例 / 不健康的旧实例）"""
    killed = 0
    for pid in port_listener_pids(port):
        try:
            subprocess.run(["taskkill", "/PID", str(pid), "/F"],
                           capture_output=True, text=True, encoding="utf-8", errors="replace", timeout=15)
            log(f"已结束端口 {port} 的残留进程 PID={pid}")
            killed += 1
        except Exception as e:
            log(f"结束 PID={pid} 失败: {e}")
    return killed


# ============================================================
# 日志滚动
# ============================================================
def rotate_logs(max_bytes: int = LOG_MAX_BYTES, keep: int = LOG_KEEP) -> list:
    """日志超过阈值就滚动一代：.1 <- .2 <- ...，保留 keep 代

    Windows 上文件正被写时不能改名（"Device or resource busy"），
    所以先重试几次，还不行就退化成截断（打开时允许共享写，截断可行）。
    """
    rotated = []
    truncated = []
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    for name in ("dashboard.stdout.log", "dashboard.stderr.log"):
        p = LOG_DIR / name
        try:
            if not p.exists() or p.stat().st_size < max_bytes:
                continue
        except Exception:
            continue
        done = False
        for attempt in range(3):
            try:
                stale = LOG_DIR / f"{name}.{keep}"
                if stale.exists():
                    stale.unlink()
                for i in range(keep - 1, 0, -1):
                    src = LOG_DIR / f"{name}.{i}"
                    if src.exists():
                        src.replace(LOG_DIR / f"{name}.{i + 1}")
                p.replace(LOG_DIR / f"{name}.1")
                rotated.append(name)
                done = True
                break
            except Exception as e:
                if attempt == 2:
                    # 文件还被占着 -> 退化成截断
                    try:
                        with open(p, "r+b"):
                            pass
                        os.truncate(str(p), 0)
                        truncated.append(name)
                        done = True
                    except Exception as e2:
                        log(f"滚动 {name} 失败: {e} / 截断也失败: {e2}")
                else:
                    time.sleep(0.5)
    if rotated:
        log(f"日志已滚动: {', '.join(rotated)} (阈值 {max_bytes // 1024 // 1024}MB)")
    if truncated:
        log(f"日志被占用无法改名, 已截断: {', '.join(truncated)}")
    return rotated + truncated


# ============================================================
# 启动
# ============================================================
def start_tdx_client(wait: int = 40) -> bool:
    """拉起通达信客户端（GUI，弹在当前会话桌面）。进程出现即算成功。"""
    exe = TDX_HOME / TDX_PROCESS
    if not exe.exists():
        log(f"通达信不存在: {exe}")
        return False
    try:
        subprocess.Popen(
            ["cmd", "/c", "start", "", str(exe)],
            shell=False,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            cwd=str(TDX_HOME),
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        )
    except Exception as e:
        log(f"启动通达信失败: {e}")
        return False
    for i in range(max(1, wait // 2)):
        time.sleep(2)
        if tdx_alive():
            log(f"通达信已启动 (等待 {(i + 1) * 2}s)")
            return True
    log(f"通达信启动超时 ({wait}s)")
    return False


def ensure_tdx_logged_in(wait: int = 90) -> bool:
    """通达信每次启动都停在登录框 —— 自动点"登录"并等它进行情界面"""
    deadline = time.time() + wait
    while time.time() < deadline:
        pids = tdx_pids()
        if not pids:
            time.sleep(3)
            continue
        pid = pids[0]
        try:
            if tdx_login.is_logged_in(pid):
                return True
            state = tdx_login.ensure_login(pid, wait_seconds=45)
        except Exception as e:
            log(f"通达信登录处理异常: {e}")
            state = "no_dialog"
        if state == "clicked":
            log("已自动点通达信登录，等待进入行情界面")
            return True
        if state == "timeout":
            log("[WARN] 已点登录但通达信未进入行情界面（可能需要账号/验证码）")
            return False
        log(f"通达信尚未就绪 (state={state})，稍后重试")
        time.sleep(5)
    return False


def tdx_tq_ready(timeout: int = 45) -> bool:
    """探 TQ 接口是否就绪。

    必须在独立子进程里探：watchdog 进程不能自己占 TQ 槽位，
    否则会把看板正在用的槽位挤掉。

    mode 必须遍历 [1,2,3,0]（复用 tdx_client._test_tdx_init）：
    通达信每个 mode 只有一个槽位，被占用/耗尽后返回"初始化失败"，
    只试单个 mode 会误报"未就绪"（实测 mode 1/2/3 可被耗尽，
    只剩 mode 0 可用）。
    """
    code = (
        f"import sys; sys.path.insert(0, {str(PROJECT_DIR)!r});"
        "import tdx_client as t;"
        "sys.exit(0 if t._test_tdx_init() else 1)"
    )
    env = os.environ.copy()
    env.setdefault("TDX_HOME", str(TDX_HOME))
    env.setdefault("PYTHONIOENCODING", "utf-8")
    env.setdefault("PYTHONUTF8", "1")
    try:
        r = subprocess.run(
            [sys.executable, "-c", code],
            cwd=str(PROJECT_DIR), env=env,
            capture_output=True, text=True,
            encoding="utf-8", errors="replace",
            timeout=timeout,
        )
        return r.returncode == 0
    except Exception:
        return False


def wait_for_tdx_ready(timeout: int = TDX_READY_TIMEOUT) -> bool:
    """等 TQ 接口真正就绪。

    重要：通达信登录后 TQ 插件还要几秒才可用。而且看板进程一旦在
    TQ 未就绪时 initialize 失败，进程内状态会被"毒化"，之后永远连不上
    （只能重启进程）。所以拉看板之前必须先探到 TQ 就绪。
    """
    deadline = time.time() + timeout
    attempt = 0
    while time.time() < deadline:
        attempt += 1
        if tdx_tq_ready(timeout=40):
            log(f"TQ 接口已就绪 (探测 {attempt} 次)")
            return True
        time.sleep(5)
    return False


def start_dashboard() -> None:
    """detach 模式拉起看板 - Windows 必须用 DETACHED_PROCESS + CREATE_NEW_PROCESS_GROUP
    否则父进程退出子进程跟着死。env 对齐 start_dashboard.bat。"""
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    rotate_logs()
    env = os.environ.copy()
    env.setdefault("PYTHONIOENCODING", "utf-8")
    env.setdefault("PYTHONUTF8", "1")
    env.setdefault("TDX_HOME", str(TDX_HOME))
    env["DASHBOARD_PORT"] = str(DASHBOARD_PORT)
    env.setdefault("DASHBOARD_BIND", "0.0.0.0")
    env.setdefault("TDX_CALL_TIMEOUT", "8")

    out_fh = open(LOG_DIR / "dashboard.stdout.log", "ab", buffering=0)
    err_fh = open(LOG_DIR / "dashboard.stderr.log", "ab", buffering=0)
    subprocess.Popen(
        [sys.executable, str(DASHBOARD_SCRIPT)],
        cwd=str(PROJECT_DIR),
        env=env,
        stdout=out_fh, stderr=err_fh, stdin=subprocess.DEVNULL,
        creationflags=DETACHED_PROCESS | CREATE_NEW_PROCESS_GROUP,
        close_fds=True,
    )


def start_dashboard_and_wait(timeout: int = 90) -> bool:
    """拉起看板并轮询健康检查"""
    start_dashboard()
    for _ in range(max(1, timeout // 3)):
        time.sleep(3)
        if dashboard_alive(DASHBOARD_PORT):
            health = dashboard_health()
            state = health.get("tdx_health", "?")
            log(f"[OK] 看板已拉起 (port {DASHBOARD_PORT}, tdx_health={state})")
            return True
    log(f"[WARN] 看板已拉起，但 {timeout}s 内健康检查仍未通过 - 查 logs/dashboard.stdout.log")
    return False


# ============================================================
# 主流程
# ============================================================
def main() -> int:
    st = load_state()

    # ---- 情况 1: 看板活着 ----
    if dashboard_alive(DASHBOARD_PORT):
        health = dashboard_health()
        tdx_state = health.get("tdx_health")
        if tdx_state == "ok":
            if st.get("tdx_fail_count") or st.get("needs_tdx_restart"):
                st["tdx_fail_count"] = 0
                st.pop("needs_tdx_restart", None)
                save_state(st)
            log(f"看板正常 (port {DASHBOARD_PORT}, tdx_health=ok)", quiet=True)
            return 0  # 一切正常, 静默

        # 活着但 TQ 异常
        if not tdx_logged_in():
            # 通达信没登录 —— 重启看板没用（进程内 TQ 不是问题所在）
            log(f"[WARN] 看板存活但 tdx_health={tdx_state}；通达信未登录，"
                f"请在客户端点一次登录（自动登录已尝试过）")
            return 0

        # 通达信已登录但看板连不上 TQ —— 进程内 TQ 状态中毒，连续 N 次才重启
        st["tdx_fail_count"] = int(st.get("tdx_fail_count", 0)) + 1
        save_state(st)
        log(f"[WARN] 看板存活但 tdx_health={tdx_state} "
            f"(连续 {st['tdx_fail_count']}/{TDX_FAIL_THRESHOLD})")

        # 上一轮 TQ 未就绪就把看板拉起来了 —— 现在 TQ 好了，立刻重启一次
        pending = bool(st.pop("needs_tdx_restart", False))
        if pending:
            log("[ACTION] 上一轮 TQ 未就绪导致看板状态中毒，现在立即重启看板")
            st["tdx_fail_count"] = 0
            save_state(st)
        if pending or st["tdx_fail_count"] >= TDX_FAIL_THRESHOLD:
            if not restart_budget_ok(st):
                log("[WARN] 1 小时内重启次数已超限，放弃自愈，等人工介入")
                return 0
            log("[ACTION] TQ 连续异常，重启看板进程")
            kill_port_listeners(DASHBOARD_PORT)
            record_restart(st)
            st["tdx_fail_count"] = 0
            save_state(st)
            time.sleep(2)
            start_dashboard_and_wait()
        return 0

    # ---- 情况 2: 看板死了 ----
    log(f"看板({DASHBOARD_PORT}) 未响应")

    # 端口上可能还挂着不健康的僵尸实例，先清掉
    if kill_port_listeners(DASHBOARD_PORT):
        time.sleep(2)

    if not tdx_alive():
        log("通达信未运行，尝试拉起")
        start_tdx_client()

    # 通达信每次启动都停在登录框 —— 自动登录
    if not tdx_logged_in():
        ensure_tdx_logged_in()
    else:
        log("通达信已登录")

    # 拉看板之前先探 TQ 是否就绪 —— 否则看板第一次 init 失败会把进程内 TQ 状态毒化
    if wait_for_tdx_ready():
        st.pop("needs_tdx_restart", None)
    else:
        log("[WARN] TQ 接口未就绪 —— 看板会先走 akshare 兜底，"
            "TQ 就绪后下一轮会重启看板以修复 TQ")
        st["needs_tdx_restart"] = True

    if not restart_budget_ok(st):
        log("[WARN] 1 小时内重启次数已超限，放弃拉起，等人工介入")
        return 0

    record_restart(st)
    save_state(st)
    start_dashboard_and_wait()
    return 0


if __name__ == "__main__":
    if not acquire_lock():
        sys.exit(0)
    try:
        sys.exit(main())
    finally:
        release_lock()
