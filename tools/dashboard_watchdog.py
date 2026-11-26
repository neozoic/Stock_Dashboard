#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
看板 keepalive —— Windows 计划任务每 5 分钟调用一次（登录时也调一次）

职责:
  1. 看板进程死了 -> 拉起（通达信不在则先拉通达信，通达信停在登录框则自动点登录）
  2. 看板活着但 TQ 断了 (tdx_health != ok) 连续 N 次 -> 重启看板进程
     （进程内 TQ 状态中毒后无法自愈，重启是唯一解）
  3. 拉起前滚动应急日志（dashboard.stdout.log / dashboard.stderr.log）；
     业务日志由看板自己的 RotatingFileHandler 写 logs/dashboard.log，这里不管

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

# 控制台程序(git/tasklist/netstat/taskkill/powershell)在 pythonw 父进程下
# 会自己开一个黑色控制台窗口并抢焦点(2026-11-23 实测: auto_sync 每 30 秒
# 弹一次)。CREATE_NO_WINDOW 让子进程静默运行，不占焦点。
_NO_WINDOW = getattr(subprocess, "CREATE_NO_WINDOW", 0)

# 用 abspath 而不是 resolve(): resolve() 会把映射盘 C:\Mac\Home 展开成 UNC
# (\psf\Home\...), 后续 CreateProcess 用 UNC 当 cwd 更容易出问题
PROJECT_DIR = Path(os.path.abspath(__file__)).parent.parent
DASHBOARD_SCRIPT = PROJECT_DIR / "stock_dashboard.py"
LOG_DIR = PROJECT_DIR / "logs"

# 仓库根目录的模块：.env 加载器 + 飞书推送。
# 用 PROJECT_DIR（abspath）而不是 resolve()，理由同上 —— resolve() 会把
# 映射盘展开成 UNC，后续 CreateProcess 用 UNC 当 cwd 更容易出问题。
sys.path.insert(0, str(PROJECT_DIR))
try:
    import env_loader
    env_loader.load_env()
except Exception:
    env_loader = None
try:
    import feishu_push
except Exception:
    feishu_push = None

DASHBOARD_PORT = int(os.environ.get("DASHBOARD_PORT", "8765"))
TDX_PROCESS = os.environ.get("TDX_EXE_NAME", "TdxW.exe")
TDX_HOME = Path(os.environ.get("TDX_HOME", r"C:\new_tdx64"))

KEEPALIVE_LOG = LOG_DIR / "keepalive.log"
STATE_FILE = LOG_DIR / "keepalive_state.json"
STATE_BACKUP_FILE = LOG_DIR / "keepalive_state.json.bak"
LOCK_FILE = LOG_DIR / "keepalive.lock"

# 日志滚动阈值
LOG_MAX_BYTES = int(os.environ.get("DASHBOARD_LOG_MAX_BYTES", 8 * 1024 * 1024))
LOG_KEEP = int(os.environ.get("DASHBOARD_LOG_KEEP", 3))

# TQ 假死连续几次才重启看板（每 5 分钟一次 -> 15 分钟容忍窗）
TDX_FAIL_THRESHOLD = int(os.environ.get("DASHBOARD_TDX_FAIL_THRESHOLD", 3))
# 1 小时内最多重启几次（总量上限，防缓慢抖动）
MAX_RESTARTS_PER_HOUR = int(os.environ.get("DASHBOARD_MAX_RESTARTS_PER_HOUR", 10))
# 崩溃自愈的突发限制：真崩溃循环会在秒级~分钟级反复重启
MAX_RESTARTS_BURST = int(os.environ.get("DASHBOARD_MAX_RESTARTS_BURST", 3))
RESTART_BURST_WINDOW = int(os.environ.get("DASHBOARD_RESTART_BURST_WINDOW", 900))
# 拉起通达信后等 TQ 接口就绪的最长时间（秒）
TDX_READY_TIMEOUT = int(os.environ.get("DASHBOARD_TDX_READY_TIMEOUT", 120))
# 看板"启动中"窗口（秒）：进程起来到 bind 端口要 ~20s（探 TQ + 首轮拉数更久）。
# 这个窗口内如果又来一次触发，不能判定"看板死了"去强杀 —— 那会烧掉一个 TQ 槽位
DASHBOARD_STARTING_WINDOW = int(os.environ.get("DASHBOARD_STARTING_WINDOW", 180))
# TQ 槽位耗尽时重启通达信客户端的限流（1 小时内最多几次）
MAX_TDX_RESTARTS_PER_HOUR = int(os.environ.get("DASHBOARD_MAX_TDX_RESTARTS_PER_HOUR", 1))

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
# 飞书通知
# ============================================================
# 看板/看门狗的异常事件推飞书 —— 用户不用守着 keepalive.log 看。
#
# 去重状态单独放 logs/keepalive_notify.json，**不碰 keepalive_state.json**：
# 后者管的是重启限流预算，那是安全-critical 的账本，
# 拿它记通知时间戳等于给限流逻辑埋雷。

NOTIFY_STATE_FILE = LOG_DIR / "keepalive_notify.json"
# 同类事件的冷却期（秒）。看板每 5 分钟醒一次，不冷却的话一次崩溃循环
# 就能刷出十几张一模一样的卡片。
NOTIFY_COOLDOWN_SEC = 1800


def _load_notified() -> dict:
    try:
        with open(NOTIFY_STATE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _save_notified(data: dict) -> None:
    try:
        LOG_DIR.mkdir(parents=True, exist_ok=True)
        tmp = NOTIFY_STATE_FILE.parent / (NOTIFY_STATE_FILE.name + ".tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False)
        os.replace(str(tmp), str(NOTIFY_STATE_FILE))
    except Exception:
        pass


def notify_event(key: str, title: str, lines: list, level: str = "warn",
                 cooldown: int = NOTIFY_COOLDOWN_SEC) -> bool:
    """发一条飞书通知；同 key 在冷却期内只发一次（防刷屏）。

    绝不抛异常 —— 通知失败不能影响看门狗的判定与自愈动作。
    """
    if feishu_push is None or not feishu_push.is_configured():
        return False
    now = time.time()
    notified = _load_notified()
    try:
        last = float(notified.get(key) or 0)
    except (TypeError, ValueError):
        last = 0.0
    if now - last < cooldown:
        return False
    try:
        ok = feishu_push.notify_system(title, lines, level=level)
    except Exception as e:
        log(f"[WARN] 飞书通知失败({key}): {e}")
        return False
    if ok:
        notified[key] = now
        # 只留最近 50 条，别让文件无限长
        if len(notified) > 50:
            for k in sorted(notified, key=lambda k: notified[k])[:-50]:
                notified.pop(k, None)
        _save_notified(notified)
    return ok


def flush_notifications(timeout: float = 15.0) -> None:
    """进程退出前把队列里的通知发完。

    watchdog 是一次性进程（计划任务每 5 分钟起一个）—— 不显式 flush 的话，
    守护线程会随进程退出被干掉，通知就永远发不出去。
    """
    if feishu_push is None:
        return
    try:
        feishu_push.shutdown(timeout)
    except Exception:
        pass


# ============================================================
# 互斥锁（登录自启 + 定时任务可能撞在一起）
# ============================================================
_lock_fh = None


def acquire_lock() -> bool:
    """同一时刻只允许一个 watchdog 实例在跑。

    用 msvcrt 文件锁（进程退出自动释放），不再用"创建标志文件"：
      标志文件方案有两个坑 —— ① os.open(O_EXCL) 与 f.write() 之间有窗口
      （共享目录上是毫秒级），另一个实例读到空内容会走 except 分支判定
      "陈旧"，从而**删掉对方的锁**，两个实例并行跑（重复拉起 + 烧槽位）；
      ② 进程崩了锁文件残留，得靠时间戳兜底。
    OS 托管的文件锁没有这两个坑。
    """
    global _lock_fh
    import msvcrt
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    try:
        fh = open(LOCK_FILE, "a+b")
        fh.seek(0)
        msvcrt.locking(fh.fileno(), msvcrt.LK_NBLCK, 1)
        _lock_fh = fh
        return True
    except OSError:
        log("[SKIP] 另一个 watchdog 实例正在运行，本次跳过")
        try:
            fh.close()
        except Exception:
            pass
        return False


def release_lock() -> None:
    global _lock_fh
    if _lock_fh is not None:
        try:
            _lock_fh.close()
        except Exception:
            pass
        _lock_fh = None


# ============================================================
# 状态（限流 + 连续失败计数）
# ============================================================
def load_state() -> dict:
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            st = json.load(f)
        if isinstance(st, dict):
            return st
        log("[WARN] keepalive 状态文件格式异常，按空状态处理")
    except FileNotFoundError:
        pass
    except Exception as e:
        # JSON 损坏时**不能**静默归零 —— 归零等于清空限流预算，
        # 崩溃循环就能无限重启（巡检 P2-19）。先试备份，
        # 备份也读不到就按"预算已耗尽"处理，等人工 --reset-budget。
        log(f"[WARN] keepalive 状态文件损坏: {e}")
        try:
            with open(STATE_BACKUP_FILE, "r", encoding="utf-8") as f:
                st = json.load(f)
            if isinstance(st, dict):
                log("[WARN] 已从备份恢复 keepalive 状态")
                return st
        except Exception:
            pass
        log("[WARN] 状态文件与备份均不可用，暂按'限流已耗尽'处理"
            "（人工介入后用 --reset-budget 解锁）")
        return {"tdx_fail_count": 0, "restarts": [], "_state_broken": True}
    return {"tdx_fail_count": 0, "restarts": []}


def save_state(st: dict) -> None:
    """原子写：先写临时文件，再 os.replace 替换。

    直接 open("w") 写一半崩溃（共享目录上更容易）会让 JSON 损坏，
    而损坏被当成"空状态"= 限流归零（巡检 P2-19）。
    """
    tmp = STATE_FILE.parent / (STATE_FILE.name + ".tmp")
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(st, f, ensure_ascii=False)
            f.flush()
            try:
                os.fsync(f.fileno())
            except OSError:
                pass
        os.replace(str(tmp), str(STATE_FILE))
    except Exception as e:
        log(f"[WARN] 写 keepalive 状态失败: {e}")
        return
    try:
        with open(STATE_BACKUP_FILE, "w", encoding="utf-8") as f:
            json.dump(st, f, ensure_ascii=False)
    except Exception:
        pass


def restart_budget_ok(st: dict) -> bool:
    """重启限流：挡住崩溃循环，但别让"早已恢复"的旧重启一直占着预算。

    只看 1 小时内的绝对次数会踩坑：早上恢复过 3 次后，即使看板随后稳定
    运行了半小时，一次正常杀进程（人工/系统更新/内存回收）也会被预算挡住，
    看板就这么一直停着（实测踩到）。改成两级：

      - 突发限制：RESTART_BURST_WINDOW(默认 15 分钟) 内最多 MAX_RESTARTS_BURST 次
        —— 真崩溃循环的形态就是短时间内反复重启
      - 总量限制：1 小时内最多 MAX_RESTARTS_PER_HOUR 次 —— 防缓慢抖动
    """
    now = time.time()
    if st.get("_state_broken"):
        # 状态文件损坏 → 无法判断重启预算，保守起见不自愈（防无限重启循环）
        return False
    restarts = [float(t) for t in st.get("restarts", [])]
    burst = [t for t in restarts if now - t < RESTART_BURST_WINDOW]
    hourly = [t for t in restarts if now - t < 3600]
    st["restarts"] = hourly
    if len(burst) >= MAX_RESTARTS_BURST:
        return False
    return len(hourly) < MAX_RESTARTS_PER_HOUR


def record_restart(st: dict) -> None:
    st.setdefault("restarts", []).append(time.time())


# ============================================================
# 探活
# ============================================================
def dashboard_alive(port: int, host: str = "127.0.0.1", timeout: float = 15.0) -> bool:
    """/api/health 可应答 = 看板活着（TCP connect 会漏判'进程僵死但 socket 残留'）

    超时给到 15s：/api/health 现在只读缓存（不再做实时 TQ 调用），
    正常响应是几十毫秒；但看板正在提交 history/做 GC 时可能慢几秒，
    原来 5s 太紧，容易把健康看板误判成"死了"（巡检 P2-22）。
    """
    import urllib.request
    try:
        with urllib.request.urlopen(f"http://{host}:{port}/api/health", timeout=timeout) as r:
            return r.status == 200
    except Exception:
        return False


def dashboard_health(port: int = DASHBOARD_PORT, timeout: float = 15.0) -> dict:
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
    """通达信进程 PID 列表。

    按行解析固定列，只取 PID 列 —— 原来把整段输出按空白切分后取"数字"，
    可能把内存列的数值（如 "123456 K"）当成 PID，导致 taskkill 误杀无关进程
    （巡检 P2-23）。
    """
    try:
        r = subprocess.run(
            ["tasklist", "/FI", f"IMAGENAME eq {TDX_PROCESS}", "/NH"],
            capture_output=True, text=True, encoding="utf-8", errors="replace", timeout=5,
            creationflags=_NO_WINDOW,
        )
    except Exception:
        return []
    pids = []
    for line in (r.stdout or "").replace("\r", "").splitlines():
        parts = line.split()
        if len(parts) < 2:
            continue
        # 无表头输出格式: "TdxW.exe   10636 Console  1  100,000 K"
        # 带表头时第一列是 "Image Name"
        if parts[0].lower() in ("image", "image name", "映像名称", "进程名"):
            continue
        if parts[1].isdigit() and int(parts[1]) > 4:
            pids.append(int(parts[1]))
    return pids


def _process_cmdline(pid: int) -> str:
    """取进程命令行（用于校验进程身份，避免误杀无关进程）"""
    try:
        r = subprocess.run(
            ["powershell", "-NoProfile", "-Command",
             f"(Get-CimInstance Win32_Process -Filter 'ProcessId={int(pid)}').CommandLine"],
            capture_output=True, text=True, encoding="utf-8", errors="replace", timeout=15,
            creationflags=_NO_WINDOW,
        )
        return (r.stdout or "").strip()
    except Exception:
        return ""


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


def kill_tdx_client(st: dict) -> int:
    """结束**自己拉起**的通达信客户端进程。

    用途：重置 TQ 的 mode 槽位。通达信每个 mode(0/1/2/3) 只有一个槽位，
    持有它的进程被强杀时槽位不会释放（实测：看板在 mode=2 被杀后，
    mode=2 的 init 直接返回"返回ID小于0"）——每重启一次看板就烧掉一个槽位，
    4 个烧光后 TQ 就再也起不来了。只有重启通达信客户端才能重置槽位。

    **只杀自己拉起的那个 PID**（巡检 P1-18）：通达信是用户在看盘/下单的软件，
    把用户正在用的会话强杀掉副作用很大。如果当前这个通达信不是本 watchdog
    拉起来的（例如用户自己开的），就不杀，改为提示人工重启。
    """
    pid = st.get("tdx_pid")
    if not pid:
        log("[WARN] 当前通达信不是本 watchdog 拉起的，不执行强杀"
            "（避免杀掉用户正在看盘/下单的会话）；"
            "如需重置 TQ 槽位请人工重启通达信")
        return 0
    try:
        subprocess.run(
            ["taskkill", "/PID", str(int(pid)), "/F"],
            capture_output=True, text=True, encoding="utf-8", errors="replace", timeout=15,
            creationflags=_NO_WINDOW,
        )
        log(f"已结束通达信进程 PID={pid}")
        st.pop("tdx_pid", None)
        return 1
    except Exception as e:
        log(f"结束通达信 PID={pid} 失败: {e}")
        return 0


def tdx_slot_reset_allowed(st: dict) -> bool:
    """重启通达信客户端的限流（1 小时内最多 N 次）"""
    now = time.time()
    recent = [float(t) for t in st.get("tdx_restarts", []) if now - float(t) < 3600]
    st["tdx_restarts"] = recent
    return len(recent) < MAX_TDX_RESTARTS_PER_HOUR


# ============================================================
# 端口残留清理
# ============================================================
def port_listener_pids(port: int) -> set:
    """返回占用该端口 LISTENING 的 PID 集合"""
    pids = set()
    try:
        r = subprocess.run(
            ["netstat", "-ano", "-p", "tcp"], capture_output=True, text=True, encoding="utf-8",
            errors="replace", timeout=10,
            creationflags=_NO_WINDOW,
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
    """干掉占用端口的残留进程（僵尸实例 / 不健康的旧实例）。

    杀之前**校验进程身份**：只杀命令行里确实是 stock_dashboard.py 的进程。
    否则 8765 被别的程序占用时会误杀无关程序（巡检 P2-21）。
    """
    killed = 0
    for pid in port_listener_pids(port):
        cmdline = _process_cmdline(pid)
        if "stock_dashboard.py" not in cmdline:
            log(f"[WARN] PID={pid} 占用端口 {port} 但不是看板进程，跳过"
                f"（命令行: {cmdline[:120]}）")
            continue
        try:
            subprocess.run(["taskkill", "/PID", str(pid), "/F"],
                           capture_output=True, text=True, encoding="utf-8", errors="replace",
                           timeout=15, creationflags=_NO_WINDOW)
            log(f"已结束端口 {port} 的残留看板进程 PID={pid}")
            killed += 1
        except Exception as e:
            log(f"结束 PID={pid} 失败: {e}")
    return killed


def request_shutdown(port: int = DASHBOARD_PORT, timeout: float = 15.0) -> bool:
    """请求看板优雅退出（POST /api/shutdown，仅本机）。

    意义：通达信每个 TQ mode 只有一个槽位，持有它的进程被强杀时槽位不会
    释放（实测看板在 mode=2 被 taskkill 后，mode=2 的 init 直接返回
    "返回ID小于0"）——每重启一次看板就烧掉一个槽位，4 个烧光 TQ 就再也
    起不来。优雅退出会走 tdx_client.close()，槽位能正常归还。
    """
    import urllib.request
    try:
        req = urllib.request.Request(
            f"http://127.0.0.1:{port}/api/shutdown", data=b"", method="POST"
        )
        # 看板设了 DASHBOARD_TOKEN 的话，/api/* 要带 Bearer —— 不带就 401，
        # 优雅退出会失败 → 退化成强杀 → 漏掉一个 TQ 槽位。
        token = os.environ.get("DASHBOARD_TOKEN", "").strip()
        if token:
            req.add_header("Authorization", f"Bearer {token}")
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status == 200
    except Exception:
        return False


def stop_dashboard(port: int = DASHBOARD_PORT, wait: int = 45) -> int:
    """停掉看板：先请求优雅退出（归还 TQ 槽位），超时再强杀残留。

    等的是"端口释放"而不是"健康检查失败"——优雅退出时服务循环先停、
    进程随后才真正退出，只看健康检查会以为已经退干净，结果新实例绑不上端口。

    返回被强杀的进程数（0 表示优雅退出成功或本来就没进程）。
    """
    if request_shutdown(port):
        log("[ACTION] 已请求看板优雅退出（归还 TQ 槽位）")
        for _ in range(max(1, wait // 2)):
            if not port_listener_pids(port):
                return 0
            time.sleep(2)
        log("[WARN] 看板优雅退出超时，改为强杀")
        notify_event("dashboard_kill", "⚠️ 看板被强杀",
                     ["优雅退出超时（等待 %ds 端口未释放），已改为强杀" % wait,
                      "强杀可能漏掉一个通达信 TQ 槽位"],
                     level="warn")
    return kill_port_listeners(port)


# ============================================================
# 日志滚动
# ============================================================
def rotate_logs(max_bytes: int = LOG_MAX_BYTES, keep: int = LOG_KEEP) -> list:
    """日志超过阈值就滚动一代：.1 <- .2 <- ...，保留 keep 代

    只处理 stdout/stderr 两个"应急输出"文件；业务日志由看板自己写
    logs/dashboard.log（Python 的 RotatingFileHandler 轮转，close 后再改名，
    Windows 上可行且无空洞）。

    Windows 上被写着的文件不能改名（"Device or resource busy"）。这里
    **不再退化成截断**：看板进程持有的文件句柄偏移量不会因截断回退，
    下一次写入会在文件头补出一整段 NUL 空洞（实测 2.9MB 的 stderr 里
    2.86MB 是空洞），既撑大文件又让 grep 认成二进制。改名失败就只告警，
    交给人工处理。
    """
    rotated = []
    blocked = []
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    for name in ("dashboard.stdout.log", "dashboard.stderr.log"):
        p = LOG_DIR / name
        try:
            if not p.exists() or p.stat().st_size < max_bytes:
                continue
        except Exception:
            continue
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
                break
            except Exception as e:
                if attempt == 2:
                    blocked.append(f"{name}({e})")
                else:
                    time.sleep(0.5)
    if rotated:
        log(f"日志已滚动: {', '.join(rotated)} (阈值 {max_bytes // 1024 // 1024}MB)")
    if blocked:
        log(f"[WARN] 日志被占用无法改名(不截断, 避免产生空洞): {', '.join(blocked)}")
    return rotated


def check_log_sizes(max_bytes: int = LOG_MAX_BYTES) -> None:
    """看板存活期间也看一眼应急日志大小。

    业务日志由看板自己轮转（logs/dashboard.log），这里只管 stdout/stderr。
    被看板占用时无法改名，也不截断（截断会在文件头留 NUL 空洞），只告警。
    """
    for name in ("dashboard.stdout.log", "dashboard.stderr.log"):
        p = LOG_DIR / name
        try:
            if p.exists() and p.stat().st_size > max_bytes:
                log(f"[WARN] {name} 已达 {p.stat().st_size // 1024 // 1024}MB，"
                    f"被看板占用无法改名（不截断以免产生空洞）")
        except Exception:
            pass


# ============================================================
# 启动
# ============================================================
def start_tdx_client(wait: int = 40):
    """拉起通达信客户端（GUI，弹在当前会话桌面）。进程出现即算成功。

    返回 (ok, pid)：pid 是**新拉起**的那个进程。
    只有自己拉起的进程才允许之后被 kill_tdx_client() 杀掉 ——
    通达信是用户在看盘/下单的软件（巡检 P1-18）。
    """
    exe = TDX_HOME / TDX_PROCESS
    if not exe.exists():
        log(f"通达信不存在: {exe}")
        return False, None
    before = set(tdx_pids())
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
        return False, None
    for i in range(max(1, wait // 2)):
        time.sleep(2)
        now_pids = set(tdx_pids())
        if now_pids:
            new = now_pids - before
            pid = sorted(new)[0] if new else sorted(now_pids)[0]
            log(f"通达信已启动 (等待 {(i + 1) * 2}s) PID={pid}")
            return True, pid
    log(f"通达信启动超时 ({wait}s)")
    return False, None


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
            notify_event("tdx_login_clicked", "🔑 通达信已自动登录",
                         ["已自动点击登录按钮，等待进入行情界面"],
                         level="info")
            return True
        if state == "timeout":
            log("[WARN] 已点登录但通达信未进入行情界面（可能需要账号/验证码）")
            notify_event("tdx_login_timeout", "⚠️ 通达信自动登录超时",
                         ["已点登录但未进入行情界面",
                          "可能需要账号/验证码，请手动在客户端登录一次"],
                         level="warn")
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
            creationflags=_NO_WINDOW,
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


def start_dashboard_and_wait(st: dict = None, timeout: int = 90) -> bool:
    """拉起看板并轮询健康检查。

    拉起前在 state 里写"启动中"标记：keepalive 的锁只覆盖自身执行窗口，
    不覆盖"锁释放 → 看板 bind 端口"这段空窗（进程起来要 ~20s）。
    没有这个标记，第二个触发源落进空窗就会误判"看板死了"，
    强杀刚拉起的实例 → 再烧一个 TQ 槽位（巡检 P0-7）。
    """
    if st is not None:
        st["dashboard_starting_at"] = time.time()
        save_state(st)
    start_dashboard()
    ok = False
    for _ in range(max(1, timeout // 3)):
        time.sleep(3)
        if dashboard_alive(DASHBOARD_PORT):
            health = dashboard_health()
            state = health.get("tdx_health", "?")
            log(f"[OK] 看板已拉起 (port {DASHBOARD_PORT}, tdx_health={state})")
            notify_event("dashboard_started", "✅ 看板已拉起",
                         [f"端口 {DASHBOARD_PORT}", f"TQ 健康状态: {state}",
                          "http://127.0.0.1:%d" % DASHBOARD_PORT],
                         level="ok")
            ok = True
            break
    if not ok:
        log(f"[WARN] 看板已拉起，但 {timeout}s 内健康检查仍未通过 - 查 logs/dashboard.stdout.log")
        notify_event("dashboard_start_unhealthy", "⚠️ 看板拉起后健康检查未通过",
                     [f"{timeout}s 内 /api/health 仍未就绪",
                      "查 logs/dashboard.stdout.log"],
                     level="warn")
    if st is not None and ok:
        # 只有**确认起来了**才清标记；起不来时保留标记 ——
        # 窗口内不重复拉起，避免第二个触发源把刚拉起、还没 bind 端口的实例杀掉
        st.pop("dashboard_starting_at", None)
        save_state(st)
    return ok


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
            check_log_sizes()
            log(f"看板正常 (port {DASHBOARD_PORT}, tdx_health=ok)", quiet=True)
            return 0  # 一切正常, 静默

        # busy=正在扫描(连接忙不是坏) / idle=非交易时段 / unknown=还没拉过第一轮
        # —— 这些都不是故障，**不重启**（巡检 P1-1 的配套修复：
        #   之前 /api/health 做实时 TQ 调用，扫描期间必然超时 → 误报 fail
        #   → 连续 3 次后重启看板，这就是重启风暴的根因）
        if tdx_state in ("busy", "idle", "unknown"):
            if st.get("tdx_fail_count"):
                st["tdx_fail_count"] = 0
                st.pop("needs_tdx_restart", None)
                save_state(st)
            check_log_sizes()
            log(f"看板正常 (port {DASHBOARD_PORT}, tdx_health={tdx_state})", quiet=True)
            return 0

        if tdx_state is None:
            # 拿不到 tdx_health = tdx_client 模块不可用（TDX_OK=False）。
            # 这不是"进程内 TQ 状态中毒"，重启看板完全无效（巡检 P1-19）——
            # 原来会当成中毒一直重启到限流耗尽，看板全程拿不到 TQ 数据。
            log("[WARN] 看板存活但 /api/health 没有 tdx_health —— "
                "tdx_client 模块可能不可用；重启看板无效，等人工排查")
            return 0

        # 走到这里只剩 tdx_state == "fail"：TQ 真的连不上
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
        notify_event("tdx_health_fail", "⚠️ 看板连不上通达信",
                     [f"TQ 状态: {tdx_state}",
                      f"连续失败 {st['tdx_fail_count']}/{TDX_FAIL_THRESHOLD} 次",
                      "通达信已登录但进程内 TQ 状态可能中毒，够数后自动重启看板"],
                     level="warn", cooldown=60)

        # 上一轮 TQ 未就绪就把看板拉起来了 —— 现在 TQ 好了，立刻重启一次
        pending = bool(st.pop("needs_tdx_restart", False))
        if pending:
            log("[ACTION] 上一轮 TQ 未就绪导致看板状态中毒，现在立即重启看板")
            notify_event("tdx_poisoned_restart", "🔄 看板因 TQ 状态中毒被重启",
                         ["上一轮 TQ 未就绪就拉起了看板，进程内状态已中毒",
                          "现在立即重启看板以修复 TQ 连接"],
                         level="warn")
            st["tdx_fail_count"] = 0
            save_state(st)
        if pending or st["tdx_fail_count"] >= TDX_FAIL_THRESHOLD:
            if not restart_budget_ok(st):
                log(f"[WARN] 重启次数超限({RESTART_BURST_WINDOW}s 内 {MAX_RESTARTS_BURST} 次 / 1h 内 {MAX_RESTARTS_PER_HOUR} 次)，放弃自愈，等人工介入")
                notify_event("restart_budget_exhausted", "🛑 自愈已停手，等人工介入",
                             [f"重启次数超限（{RESTART_BURST_WINDOW}s 内 {MAX_RESTARTS_BURST} 次 / 1h 内 {MAX_RESTARTS_PER_HOUR} 次）",
                              "看板不会自动重启了，需要人工介入",
                              "处理完用 --reset-budget 清空预算"],
                             level="error")
                return 0
            # 重启看板前先确认 TQ 槽位还够用：槽位被烧光时重启看板只会
            # 再烧一个（还起不来），此时该做的是重启通达信重置槽位
            if not tdx_tq_ready(timeout=40) and tdx_slot_reset_allowed(st):
                log("[ACTION] TQ 探测失败（疑似 mode 槽位耗尽），先重启通达信重置槽位")
                notify_event("tq_slot_reset", "🔧 重启通达信以重置 TQ 槽位",
                             ["TQ 探测失败，疑似 mode 槽位耗尽",
                              "重启通达信客户端重置槽位（不动看板）"],
                             level="warn")
                kill_tdx_client(st)
                time.sleep(3)
                ok, pid = start_tdx_client()
                if ok and pid:
                    st["tdx_pid"] = pid
                ensure_tdx_logged_in()
                st.setdefault("tdx_restarts", []).append(time.time())
                save_state(st)
                wait_for_tdx_ready()
            log("[ACTION] TQ 连续异常，重启看板进程")
            notify_event("dashboard_restart_tq", "🔄 看板因 TQ 异常被重启",
                         [f"TQ 连续异常 {TDX_FAIL_THRESHOLD} 次",
                          "重启看板进程以恢复 TQ 连接（优雅退出，槽位能归还）"],
                         level="warn")
            stop_dashboard()
            record_restart(st)
            st["tdx_fail_count"] = 0
            save_state(st)
            time.sleep(2)
            start_dashboard_and_wait(st)
        return 0

    # ---- 情况 2: 看板没响应 ----
    # 但要排除"上一轮刚拉起、还在启动窗口内"的情况：
    # 看板进程起来到 bind 端口要 ~20s，这个窗口里第二次触发会误判"看板死了"
    # → 强杀刚拉起的实例 → **再烧一个 TQ 槽位**（巡检 P0-7）。
    # 启动窗口由 state 里的 dashboard_starting_at 记录。
    starting_at = float(st.get("dashboard_starting_at") or 0)
    if starting_at:
        elapsed = time.time() - starting_at
        if elapsed < DASHBOARD_STARTING_WINDOW:
            log(f"看板正在启动中（{int(DASHBOARD_STARTING_WINDOW - elapsed)}s 后再判定），"
                f"本次不重复拉起")
            return 0
        st.pop("dashboard_starting_at", None)
        save_state(st)

    log(f"看板({DASHBOARD_PORT}) 未响应")

    # **限流检查必须在所有破坏性动作之前**（巡检 P2-18）：
    # 原来它排在"杀通达信 + 自动重登 + 重拉通达信"之后，
    # 超限时通达信已经白重启一次，而看板反而再也不会被拉起。
    if not restart_budget_ok(st):
        log(f"[WARN] 重启次数超限({RESTART_BURST_WINDOW}s 内 {MAX_RESTARTS_BURST} 次 / 1h 内 {MAX_RESTARTS_PER_HOUR} 次)，放弃拉起，等人工介入")
        notify_event("budget_exhausted_no_start", "🛑 看板未被拉起，等人工介入",
                     [f"重启次数超限（{RESTART_BURST_WINDOW}s 内 {MAX_RESTARTS_BURST} 次 / 1h 内 {MAX_RESTARTS_PER_HOUR} 次）",
                      "看板已无响应且不会自动拉起，需要人工介入",
                      "处理完用 --reset-budget 清空预算"],
                     level="error")
        return 0

    # 端口上可能还挂着不健康的僵尸实例，先清掉（活着就优雅退，退不掉再强杀）
    if stop_dashboard():
        time.sleep(2)

    if not tdx_alive():
        log("通达信未运行，尝试拉起")
        notify_event("tdx_not_running", "⚠️ 通达信掉线，正在拉起",
                     ["看板无响应且通达信没在运行",
                      "正在拉起通达信客户端并自动登录"],
                     level="warn")
        ok, pid = start_tdx_client()
        if ok and pid:
            st["tdx_pid"] = pid
            save_state(st)

    # 通达信每次启动都停在登录框 —— 自动登录
    if not tdx_logged_in():
        ensure_tdx_logged_in()
    else:
        log("通达信已登录")

    # 拉看板之前先探 TQ 是否就绪 —— 否则看板第一次 init 失败会把进程内 TQ 状态毒化
    if wait_for_tdx_ready():
        st.pop("needs_tdx_restart", None)
    elif tdx_logged_in() and tdx_slot_reset_allowed(st):
        # 通达信已登录但 TQ 探测失败 —— 大概率是 mode 槽位被烧光
        # （看板被强杀会漏掉槽位，每重启一次看板烧一个，4 个烧光 TQ 就再也起不来）
        # 只有重启通达信客户端才能重置槽位
        log("[ACTION] 通达信已登录但 TQ 探测失败，判定为 mode 槽位耗尽，"
            "重启通达信客户端重置槽位")
        notify_event("tdx_restart_for_tq", "🔧 重启通达信以重置 TQ 槽位",
                     ["通达信已登录但 TQ 探测失败，判定 mode 槽位耗尽",
                      "重启通达信客户端重置槽位（不动看板）"],
                     level="warn")
        kill_tdx_client(st)
        time.sleep(3)
        ok, pid = start_tdx_client()
        if ok and pid:
            st["tdx_pid"] = pid
        ensure_tdx_logged_in()
        st.setdefault("tdx_restarts", []).append(time.time())
        save_state(st)
        if wait_for_tdx_ready():
            st.pop("needs_tdx_restart", None)
            log("[OK] 重启通达信后 TQ 已就绪")
        else:
            log("[WARN] 重启通达信后 TQ 仍未就绪 —— 看板先走 akshare 兜底")
            notify_event("tq_not_ready_after_reset", "⚠️ 重启通达信后 TQ 仍未就绪",
                         ["槽位重置后 TQ 仍探测不到",
                          "看板先走 akshare 兜底，TQ 就绪后下一轮会重启看板"],
                         level="warn")
            st["needs_tdx_restart"] = True
    else:
        log("[WARN] TQ 接口未就绪 —— 看板会先走 akshare 兜底，"
            "TQ 就绪后下一轮会重启看板以修复 TQ")
        notify_event("tq_not_ready", "⚠️ TQ 接口未就绪",
                     ["TQ 探测失败，看板会先走 akshare 兜底",
                      "TQ 就绪后下一轮会重启看板以修复 TQ"],
                     level="warn")
        st["needs_tdx_restart"] = True

    started = start_dashboard_and_wait(st)
    if started:
        record_restart(st)
        save_state(st)
    else:
        # 拉起失败（端口被占/启动异常）**不**占重启预算 ——
        # 失败的尝试不该把预算吃光，否则看板彻底无人拉起（巡检 P3）
        log("[WARN] 拉起未成功，不计入重启限流预算")
    return 0


if __name__ == "__main__":
    # 运维开关：--reset-budget 清空重启限流计数。
    # 人工重启（调试/升级）也会被记进崩溃自愈的预算里，连续几次之后
    # keepalive 就会以为在崩溃循环而拒绝拉起 —— 人工介入后用它解锁。
    #
    # 注意：必须**先拿锁再改状态**（巡检 P2-19/P3）—— 原来在拿锁之前写，
    # 会和正在运行的实例互相覆盖 state。
    if not acquire_lock():
        sys.exit(0)
    try:
        if "--reset-budget" in sys.argv:
            st = load_state()
            st["restarts"] = []
            st["tdx_restarts"] = []
            st["tdx_fail_count"] = 0
            st.pop("needs_tdx_restart", None)
            st.pop("dashboard_starting_at", None)
            save_state(st)
            log("[ACTION] 重启限流计数已清空")
        sys.exit(main())
    finally:
        # watchdog 是一次性进程 —— 不 flush 的话，队列里的飞书通知
        # 会随守护线程一起被干掉，永远发不出去
        flush_notifications(15.0)
        release_lock()
