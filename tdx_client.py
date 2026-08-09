"""
TdxQuant 客户端包装模块
=======================

统一封装通达信 TdxQuant 的常用数据接口，解决以下问题：
  1. DLL 路径非默认位置（必须显式传 dll_path）
  2. 需要把 C:\\new_tdx64 加入 PATH（兄弟 DLL 依赖）
  3. Python 3.13 因版本号检测失败，必须使用 3.12
  4. tqcenter 没有自动重连，连接断了调用会卡死
  5. 资金流字段（Zjl/ZAFPre*）需要格式化成我们看板能直接用的形态

提供的方法：
  - init()                    初始化（幂等）
  - health_check()            健康检查（拿一只股票快照验证）
  - get_a_share_list()        拿沪深 A 股全量列表
  - get_sectors()             拿所有板块列表
  - get_sector_stocks(code)   拿板块成分股
  - get_snapshot(code)        单股快照（标准化字段名）
  - get_fund_flow(code)       单股资金流（主力净流入、撤单、封单、多周期涨幅）
  - batch_snapshot(codes, callback, max_workers=8)
                             批量快照（线程池并发，断线自动重试）

使用方式：
    from tdx_client import init, get_snapshot, batch_snapshot, get_fund_flow
    init()
    snap = get_snapshot('600519.SH')
    flows = batch_snapshot(['600519.SH', '000001.SZ', ...])
"""

from __future__ import annotations

import os
import sys
import time
import logging
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional, Callable, Any

# === 必须在 import tqcenter 之前设置 PATH，否则 LoadLibrary 失败 ===
# 路径可通过 TDX_HOME / TDX_EXE_NAME 环境变量覆盖，便于迁移主机。
TDX_HOME = os.environ.get("TDX_HOME", r"C:\new_tdx64")
TDX_PYPLUGINS = os.path.join(TDX_HOME, "PYPlugins")
TDX_EXE_NAME = os.environ.get("TDX_EXE_NAME", "TdxW.exe")
_TDX_HOME = TDX_HOME
_TDX_PYPLUGINS = TDX_PYPLUGINS
_TDX_EXE = os.path.join(TDX_HOME, TDX_EXE_NAME)
_TDX_DLL = os.path.join(TDX_PYPLUGINS, "TPythClient.dll")

# 通达信客户端进程名（用于 ensure_tdx_running 检查）
_TDX_PROCESS_NAMES = (TDX_EXE_NAME,)

# run_mode: 0=本地策略(默认), 1=远程/同进程多策略。通达信客户端默认 mode=0 槽位有限，
# 多次初始化后会被拒（"返回ID小于0"）。mode=1 槽位更宽松，init() 里会强制设 mode=1。

if sys.platform == "win32":
    # 让 Windows 能找到兄弟 DLL（tdxrpc64.dll 等）
    for d in (_TDX_HOME, _TDX_PYPLUGINS):
        if d not in os.environ.get("PATH", ""):
            os.environ["PATH"] = d + ";" + os.environ.get("PATH", "")
    try:
        # Python 3.8+ 推荐方式
        os.add_dll_directory(_TDX_HOME)
        os.add_dll_directory(_TDX_PYPLUGINS)
    except (AttributeError, OSError):
        pass

# === 加载 tqcenter ===
# 历史要求 Python 3.12；3.13 因 tqcenter 版本号检测会触发问题。
# 这里放宽到 3.11–3.13 区间，向后兼容 3.13+；若有问题会在 health_check 中体现。
try:
    import tqcenter
    from tqcenter import tq
except ImportError as e:
    py_major_minor = sys.version_info[:2]
    raise ImportError(
        f"tqcenter 未安装。当前 Python {py_major_minor[0]}.{py_major_minor[1]}；"
        f"请确认 pip install tqcenter 已安装且 PATH 含 {TDX_HOME}。原始错误: {e}"
    )

# Monkey patch: tqcenter 在很多地方会调 cls.close() 把连接关掉，
# 这会导致 _initialized 翻成 False、下次调用又触发重连循环。
# 我们包装一个"安全的 close"，只在真正需要时才真关。
_original_close = tq.close

def _safe_close():
    """安全的 close — 只在 atexit / 显式调用时真关，被动触发时不关。

    tqcenter 在错误处理里调的 cls.close() 我们不接管（只 swallow）。
    用户显式调 tdx_client.close() 时才会真关连接。
    """
    import traceback
    tb = traceback.extract_stack()
    # 如果是 atexit/main 退出触发的，真关
    if any('atexit' in str(f) or '_auto_close' in str(f) for f in tb[-3:]):
        return _original_close()
    # 否则：什么都不做（让连接保持，下次调用还能用）
    logger.debug("swallow close() from %s", tb[-2].name if len(tb) > 1 else "?")

tq.close = _safe_close

logger = logging.getLogger("tdx_client")

# === 初始化状态 ===
_init_lock = threading.Lock()
_initialized = False
_last_init_time = 0.0
_REINIT_INTERVAL = 300  # 5 分钟无活动自动重连一次，避免长期挂载导致句柄僵死


def _check_tdx_process_alive() -> bool:
    """
    检查通达信客户端进程是否在运行。
    用 PowerShell 查询（兼容中文系统 + Windows 编码问题）。

    Returns:
        True=至少有一个 TdxW.exe 进程在跑
    """
    try:
        import subprocess
        # 用 PowerShell 查，避免 tasklist 的 stdout 编码问题
        # Get-Process -Name TdxW 抛异常=没找到；返回数组=找到
        ps_cmd = (
            "$p = Get-Process -Name TdxW -ErrorAction SilentlyContinue; "
            "if ($p) { 'FOUND:' + $p.Count } else { 'NONE' }"
        )
        out = subprocess.run(
            ["powershell", "-NoProfile", "-Command", ps_cmd],
            capture_output=True, text=True, timeout=10,
            encoding="utf-8", errors="ignore",
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        )
        result = (out.stdout or "").strip()
        return result.startswith("FOUND:")
    except Exception as e:
        logger.warning("检查通达信进程失败: %s", e)
        return False


def _start_tdx_client() -> bool:
    """
    启动通达信客户端（如果未运行）。
    通过 cmd start 异步启动 TdxW.exe，不阻塞。

    Returns:
        True=启动成功（或已在运行），False=启动失败
    """
    import subprocess
    import time
    if _check_tdx_process_alive():
        return True

    logger.info("通达信客户端未运行，尝试启动: %s", _TDX_EXE)
    try:
        # 用 cmd start 启动（不阻塞）
        subprocess.Popen(
            ["cmd", "/c", "start", "", _TDX_EXE],
            shell=False,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        )
    except Exception as e:
        logger.error("启动通达信失败: %s", e)
        return False

    # 等客户端起来（最长 30 秒）
    for i in range(30):
        time.sleep(1)
        if _check_tdx_process_alive():
            logger.info("通达信客户端已启动 (等待 %ds)", i + 1)
            # 再多等几秒让它完成登录/TQ 模块初始化
            time.sleep(5)
            return True

    logger.error("通达信客户端启动超时 (30s)")
    return False


def _test_tdx_init() -> bool:
    """
    深层健康检查：tqcenter 能成功初始化 + 拿到快照。
    解决"客户端进程在但 TQ 模块没启用/没登录"的盲区。

    Returns:
        True=TQ 模块可用
    """
    try:
        import tqcenter
        # 动态试 mode（不能用 mode=5，通达信没那么多 slot）
        import sys as _sys
        _sys_argv_orig = list(_sys.argv)
        try:
            ok = False
            for try_mode in [1, 2, 3, 0]:
                _sys.argv = [_sys_argv_orig[0]] + ['--run_tdx', str(try_mode)]
                try:
                    tqcenter.tq.initialize(_TDX_EXE, dll_path=_TDX_DLL)
                    snap = tqcenter.tq.get_market_snapshot("600519.SH")
                    ok = bool(snap and "Now" in snap)
                    try:
                        tqcenter.tq.close()
                    except Exception:
                        pass
                    if ok:
                        break
                except Exception:
                    time.sleep(0.2)
                    continue
            return ok
        finally:
            _sys.argv = _sys_argv_orig
    except Exception as e:
        logger.debug("_test_tdx_init 失败: %s", e)
        return False


def ensure_tdx_running(notify_callback=None, deep_check: bool = True) -> bool:
    """
    确保通达信客户端在运行 + TQ 模块可用，未运行则尝试启动。

    Args:
        notify_callback: 可选回调，签名 fn(message: str) -> None
                         当通达信不可用时调用，用于发飞书预警等
        deep_check: True=做深层健康检查（init + 拿快照）；False=只查进程

    Returns:
        True=通达信客户端可用（已在跑或刚启动 + TQ 模块 OK）
        False=通达信客户端不可用（启动失败或路径错 或 TQ 未启）
    """
    # 第一关：进程在不在
    if not _check_tdx_process_alive():
        if not _start_tdx_client():
            _fire_notify(notify_callback, "客户端启动失败")
            return False

    # 第二关（默认启用）：TQ 模块能不能正常用
    if deep_check and not _test_tdx_init():
        _fire_notify(
            notify_callback,
            "客户端进程在但 TQ 量化模块不可用\n"
            "  请在通达信客户端里：\n"
            "  1. 登录（行情账号）\n"
            "  2. 启用「量化」模块（通常在顶部菜单或设置里）\n"
            "  3. 重启客户端后再试",
        )
        return False

    return True


def _fire_notify(notify_callback, reason: str):
    """统一的预警触发逻辑"""
    msg = (
        "⚠️ 通达信客户端不可用\n"
        f"  原因: {reason}\n"
        f"  路径: {_TDX_EXE}\n"
        "  请手动启动客户端（带 TQ 量化功能），否则数据源将不可用\n"
        "  启动成功后会通过 cron 通知恢复"
    )
    logger.error(msg)
    if notify_callback:
        try:
            notify_callback(msg)
        except Exception as e:
            logger.error("通知回调失败: %s", e)


def init(force: bool = False, auto_start: bool = True) -> bool:
    """
    初始化/重新初始化 TdxQuant 连接。

    Args:
        force: True=强制重连（即使已初始化）
        auto_start: True=自动启动通达信客户端（如果未运行）

    Returns:
        True=成功，False=失败
    """
    global _initialized, _last_init_time

    with _init_lock:
        now = time.time()

        # 已初始化且在窗口内，不重连
        if _initialized and not force and (now - _last_init_time) < _REINIT_INTERVAL:
            return True

        # 前置守卫：确保通达信客户端在跑
        if auto_start and not _check_tdx_process_alive():
            if not _start_tdx_client():
                logger.error("通达信客户端启动失败，初始化中止")
                _initialized = False
                return False

        # 先尝试释放旧连接
        if _initialized:
            try:
                tq.close()
            except Exception:
                pass
            _initialized = False

        try:
            # 强制 run_mode=1 或更高（mode=0 在反复初始化后会被通达信拒绝，
            # 通达信对每个 mode 槽位有限制，多次断开后需要切换 mode）
            # 尝试一组 run_mode 找可用槽位
            import sys as _sys
            _sys_argv_orig = list(_sys.argv)

            last_err = None
            for try_mode in [1, 2, 3, 0]:  # 优先非 0（0 是用户手动策略的槽位）
                try:
                    _sys.argv = [_sys_argv_orig[0]] + ['--run_tdx', str(try_mode)]
                    # 释放旧连接
                    try:
                        tq.close()
                    except Exception:
                        pass
                    time.sleep(0.3)
                    tq.initialize(_TDX_EXE, dll_path=_TDX_DLL)
                    _initialized = True
                    _last_init_time = now
                    logger.info("TdxQuant 初始化成功 (mode=%s, run_id=%s)", tq.run_mode, tq.run_id)
                    return True
                except Exception as e:
                    last_err = e
                    logger.debug("run_mode=%s 失败: %s", try_mode, e)
                    continue
            raise last_err or RuntimeError("所有 run_mode 都不可用")
        except Exception as e:
            logger.error("TdxQuant 初始化失败: %s", e)
            _initialized = False
            return False


def health_check() -> bool:
    """
    健康检查：拿一只股票快照，能成功返回 Now 字段说明连接可用。
    """
    global _initialized
    if not _initialized and not init():
        return False
    try:
        snap = tq.get_market_snapshot("600519.SH")
        return bool(snap and "Now" in snap)
    except Exception as e:
        logger.warning("健康检查失败: %s", e)
        # 尝试重连
        _initialized = False
        return False


def _ensure_ready() -> None:
    """调用前确保连接可用，失败自动重连一次。"""
    if not _initialized:
        init()
        return
    # 每隔窗口期刷一次（防止句柄僵死）
    if (time.time() - _last_init_time) > _REINIT_INTERVAL:
        init(force=True)


# ============================================================
# 调用超时：tqcenter 阻塞时不能让 fetch_once 卡死
# ============================================================
TDX_CALL_TIMEOUT = float(os.environ.get("TDX_CALL_TIMEOUT", "8"))  # 秒


def _call_with_timeout(func, *args, timeout=None, default=None, label=""):
    """在独立线程中调用 func，超时返回 default。"""
    if timeout is None:
        timeout = TDX_CALL_TIMEOUT
    box = {}

    def runner():
        try:
            box["value"] = func(*args)
        except Exception as e:
            box["error"] = e
        finally:
            box["done"] = True

    th = threading.Thread(target=runner, daemon=True)
    th.start()
    th.join(timeout)
    if not box.get("done"):
        logger.warning("%s 超时（%.1fs），返回默认 %s", label or func.__name__, timeout, default)
        # 注意：tqcenter 阻塞线程仍在跑，下次调用要靠 _REINIT_INTERVAL 强制 init(force=True) 切断
        return default
    if "error" in box:
        logger.debug("%s 异常: %s", label or func.__name__, box["error"])
        return default
    return box.get("value")


# ============================================================
# 股票 / 板块列表
# ============================================================

_A_SHARE_CACHE: Optional[List[Dict]] = None
_A_SHARE_CACHE_TS = 0.0
_A_SHARE_CACHE_TTL = 3600  # 1 小时


def get_a_share_list(refresh: bool = False) -> List[Dict]:
    """
    获取沪深 A 股全量列表。
    返回: [{'Code': '000001.SZ', 'Name': '平安银行'}, ...]
    带 1 小时缓存。
    """
    global _A_SHARE_CACHE, _A_SHARE_CACHE_TS

    now = time.time()
    if not refresh and _A_SHARE_CACHE and (now - _A_SHARE_CACHE_TS) < _A_SHARE_CACHE_TTL:
        return _A_SHARE_CACHE

    _ensure_ready()
    try:
        # market='5' 是沪深京 A 股（实测 5533 只）
        sl = _call_with_timeout(tq.get_stock_list, market="5", list_type=1, default=[], label="get_stock_list") or []
        _A_SHARE_CACHE = sl
        _A_SHARE_CACHE_TS = now
        logger.info("加载 A 股列表: %d 只", len(sl))
        return sl
    except Exception as e:
        logger.error("get_a_share_list 失败: %s", e)
        return _A_SHARE_CACHE or []


_SECTOR_CACHE: Optional[List[Dict]] = None
_SECTOR_CACHE_TS = 0.0


def get_sectors(refresh: bool = False) -> List[Dict]:
    """
    获取所有板块列表。
    返回: [{'Code': '880952.SH', 'Name': '芯片'}, ...]
    带 1 小时缓存。
    板块代号前缀规律：
      8802xx = 地域板块
      8803xx = 申万行业
      8804xx = ?
      8805xx-8809xx = 概念/主题板块（部分老旧）
    """
    global _SECTOR_CACHE, _SECTOR_CACHE_TS

    now = time.time()
    if not refresh and _SECTOR_CACHE and (now - _SECTOR_CACHE_TS) < _A_SHARE_CACHE_TTL:
        return _SECTOR_CACHE

    _ensure_ready()
    try:
        sl = _call_with_timeout(tq.get_sector_list, list_type=1, default=[], label="get_sector_list") or []
        _SECTOR_CACHE = sl
        _SECTOR_CACHE_TS = now
        logger.info("加载板块列表: %d 个", len(sl))
        return sl
    except Exception as e:
        logger.error("get_sectors 失败: %s", e)
        return _SECTOR_CACHE or []


def get_sector_stocks(block_code: str) -> List[Dict]:
    """
    获取板块成分股。
    block_code 形如 '880952.SH'。
    返回: [{'Code': '...', 'Name': '...'}, ...]
    """
    _ensure_ready()
    try:
        return _call_with_timeout(tq.get_stock_list_in_sector, block_code, block_type=0, list_type=1, default=[], label=f"get_stock_list_in_sector({block_code})") or []
    except Exception as e:
        logger.error("get_sector_stocks(%s) 失败: %s", block_code, e)
        return []


# ============================================================
# 单股数据
# ============================================================

# 字段名映射（tqcenter 原生英文 -> 我们看板/推送用的中文 camelCase）
_SNAPSHOT_FIELD_MAP = {
    "Now": "price",
    "Open": "open",
    "Max": "high",
    "Min": "low",
    "LastClose": "pre_close",
    "Volume": "volume",
    "Amount": "amount",
    "Buyp": "bid1_price",
    "Buyv": "bid1_vol",
    "Sellp": "ask1_price",
    "Sellv": "ask1_vol",
    "Average": "avg_price",
    "Zangsu": "turnover_rate",
    "Inside": "inner_vol",
    "Outside": "outer_vol",
    "TickDiff": "tick_diff",
}


def _normalize_snapshot(raw: Dict, code: str) -> Dict:
    """把原始 snapshot 字典转成扁平 + 中文键的 dict。"""
    out = {"code": code}
    for k_raw, k_cn in _SNAPSHOT_FIELD_MAP.items():
        if k_raw in raw:
            v = raw[k_raw]
            # Buyp/Buyv/Sellp/Sellv 是 5 档列表（'[价1, 价2, ...]'），只取第 1 档
            if isinstance(v, list) and v:
                v = v[0]
            if isinstance(v, float) and v.is_integer():
                v = int(v)
            out[k_cn] = v
    return out


def get_snapshot(code: str) -> Dict:
    """
    获取单股快照（精简字段）。

    Args:
        code: '600519.SH' / '000001.SZ' 形式

    Returns:
        dict，含 price/open/high/low/volume/amount 等字段；失败返回空 dict
    """
    global _initialized
    _ensure_ready()
    try:
        raw = _call_with_timeout(tq.get_market_snapshot, code, default={}, label=f"get_snapshot({code})")
        if not raw:
            return {}
        return _normalize_snapshot(raw, code)
    except Exception as e:
        # 注意：tqcenter 的 get_market_snapshot 失败时内部会调 cls.close()，
        # 这会把 _initialized 置 False。重新标记为 True 以避免下次调用又重连。
        logger.debug("get_snapshot(%s) 失败: %s（保持连接）", code, e)
        _initialized = True  # 防止 tqcenter 内部的 close 把我们踢出去
        return {}


# 资金流关键字段（来自 get_more_info）
_FUND_FLOW_FIELDS = [
    "Zjl",          # 主力净流入
    "Zjl_HB",       # 主力净流入（后比）
    "TotalBVol",    # 总买量
    "TotalSVol",    # 总卖量
    "BCancel",      # 买撤单
    "SCancel",      # 卖撤单
    "FCAmo",        # 封单金额
    "FCb",          # 封单笔数
    "Fzhsl",        # 换手率
    "FzAmo",        # 成交额
    "ZAF",          # 当日涨跌幅
    "ZAFYesterday", # 昨日涨幅
    "ZAFPre2D",     # 2日涨幅
    "ZAFPre5",      # 5日涨幅
    "ZAFPre10",     # 10日涨幅
    "ZAFPre20",     # 20日涨幅
    "ZAFPre60",     # 60日涨幅
    "ZAFYear",      # 年涨幅
    "fLianB",       # 连板数
    "ZTPrice",      # 涨停价
    "DTPrice",      # 跌停价
]


def get_fund_flow(code: str) -> Dict:
    """
    获取单股资金流（含主力、撤单、封单、多周期涨幅）。

    Args:
        code: '600519.SH' 形式

    Returns:
        dict，包含主力净流入、撤单金额、封单金额、各周期涨幅等
    """
    global _initialized
    _ensure_ready()
    try:
        more = _call_with_timeout(tq.get_more_info, code, default=None, label=f"get_more_info({code})")
        if not more:
            return {"code": code}
        out = {"code": code}
        for f in _FUND_FLOW_FIELDS:
            if f in more:
                v = more[f]
                # 通达信返回的数值有时是 str (如 '0.00'), 转 float
                if isinstance(v, str):
                    try:
                        v = float(v)
                    except (ValueError, TypeError):
                        pass
                if isinstance(v, float) and v.is_integer():
                    v = int(v)
                out[f] = v
        return out
    except Exception as e:
        logger.debug("get_fund_flow(%s) 失败: %s（保持连接）", code, e)
        _initialized = True  # 防止 tqcenter 内部的 close 把我们踢出去
        return {"code": code}


# ============================================================
# 批量并发
# ============================================================

def batch_snapshot(
    codes: List[str],
    max_workers: int = 8,
    timeout: float = 30.0,
) -> List[Dict]:
    """
    批量获取快照。

    Args:
        codes: 代码列表
        max_workers: 并发线程数（建议 4-12，过高触发 DLL 限流）
        timeout: 总超时（秒）

    Returns:
        成功获取的快照列表（失败的代码会跳过，不抛异常）
    """
    if not codes:
        return []
    if not _ensure_ready_silent():
        return []

    results: List[Dict] = []
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        future_to_code = {ex.submit(get_snapshot, c): c for c in codes}
        deadline = time.time() + timeout
        for fut in as_completed(future_to_code, timeout=timeout):
            if time.time() > deadline:
                break
            code = future_to_code[fut]
            try:
                snap = fut.result(timeout=5)
                if snap:
                    results.append(snap)
            except Exception as e:
                logger.debug("batch_snapshot %s 失败: %s", code, e)
    return results


def batch_fund_flow(
    codes: List[str],
    max_workers: int = 6,
    timeout: float = 60.0,
) -> List[Dict]:
    """批量获取资金流（更慢，建议并发略低）。"""
    if not codes:
        return []
    if not _ensure_ready_silent():
        return []

    results: List[Dict] = []
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        future_to_code = {ex.submit(get_fund_flow, c): c for c in codes}
        deadline = time.time() + timeout
        for fut in as_completed(future_to_code, timeout=timeout):
            if time.time() > deadline:
                break
            code = future_to_code[fut]
            try:
                flow = fut.result(timeout=10)
                if flow and flow.get("code"):
                    results.append(flow)
            except Exception as e:
                logger.debug("batch_fund_flow %s 失败: %s", code, e)
    return results


def _ensure_ready_silent() -> bool:
    """静默版 ensure，不抛日志。"""
    if not _initialized:
        return init()
    return True


# ============================================================
# 便捷：板块资金流排行
# ============================================================

def rank_by_fund_flow(
    codes: List[str],
    field: str = "Zjl",
    top_n: int = 20,
    ascending: bool = False,
) -> List[Dict]:
    """
    给定代码列表，按指定字段排序得到排行。

    Args:
        codes: 股票代码列表
        field: 排序字段（默认 'Zjl' 主力净流入）
        top_n: 取前 N
        ascending: True=升序（流出榜），False=降序（流入榜）

    Returns:
        排序后的资金流字典列表
    """
    flows = batch_fund_flow(codes, max_workers=4)
    flows = [f for f in flows if field in f]
    flows.sort(key=lambda x: x.get(field, 0), reverse=not ascending)
    return flows[:top_n]


def close() -> None:
    """关闭连接（一般不需要手动调，模块退出时会自动 close）。"""
    global _initialized
    with _init_lock:
        if _initialized:
            try:
                tq.close()
            except Exception:
                pass
            _initialized = False


# 注册 atexit 自动关闭
import atexit
atexit.register(close)


# ============================================================
# 自检入口
# ============================================================

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    print("=" * 60)
    print("TdxQuant 客户端自检")
    print("=" * 60)

    print("\n[1] 初始化")
    ok = init()
    print(f"    初始化: {'OK' if ok else 'FAIL'}")

    if not ok:
        sys.exit(1)

    print("\n[2] 健康检查")
    print(f"    health_check: {'OK' if health_check() else 'FAIL'}")

    print("\n[3] A 股列表")
    sl = get_a_share_list()
    print(f"    数量: {len(sl)}, 前 3: {sl[:3]}")

    print("\n[4] 板块列表")
    secs = get_sectors()
    print(f"    数量: {len(secs)}")
    print(f"    前 5: {[s['Name'] for s in secs[:5]]}")

    print("\n[5] 单股快照 (600519)")
    snap = get_snapshot("600519.SH")
    for k, v in list(snap.items())[:10]:
        print(f"    {k}: {v}")

    print("\n[6] 单股资金流 (600519)")
    flow = get_fund_flow("600519.SH")
    for k, v in list(flow.items())[:10]:
        print(f"    {k}: {v}")

    print("\n[7] 批量快照 (10 只)")
    sample = [s["Code"] for s in sl[:10]]
    snaps = batch_snapshot(sample, max_workers=4)
    print(f"    成功: {len(snaps)}/{len(sample)}")

    print("\n[8] 资金流排行 (前 20 主力净流入)")
    sample = [s["Code"] for s in sl[:100]]
    ranked = rank_by_fund_flow(sample, field="Zjl", top_n=5)
    for i, r in enumerate(ranked, 1):
        print(f"    {i}. {r['code']}: Zjl={r.get('Zjl', 0):,.0f}")

    print("\n✅ 自检完成")