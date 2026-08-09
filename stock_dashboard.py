"""
A 股资金流实时看板 - 后端 (通达信 TdxQuant 数据源)
启动: python stock_dashboard.py
访问: http://localhost:8765/

数据源策略:
- 个股资金流 / 撤单 / 封单：通达信 (tdx_client) — 实时、准确、不限流
- 行业资金流聚合：通达信 — 取板块成分股 → 各股 Zjl 求和（替代同花顺聚合接口）
- 概念资金流聚合：通达信 — 同上，8809xx 系列概念板块
- 个股价格/涨幅：通达信 — 实时盘口

优势:
- 不依赖外网（断网仍能跑）
- 不限流、不封 IP
- 数据精度高（撤单/封单是同花顺没有的）
- akshare 偶尔 mini_racer 崩溃 → 完全规避

降级策略:
- 通达信不可用 → 静默使用 akshare (sector_fund_flow_rank)

时区:
- 本机系统时区不是北京时间 (如 UTC-7 Mountain Time)
- 所有时间相关逻辑（in_trading_hours / timestamp / 趋势时间戳）统一用北京时间
- 通过 _BJ_NOW() 获取北京时间
"""
import os
# 默认行为：强制清代理环境变量，避免公司/系统代理干扰 TDX 与 akshare 数据源。
# 若新机器需要保留代理（如企业代理访问 akshare），可在启动前设置：
#   set DASHBOARD_KEEP_PROXY=1
if os.environ.get("DASHBOARD_KEEP_PROXY") != "1":
    for k in ("HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy", "ALL_PROXY", "all_proxy"):
        os.environ.pop(k, None)
    os.environ["NO_PROXY"] = "*"
    os.environ["no_proxy"] = "*"

import json
import time
import threading
import logging
import warnings
from http.server import BaseHTTPRequestHandler, HTTPServer
from socketserver import ThreadingMixIn
from datetime import datetime, timezone, timedelta
from typing import Dict, List

warnings.filterwarnings("ignore")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("dashboard")

# === 北京时间工具 ===
# 本机时区不是 Asia/Shanghai（A 股必须用北京时间判断交易时段 + 打时间戳）
BJ_TZ = timezone(timedelta(hours=8))


def bj_now() -> datetime:
    """获取当前北京时间（aware datetime）"""
    return datetime.now(BJ_TZ)


def _call_with_timeout(func, *args, timeout=None, default=None, label=""):
    """在独立 daemon 线程中调用 func，超时返回 default。

    用途：
    - 阻塞的 TDX 调用 / akshare 同步请求 — 避免 handler 雪崩
    - /api/health 等小接口的内部同步依赖

    返回值：
    - 正常：函数返回值
    - 超时：default
    - 异常：default（同时记录 debug 日志）

    注意：超时线程仍在后台运行；若函数会改全局状态，需在调用方谨慎处理。
    """
    if timeout is None:
        timeout = 8.0
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
        logger.warning("%s 超时（%.1fs）", label or func.__name__, timeout)
        return default
    if "error" in box:
        logger.debug("%s 异常: %s", label or func.__name__, box["error"])
        return default
    return box.get("value")
    return datetime.now(BJ_TZ)


def bj_now_naive() -> datetime:
    """获取当前北京时间（naive datetime，向下兼容旧代码）"""
    return bj_now().replace(tzinfo=None)

# === 通达信客户端 ===
try:
    import tdx_client
    TDX_OK = True
except Exception as e:
    logger.warning("tdx_client 加载失败: %s — 看板将使用 akshare 兜底", e)
    TDX_OK = False

# === 实时异动监测 ===
try:
    import alert_monitor
    ALERT_OK = True
except Exception as e:
    logger.warning("alert_monitor 加载失败: %s", e)
    ALERT_OK = False


def _notify_tdx_down(message: str):
    """通达信客户端不可用时的通知回调 — 写日志 + 飞书消息

    当前阶段：只写日志（看板进程没接飞书 webhook）
    推送脚本会接管真正的飞书通知
    """
    logger.error("[CRITICAL] %s", message)


# 看板启动时做一次通达信守护（如果客户端未运行则尝试启动）
if TDX_OK:
    logger.info("看板启动前置检查：通达信客户端进程")
    if not tdx_client.ensure_tdx_running(notify_callback=_notify_tdx_down):
        logger.error("[WARN] 通达信客户端启动失败，看板将持续报错直到客户端可用")
    else:
        logger.info("[OK] 通达信客户端就绪")

# akshare 仅作为兜底
try:
    import akshare as ak
    AK_OK = True
except Exception:
    AK_OK = False


# 演示数据：6 个交易日的 TOP10 流入/流出快照（真实股票 + 合理金额）。
# 真实运行后会被 daily_snapshot_loop 的每日保存覆盖；这里仅用于让前端一启动就能看到效果。

# 当日（8 月 8 日）个股 TOP10 流入/流出
DATA_LOCK = threading.Lock()
FETCH_LOCK = threading.Lock()
LATEST = {
    "timestamp": None,
    "stock_top": [],
    "stock_bottom": [],
    "industry_top": [],
    "industry_bottom": [],
    "concept_top": [],
    "concept_bottom": [],
    # 近 N 日累计资金流（akshare 3日/5日/10日排行），前端单独拉
    "period": {
        "3d": {"stock_top": [], "stock_bottom": [], "error": None, "updated": None},
        "5d": {"stock_top": [], "stock_bottom": [], "error": None, "updated": None},
        "10d": {"stock_top": [], "stock_bottom": [], "error": None, "updated": None},
    },
    # 每日收盘快照（按日期 key，环形缓冲近 10 天）。启动后约 6 个交易日才能填满 T-5..T 序列
    "daily_summaries": {},
    "market_net": 0.0,
    "history": {
        "timestamps": [],
        "industry": {},
        "stock": {},
        "concept": {},
        "market": [],
    },
    "is_trading": False,
    "error": None,
    "source": "tdx",  # "tdx" / "akshare" / "mixed"
}
_alert_monitor_inst = None  # 实时异动监测器实例（main 里赋值，_serve_alerts 用）
HISTORY_MAX = 240
IND_TOP_N = 10
STOCK_TOP_N = 10
CONCEPT_TOP_N = 10

# fetch_once 各数据源的采集 TOP N（与展示 N 区别：多拉一些给 fallback 用）
DEFAULT_FETCH_STOCK_TOP_N = 20
DEFAULT_FETCH_INDUSTRY_TOP_N = 20
DEFAULT_FETCH_CONCEPT_TOP_N = 15
DEFAULT_FETCH_INDUSTRY_BOTTOM_N = 10
DEFAULT_FETCH_CONCEPT_BOTTOM_N = 8

# 概念板块代码前缀（通达信 8809xx 系列）
CONCEPT_PREFIXES = ("8809",)

# 申万行业代码前缀（通达信 881xxx 系列，6 位数字）
INDUSTRY_PREFIXES = ("881",)


def in_trading_hours(now=None):
    now = now or bj_now_naive()
    if now.weekday() > 4:
        return False
    t = now.time()
    from datetime import time as dtime
    return (dtime(9, 30) <= t <= dtime(11, 30)) or (dtime(13, 0) <= t <= dtime(15, 0))


# ============================================================
# 通达信数据源
# ============================================================

def _format_stock_row(code, name, fund_flow_dict, snapshot_dict=None):
    """格式化成前端需要的 dict: {代码, 名称, 涨跌幅, 净额}"""
    zaf = fund_flow_dict.get("ZAF") or 0
    # 净额：主力净流入，单位元 → 转亿
    zjl = fund_flow_dict.get("Zjl") or 0
    zjl_yi = zjl / 1e8  # 元 → 亿
    return {
        "股票代码": code,
        "股票简称": name or "",
        "涨跌幅": round(zaf, 2),
        "净额": f"{zjl_yi:.2f}亿",  # 前端 fmt 函数能识别 "亿"
        # 扩展字段（前端暂时不显示，但 API 返回供将来用）
        "_主力净流入": zjl,
        "_主力净流入_亿": round(zjl_yi, 4),
        "_换手率": fund_flow_dict.get("Fzhsl"),
        "_买撤单": fund_flow_dict.get("BCancel"),
        "_卖撤单": fund_flow_dict.get("SCancel"),
        "_封单金额": fund_flow_dict.get("FCAmo"),
        "_封单笔数": fund_flow_dict.get("FCb"),
    }


# 行业 / 概念 / 个股行情共享的缓存（每个 fetch_once 周期内只算一次全 A 股）
_PERIOD_CACHE = {
    "ts": 0.0,
    "stock_flows": [],   # 全 A 股 fund_flow list
    "stock_snaps": [],   # 全 A 股 snapshot list
    "stock_names": {},   # code -> name
    "all_codes": [],     # 全 A 股 codes
}
_SECTOR_STOCKS_CACHE = {
    "ts": 0.0,
    "data": {},          # block_code -> [codes]
}
_CACHE_TTL = 240       # 行情缓存 4 分钟（通达信全 A 股拉一次 ~80s）


def _get_full_market_data():
    """
    一次 fetch_once 周期内共享的全 A 股数据。
    TTL: 4 分钟（与 fetch_interval 一致 + 容错）。
    """
    global _PERIOD_CACHE
    now = time.time()
    if _PERIOD_CACHE["ts"] and (now - _PERIOD_CACHE["ts"]) < _CACHE_TTL:
        return _PERIOD_CACHE

    if not TDX_OK:
        return _PERIOD_CACHE

    try:
        if not tdx_client.health_check():
            tdx_client.init(force=True)
        sl = tdx_client.get_a_share_list()
        codes = [s["Code"] for s in sl if s["Code"].endswith((".SH", ".SZ"))]
        names = {s["Code"]: s["Name"] for s in sl}

        # 一次拉所有 fund_flow（并发调低+串行重试避免触发通达信连接数限制）
        flows = tdx_client.batch_fund_flow(codes, max_workers=4, timeout=240)
        # snapshot 是次要数据，fund_flow 够用就行，失败不阻塞
        snaps = []
        try:
            snaps = tdx_client.batch_snapshot(codes, max_workers=4, timeout=180)
        except Exception:
            pass

        _PERIOD_CACHE.update({
            "ts": now,
            "stock_flows": flows,
            "stock_snaps": snaps,
            "stock_names": names,
            "all_codes": codes,
        })
        logger.info("全 A 股数据缓存刷新: flows=%d snaps=%d", len(flows), len(snaps))
    except Exception as e:
        logger.exception("_get_full_market_data 失败")
    return _PERIOD_CACHE


def fetch_individual_flows_tdx(top_n=30):
    """
    从通达信拿全 A 股资金流，按 Zjl 排序，取 TOP。
    使用共享缓存 _get_full_market_data。
    """
    if not TDX_OK:
        return [], [], "tdx_client 不可用"
    try:
        cache = _get_full_market_data()
        flows = cache["stock_flows"]
        names = cache["stock_names"]
        valid = [f for f in flows if f.get("Zjl") is not None]
        valid.sort(key=lambda x: x.get("Zjl", 0), reverse=True)
        top = [_format_stock_row(f["code"], names.get(f["code"], ""), f)
               for f in valid[:top_n]]
        bottom = [_format_stock_row(f["code"], names.get(f["code"], ""), f)
                  for f in valid[-top_n:][::-1]]
        return top, bottom, None
    except Exception as e:
        logger.exception("fetch_individual_flows_tdx 失败")
        return [], [], str(e)


def _get_sector_stocks_cached(block_code):
    """获取板块成分股，带 4 分钟缓存"""
    global _SECTOR_STOCKS_CACHE
    now = time.time()
    if _SECTOR_STOCKS_CACHE["ts"] and (now - _SECTOR_STOCKS_CACHE["ts"]) < _CACHE_TTL:
        if block_code in _SECTOR_STOCKS_CACHE["data"]:
            return _SECTOR_STOCKS_CACHE["data"][block_code]
    stocks = tdx_client.get_sector_stocks(block_code)
    codes = [s["Code"] for s in stocks if isinstance(s, dict)
             and s.get("Code", "").endswith((".SH", ".SZ"))]
    _SECTOR_STOCKS_CACHE["data"][block_code] = codes
    _SECTOR_STOCKS_CACHE["ts"] = now
    time.sleep(0.02)
    return codes


def fetch_sector_aggregate_tdx(prefix_filter=None, top_n=15, max_sectors=80):
    """
    对板块做资金流聚合：取板块 → 成分股 → 各股 Zjl 求和 → 排序。
    使用共享缓存避免重复拉全 A 股行情。
    """
    if not TDX_OK:
        return [], "tdx_client 不可用"
    try:
        sectors = tdx_client.get_sectors()

        if prefix_filter:
            sectors = [s for s in sectors if s["Code"].split(".")[0].startswith(prefix_filter)]

        sectors = sectors[:max_sectors]
        logger.info("聚合板块数: %d (前缀=%s)", len(sectors), prefix_filter)

        # 1) 拿所有板块的成分股（带缓存）
        all_codes = set()
        sec_to_codes = {}
        for sec in sectors:
            try:
                codes = _get_sector_stocks_cached(sec["Code"])
                sec_to_codes[sec["Code"]] = codes
                all_codes.update(codes)
            except Exception as e:
                logger.debug("板块 %s 成分股失败: %s", sec["Code"], e)
                sec_to_codes[sec["Code"]] = []

        logger.info("涉及唯一股票数: %d", len(all_codes))

        # 2) 共享缓存里取
        cache = _get_full_market_data()
        flow_by_code = {f["code"]: f for f in cache["stock_flows"]}
        cache_codes = set(flow_by_code.keys())
        missing = all_codes - cache_codes
        if missing:
            logger.info("板块 %s 缺 %d 只, 单独补拉", prefix_filter, len(missing))
            extra = tdx_client.batch_fund_flow(list(missing), max_workers=4, timeout=90)
            for f in extra:
                flow_by_code[f["code"]] = f

        # 3) 聚合
        rows = []
        for sec in sectors:
            codes = sec_to_codes.get(sec["Code"], [])
            if not codes:
                continue
            flows = [flow_by_code[c] for c in codes if c in flow_by_code]
            if not flows:
                continue
            total_zjl = sum(f.get("Zjl", 0) or 0 for f in flows)
            zafs = [f.get("ZAF") for f in flows if f.get("ZAF") is not None]
            avg_zaf = sum(zafs) / len(zafs) if zafs else 0
            rows.append({
                "行业": sec["Name"],
                "行业-涨跌幅": round(avg_zaf, 2),
                "净额": f"{total_zjl / 1e8:.2f}亿",
                "_主力净流入_亿": round(total_zjl / 1e8, 4),
                "_成分股数": len(codes),
            })

        rows.sort(key=lambda x: x["_主力净流入_亿"], reverse=True)
        return rows, None
    except Exception as e:
        logger.exception("fetch_sector_aggregate_tdx 失败")
        return [], str(e)


def fetch_industry_top_tdx(top_n=15):
    """行业资金流排行"""
    rows, err = fetch_sector_aggregate_tdx(prefix_filter=INDUSTRY_PREFIXES, top_n=top_n)
    return rows[:top_n], rows[-8:][::-1] if len(rows) >= 8 else [], err


def fetch_concept_top_tdx(top_n=15):
    """概念资金流排行"""
    rows, err = fetch_sector_aggregate_tdx(prefix_filter=CONCEPT_PREFIXES, top_n=top_n)
    return rows[:top_n], rows[-5:][::-1] if len(rows) >= 5 else [], err


# ============================================================
# akshare 兜底（保留原有逻辑）
# ============================================================

def parse_money_to_yi(v):
    if v is None or v == "":
        return 0.0
    if isinstance(v, (int, float)):
        return float(v)
    s = str(v).strip().replace(",", "")
    try:
        if "亿" in s:
            return float(s.replace("亿", ""))
        if "万" in s:
            return float(s.replace("万", "")) / 1e4
        return float(s)
    except (ValueError, TypeError):
        return 0.0


def sort_by_money(df, col, ascending=False):
    if df is None or df.empty or col not in df.columns:
        return df
    return df.assign(_sort=df[col].apply(parse_money_to_yi)).sort_values(
        "_sort", ascending=ascending
    ).drop(columns=["_sort"]).reset_index(drop=True)


def fetch_akshare_fallback():
    """通达信失败时用 akshare 兜底"""
    if not AK_OK:
        return None
    snap = {
        "stock_top": [], "stock_bottom": [],
        "industry_top": [], "industry_bottom": [],
        "concept_top": [], "concept_bottom": [],
        "source": "akshare",
    }
    try:
        df = ak.stock_fund_flow_individual(symbol="即时")
        if df is not None and not df.empty:
            df = df.drop_duplicates(subset=["股票代码"], keep="first")
            top = sort_by_money(df, "净额", ascending=False).head(20)
            bot = sort_by_money(df, "净额", ascending=True).head(10)
            snap["stock_top"] = top.fillna("").to_dict(orient="records")
            snap["stock_bottom"] = bot.fillna("").to_dict(orient="records")
    except Exception as e:
        snap["stock_error"] = str(e)

    try:
        df = ak.stock_fund_flow_industry(symbol="即时")
        if df is not None and not df.empty:
            df = df.drop_duplicates(subset=["行业"], keep="first")
            top = sort_by_money(df, "净额", ascending=False).head(20)
            bot = sort_by_money(df, "净额", ascending=True).head(8)
            snap["industry_top"] = top.fillna("").to_dict(orient="records")
            snap["industry_bottom"] = bot.fillna("").to_dict(orient="records")
    except Exception as e:
        snap["industry_error"] = str(e)


# 近 N 日累计资金流排行（akshare 同花顺源）
_AKSHARE_PERIOD_MAP = {
    "3d": "3日排行",
    "5d": "5日排行",
    "10d": "10日排行",
}


def fetch_akshare_period(period: str):
    """拉取"近 N 日排行"快照（period ∈ {3d,5d,10d}）.

    返回 {stock_top, stock_bottom, source, period, error}
    """
    if not AK_OK:
        return None
    sym = _AKSHARE_PERIOD_MAP.get(period)
    if not sym:
        return None
    out = {"period": period, "source": "akshare", "stock_top": [], "stock_bottom": [], "error": None}
    try:
        df = ak.stock_fund_flow_individual(symbol=sym)
        if df is not None and not df.empty:
            df = df.drop_duplicates(subset=["股票代码"], keep="first")
            top = sort_by_money(df, "净额", ascending=False).head(20)
            bot = sort_by_money(df, "净额", ascending=True).head(10)
            out["stock_top"] = top.fillna("").to_dict(orient="records")
            out["stock_bottom"] = bot.fillna("").to_dict(orient="records")
    except Exception as e:
        out["error"] = f"{period} stock: {e}"
        logger.warning("fetch_akshare_period(%s) 失败: %s", period, e)
    return out


def sum_period_amount(rows, key=None):
    """对快照中每行的净额列求和（亿元）。自动识别"净额"与"资金流入净额"两种字段名。"""
    total = 0.0
    for r in rows or []:
        # 优先 "资金流入净额"（3/5/10日排行），fallback "净额"（即时）
        v = r.get("资金流入净额") if key is None else r.get(key, 0)
        if v in (None, "", 0):
            v = r.get("净额", 0)
        try:
            s = str(v).strip().replace(",", "")
            if "亿" in s:
                total += float(s.replace("亿", ""))
            elif "万" in s:
                total += float(s.replace("万", "")) / 1e4
            else:
                total += float(s) if s else 0.0
        except (ValueError, TypeError):
            pass
    return round(total, 2)


def build_period_compare():
    """合并 1d/3d/5d/10d 四个时间分段的 TOP 流入/流出累计总额 + 近5日明细。"""
    # 1d = 即时（即"当日"）
    periods = ["1d", "3d", "5d", "10d"]
    x_labels = ["当日", "近3日", "近5日", "近10日"]
    inflow_totals = []
    outflow_totals = []
    errors = {}

    # 1d 来自 LATEST["stock_top/bottom"]（即 fetch_once 填的当日）
    with DATA_LOCK:
        top_1d = list(LATEST.get("stock_top", []))
        bot_1d = list(LATEST.get("stock_bottom", []))
        period_cache = {k: dict(v) for k, v in LATEST.get("period", {}).items()}

    inflow_totals.append(sum_period_amount(top_1d))
    outflow_totals.append(abs(sum_period_amount(bot_1d)))

    for p in ("3d", "5d", "10d"):
        snap = period_cache.get(p, {})
        inflow_totals.append(sum_period_amount(snap.get("stock_top", [])))
        outflow_totals.append(abs(sum_period_amount(snap.get("stock_bottom", []))))
        if snap.get("error"):
            errors[p] = snap["error"]

    return {
        "x_labels": x_labels,
        "inflow_totals": inflow_totals,  # 亿元
        "outflow_totals": outflow_totals,  # 亿元（取绝对值）
        "periods": periods,
        "errors": errors,
        "top_inflow_5d": period_cache.get("5d", {}).get("stock_top", [])[:10],
        "top_outflow_5d": period_cache.get("5d", {}).get("stock_bottom", [])[:10],
    }

    try:
        df = ak.stock_fund_flow_concept(symbol="即时")
        if df is not None and not df.empty:
            df = df.drop_duplicates(subset=["行业"], keep="first")
            top = sort_by_money(df, "净额", ascending=False).head(15)
            bot = sort_by_money(df, "净额", ascending=True).head(5)
            snap["concept_top"] = top.fillna("").to_dict(orient="records")
            snap["concept_bottom"] = bot.fillna("").to_dict(orient="records")
    except Exception as e:
        snap["concept_error"] = str(e)

    return snap


# ============================================================
# 主拉数
# ============================================================

def _accumulate_error(snapshot, prefix, err):
    """统一错误字段拼接：'stock: ... | industry: ...' 风格。

    - err 为 None/空：不拼接
    - snapshot['error'] 已存在：在尾部追加 ' | prefix: err'
    - 不存在：以 'prefix: err' 开头

    异常会被 _classify_error 贴上简短标签（NETWORK/TIMEOUT/SCHEMA/UNKNOWN），
    便于 dashboard 顶部 errEl 显示具体类别。
    """
    if err is None or err == "":
        return
    s = str(err)
    if snapshot.get("error"):
        snapshot["error"] = f"{snapshot['error']} | {prefix}: {s}"
    else:
        snapshot["error"] = f"{prefix}: {s}"


# 异常分类 → 简短 code，方便日志与 dashboard 区分问题
_ERROR_CODE_MAP = [
    (ConnectionError, "NETWORK"),
    (TimeoutError, "TIMEOUT"),
    (PermissionError, "AUTH"),
    (KeyError, "SCHEMA"),
    (ValueError, "SCHEMA"),
    (TypeError, "SCHEMA"),
    (OSError, "NETWORK"),
]


def _classify_error(err) -> str:
    """把异常归到 NETWORK/TIMEOUT/AUTH/SCHEMA/UNKNOWN 之一。

    用于在快照 error 字段前加 [CODE] 前缀，便于 dashboard 与日志快速分类。
    """
    if err is None:
        return "UNKNOWN"
    for exc_type, code in _ERROR_CODE_MAP:
        if isinstance(err, exc_type):
            return code
    return "UNKNOWN"


def _has_real_money(rows):
    """检查 rows 中是否有任何一行的净额不是 0/None（用于探测 tdx 资金流全 0 的 fallback 触发）。"""
    if not rows:
        return False
    for r in rows:
        m = r.get("净额")
        if m is None:
            continue
        if isinstance(m, str):
            if "亿" in m:
                try:
                    v = float(m.replace("亿", "").strip())
                    if abs(v) > 0.001:
                        return True
                except ValueError:
                    pass
        elif isinstance(m, (int, float)) and abs(m) > 0.001:
            return True
    return False


# history series 上限：避免长期跑 series key 单调增长
MAX_SERIES_KEYS = 500
SERIES_ZERO_STREAK_THRESHOLD = 30  # 连续 N 个 None 视为可淘汰


def _push_series(target_dict, name, value, zero_streak_map):
    """单 series 追加 + 长度裁剪 + key 上限保护。

    闭包外的 zero_streak_map 用于跟踪每个 series 连续 None 的次数。
    """
    if not isinstance(name, str) or not name:
        return
    if name not in target_dict:
        if len(target_dict) >= MAX_SERIES_KEYS:
            # 优先淘汰：连续 None 数 >= 阈值的 series
            drop_key = next((k for k, n in zero_streak_map.items()
                             if n >= SERIES_ZERO_STREAK_THRESHOLD), None)
            if not drop_key:
                # 否则踢最早的 key（dict 插入有序）
                drop_key = next(iter(target_dict), None)
            if drop_key:
                target_dict.pop(drop_key, None)
                zero_streak_map.pop(drop_key, None)
        # 新 series 用 None 填充历史空位
        target_dict[name] = [None] * (len(target_dict.get("__ts__", [])))
    target_dict[name].append(value)
    if value is None or value == 0:
        zero_streak_map[name] = zero_streak_map.get(name, 0) + 1
    else:
        zero_streak_map[name] = 0
    # 用 del 实现原地裁剪，避免 O(N) 拷贝
    if len(target_dict[name]) > HISTORY_MAX:
        del target_dict[name][:len(target_dict[name]) - HISTORY_MAX]


def _fetch_via_tdx(now):
    """阶段 1：通达信主路径拉数。

    返回 (snapshot_partial, tdx_used, tdx_no_fund_flow)：
    - snapshot_partial 含 stock_top/bottom, industry_top/bottom, concept_top/bottom
    - tdx_used=False 表示整个 tdx 失败（调用方应走 akshare fallback）
    - tdx_no_fund_flow=True 表示 tdx 跑通但资金流全 0（仍调用 akshare 但 source="mixed"）
    """
    snapshot = {}
    if not TDX_OK:
        return snapshot, False, False
    tdx_no_fund_flow = False
    try:
        stock_top, stock_bot, err = fetch_individual_flows_tdx(top_n=DEFAULT_FETCH_STOCK_TOP_N)
        snapshot["stock_top"] = stock_top
        snapshot["stock_bottom"] = stock_bot
        _accumulate_error(snapshot, "stock", err)

        ind_top, ind_bot, err = fetch_industry_top_tdx(top_n=DEFAULT_FETCH_INDUSTRY_TOP_N)
        snapshot["industry_top"] = ind_top
        snapshot["industry_bottom"] = ind_bot
        _accumulate_error(snapshot, "industry", err)

        con_top, con_bot, err = fetch_concept_top_tdx(top_n=DEFAULT_FETCH_CONCEPT_TOP_N)
        snapshot["concept_top"] = con_top
        snapshot["concept_bottom"] = con_bot
        _accumulate_error(snapshot, "concept", err)

        # 探测：tdx 跑通但所有净额都是 0 → fallback 到 akshare
        if not (_has_real_money(stock_top) or _has_real_money(ind_top) or _has_real_money(con_top)):
            logger.warning(
                "tdx 资金流全 0（TQ server 没给资金流分类数据，可能是 level-1 客户端或午盘累积中），fallback 到 akshare"
            )
            tdx_no_fund_flow = True
        return snapshot, True, tdx_no_fund_flow
    except Exception as e:
        logger.exception("通达信主路径失败，切换 akshare 兜底")
        _accumulate_error(snapshot, f"tdx[{_classify_error(e)}]", e)
        return snapshot, False, False


def _fetch_via_akshare_fallback(snapshot):
    """阶段 2：akshare 兜底拼接。

    仅覆写资金流相关 key，保留 tdx 的行情快照（如果有）。
    返回是否成功。
    """
    fb = fetch_akshare_fallback()
    if not fb:
        _accumulate_error(snapshot, "akshare", "也不可用")
        return False
    for k in ("stock_top", "stock_bottom", "industry_top", "industry_bottom",
              "concept_top", "concept_bottom"):
        if fb.get(k):
            snapshot[k] = fb[k]
    _accumulate_error(snapshot, "akshare stock", fb.get("stock_error"))
    _accumulate_error(snapshot, "akshare industry", fb.get("industry_error"))
    _accumulate_error(snapshot, "akshare concept", fb.get("concept_error"))
    return True


def _compose_snapshot(now, tdx_no_fund_flow):
    """阶段 1+2：组合 snapshot 顶层字段（timestamp/is_trading/error/source）。"""
    return {
        "timestamp": now.strftime("%Y-%m-%d %H:%M:%S"),
        "is_trading": in_trading_hours(now),
        "error": None,
        "source": "tdx",
    }


# _append_history 已被 _commit_snapshot_and_history 取代（P3-1 原子提交）。


def _commit_snapshot_and_history(snapshot):
    """阶段 4：单 DATA_LOCK 内原子完成 history 追加 + LATEST 替换。

    P3-1：把 history 与 snapshot 写入合并到同一临界区，避免
    HTTP /api/trend 在中间窗口期看到「时间戳已跳但 series 未更新」的撕裂。
    """
    if not any(snapshot.get(k) for k in ("stock_top", "stock_bottom", "industry_top",
                                          "industry_bottom", "concept_top", "concept_bottom")):
        logger.debug("本次 fetch 数据全空，保留 LATEST 已有数据")
        return False

    now_ts = bj_now_naive().strftime("%H:%M:%S")
    with DATA_LOCK:
        # ---- history 追加（一次性提交）----
        hist = LATEST["history"]
        hist["timestamps"].append(now_ts)
        if len(hist["timestamps"]) > HISTORY_MAX:
            del hist["timestamps"][:len(hist["timestamps"]) - HISTORY_MAX]
        n_ts = len(hist["timestamps"]) - 1
        zero_streak = {}

        for r in snapshot.get("industry_top", [])[:IND_TOP_N]:
            name = str(r.get("行业", ""))
            if name and name not in hist["industry"]:
                hist["industry"][name] = [None] * n_ts
            _push_series(hist["industry"], name, parse_money_to_yi(r.get("净额")), zero_streak)
        for r in snapshot.get("stock_top", [])[:STOCK_TOP_N]:
            name = str(r.get("股票代码", ""))
            if name and name not in hist["stock"]:
                hist["stock"][name] = [None] * n_ts
            _push_series(hist["stock"], name, parse_money_to_yi(r.get("净额")), zero_streak)
        for r in snapshot.get("concept_top", [])[:CONCEPT_TOP_N]:
            name = str(r.get("行业", ""))
            if name and name not in hist["concept"]:
                hist["concept"][name] = [None] * n_ts
            _push_series(hist["concept"], name, parse_money_to_yi(r.get("净额")), zero_streak)

        market_net = sum(parse_money_to_yi(r.get("净额")) for r in snapshot.get("industry_top", [])) \
                   + sum(parse_money_to_yi(r.get("净额")) for r in snapshot.get("industry_bottom", []))
        hist["market"].append(round(market_net, 2))
        if len(hist["market"]) > HISTORY_MAX:
            del hist["market"][:len(hist["market"]) - HISTORY_MAX]
        LATEST["market_net"] = round(market_net, 2)

        # ---- LATEST 替换（同一锁内，紧跟 history）----
        for k in ("timestamp", "stock_top", "stock_bottom", "industry_top", "industry_bottom",
                 "concept_top", "concept_bottom", "is_trading", "error", "source"):
            LATEST[k] = snapshot.get(k)
    return True


def _commit_snapshot(snapshot):
    """阶段 4 兼容入口（仅写 LATEST 不写 history，保留以兼容旧调用）。

    推荐使用 _commit_snapshot_and_history() 同时完成 history + LATEST。
    """
    return _commit_snapshot_and_history(snapshot)


def fetch_once():
    """拉一次完整数据，存到 LATEST（线程安全）。

    P3-1 原子化后流程：
      1. _fetch_via_tdx + _fetch_via_akshare_fallback → 组装 snapshot
      2. _commit_snapshot_and_history → 单 DATA_LOCK 内一次性 history + LATEST
    """
    with FETCH_LOCK:
        now = bj_now_naive()
        snapshot = _compose_snapshot(now, False)

        # === 阶段 1：TDX 主路径 ===
        tdx_snap, tdx_used, tdx_no_fund_flow = _fetch_via_tdx(now)
        snapshot.update(tdx_snap)
        # === 阶段 1.5：akshare 兜底（TDX 失败 / TDX 资金流全 0）===
        if not tdx_used or tdx_no_fund_flow:
            akshare_ok = _fetch_via_akshare_fallback(snapshot)
            if tdx_no_fund_flow:
                snapshot["source"] = "mixed(tdx+akshare)" if akshare_ok else "tdx"
            elif not tdx_used:
                snapshot["source"] = "akshare" if akshare_ok else "tdx"

        # === 阶段 2+3：history 累积 + LATEST 替换，单 DATA_LOCK 内原子提交 ===
        _commit_snapshot_and_history(snapshot)


def background_loop(interval=60, stop_event=None):
    """后台线程：每 interval 秒拉一次（通达信比 akshare 慢，间隔拉长到 60s）

"""
    print(f"[bg] started, interval={interval}s, source={'tdx' if TDX_OK else 'akshare'}", flush=True)
    if stop_event is None:
        stop_event = threading.Event()
    while not stop_event.is_set():
        # 整轮 watchdog：fetch_once 在 daemon 线程里跑，超时则放弃本轮（保留 LATEST 旧值）
        watchdog = _call_with_timeout(fetch_once, timeout=interval - 5, default=None, label="background_loop.fetch_once")
        if watchdog is None:
            logger.warning("[bg] 本轮拉数超过 %ds watchdog 超时", interval - 5)
        # 间隔睡眠，但能被 stop_event 立即唤醒
        if stop_event.wait(timeout=interval):
            break
    print("[bg] stopped", flush=True)


# 后台线程：每 600s 拉一次近 N 日累计（akshare 接口，慢）
PERIOD_FETCH_INTERVAL = 600  # 10 分钟


def period_background_loop(interval=PERIOD_FETCH_INTERVAL, stop_event=None):
    print(f"[bg-period] started, interval={interval}s", flush=True)
    if stop_event is None:
        stop_event = threading.Event()
    time.sleep(8)  # 启动后稍等，避免与主拉数同时
    while not stop_event.is_set():
        for period in ("3d", "5d", "10d"):
            if stop_event.is_set():
                break
            try:
                t0 = time.time()
                snap = fetch_akshare_period(period)
                if snap:
                    with DATA_LOCK:
                        LATEST["period"][period] = {
                            "stock_top": snap["stock_top"],
                            "stock_bottom": snap["stock_bottom"],
                            "error": snap.get("error"),
                            "updated": bj_now_naive().strftime("%Y-%m-%d %H:%M:%S"),
                        }
                    print(f"[bg-period] {period} done in {time.time()-t0:.1f}s "
                          f"(top={len(snap['stock_top'])}, bot={len(snap['stock_bottom'])})", flush=True)
                else:
                    print(f"[bg-period] {period} skipped (akshare unavailable)", flush=True)
            except Exception as e:
                print(f"[bg-period] {period} error: {e}", flush=True)
            if stop_event.wait(timeout=2):
                break  # 错峰间能被 stop_event 立即唤醒
        if stop_event.wait(timeout=interval):
            break
    print("[bg-period] stopped", flush=True)


# 每日收盘快照：每日 15:30 后保存当日 TOP10 流入/流出，环形缓冲近 10 天
DAILY_MAX_DAYS = 10
DAILY_SAVE_HOUR = 15  # 15:00 后（实际 15:30 后才会触发第一次）
DAILY_SAVE_MINUTE = 30


def save_daily_summary_if_due():
    """检查今日是否已收盘且未保存过，是则保存一次。"""
    now = bj_now_naive()
    # 周末不保存
    if now.weekday() > 4:
        return False
    if now.hour < DAILY_SAVE_HOUR or (now.hour == DAILY_SAVE_HOUR and now.minute < DAILY_SAVE_MINUTE):
        return False
    date_key = now.strftime("%Y-%m-%d")
    with DATA_LOCK:
        if date_key in LATEST["daily_summaries"]:
            return False
        top = list(LATEST.get("stock_top", []))[:10]
        bot = list(LATEST.get("stock_bottom", []))[:10]
        ind_top = list(LATEST.get("industry_top", []))[:10]
        ind_bot = list(LATEST.get("industry_bottom", []))[:10]
        con_top = list(LATEST.get("concept_top", []))[:15]
        con_bot = list(LATEST.get("concept_bottom", []))[:15]
    if not top and not bot:
        return False
    inflow_total = sum_period_amount(top)
    outflow_total = abs(sum_period_amount(bot))
    with DATA_LOCK:
        LATEST["daily_summaries"][date_key] = {
            "stock_top": top,
            "stock_bottom": bot,
            "industry_top": ind_top,
            "industry_bottom": ind_bot,
            "concept_top": con_top,
            "concept_bottom": con_bot,
            "inflow_total": inflow_total,
            "outflow_total": outflow_total,
            "saved_at": now.strftime("%Y-%m-%d %H:%M:%S"),
        }
        # 环形缓冲：保留最近 N 天
        keys = sorted(LATEST["daily_summaries"].keys())
        if len(keys) > DAILY_MAX_DAYS:
            for k in keys[:-DAILY_MAX_DAYS]:
                LATEST["daily_summaries"].pop(k, None)
    print(f"[daily-snapshot] 已保存 {date_key} inflow={inflow_total}亿 outflow={outflow_total}亿", flush=True)
    return True


def daily_snapshot_loop(interval=60, stop_event=None):
    """每分钟检查一次是否需要保存当日快照。"""
    print(f"[daily-snapshot] started, interval={interval}s", flush=True)
    if stop_event is None:
        stop_event = threading.Event()
    if stop_event.wait(timeout=30):
        return
    while not stop_event.is_set():
        try:
            save_daily_summary_if_due()
        except Exception as e:
            print(f"[daily-snapshot] error: {e}", flush=True)
        if stop_event.wait(timeout=interval):
            break
    print("[daily-snapshot] stopped", flush=True)


# ====== HTTP 服务 ======
class ThreadingHTTPServer(ThreadingMixIn, HTTPServer):
    """线程化 HTTP 服务：每个请求一个 daemon 线程。

    适合 I/O 密集型的看板场景（dashboard.html + 多个 /api/* JSON）。
    与父类 HTTPServer 不同：Handler.do_GET 不再阻塞其它连接。
    daemon_threads=True 让 Ctrl+C 时不必 join 每个线程。
    """
    daemon_threads = True


class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        # 屏蔽 BaseHTTPRequestHandler 默认的 stderr access log（刷屏且无级别）
        pass

    def _send_json_response(self, status, payload, extra_headers=None):
        """统一 JSON 响应出口（含错误响应）。

        - 锁外序列化：调用方在持锁时仅构造 dict，序列化放到此方法内
        - 错误统一形态 `{ok: False, code, error}`，前端可按 code 判断
        - wfile.write 包 try/except，断连时不抛 stacktrace
        """
        try:
            data = json.dumps(payload, ensure_ascii=False, default=str).encode("utf-8")
        except (TypeError, ValueError) as e:
            logger.warning("_send_json_response 序列化失败: %s", e)
            data = b'{"ok":false,"code":500,"error":"serialization_failed"}'
            status = 500
        try:
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(data)))
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Cache-Control", "no-store")
            if extra_headers:
                for k, v in extra_headers.items():
                    self.send_header(k, v)
            self.end_headers()
            self.wfile.write(data)
        except (BrokenPipeError, ConnectionResetError):
            # 客户端断开（页面刷新 / fetch abort）属于正常路径
            logger.debug("client disconnected during response: %s", self.path)
        except OSError as e:
            logger.warning("send failed (%s): %s", self.path, e)

    def _send_not_found(self):
        """统一 404 响应：API 路径返 JSON，文件路径返 HTML。"""
        if self.path.startswith("/api/"):
            self._send_json_response(404, {"ok": False, "code": 404, "error": "not_found", "path": self.path})
        else:
            self.send_error(404)

    def do_GET(self):
        if self.path == "/" or self.path == "/index.html":
            self._serve_file("dashboard.html", "text/html; charset=utf-8")
        elif self.path == "/api/data":
            self._serve_json()
        elif self.path == "/api/history":
            self._serve_history()
        elif self.path == "/api/trend":
            self._serve_trend()
        elif self.path == "/api/health":
            self._serve_health()
        elif self.path == "/api/alerts":
            self._serve_alerts()
        elif self.path.startswith("/api/data/period_compare"):
            self._serve_period_compare()
        elif self.path.startswith("/api/data/daily_top"):
            self._serve_daily_top()
        elif self.path.startswith("/api/data/latest"):
            self._serve_latest_snapshot()
        elif self.path.startswith("/api/data/period"):
            self._serve_period()
        else:
            # 兜底：同目录下任意静态文件（如 echarts.min.js、favicon.ico 等）
            # 仅允许白名单后缀，避免路径穿越
            from urllib.parse import unquote
            name = unquote(self.path.lstrip("/"))
            if ".." in name or name.startswith("/") or "\\" in name:
                # 路径穿越防御：拒绝 ../、绝对路径、反斜杠；返 JSON 错误便于前端识别
                logger.warning("拒绝路径穿越请求: %s", self.path)
                self._send_json_response(400, {"ok": False, "code": 400, "error": "invalid_path"})
                return
            ext = os.path.splitext(name)[1].lower()
            ctype_map = {
                ".js": "application/javascript; charset=utf-8",
                ".css": "text/css; charset=utf-8",
                ".html": "text/html; charset=utf-8",
                ".htm": "text/html; charset=utf-8",
                ".json": "application/json; charset=utf-8",
                ".png": "image/png",
                ".svg": "image/svg+xml",
                ".ico": "image/x-icon",
            }
            ctype = ctype_map.get(ext, "application/octet-stream")
            self._serve_file(name, ctype)

    def _serve_period(self):
        """GET /api/data/period?p=3d|5d|10d — 近 N 日累计资金流排行"""
        from urllib.parse import urlparse, parse_qs
        qs = parse_qs(urlparse(self.path).query)
        period = (qs.get("p", ["3d"])[0] or "3d").strip()
        if period not in ("3d", "5d", "10d"):
            period = "3d"
        with DATA_LOCK:
            payload = dict(LATEST["period"].get(period, {}))
        self._send_json_response(200, {"period": period, **payload})

    def _serve_period_compare(self):
        """GET /api/data/period_compare — 1d/3d/5d/10d 累计流入流出总额 + 近5日明细。"""
        payload = build_period_compare()
        self._send_json_response(200, payload)

    def _serve_daily_top(self):
        """GET /api/data/daily_top?n=6 — 最近 N 个交易日的 TOP 流入/流出快照（含每日明细）。"""
        from urllib.parse import urlparse, parse_qs
        qs = parse_qs(urlparse(self.path).query)
        try:
            n = int(qs.get("n", ["6"])[0])
        except (ValueError, TypeError):
            n = 6
        n = max(1, min(n, DAILY_MAX_DAYS))
        with DATA_LOCK:
            summaries = {k: dict(v) for k, v in LATEST["daily_summaries"].items()}
        sorted_dates = sorted(summaries.keys())
        last_n_dates = sorted_dates[-n:]  # 升序：早 → 晚
        daily = []
        for date in last_n_dates:
            entry = summaries[date]
            top = entry.get("stock_top", [])
            bot = entry.get("stock_bottom", [])
            # 若 summary 里没显式存 inflow/outflow_total，从明细实时算（兼容 demo 数据）
            inflow_total = entry.get("inflow_total")
            if inflow_total is None:
                inflow_total = sum_period_amount(top)
            outflow_total = entry.get("outflow_total")
            if outflow_total is None:
                outflow_total = abs(sum_period_amount(bot))
            daily.append({
                "date": date,
                "inflow_total": inflow_total,
                "outflow_total": outflow_total,
                "stock_top": top,
                "stock_bottom": bot,
                "industry_top": entry.get("industry_top", []),
                "industry_bottom": entry.get("industry_bottom", []),
                "concept_top": entry.get("concept_top", []),
                "concept_bottom": entry.get("concept_bottom", []),
                "saved_at": entry.get("saved_at"),
            })
        payload = {
            "daily": daily,
            "have_count": len(daily),
            "requested": n,
        }
        self._send_json_response(200, payload)

    def _serve_latest_snapshot(self):
        """GET /api/data/latest — 返回最近 1 日的完整 TOP 快照（休市时给前端兜底渲染）。

        优先返回 daily_summaries 中最近一日（升序排序最后一项）；
        若 daily_summaries 为空（首次启动且未保存），fallback 到 LATEST 实时数据。
        """
        with DATA_LOCK:
            summaries = {k: dict(v) for k, v in LATEST["daily_summaries"].items()}
            live_stock_top = list(LATEST.get("stock_top", []))
            live_industry_top = list(LATEST.get("industry_top", []))
            live_concept_top = list(LATEST.get("concept_top", []))
            live_stock_bottom = list(LATEST.get("stock_bottom", []))
            live_industry_bottom = list(LATEST.get("industry_bottom", []))
            live_concept_bottom = list(LATEST.get("concept_bottom", []))
        sorted_dates = sorted(summaries.keys())
        if sorted_dates:
            latest_date = sorted_dates[-1]
            entry = summaries[latest_date]
            payload = {
                "source": "daily_snapshot",
                "date": latest_date,
                "saved_at": entry.get("saved_at"),
                "stock_top": entry.get("stock_top", []),
                "stock_bottom": entry.get("stock_bottom", []),
                "industry_top": entry.get("industry_top", []),
                "industry_bottom": entry.get("industry_bottom", []),
                "concept_top": entry.get("concept_top", []),
                "concept_bottom": entry.get("concept_bottom", []),
            }
        elif live_stock_top or live_industry_top or live_concept_top:
            payload = {
                "source": "live",
                "date": None,
                "saved_at": None,
                "stock_top": live_stock_top,
                "stock_bottom": live_stock_bottom,
                "industry_top": live_industry_top,
                "industry_bottom": live_industry_bottom,
                "concept_top": live_concept_top,
                "concept_bottom": live_concept_bottom,
            }
        else:
            payload = {
                "source": "empty",
                "date": None,
                "saved_at": None,
                "stock_top": [],
                "stock_bottom": [],
                "industry_top": [],
                "industry_bottom": [],
                "concept_top": [],
                "concept_bottom": [],
            }
        self._send_json_response(200, payload)

    def _serve_file(self, name, ctype):
        path = os.path.join(os.path.dirname(os.path.abspath(__file__)), name)
        try:
            with open(path, "rb") as f:
                data = f.read()
            self.send_response(200)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(data)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(data)
        except (BrokenPipeError, ConnectionResetError):
            logger.debug("client disconnected during file response: %s", name)
        except OSError as e:
            logger.warning("send file failed (%s): %s", name, e)
        except FileNotFoundError:
            self._send_not_found()

    def _serve_json(self):
        # 锁内只读拷贝 dict（list 是共享引用可接受，因为只读不写）
        with DATA_LOCK:
            payload = {
                "timestamp": LATEST["timestamp"],
                "is_trading": LATEST["is_trading"],
                "error": LATEST["error"],
                "source": LATEST.get("source"),
                "stock_top": LATEST["stock_top"],
                "stock_bottom": LATEST["stock_bottom"],
                "industry_top": LATEST["industry_top"],
                "industry_bottom": LATEST["industry_bottom"],
                "concept_top": LATEST["concept_top"],
                "concept_bottom": LATEST["concept_bottom"],
            }
        # 序列化 + 网络写出放锁外，避免慢 IO 阻塞其它请求
        self._send_json_response(200, payload)

    def _serve_history(self):
        with DATA_LOCK:
            payload = {"history": LATEST["history"]}
        self._send_json_response(200, payload)

    def _serve_trend(self):
        with DATA_LOCK:
            hist = LATEST["history"]
            # names 从 LATEST["stock_top/industry_top/concept_top"] + history 字典 key 合并获取
            # 这样 history 里有但 TOP 列表里没有的 key（流出股、流出行业/概念）也能正确显示名字
            stock_top = LATEST["stock_top"][:STOCK_TOP_N]
            stock_bot = LATEST["stock_bottom"][:STOCK_TOP_N]
            stock_nm = {str(r.get("股票代码", "")): str(r.get("股票简称", "")) for r in stock_top + stock_bot}
            for code in hist["stock"].keys():
                if code not in stock_nm:
                    # history key 是股票代码本身，名字就用代码（兜底）
                    stock_nm[code] = code
            industry_top = LATEST["industry_top"][:IND_TOP_N]
            industry_bot = LATEST["industry_bottom"][:IND_TOP_N]
            ind_nm = {str(r.get("行业", "")): str(r.get("行业", "")) for r in industry_top + industry_bot}
            for k in hist["industry"].keys():
                if k not in ind_nm:
                    ind_nm[k] = k
            concept_top = LATEST["concept_top"][:CONCEPT_TOP_N]
            concept_bot = LATEST["concept_bottom"][:CONCEPT_TOP_N]
            con_nm = {str(r.get("行业", "")): str(r.get("行业", "")) for r in concept_top + concept_bot}
            for k in hist["concept"].keys():
                if k not in con_nm:
                    con_nm[k] = k
            payload = {
                "timestamps": hist["timestamps"],
                "industry": hist["industry"],
                "stock": hist["stock"],
                "concept": hist["concept"],
                "market": hist["market"],
                "names": {
                    "stock": stock_nm,
                    "industry": ind_nm,
                    "concept": con_nm,
                },
                "market_net": LATEST["market_net"],
            }
        self._send_json_response(200, payload)

    def _serve_health(self):
        health = {
            "tdx_client_available": TDX_OK,
            "akshare_available": AK_OK,
            "current_source": LATEST.get("source"),
            "last_timestamp": LATEST["timestamp"],
            "last_error": LATEST["error"],
        }
        if TDX_OK:
            try:
                # tdx_client.health_check 是阻塞调用：包超时防止 /api/health 雪崩
                fut_health = _call_with_timeout(tdx_client.health_check, timeout=3.0, default=False)
                health["tdx_health"] = "ok" if fut_health else "fail"
            except Exception as e:
                health["tdx_health"] = f"error: {e}"
        self._send_json_response(200, health)

    def _serve_alerts(self):
        """实时异动监测状态"""
        global _alert_monitor_inst
        if not _alert_monitor_inst:
            payload = {"enabled": False, "reason": "alert_monitor 未启动"}
        else:
            m = _alert_monitor_inst
            payload = {
                "enabled": True,
                "full_watchlist_size": len(m._full_watchlist),
                "hot_watchlist_size": len(m._hot_watchlist),
                "subscribed_size": len(m._subscribed),
                "alerts_sent": m.stats['alerts_sent'],
                "events_received": m.stats['events_received'],
                "scans_done": m.stats['scans_done'],
                "started_at": m.stats['started_at'],
                "scan_interval_sec": m.scan_interval_sec,
                "hot_watchlist_sample": sorted(m._hot_watchlist)[:10],
            }
        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(data)


def main(port=None, fetch_interval=180):
    # 端口 / 绑定可通过环境变量覆盖
    if port is None:
        port = int(os.environ.get("DASHBOARD_PORT", "8765"))
    bind_host = os.environ.get("DASHBOARD_BIND", "0.0.0.0")
    print(f"[{bj_now_naive():%H:%M:%S}] 启动 A 股资金流看板")
    print(f"  数据源: {'通达信 TdxQuant' if TDX_OK else 'akshare 兜底'}")
    print(f"  访问地址: http://localhost:{port}/")
    print(f"  后台拉数间隔: {fetch_interval}s")
    print(f"  Ctrl+C 停止")

    # 启动实时异动监测（如果有 alert_monitor）
    global _alert_monitor_inst
    if ALERT_OK and TDX_OK:
        try:
            # 复用看板的 tqcenter 实例，避免重复 init 占槽位
            # 注意：如果 tdx_client 还没 init，alert_monitor 也会失败 — 后台会持续报错
            _alert_monitor_inst = alert_monitor.RealtimeAlertMonitor()
            # 直接复用 tdx_client.tq（即使未 init 也无妨，alert_monitor 内部会报错但不影响启动）
            _alert_monitor_inst.init_tq(existing_tq=tdx_client.tq)
            alert_monitor.default_watchlist(_alert_monitor_inst)
            _alert_monitor_inst.start()
            print(f"  实时异动监测: [OK] 已启动（候选池 {len(_alert_monitor_inst._full_watchlist)} 只，实时订阅 {len(_alert_monitor_inst._hot_watchlist)} 只）", flush=True)
        except Exception as e:
            print(f"  实时异动监测: [FAIL] 启动失败: {e}", flush=True)
            _alert_monitor_inst = None

    # 优雅退出：后台线程用 stop_event 控制，非 daemon；Ctrl+C 时 join
    stop_event = threading.Event()

    def make_thread(target, *args):
        th = threading.Thread(target=target, args=args, daemon=False)
        th.start()
        return th

    bg_thread = make_thread(background_loop, fetch_interval, stop_event)
    period_thread = make_thread(period_background_loop, PERIOD_FETCH_INTERVAL, stop_event)
    daily_thread = make_thread(daily_snapshot_loop, 60, stop_event)

    # 立即拉一次（独立线程）
    def initial_fetch():
        try:
            fetch_once()
        except Exception as e:
            print(f"[init] error: {e}", flush=True)
    threading.Thread(target=initial_fetch, daemon=True).start()

    srv = ThreadingHTTPServer((bind_host, port), Handler)
    print(f"  [OK] HTTP 服务启动: {bind_host}:{port}（线程化）", flush=True)
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        print("\n[stop] Ctrl+C 收到，开始优雅退出", flush=True)
    finally:
        # 通知后台线程停
        stop_event.set()
        # 关 HTTP
        srv.shutdown()
        # 等后台线程（最多 10s）
        for name, th in [("background_loop", bg_thread),
                          ("period_background_loop", period_thread),
                          ("daily_snapshot_loop", daily_thread)]:
            th.join(timeout=10)
            if th.is_alive():
                logger.warning("[stop] %s 未能在 10s 内退出，强制离开", name)
        # 关 TDX 资源（如果有）
        if TDX_OK:
            try:
                tdx_client.close()
            except Exception:
                pass
        print("[stop] 退出完成", flush=True)


if __name__ == "__main__":
    main()