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
"""
import os
for k in ("HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy", "ALL_PROXY", "all_proxy"):
    os.environ.pop(k, None)
os.environ["NO_PROXY"] = "*"
os.environ["no_proxy"] = "*"

import json
import time
import threading
import logging
import warnings
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from datetime import datetime, time as dtime
from typing import Dict, List
try:
    from zoneinfo import ZoneInfo
    BJ_TZ = ZoneInfo("Asia/Shanghai")
except Exception:
    BJ_TZ = None  # 老 Python 退化；本机 3.12 必有

# 当前北京时间（机器本地时区不是北京时区，本机 MDT UTC-7）
def bj_now() -> datetime:
    if BJ_TZ is not None:
        return datetime.now(BJ_TZ)
    # 退化：假设系统本地 == 北京（不靠谱但比 datetime.now() 强）
    return datetime.now()

warnings.filterwarnings("ignore")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("dashboard")

# === 通达信客户端 ===
try:
    import tdx_client
    TDX_OK = True
except Exception as e:
    logger.warning("tdx_client 加载失败: %s — 看板将使用 akshare 兜底", e)
    TDX_OK = False

# akshare 仅作为兜底
try:
    import akshare as ak
    AK_OK = True
except Exception:
    AK_OK = False


# ====== 数据缓存 ======
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
HISTORY_MAX = 240
IND_TOP_N = 20
STOCK_TOP_N = 20
CONCEPT_TOP_N = 15

# 概念板块代码前缀（通达信 8809xx 系列）
CONCEPT_PREFIXES = ("8809",)

# 申万行业代码前缀（通达信 881xxx 系列，6 位数字）
INDUSTRY_PREFIXES = ("881",)


def in_trading_hours(now=None):
    """判断当前是否在 A 股交易时段（北京时间为准）。
    
    注意：本机时区是 MDT（UTC-7），不能直接用 datetime.now()。
    传入的 now 应当是带时区的北京时间；如果没传，自动取 bj_now()。
    """
    if now is None:
        now = bj_now()
    # 兜底：万一传进来的 now 是裸 datetime（无 tzinfo），假设它是北京时间
    if now.tzinfo is None and BJ_TZ is not None:
        now = now.replace(tzinfo=BJ_TZ)
    if now.weekday() > 4:  # 周六/周日
        return False
    t = now.time()
    return (dtime(9, 30) <= t <= dtime(11, 30)) or (dtime(13, 0) <= t <= dtime(15, 0))


# ============================================================
# 通达信数据源
# ============================================================

def _format_stock_row(code, name, fund_flow_dict, snapshot_dict=None):
    """格式化成前端需要的 dict: {代码, 名称, 涨跌幅, 净额}

    字段语义（来自 tdx_client._FUND_FLOW_FIELDS 注释）：
      - ZAF: 当日涨跌幅（%）
      - Zjl: 主力净流入（元）— 通达信早盘 9:30-10:30 区间此字段全 0（数据延迟）
      - OpenAmo: 开盘成交额（元）— 早盘有值，可作净额兜底
    """
    zaf = fund_flow_dict.get("ZAF") or 0  # 涨跌幅（%）
    zjl = fund_flow_dict.get("Zjl") or 0
    open_amo = fund_flow_dict.get("OpenAmo") or 0  # 开盘成交额（元）

    # 净额显示策略：Zjl 有值时用它（真资金净额），否则用 "—" 占位
    # 不用 OpenAmo 顶替（语义不同，OpenAmo 是成交额不是净额，会误导）
    if zjl:
        net_yi = zjl / 1e8
        net_str = f"{net_yi:.2f}亿"
    else:
        net_str = "—"  # 早盘/午休/收盘后通达信没累计完

    return {
        "股票代码": code,
        "股票简称": name or "",
        "涨跌幅": round(zaf, 2),
        "净额": net_str,
        # 扩展字段（前端暂时不显示，但 API 返回供将来用）
        "_主力净流入": zjl,
        "_主力净流入_亿": round(zjl / 1e8, 4),
        "_开盘成交额_元": open_amo,
        "_开盘成交额_亿": round(open_amo / 1e8, 4),
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
            if total_zjl:
                net_str = f"{total_zjl / 1e8:.2f}亿"
            else:
                net_str = "—"  # 早盘/午休/收盘后
            rows.append({
                "行业": sec["Name"],
                "行业-涨跌幅": round(avg_zaf, 2),
                "净额": net_str,
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
    return rows[:top_n], rows[-top_n:][::-1] if len(rows) >= top_n else [], err


def fetch_concept_top_tdx(top_n=15):
    """概念资金流排行"""
    rows, err = fetch_sector_aggregate_tdx(prefix_filter=CONCEPT_PREFIXES, top_n=top_n)
    return rows[:top_n], rows[-top_n:][::-1] if len(rows) >= top_n else [], err


# ============================================================
# akshare 兜底（保留原有逻辑）
# ============================================================

def parse_money_to_yi(v, default_unit="wan"):
    """把 A 股资金流的金额字符串统一转成"亿"（float）

    单位识别：
      - '13.09亿'    → 13.09
      - '3420.00万'  → 0.3420
      - '9841.00'    （akshare 个股纯数字无后缀）→ 0.9841
      - '65.64'      （akshare 行业/概念纯数字无后缀）→ 65.64
      - 纯数字 0.0   → 0.0
      - 异常值       → 0.0

    Args:
        v: 值
        default_unit: 无单位纯数字的默认单位
            "wan" = 万（akshare 个股行为）
            "yi"  = 亿（akshare 行业/概念行为）
    """
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
        # 纯数字 — 按 default_unit 推断
        if default_unit == "yi":
            return float(s)
        else:  # "wan"
            return float(s) / 1e4
    except (ValueError, TypeError):
        return 0.0


def sort_by_money(df, col, ascending=False, default_unit="wan"):
    """按带亿/万/纯数字的金额列正确排序（P11 修复）

    Args:
        default_unit: 传给 parse_money_to_yi 的默认单位
            个股用 "wan"（默认），行业/概念用 "yi"
    """
    if df is None or df.empty or col not in df.columns:
        return df
    return df.assign(_sort=df[col].apply(lambda v: parse_money_to_yi(v, default_unit))).sort_values(
        "_sort", ascending=ascending
    ).drop(columns=["_sort"]).reset_index(drop=True)


def fetch_akshare_fallback():
    """通达信失败时用 akshare 兜底

    P37 修复：fund_flow 系列返回的 净额/流入/流出 列是带'亿/万'的字符串
    (P11)。排序前必须 parse_money_to_yi 转成 float (单位:亿)，不能用
    字符串字典序排。akshare 默认按"涨跌幅"排 (P10)，必须手动按净额重排。

    注意：akshare 默认无超时，长跑进程里偶发 mini_racer 卡死。
    三个 ak.* 调用都包 try/except，单个失败不影响其他。
    """
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
            # P11 修复：akshare 个股"净额"列是纯数字无后缀（实际单位=万）
            top = sort_by_money(df, "净额", ascending=False).head(20)
            bot = sort_by_money(df, "净额", ascending=True).head(10)
            top_records = top.fillna("").to_dict(orient="records")
            bot_records = bot.fillna("").to_dict(orient="records")
            # 修复：akshare 返回的"股票代码"是 int (e.g. 977)，
            # 前导 0 丢失（真实应为 "000977"）。补成 6 位字符串。
            for r in top_records + bot_records:
                code = r.get("股票代码")
                if isinstance(code, int):
                    r["股票代码"] = str(code).zfill(6)
                elif isinstance(code, str) and code.isdigit():
                    r["股票代码"] = code.zfill(6)
            snap["stock_top"] = top_records
            snap["stock_bottom"] = bot_records
        else:
            snap["stock_error"] = "ak.stock_fund_flow_individual 返回空"
    except Exception as e:
        snap["stock_error"] = f"{type(e).__name__}: {e}"
        logger.exception("akshare 个股拉取失败")

    try:
        df = ak.stock_fund_flow_industry(symbol="即时")
        if df is not None and not df.empty:
            df = df.drop_duplicates(subset=["行业"], keep="first")
            # 行业/概念的"净额"列是纯数字无后缀，单位是"亿"（P11 区分点）
            top = sort_by_money(df, "净额", ascending=False, default_unit="yi").head(20)
            bot = sort_by_money(df, "净额", ascending=True, default_unit="yi").head(8)
            snap["industry_top"] = top.fillna("").to_dict(orient="records")
            snap["industry_bottom"] = bot.fillna("").to_dict(orient="records")
        else:
            snap["industry_error"] = "ak.stock_fund_flow_industry 返回空"
    except Exception as e:
        snap["industry_error"] = f"{type(e).__name__}: {e}"
        logger.exception("akshare 行业拉取失败")

    try:
        df = ak.stock_fund_flow_concept(symbol="即时")
        if df is not None and not df.empty:
            df = df.drop_duplicates(subset=["行业"], keep="first")
            # 概念的"净额"列是纯数字无后缀，单位是"亿"（P11 区分点）
            top = sort_by_money(df, "净额", ascending=False, default_unit="yi").head(15)
            bot = sort_by_money(df, "净额", ascending=True, default_unit="yi").head(5)
            snap["concept_top"] = top.fillna("").to_dict(orient="records")
            snap["concept_bottom"] = bot.fillna("").to_dict(orient="records")
        else:
            snap["concept_error"] = "ak.stock_fund_flow_concept 返回空"
    except Exception as e:
        snap["concept_error"] = f"{type(e).__name__}: {e}"
        logger.exception("akshare 概念拉取失败")

    return snap


# ============================================================
# P37 修复：通达信资金流字段结构性 0 的兜底探测
# ============================================================
# 背景：tdx `get_more_info` 的 Zjl/TotalBVol/BCancel 等资金流分类字段
# 会返回 '0.00' (str)，但 get_market_snapshot 的价格/成交量正常。
# 这时 tdx 主路径**不抛异常**，但资金流全是 0 — 必须主动探测并 fallback。
def _has_real_money(rows, threshold=0.2):
    """检查 rows 中是否真有非零净额（修复 P37 假阳性）

    Args:
        rows: list of dict, 每条必须含 '净额' 字段
        threshold: 非零行比例阈值，低于此视为数据源异常
    Returns:
        True = 数据有真值, False = 全 0 / 数据源异常
    """
    if not rows:
        return False
    nonzero = 0
    for r in rows:
        v = r.get("净额", 0)
        if isinstance(v, str):
            # 带'亿'/'万'的字符串（tdx 已经是 float 元，_format_stock_row 包装成 "X.XX亿"）
            s = v.strip().replace(",", "")
            if s in ("", "—", "-"):
                continue
            mult = 1.0
            if s.endswith("亿"):
                s, mult = s[:-1], 1.0
            elif s.endswith("万"):
                s, mult = s[:-1], 0.0001  # 万→亿
            try:
                f = float(s) * mult
                if abs(f) > 0.001:
                    nonzero += 1
            except (ValueError, TypeError):
                pass
        elif isinstance(v, (int, float)) and abs(v) > 0.001:
            nonzero += 1
    return nonzero >= len(rows) * threshold


# ============================================================
# 主拉数
# ============================================================

def fetch_once():
    """拉一次完整数据，存到 LATEST（线程安全）"""
    with FETCH_LOCK:
        now = bj_now()  # 用北京时间，不用 datetime.now()（本机 MDT）
        snapshot = {
            "timestamp": now.strftime("%Y-%m-%d %H:%M:%S"),
            "is_trading": in_trading_hours(now),
            "error": None,
            "source": "tdx",
        }

        # === 通达信主路径 ===
        use_tdx = TDX_OK
        if use_tdx:
            try:
                stock_top, stock_bot, err = fetch_individual_flows_tdx(top_n=20)
                snapshot["stock_top"] = stock_top
                snapshot["stock_bottom"] = stock_bot
                if err:
                    snapshot["error"] = f"stock: {err}"

                ind_top, ind_bot, err = fetch_industry_top_tdx(top_n=5)
                snapshot["industry_top"] = ind_top
                snapshot["industry_bottom"] = ind_bot
                if err:
                    snapshot["error"] = (snapshot["error"] or "") + f" | industry: {err}"

                con_top, con_bot, err = fetch_concept_top_tdx(top_n=5)
                snapshot["concept_top"] = con_top
                snapshot["concept_bottom"] = con_bot
                if err:
                    snapshot["error"] = (snapshot["error"] or "") + f" | concept: {err}"
            except Exception as e:
                logger.exception("通达信主路径失败，切换 akshare 兜底")
                snapshot["error"] = f"tdx 整体失败: {e}"
                use_tdx = False

        # === akshare 兜底 ===
        # 触发条件二选一（任一即触发）：
        #   a) 通达信主路径整体失败（use_tdx=False）— 老逻辑
        #   b) 通达信跑通了但资金流字段全是 0（P37 假阳性）— 新增
        p37_triggered = False
        if use_tdx:
            # P37 探测：三个排行（个股/行业/概念）任一有真资金流就 OK
            stock_ok = _has_real_money(snapshot.get("stock_top") or [])
            ind_ok = _has_real_money(snapshot.get("industry_top") or [])
            con_ok = _has_real_money(snapshot.get("concept_top") or [])
            if not (stock_ok or ind_ok or con_ok):
                logger.warning(
                    "P37 命中：通达信资金流字段结构性返回 0（行情正常），"
                    "切换 akshare 兜底资金流"
                )
                p37_triggered = True
                use_tdx = False
                snapshot["error"] = (
                    (snapshot.get("error") or "")
                    + " | tdx 资金流全 0 (P37) — fallback akshare"
                ).strip(" |")

        if not use_tdx:
            fb = fetch_akshare_fallback()
            if fb:
                # P37 设计：只覆写资金流相关 key，保留 tdx 的行情快照（如果有）
                # 但目前 snapshot 里没有 tdx 行情快照（tdx client 没把 snapshot
                # 塞进 LATEST），所以是直接覆写。
                # 后续如果加 tdx 行情字段进 snapshot，再做"混合"细节
                snapshot.update(fb)
                if p37_triggered:
                    snapshot["source"] = "akshare(P37 fallback)"
            else:
                snapshot["error"] = (snapshot["error"] or "") + " | akshare 也不可用"

        # === 趋势记录 ===
        ts = now.strftime("%H:%M:%S")
        with DATA_LOCK:
            hist = LATEST["history"]
            hist["timestamps"].append(ts)
            if len(hist["timestamps"]) > HISTORY_MAX:
                hist["timestamps"] = hist["timestamps"][-HISTORY_MAX:]

            def push_series(target_dict, name, value):
                if name not in target_dict:
                    target_dict[name] = [None] * (len(hist["timestamps"]) - 1)
                target_dict[name].append(value)
                if len(target_dict[name]) > HISTORY_MAX:
                    target_dict[name] = target_dict[name][-HISTORY_MAX:]

            # P11 修复：根据数据源决定 parse_money_to_yi 的默认单位
            # tdx 包装的"净额"是 "X.XX亿" 字符串（带单位），parse 走"亿"分支没问题
            # akshare 个股"净额"是纯数字无后缀（实际单位=万）→ default_unit="wan"
            # akshare 行业/概念"净额"是纯数字无后缀（实际单位=亿）→ default_unit="yi"
            src = snapshot.get("source", "tdx")
            if src == "tdx":
                unit = "yi"
            elif "industry" in str(src) or "concept" in str(src):
                unit = "yi"
            else:
                # 兜底：akshare 个股来源，按万
                unit = "wan"

            for r in snapshot.get("industry_top", [])[:IND_TOP_N]:
                push_series(hist["industry"], str(r.get("行业", "")),
                            parse_money_to_yi(r.get("净额"), unit))
            for r in snapshot.get("stock_top", [])[:STOCK_TOP_N]:
                push_series(hist["stock"], str(r.get("股票代码", "")),
                            parse_money_to_yi(r.get("净额"), unit))
            for r in snapshot.get("concept_top", [])[:CONCEPT_TOP_N]:
                push_series(hist["concept"], str(r.get("行业", "")),
                            parse_money_to_yi(r.get("净额"), unit))

            market_net = sum(parse_money_to_yi(r.get("净额"), unit)
                             for r in snapshot.get("industry_top", [])) \
                       + sum(parse_money_to_yi(r.get("净额"), unit)
                             for r in snapshot.get("industry_bottom", []))
            hist["market"].append(round(market_net, 2))
            if len(hist["market"]) > HISTORY_MAX:
                hist["market"] = hist["market"][-HISTORY_MAX:]
            LATEST["market_net"] = round(market_net, 2)

            for k in ("timestamp", "stock_top", "stock_bottom", "industry_top", "industry_bottom",
                     "concept_top", "concept_bottom", "is_trading", "error", "source"):
                LATEST[k] = snapshot.get(k)
        return snapshot


def background_loop(interval=60):
    """后台线程：每 interval 秒拉一次（通达信比 akshare 慢，间隔拉长到 60s）"""
    print(f"[bg] started, interval={interval}s, source={'tdx' if TDX_OK else 'akshare'}", flush=True)
    time.sleep(5)
    while True:
        try:
            t0 = time.time()
            print(f"[bg] fetching @ {datetime.now():%H:%M:%S}", flush=True)
            fetch_once()
            print(f"[bg] done in {time.time()-t0:.1f}s @ {datetime.now():%H:%M:%S}", flush=True)
        except Exception as e:
            print(f"[bg] fetch error: {e}", flush=True)
        time.sleep(interval)


# ====== HTTP 服务 ======
class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        pass

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
        else:
            self.send_error(404)

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
        except FileNotFoundError:
            self.send_error(404)

    def _serve_json(self):
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
                # akshare 三个拉取的细粒度错误（fetch_akshare_fallback 设置）
                "stock_error": LATEST.get("stock_error"),
                "industry_error": LATEST.get("industry_error"),
                "concept_error": LATEST.get("concept_error"),
            }
        data = json.dumps(payload, ensure_ascii=False, default=str).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(data)

    def _serve_history(self):
        with DATA_LOCK:
            payload = {"history": LATEST["history"]}
        data = json.dumps(payload, ensure_ascii=False, default=str).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(data)

    def _serve_trend(self):
        with DATA_LOCK:
            hist = LATEST["history"]
            stock_top = LATEST["stock_top"][:STOCK_TOP_N]
            stock_nm = {str(r.get("股票代码", "")): str(r.get("股票简称", "")) for r in stock_top}
            industry_top = LATEST["industry_top"][:IND_TOP_N]
            ind_nm = {str(r.get("行业", "")): str(r.get("行业", "")) for r in industry_top}
            concept_top = LATEST["concept_top"][:CONCEPT_TOP_N]
            con_nm = {str(r.get("行业", "")): str(r.get("行业", "")) for r in concept_top}
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
        data = json.dumps(payload, ensure_ascii=False, default=str).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(data)

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
                health["tdx_health"] = "ok" if tdx_client.health_check() else "fail"
            except Exception as e:
                health["tdx_health"] = f"error: {e}"
        data = json.dumps(health, ensure_ascii=False).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(data)


def main(port=8765, fetch_interval=180):
    print(f"[{datetime.now():%H:%M:%S}] 启动 A 股资金流看板")
    print(f"  数据源: {'通达信 TdxQuant' if TDX_OK else 'akshare 兜底'}")
    print(f"  访问地址: http://localhost:{port}/")
    print(f"  后台拉数间隔: {fetch_interval}s")
    print(f"  Ctrl+C 停止")

    # 启动后台拉数线程
    t = threading.Thread(target=background_loop, args=(fetch_interval,), daemon=True)
    t.start()

    # 立即拉一次（独立线程）
    def initial_fetch():
        try:
            fetch_once()
        except Exception as e:
            print(f"[init] error: {e}", flush=True)
    threading.Thread(target=initial_fetch, daemon=True).start()

    srv = ThreadingHTTPServer(("0.0.0.0", port), Handler)
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        print("\n停止")
        srv.shutdown()


if __name__ == "__main__":
    main()