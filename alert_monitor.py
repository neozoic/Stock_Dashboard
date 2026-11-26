"""
实时异动监测模块（通达信 subscribe_hq 推送式订阅）

机制：
- 候选池股票（最多 100 只）订阅通达信实时推送
- 回调触发后主动 get_more_info 拿资金流
- 检测异动（主力大额流入、涨跌停、涨幅突变）
- 异动以卡片形式推飞书（入队异步，限速发送，不阻塞监测线程）；未配通道时输出到 stdout
- 同时通过 tq.send_warn 回推到通达信 TQ 策略信号窗口（TDX_SEND_WARN=0 可关闭）

集成方式：
- 由 stock_dashboard.py 在后台线程启动
- 由 stock_flow_push.py 在每次推送前刷新候选池

设计原则：
- 不阻塞主流程（独立线程）
- 飞书推送要节流（每只股票 5 分钟最多 1 条）
- 候选池支持动态调整（增删订阅）
"""

import os
import time
import json
import logging
import threading
from typing import Dict, List, Set, Optional
from datetime import datetime, timedelta, timezone, time as dtime
from queue import Queue

import feishu_push

logger = logging.getLogger("alert_monitor")

# === 飞书 webhook（如果设了）===
# 保留这个常量：外部脚本可能读它。真正的发送由 feishu_push 负责
# （走队列异步发，支持自建应用通道 + 群机器人 webhook 两条路）。
FEISHU_WEBHOOK = os.environ.get("STOCK_FLOW_FEISHU_WEBHOOK", "").strip()

# === 飞书推送的异动类型白名单 ===
# 逗号分隔；留空 = 全推。
# 默认只推"更重要"的那几类：主力大额流入/流出 + 触及涨跌停。
# price_jump（价格异动）量最大但信息密度最低 —— 实测 2026-11-26
# 它占全天异动的 33%（2064/6322），而真正值得看的涨跌停才 497 只。
# turnover_spike 同理。想收就在这里加回去，例如：
#   FEISHU_ALERT_TYPES=big_inflow,big_outflow,limit_up,limit_down,price_jump
FEISHU_ALERT_TYPES = {
    t.strip() for t in os.environ.get(
        "FEISHU_ALERT_TYPES",
        "big_inflow,big_outflow,limit_up,limit_down",
    ).split(",") if t.strip()
}

# === 通达信 TQ 信号窗口回推（tq.send_warn）===
# 默认开启；设 TDX_SEND_WARN=0 关闭。推送失败只记日志，绝不影响异动监测主流程。
TDX_SEND_WARN = os.environ.get("TDX_SEND_WARN", "1").strip() != "0"

# 实时订阅（hot）上限 — tq.subscribe_hq 单连接订阅数上限
MAX_HOT_SUBSCRIBE = 100

# 涨跌停检测：zaf 绝对值 ≥ LIMIT_TRIGGER_PCT 视为触及涨跌停，记录阈值 ±LIMIT_FULL_PCT
LIMIT_TRIGGER_PCT = 9.9
LIMIT_FULL_PCT = 10.0

# 各板块的涨跌停幅度（%）。
# 原来固定 ±9.9% —— 20cm 板（科创板/创业板）跌 9.9% 根本不是跌停，
# 却照样被判"触及跌停"。实测今天有 234 只 20cm 板进了跌停告警，
# 一半以上是假的（巡检遗留项：涨跌停不区分板块）。
LIMIT_PCT_MAIN = 10.0      # 主板（沪 60x / 深 00x / 001 / 002 / 003）
LIMIT_PCT_STAR = 20.0      # 科创板 688 / 689
LIMIT_PCT_CHINEXT = 20.0   # 创业板 300 / 301
LIMIT_PCT_BSE = 30.0       # 北交所（8 开头 / 43 / 83 / 87 / 88 / 92）
LIMIT_PCT_ST = 5.0         # ST / *ST


def limit_pct_for(code: str, name: str = "") -> float:
    """按板块返回该股的涨跌停幅度（%）。

    ST 优先 —— ST 股的涨跌幅限制跟着股票名称走（名字里带 ST 或 *ST）。
    判不出名字就按代码前缀判；都判不出就按主板 10% 走。
    """
    nm = (name or "").upper().replace(" ", "")
    if "ST" in nm:
        return LIMIT_PCT_ST
    c = (code or "").split(".")[0]
    if c.startswith(("688", "689")):
        return LIMIT_PCT_STAR
    if c.startswith(("300", "301")):
        return LIMIT_PCT_CHINEXT
    if c.startswith(("8", "43", "83", "87", "88", "92")):
        return LIMIT_PCT_BSE
    return LIMIT_PCT_MAIN


# ============================================================
# 异动类型定义
# ============================================================

class AlertType:
    """异动类型枚举"""
    BIG_INFLOW = "big_inflow"          # 主力大额流入
    BIG_OUTFLOW = "big_outflow"        # 主力大额流出
    LIMIT_UP = "limit_up"              # 涨停
    LIMIT_DOWN = "limit_down"          # 跌停
    PRICE_JUMP = "price_jump"          # 价格异动（涨幅突变）
    TURNOVER_SPIKE = "turnover_spike"  # 换手率/成交量异动


class Alert:
    """异动事件"""
    def __init__(self, code: str, name: str, alert_type: str,
                 current_zjl: float, current_zaf: float, threshold: float,
                 extra: Optional[Dict] = None):
        self.code = code
        self.name = name
        self.alert_type = alert_type
        self.current_zjl = current_zjl  # 元
        self.current_zaf = current_zaf  # %
        self.threshold = threshold
        self.timestamp = time.time()
        self.extra = extra or {}

    def to_markdown(self) -> str:
        """格式化成飞书 markdown 消息"""
        # 北京时间 (+8)
        from datetime import timezone, timedelta as td
        bj_now = datetime.now(timezone(td(hours=8))).replace(tzinfo=None)
        ts = bj_now.strftime("%H:%M:%S")

        type_emoji = _ALERT_TYPE_EMOJI.get(self.alert_type, "[?]")

        type_label = _ALERT_REASON_LABEL.get(self.alert_type, self.alert_type)

        zjl_yi = self.current_zjl / 1e8
        direction = "净流入" if self.current_zjl >= 0 else "净流出"
        sign = "+" if self.current_zjl >= 0 else ""

        return (
            f"{type_emoji} **{type_label}**  @ {ts}\n"
            f"  {self.name} ({self.code})\n"
            f"  主力{direction}: **{sign}{zjl_yi:.2f}亿**\n"
            f"  涨幅: {self.current_zaf:+.2f}%\n"
            f"  阈值: {self.threshold:.2f}\n"
        )


# ============================================================
# 交易时段判断
# ============================================================
_trade_calendar: Optional[Set[str]] = None
_trade_calendar_last_try = 0.0
_trade_calendar_lock = threading.Lock()


def _get_trade_calendar():
    """取 A 股交易日历（含法定节假日），失败返回 None。

    来源：akshare.tool_trade_date_hist_sina（8797 个交易日，覆盖到 2026-12-31）。
    每天只取一次，失败后 1 小时内不重试（免得每次调用都去打网络）。
    """
    global _trade_calendar, _trade_calendar_last_try
    with _trade_calendar_lock:
        if _trade_calendar is not None:
            return _trade_calendar
        if time.time() - _trade_calendar_last_try < 3600:
            return None
        _trade_calendar_last_try = time.time()
        try:
            import akshare as ak
            df = ak.tool_trade_date_hist_sina()
            cal = {str(d) for d in df["trade_date"].tolist()}
            _trade_calendar = cal
            logger.info("已加载 A 股交易日历: %d 个交易日", len(cal))
            return cal
        except Exception as e:
            logger.debug("取 A 股交易日历失败，降级为只判 weekday: %s", e)
            return None


def in_trading_hours(now=None) -> bool:
    """是否 A 股交易时段（北京时间，周一至周五 9:30-11:30 / 13:00-15:00）。

    与 stock_dashboard.in_trading_hours 保持一致，这里单独实现一份：
    alert_monitor 是被 stock_dashboard 导入的，反向 import 会形成循环依赖。

    本机时区不是 Asia/Shanghai，必须显式按 UTC+8 换算。

    **法定节假日也要排除**：原来只判 weekday —— 国庆/春节这些"工作日休市日"
    会被当成交易日，于是反复推送**上一交易日收盘时**的涨停股（巡检 P1-11）。
    交易日历取不到时降级为只判 weekday（宁可多扫，也不要完全不扫）。
    """
    now = now or datetime.now(timezone(timedelta(hours=8)))
    if now.weekday() > 4:
        return False
    cal = _get_trade_calendar()
    if cal is not None and now.strftime("%Y-%m-%d") not in cal:
        return False
    t = now.time()
    return (dtime(9, 30) <= t <= dtime(11, 30)) or (dtime(13, 0) <= t <= dtime(15, 0))


# ============================================================
# 节流器（防止同一只股票重复推送）
# ============================================================

class Throttler:
    """每只股票 + 异动类型 组合，5 分钟最多推 1 次。

    另外加**日级去重**：同一 code + 类型，只推一次/天。
    原来只有 5 分钟窗口 —— 一只涨停股从 9:30 到 15:00 能被推 60 多次，
    全市场扫描一轮能产生几千条推送（巡检 P1-12）。

    ⚠️ 日级去重的 key **不能带涨幅**：涨停股的 zaf 会从 10.00 漂到 10.02，
    带上 price 就每次都命中不了 —— 这个去重从上线起就没生效过，
    实测 2026-11-26 一只票（688802.SH）一天被推了 22 遍跌停。

    日级去重集合**落盘**（logs/alert_dedupe.json）：只放内存里的话，
    看板一重启就清空 → 今天已经推过的几百只票会全部重推一遍，
    等于把用户刚收过的告警再刷一次。
    """

    def __init__(self, interval_sec: float = 300, state_file: str = None):
        self.interval = interval_sec
        self._last_sent: Dict[str, float] = {}
        self._daily_sent: Set[str] = set()
        self._daily_key = ""
        self._lock = threading.Lock()
        self._state_file = state_file or os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "logs", "alert_dedupe.json")
        self._load_daily()

    # ---------- 日级去重的落盘 ----------

    def _load_daily(self) -> None:
        """读回当天已推过的组合（换天就丢弃）"""
        today = datetime.now(timezone(timedelta(hours=8))).strftime("%Y-%m-%d")
        self._daily_key = today
        try:
            with open(self._state_file, "r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception:
            return
        if not isinstance(data, dict) or data.get("day") != today:
            return
        items = data.get("sent")
        if isinstance(items, list):
            self._daily_sent = {str(x) for x in items}

    def _save_daily(self) -> None:
        """原子写（tmp + replace）：文件损坏等于去重失效 → 会被当成全部重推"""
        try:
            os.makedirs(os.path.dirname(self._state_file), exist_ok=True)
            tmp = self._state_file + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump({"day": self._daily_key, "sent": sorted(self._daily_sent)},
                          f, ensure_ascii=False)
            os.replace(tmp, self._state_file)
        except Exception:
            pass

    def should_send(self, code: str, alert_type: str, price=None) -> bool:
        # price 保留是为了兼容旧调用签名，但**不参与**去重 key（见上面说明）
        key = f"{code}:{alert_type}"
        now = time.time()
        day = datetime.now(timezone(timedelta(hours=8))).strftime("%Y-%m-%d")
        with self._lock:
            if self._daily_key != day:
                self._daily_key = day
                self._daily_sent.clear()
                self._save_daily()
            last = self._last_sent.get(key, 0)
            if now - last < self.interval:
                return False
            day_key = f"{day}|{key}"
            if day_key in self._daily_sent:
                return False
            self._daily_sent.add(day_key)
            self._last_sent[key] = now
            self._save_daily()
            return True


# ============================================================
# 异动检测器
# ============================================================

class AlertDetector:
    """检测异动的阈值管理器"""
    def __init__(
        self,
        big_inflow_yi: float = 0.5,      # 主力单笔流入超 5000 万
        big_outflow_yi: float = 0.5,     # 主力单笔流出超 5000 万
        price_jump_pct: float = 2.0,     # 涨幅突变 ±2%
        limit_up_trigger: bool = True,   # 触发涨停预警
        limit_down_trigger: bool = True,
    ):
        self.big_inflow_yi = big_inflow_yi
        self.big_outflow_yi = big_outflow_yi
        self.price_jump_pct = price_jump_pct
        self.limit_up_trigger = limit_up_trigger
        self.limit_down_trigger = limit_down_trigger

        # 每只股票上次的状态
        self._last_state: Dict[str, Dict] = {}
        self._lock = threading.Lock()

    def update_state(self, code: str, info: Dict):
        """更新某只股票的最新状态（用于下次对比）"""
        with self._lock:
            self._last_state[code] = {
                'zjl': info.get('Zjl', 0) or 0,
                'zaf': info.get('ZAF', 0) or 0,
                'now': info.get('Now', 0) or 0,
                'fzhsl': info.get('Fzhsl', 0) or 0,
                'ts': time.time(),
            }

    def detect(self, code: str, name: str, info: Dict) -> List[Alert]:
        """检测异动，返回 Alert 列表"""
        alerts = []

        zjl = _to_float(info.get('Zjl'))
        zaf = _to_float(info.get('ZAF'))
        now_price = _to_float(info.get('Now'))

        # 1. 主力大额流入/流出
        if zjl >= self.big_inflow_yi * 1e8:
            alerts.append(Alert(
                code, name, AlertType.BIG_INFLOW,
                zjl, zaf, self.big_inflow_yi,
                extra={'zjl_yi': zjl / 1e8}
            ))
        elif zjl <= -self.big_outflow_yi * 1e8:
            alerts.append(Alert(
                code, name, AlertType.BIG_OUTFLOW,
                zjl, zaf, -self.big_outflow_yi,
                extra={'zjl_yi': zjl / 1e8}
            ))

        # 2. 涨跌停（按板块判定幅度：主板 10% / 创业板科创板 20% / 北交所 30% / ST 5%）
        full_pct = limit_pct_for(code, name)
        trigger_pct = full_pct - 0.1
        if self.limit_up_trigger and zaf >= trigger_pct:
            alerts.append(Alert(code, name, AlertType.LIMIT_UP, zjl, zaf, full_pct))
        if self.limit_down_trigger and zaf <= -trigger_pct:
            alerts.append(Alert(code, name, AlertType.LIMIT_DOWN, zjl, zaf, -full_pct))

        # 3. 价格异动（和上次比）
        with self._lock:
            prev = self._last_state.get(code)
        if prev:
            try:
                prev_zaf = float(prev.get('zaf', 0) or 0)
            except (TypeError, ValueError):
                prev_zaf = 0.0
            zaf_diff = zaf - prev_zaf
            if abs(zaf_diff) >= self.price_jump_pct:
                alerts.append(Alert(
                    code, name, AlertType.PRICE_JUMP,
                    zjl, zaf, self.price_jump_pct,
                    extra={'zaf_diff': zaf_diff, 'prev_zaf': prev_zaf}
                ))

        # 更新状态
        self.update_state(code, info)

        return alerts


# ============================================================
# 飞书推送
# ============================================================
# 全部走 feishu_push 的队列：入队即返回，后台线程限速发送。
# 为什么不再直接 requests.post —— 那玩意是同步的，跑在告警扫描线程/订阅线程里，
# 外网一不通每条就阻塞 10 秒（实测峰值 193 条/60 秒 → 扫描线程最坏卡 32 分钟）。
# 详见 feishu_push 模块头与巡检报告 P1-13。

def send_feishu_text(text: str):
    """把一段文本推给飞书（非阻塞：入队即返回）"""
    if not feishu_push.is_configured():
        # 没配任何通道时输出到 stdout（agent 能看到）
        print(f"\n{text}\n", flush=True)
        return False
    return feishu_push.enqueue_text(text)


def send_feishu_alert(alert):
    """把一条异动告警推给飞书（非阻塞：入队即返回）

    走卡片格式 —— 异动量很大，纯文本在群里根本扫不出重点。
    只有 FEISHU_ALERT_TYPES 白名单里的类型才推（"只推更重要的"）。
    """
    if FEISHU_ALERT_TYPES and alert.alert_type not in FEISHU_ALERT_TYPES:
        return False
    if not feishu_push.is_configured():
        # 没配通道时退回 stdout，保留本地排查能力
        print(f"\n{alert.to_markdown()}\n", flush=True)
        return False
    label = _ALERT_REASON_LABEL.get(alert.alert_type, alert.alert_type)
    return feishu_push.notify_alert(alert, label=label)


# ============================================================
# 通达信 TQ 信号窗口回推（tq.send_warn）
# ============================================================

def _to_float(v, default: float = 0.0) -> float:
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


# bs_flag 映射：0=买(红B), 1=卖(绿S), 2=未知(黄双叠三角)
# 涨停/大单流入=0，跌停=1，其他=2
_ALERT_BS_FLAG = {
    AlertType.LIMIT_UP: 0,
    AlertType.BIG_INFLOW: 0,
    AlertType.LIMIT_DOWN: 1,
}

_ALERT_TYPE_EMOJI = {
    AlertType.BIG_INFLOW: "[IN]",
    AlertType.BIG_OUTFLOW: "[OUT]",
    AlertType.LIMIT_UP: "[UP]",
    AlertType.LIMIT_DOWN: "[DOWN]",
    AlertType.PRICE_JUMP: "[JUMP]",
    AlertType.TURNOVER_SPIKE: "[VOL]",
}

_ALERT_REASON_LABEL = {
    AlertType.BIG_INFLOW: "主力大额流入",
    AlertType.BIG_OUTFLOW: "主力大额流出",
    AlertType.LIMIT_UP: "触及涨停",
    AlertType.LIMIT_DOWN: "触及跌停",
    AlertType.PRICE_JUMP: "价格异动",
    AlertType.TURNOVER_SPIKE: "成交量异动",
}


def send_warn_to_tdx(alert: 'Alert', info: Optional[Dict] = None) -> bool:
    """把异动信号推回通达信 TQ 策略信号窗口（tq.send_warn）。

    - 受环境变量 TDX_SEND_WARN 控制（默认 1 开，0 关）
    - 走 tdx_client.send_warn_signals（内部 _call_with_timeout 8s 超时）
    - 任何失败/异常都静默处理（只记 debug 日志），绝不影响异动监测主流程

    Returns:
        True=推送成功；False=关闭/失败
    """
    if not TDX_SEND_WARN:
        return False
    try:
        import tdx_client
        from datetime import timezone, timedelta as _td
        info = info or {}
        price = _to_float(info.get("Now"))
        ts = datetime.now(timezone(_td(hours=8))).strftime("%Y%m%d%H%M%S")
        reason = _ALERT_REASON_LABEL.get(alert.alert_type, str(alert.alert_type))
        bs_flag = _ALERT_BS_FLAG.get(alert.alert_type, 2)
        ok = tdx_client.send_warn_signals(
            [alert.code], [ts], [price], [bs_flag], [price], [reason],
        )
        if not ok:
            logger.debug("send_warn 未推送: %s %s", alert.code, alert.alert_type)
        return ok
    except Exception as e:
        logger.debug("send_warn 异常（已忽略）: %s", e)
        return False


# ============================================================
# 实时监测主循环
# ============================================================

class RealtimeAlertMonitor:
    """
    实时异动监测器（独立线程运行）

    用法：
        monitor = RealtimeAlertMonitor()
        monitor.set_watchlist(['600519.SH', '000001.SZ', ...])  # 候选池
        monitor.start()  # 启动后台线程
        ...
        monitor.stop()
    """

    def __init__(self):
        from tqcenter import tq as _tq  # noqa
        self._tq = _tq

        self.detector = AlertDetector()
        self.throttler = Throttler(interval_sec=300)

        # === 双层候选池 ===
        # hot_watchlist: 100 只以内（实时订阅推送） — 秒级检测
        # full_watchlist: 完整候选池（如全 A 5317 只）— 定期轮询
        self._hot_watchlist: Set[str] = set()      # 实时订阅（<=100）
        self._full_watchlist: Set[str] = set()     # 完整候选池（任意大小）
        self._names: Dict[str, str] = {}  # code -> name
        self._subscribed: Set[str] = set()
        self._lock = threading.Lock()

        self._running = False
        self._stop_event = threading.Event()  # stop() 时唤醒扫描线程，不用等满一个间隔
        self._thread: Optional[threading.Thread] = None
        self._scan_thread: Optional[threading.Thread] = None
        self._event_queue: Queue = Queue()
        self._initialized = False

        # 轮询配置
        self.scan_interval_sec = 60     # 全市场扫描间隔（兜底层）
        self.scan_chunk_size = 200      # 每次批量拉多少只的 fund_flow
        # 从 500 降到 200：单批持 TQ 锁的时间更短（~5s vs ~12s），
        # 看板主拉数最多等 1~2 秒就能插进来（2026-11-23 01:58 修）
        # 扫描游标：每轮从上次停下的位置继续，保证全集都能轮到
        # （原来每轮都从 to_scan[0] 开始，而 to_scan 来自 set 迭代顺序，
        #  排在后面的约一半代码永远扫不到，巡检 P1-10）
        self._scan_cursor = 0

        # 统计（多线程自增，需加锁）
        self.stats = {
            'alerts_sent': 0,
            'warn_sent': 0,          # send_warn 回推成功数
            'events_received': 0,   # 订阅推送
            'scans_done': 0,         # 兜底扫描次数
            'started_at': None,
        }
        self._stats_lock = threading.Lock()

    def _bump_stat(self, key: str, n: int = 1):
        """线程安全的 stats 计数器自增（订阅回调/扫描线程/推送线程三方并发）"""
        with self._stats_lock:
            self.stats[key] = self.stats.get(key, 0) + n

    def init_tq(self, existing_tq=None) -> bool:
        """初始化 tqcenter。

        Args:
            existing_tq: 已初始化的 tq 对象（复用模式，避免重复 init）
        """
        if self._initialized:
            return True

        # 复用模式
        if existing_tq is not None:
            self._tq = existing_tq
            self._initialized = True
            logger.info("AlertMonitor: 复用已有 tq (run_id=%s)", existing_tq.run_id)
            return True

        # 自己 init
        try:
            import sys as _sys
            # 复用 tdx_client 的路径常量与初始化锁（避免和看板 init 并发篡改 sys.argv / 抢槽位）
            from tdx_client import _TDX_EXE, _TDX_DLL, _init_lock
            _sys_argv_orig = list(_sys.argv)
            try:
                with _init_lock:
                    # 用 mode=2/3 避开看板的 mode=0（避免回调表冲突）
                    for try_mode in [2, 3, 1, 0]:
                        _sys.argv = [_sys_argv_orig[0]] + ['--run_tdx', str(try_mode)]
                        try:
                            self._tq.initialize(
                                _TDX_EXE,
                                dll_path=_TDX_DLL,
                            )
                            self._initialized = True
                            logger.info("AlertMonitor: tq init OK (mode=%s, run_id=%s)",
                                        self._tq.run_mode, self._tq.run_id)
                            return True
                        except Exception as e:
                            time.sleep(0.3)
                            continue
                raise RuntimeError("所有 run_mode 都不可用")
            finally:
                _sys.argv = _sys_argv_orig
        except Exception as e:
            logger.error("AlertMonitor init 失败: %s", e)
            return False

    def set_watchlist(self, codes: List[str], names: Optional[Dict[str, str]] = None,
                   hot_codes: Optional[List[str]] = None):
        """
        设置候选池。

        Args:
            codes: 完整候选池（任意大小），由兜底线程定期扫描
            names: code -> name 映射
            hot_codes: 实时订阅的"热门"子集（最多 100 只），秒级检测
                       如果不传，默认取 codes 前 100 只
        """
        with self._lock:
            self._full_watchlist = set(codes)
            if names:
                self._names.update(names)

            # 决定 hot 列表
            if hot_codes is None:
                hot_codes = list(codes)[:MAX_HOT_SUBSCRIBE]
            else:
                hot_codes = hot_codes[:MAX_HOT_SUBSCRIBE]
            self._hot_watchlist = set(hot_codes)

        logger.info("设置候选池: 全集 %d 只, 实时订阅 %d 只",
                    len(self._full_watchlist), len(self._hot_watchlist))
        # 通知订阅线程更新
        self._event_queue.put(('resubscribe', None))

    def add_to_watchlist(self, codes: List[str], names: Optional[Dict[str, str]] = None):
        """动态添加股票到完整候选池（不会自动加入 hot 实时订阅）"""
        with self._lock:
            self._full_watchlist.update(codes)
            if names:
                self._names.update(names)
        logger.info("新增到全集: %d 只, 总数 %d",
                    len(codes), len(self._full_watchlist))

    def _subscribe(self, codes: List[str]):
        """实际调用 tq.subscribe_hq（最多 100 只）"""
        # 先退订当前订阅（只维护自有 _subscribed 副本并加锁，不读 tqcenter 私有状态）
        with self._lock:
            cur = list(self._subscribed)
            self._subscribed.clear()
        if cur:
            try:
                self._tq.unsubscribe_hq(stock_list=cur)
            except Exception as e:
                logger.debug("清空旧订阅失败: %s", e)

        if not codes:
            return
        if len(codes) > MAX_HOT_SUBSCRIBE:
            logger.warning("订阅数 %d 超过 100，截断", len(codes))
            codes = codes[:MAX_HOT_SUBSCRIBE]

        def on_data(datas):
            try:
                obj = json.loads(datas)
                code = obj.get('Code')
                # ErrorId 可能是 str '0' 或 int 0（取决于 tqcenter 版本），统一转 str 比较
                if code and str(obj.get('ErrorId')) == '0':
                    self._event_queue.put(('update', code))
                    self._bump_stat('events_received')
            except Exception:
                pass

        try:
            res = self._tq.subscribe_hq(stock_list=codes, callback=on_data)
            # tqcenter 订阅失败时只 print 然后 return None（不抛异常）——
            # 原来无条件打印"订阅成功"并写入 _subscribed，状态与实际不符，
            # 排障时极具误导（巡检 P2-13）。
            if not res:
                # 连接刚重建时 DLL 还没就绪，第一次订阅常失败 —— 等一下重试一次
                logger.warning("订阅返回空结果，2s 后重试（%d 只）", len(codes))
                time.sleep(2)
                res = self._tq.subscribe_hq(stock_list=codes, callback=on_data)
            if not res:
                logger.error("订阅失败：tqcenter 返回空结果（%d 只未订阅）", len(codes))
                return
            with self._lock:
                self._subscribed.update(codes)
            logger.info("订阅成功: %d 只", len(codes))
        except Exception as e:
            logger.warning("订阅异常，2s 后重试: %s", e)
            time.sleep(2)
            try:
                res = self._tq.subscribe_hq(stock_list=codes, callback=on_data)
                if res:
                    with self._lock:
                        self._subscribed.update(codes)
                    logger.info("重试后订阅成功: %d 只", len(codes))
                else:
                    logger.error("重试后订阅仍失败（%d 只未订阅）", len(codes))
            except Exception as e2:
                logger.error("重试后订阅异常: %s", e2)

    def start(self):
        """启动后台监测线程（订阅线程 + 兜底扫描线程）"""
        if self._running:
            return
        if not self.init_tq():
            logger.error("AlertMonitor 启动失败：tq init 失败")
            return
        self._running = True
        # stop() 置位过 _stop_event 后必须 clear —— 否则 stop→start 之后
        # _stop_event.wait(...) 立即返回，扫描线程变成无节流死循环
        # （每秒一轮全市场 5000 次调用，巡检 P2-12）
        self._stop_event.clear()
        self.stats['started_at'] = time.time()

        # 线程 1: 订阅推送处理（秒级）
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

        # 线程 2: 兜底扫描（每 scan_interval_sec 秒扫一次全集的非 hot 部分）
        self._scan_thread = threading.Thread(target=self._scan_loop, daemon=True)
        self._scan_thread.start()

        logger.info("AlertMonitor 启动（订阅线程 + 兜底扫描线程）")

    def stop(self):
        self._running = False
        self._stop_event.set()  # 唤醒扫描线程，不用等满一个间隔
        if self._thread:
            self._thread.join(timeout=5)
        if self._scan_thread:
            self._scan_thread.join(timeout=5)
        # 清理订阅
        with self._lock:
            subscribed = list(self._subscribed)
            self._subscribed.clear()
        if subscribed:
            try:
                self._tq.unsubscribe_hq(stock_list=subscribed)
            except Exception:
                pass
        logger.info("AlertMonitor 停止")

    def _run(self):
        """订阅推送处理线程"""
        import tdx_client
        # 初始订阅 hot_watchlist
        with self._lock:
            initial_codes = list(self._hot_watchlist)
        self._subscribe(initial_codes)
        last_generation = tdx_client.get_connection_generation()

        while self._running:
            try:
                try:
                    event_type, payload = self._event_queue.get(timeout=1)
                except Exception:
                    event_type = None

                if event_type == 'update':
                    code = payload
                    self._handle_update(code)
                elif event_type == 'resubscribe':
                    with self._lock:
                        codes = list(self._hot_watchlist)
                    self._subscribe(codes)

                # TQ 连接被重建后，DLL 侧的实时订阅会随之消失 —— 而 tq.run_id
                # 重连前后不变（实测都是 1），只能靠 tdx_client 的连接世代号判断。
                # 不重订阅的后果：/api/alerts 一直显示 subscribed_size=100，
                # 但 events_received 永远 0，实时异动静默失效（巡检 P0-8）。
                gen = tdx_client.get_connection_generation()
                if gen != last_generation:
                    last_generation = gen
                    logger.warning("TQ 连接已重建（generation=%s），重新订阅实时行情", gen)
                    # 连接刚重建时 DLL 还没完全就绪，立刻订阅会抛异常
                    time.sleep(3)
                    with self._lock:
                        codes = list(self._hot_watchlist)
                    self._subscribe(codes)

            except Exception as e:
                logger.exception("订阅线程异常: %s", e)
                time.sleep(1)

    def _scan_loop(self):
        """兜底扫描线程：每 scan_interval_sec 秒扫一次全集"""
        logger.info("兜底扫描线程启动 (间隔 %ds)", self.scan_interval_sec)
        while self._running:
            try:
                if not in_trading_hours():
                    # 非交易时段 TQ 数据不再变化，扫了也只会把收盘时的同一批
                    # 涨跌停反复推一遍（实测周末刷出近 900 条重复异动），
                    # 还会白耗每轮约 5000 次 TQ 调用。开盘后自动恢复。
                    self._stop_event.wait(self.scan_interval_sec)
                    continue
                self._scan_full_market_once()
                self._bump_stat('scans_done')
            except Exception as e:
                logger.exception("兜底扫描异常: %s", e)
            # Event.wait 可中断：stop() 时立即退出，不用等满一个间隔
            self._stop_event.wait(self.scan_interval_sec)

    def _scan_full_market_once(self):
        """扫一次全集（hot 之外的股票）

        两个关键修正（2026-11-22 巡检 P1-9 / P1-10）：
        ① 原来调 `tq.get_more_info_batch(...)` —— tqcenter 里**没有这个方法**，
           每轮都 AttributeError 再回退成逐只**串行**调用（一批 500 只）。
           现在走 tdx_client.batch_get_more_info（并发 + 超时封装）。
        ② 原来每轮都从 to_scan[0] 开始 —— to_scan 来自 set 迭代顺序（进程内固定），
           排在后面的约一半代码**永远扫不到**。现在维护游标，每轮接着上轮扫。
        """
        import tdx_client
        with self._lock:
            full = list(self._full_watchlist)
            hot = set(self._hot_watchlist)

        # 只扫全集 - hot 之外的股票（hot 已经被实时订阅覆盖）
        to_scan = [c for c in full if c not in hot]
        if not to_scan:
            return

        # TQ 优先权：**按批持锁、批间让行**。
        # 整轮持锁是不行的 —— 一轮 5000 只要几分钟，看板主拉数 30s 等不到就被饿死，
        # 健康缓存变 fail → keepalive 连续 3 次误判 → 重启看板
        # （2026-11-23 01:00 交易时段实测）。
        # 改成每批（500 只）持一次锁，批与批之间释放 —— 看板最多等 1~2 秒就能插进来。
        self._scan_full_market_once_batched(to_scan)

    def _scan_full_market_once_batched(self, to_scan: List[str]):
        """按批扫描，每批独立持 TQ 锁，批间让行给看板"""
        import tdx_client
        # 游标轮转：本轮从上次停下的位置继续
        cursor = int(self._scan_cursor or 0)
        if cursor >= len(to_scan):
            cursor = 0
        ordered = to_scan[cursor:] + to_scan[:cursor]

        # 分批拉（每批 chunk_size 只），每轮设总时限避免失控
        round_deadline = time.time() + max(self.scan_interval_sec, 60)
        scanned = 0
        for i in range(0, len(ordered), self.scan_chunk_size):
            if not self._running or time.time() > round_deadline:
                logger.info("本轮扫描终止（stop 或超过每轮时限）: %d/%d", i, len(ordered))
                break
            chunk = ordered[i:i + self.scan_chunk_size]
            # 每批独立持锁：拿不到就等一小会儿再试（看板正在拉数就让路），
            # 批与批之间释放锁 —— 看板最多等 1~2 秒就能插进来
            got = tdx_client.acquire_tq(priority=False)
            if not got:
                for _ in range(12):        # 最多等 ~12s
                    if not self._running:
                        return
                    time.sleep(1)
                    if tdx_client.acquire_tq(priority=False):
                        got = True
                        break
            if not got:
                logger.info("TQ 一直被看板占用，本轮扫描到此为止: %d/%d", i, len(ordered))
                break
            try:
                try:
                    flows = tdx_client.batch_get_more_info(
                        chunk, field_list=['Zjl', 'ZAF', 'Now', 'Fzhsl'],
                        max_workers=4, timeout=45,
                    )
                except Exception as e:
                    logger.warning("扫描 batch 失败: %s", e)
                    flows = []
            finally:
                tdx_client.release_tq()
                time.sleep(0.3)   # 批间让行，让看板的拉数有机会插进来
            by_code = {f.get("code"): f for f in flows if f and f.get("code")}
            scanned += len(chunk)

            # 检测异动
            for code in chunk:
                info = by_code.get(code)
                if not info:
                    continue
                name = self._names.get(code, code)
                alerts = self.detector.detect(code, name, info)
                for alert in alerts:
                    if self.throttler.should_send(code, alert.alert_type,
                                                  price=alert.current_zaf):
                        logger.info("异动(扫描): %s %s", code, alert.alert_type)
                        send_feishu_alert(alert)
                        if send_warn_to_tdx(alert, info):
                            self._bump_stat('warn_sent')
                        self._bump_stat('alerts_sent')
            logger.debug("扫描进度: %d/%d", min(i + self.scan_chunk_size, len(ordered)), len(ordered))
        # 记住本轮停在哪，下轮从那儿继续（保证全集轮转覆盖）
        self._scan_cursor = (cursor + scanned) % max(1, len(ordered))

    def _handle_update(self, code: str):
        """处理单只股票的更新通知"""
        if not in_trading_hours():
            # 非交易时段 TQ 可能还在推收盘前的快照，推出去就是重复的假异动
            return
        try:
            # 走 tdx_client 超时封装：直调 self._tq 在断连时会永久卡死本线程
            import tdx_client
            info = tdx_client._call_with_timeout(
                self._tq.get_more_info, code, default=None, label=f"alert_update({code})"
            )
            if not info:
                return
            name = self._names.get(code, code)

            alerts = self.detector.detect(code, name, info)

            for alert in alerts:
                if self.throttler.should_send(code, alert.alert_type,
                                              price=alert.current_zaf):
                    logger.info("异动: %s %s", code, alert.alert_type)
                    send_feishu_alert(alert)
                    if send_warn_to_tdx(alert, info):
                        self._bump_stat('warn_sent')
                    self._bump_stat('alerts_sent')
        except Exception as e:
            logger.debug("_handle_update(%s) 失败: %s", code, e)


# ============================================================
# 便捷函数：默认候选池
# ============================================================

def default_watchlist(monitor_instance: 'RealtimeAlertMonitor'):
    """
    设置默认候选池：
      - 完整候选池：所有非 ST A 股（~5300 只）由兜底线程定期扫描
      - 实时订阅：前 100 只（按代码排序）做秒级推送

    生产场景下：
      - 开盘前应先调 tdx_client.get_a_share_list() 拿全量列表
      - 然后用 stock_dashboard 的 fetch_once 算出当日 TOP 100，更新 hot
    """
    import tdx_client

    def _collect(sl):
        """过滤 ST/退 + 校验 .SH/.SZ 后缀，返回 (codes, names)。"""
        codes = []
        names = {}
        for s in sl:
            code = s.get('Code', '')
            name = (s.get('Name', '') or '').strip()
            if 'ST' in name.upper() or '退' in name:
                continue
            if not code.endswith(('.SH', '.SZ')):
                continue
            codes.append(code)
            names[code] = name
        return codes, names

    try:
        # 直接用已初始化的 tqcenter（不调 init，避免占槽位）
        from tqcenter import tq as _tq
        sl = _tq.get_stock_list(market='5', list_type=1) or []
        normal_codes, names = _collect(sl)
        logger.info("非 ST A 股候选池: %d 只 (via _tq.get_stock_list)", len(normal_codes))
        if normal_codes:
            hot = sorted(normal_codes)[:MAX_HOT_SUBSCRIBE]
            monitor_instance.set_watchlist(normal_codes, names=names, hot_codes=hot)
            return
    except Exception as e:
        logger.warning("通过 _tq 拿全 A 股列表失败: %s，尝试 tdx_client", e)

    try:
        tdx_client.init()
        sl = tdx_client.get_a_share_list()
        normal_codes, names = _collect(sl)
        logger.info("非 ST A 股候选池: %d 只 (via tdx_client)", len(normal_codes))
        hot = sorted(normal_codes)[:MAX_HOT_SUBSCRIBE]
        monitor_instance.set_watchlist(normal_codes, names=names, hot_codes=hot)
    except Exception as e:
        logger.exception("default_watchlist 失败: %s", e)
        # 回退：用固定热门股
        hot_defaults = [
            '600519.SH', '000001.SZ', '600036.SH', '601318.SH', '000858.SZ',
            '601398.SH', '601988.SH', '600028.SH', '600030.SH', '600276.SH',
            '601012.SH', '600887.SH', '601888.SH', '002594.SZ', '300750.SZ',
            '002475.SZ', '002714.SZ', '600900.SH', '601166.SH', '600050.SH',
            '300015.SZ', '000333.SZ', '000568.SZ', '002230.SZ', '600196.SH',
            '603259.SH', '600436.SH', '601633.SH', '601995.SH', '600585.SH',
        ]
        monitor_instance.set_watchlist(hot_defaults, hot_codes=hot_defaults)


# ============================================================
# 自检
# ============================================================

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    monitor = RealtimeAlertMonitor()
    default_watchlist(monitor)
    monitor.start()

    print("=" * 60)
    print("AlertMonitor 自检（运行 30 秒）")
    print("=" * 60)

    try:
        for i in range(30):
            time.sleep(1)
            if i % 5 == 0:
                print(f"[{i}s] 推送数={monitor.stats['alerts_sent']}, "
                      f"接收事件={monitor.stats['events_received']}, "
                      f"订阅数={len(monitor._subscribed)}")
    except KeyboardInterrupt:
        pass

    monitor.stop()
    print("\n[OK] 自检完成")