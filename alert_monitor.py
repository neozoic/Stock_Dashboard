"""
实时异动监测模块（通达信 subscribe_hq 推送式订阅）

机制：
- 候选池股票（最多 100 只）订阅通达信实时推送
- 回调触发后主动 get_more_info 拿资金流
- 检测异动（主力大额流入、涨跌停、涨幅突变）
- 通过飞书 webhook 推送（如果配了）或 stdout 输出
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

logger = logging.getLogger("alert_monitor")

# === 飞书 webhook（如果设了）===
FEISHU_WEBHOOK = os.environ.get("STOCK_FLOW_FEISHU_WEBHOOK", "").strip()
if not FEISHU_WEBHOOK:
    logger.warning(
        "飞书 Webhook 未配置（环境变量 STOCK_FLOW_FEISHU_WEBHOOK 为空）。"
        "异动推送将只输出到 stdout，不会发送到飞书。"
    )

# === 通达信 TQ 信号窗口回推（tq.send_warn）===
# 默认开启；设 TDX_SEND_WARN=0 关闭。推送失败只记日志，绝不影响异动监测主流程。
TDX_SEND_WARN = os.environ.get("TDX_SEND_WARN", "1").strip() != "0"

# 实时订阅（hot）上限 — tq.subscribe_hq 单连接订阅数上限
MAX_HOT_SUBSCRIBE = 100

# 涨跌停检测：zaf 绝对值 ≥ LIMIT_TRIGGER_PCT 视为触及涨跌停，记录阈值 ±LIMIT_FULL_PCT
LIMIT_TRIGGER_PCT = 9.9
LIMIT_FULL_PCT = 10.0


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
def in_trading_hours(now=None) -> bool:
    """是否 A 股交易时段（北京时间，周一至周五 9:30-11:30 / 13:00-15:00）。

    与 stock_dashboard.in_trading_hours 保持一致，这里单独实现一份：
    alert_monitor 是被 stock_dashboard 导入的，反向 import 会形成循环依赖。

    本机时区不是 Asia/Shanghai，必须显式按 UTC+8 换算。
    """
    now = now or datetime.now(timezone(timedelta(hours=8)))
    if now.weekday() > 4:
        return False
    t = now.time()
    return (dtime(9, 30) <= t <= dtime(11, 30)) or (dtime(13, 0) <= t <= dtime(15, 0))


# ============================================================
# 节流器（防止同一只股票重复推送）
# ============================================================

class Throttler:
    """每只股票 + 异动类型 组合，5 分钟最多推 1 次"""
    def __init__(self, interval_sec: float = 300):
        self.interval = interval_sec
        self._last_sent: Dict[str, float] = {}
        self._lock = threading.Lock()

    def should_send(self, code: str, alert_type: str) -> bool:
        key = f"{code}:{alert_type}"
        now = time.time()
        with self._lock:
            last = self._last_sent.get(key, 0)
            if now - last < self.interval:
                return False
            self._last_sent[key] = now
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

        # 2. 涨跌停（zaf 接近 ±10%）
        if self.limit_up_trigger and zaf >= LIMIT_TRIGGER_PCT:
            alerts.append(Alert(code, name, AlertType.LIMIT_UP, zjl, zaf, LIMIT_FULL_PCT))
        if self.limit_down_trigger and zaf <= -LIMIT_TRIGGER_PCT:
            alerts.append(Alert(code, name, AlertType.LIMIT_DOWN, zjl, zaf, -LIMIT_FULL_PCT))

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

def send_feishu_text(text: str):
    """发飞书 webhook 消息（如果配了）"""
    if not FEISHU_WEBHOOK:
        # 没配 webhook 时输出到 stdout（agent 能看到）
        print(f"\n{text}\n", flush=True)
        return
    try:
        import requests
        r = requests.post(
            FEISHU_WEBHOOK,
            json={"msg_type": "text", "content": {"text": text}},
            timeout=10,
        )
        if r.status_code != 200:
            logger.error("飞书通知失败: HTTP %s", r.status_code)
    except Exception as e:
        logger.error("飞书通知异常: %s", e)


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
        self.scan_chunk_size = 500      # 每次批量拉多少只的 fund_flow

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
            self._tq.subscribe_hq(stock_list=codes, callback=on_data)
            with self._lock:
                self._subscribed.update(codes)
            logger.info("订阅成功: %d 只", len(codes))
        except Exception as e:
            logger.error("订阅失败: %s", e)

    def start(self):
        """启动后台监测线程（订阅线程 + 兜底扫描线程）"""
        if self._running:
            return
        if not self.init_tq():
            logger.error("AlertMonitor 启动失败：tq init 失败")
            return
        self._running = True
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
        # 初始订阅 hot_watchlist
        with self._lock:
            initial_codes = list(self._hot_watchlist)
        self._subscribe(initial_codes)

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
        """扫一次全集（hot 之外的股票）"""
        with self._lock:
            full = list(self._full_watchlist)
            hot = set(self._hot_watchlist)

        # 只扫全集 - hot 之外的股票（hot 已经被实时订阅覆盖）
        to_scan = [c for c in full if c not in hot]
        if not to_scan:
            return

        # 分批拉 fund_flow（每批 chunk_size 只），每轮设总时限避免失控
        round_deadline = time.time() + max(self.scan_interval_sec, 60)
        for i in range(0, len(to_scan), self.scan_chunk_size):
            if not self._running or time.time() > round_deadline:
                logger.info("本轮扫描终止（stop 或超过每轮时限）: %d/%d", i, len(to_scan))
                break
            chunk = to_scan[i:i + self.scan_chunk_size]
            try:
                flows = self._tq.get_more_info_batch(
                    stock_list=chunk,
                    field_list=['Zjl', 'ZAF', 'Now', 'Fzhsl'],
                )
            except AttributeError:
                # tqcenter 没有批量接口，回退到逐个调（走 tdx_client 超时封装，防止断连卡死）
                import tdx_client
                flows = [
                    tdx_client._call_with_timeout(
                        self._tq.get_more_info, c, default={}, label=f"scan({c})"
                    ) for c in chunk
                ]
            except Exception as e:
                logger.debug("扫描 batch 失败: %s, 回退逐个", e)
                import tdx_client
                flows = [
                    tdx_client._call_with_timeout(
                        self._tq.get_more_info, c, default={}, label=f"scan({c})"
                    ) for c in chunk
                ]

            # 检测异动
            for code, info in zip(chunk, flows):
                if not info:
                    continue
                name = self._names.get(code, code)
                alerts = self.detector.detect(code, name, info)
                for alert in alerts:
                    if self.throttler.should_send(code, alert.alert_type):
                        msg = alert.to_markdown()
                        logger.info("异动(扫描): %s %s", code, alert.alert_type)
                        send_feishu_text(msg)
                        if send_warn_to_tdx(alert, info):
                            self._bump_stat('warn_sent')
                        self._bump_stat('alerts_sent')
            logger.debug("扫描进度: %d/%d", min(i + self.scan_chunk_size, len(to_scan)), len(to_scan))

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
                if self.throttler.should_send(code, alert.alert_type):
                    msg = alert.to_markdown()
                    logger.info("异动: %s %s", code, alert.alert_type)
                    send_feishu_text(msg)
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