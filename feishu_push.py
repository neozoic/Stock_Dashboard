"""
飞书推送模块
============

把看板的异动告警与系统事件推到飞书群。

**为什么要这个模块**：以前的 `alert_monitor.send_feishu_text()` 是同步
`requests.post(..., timeout=10)`，而且跑在告警扫描线程 / 订阅线程里。
本机外网对 open.feishu.cn 是**间歇性不通**的（DNS 解析失败、连接被拒，
11-22 / 11-23 / 11-24 都有记录），一旦不通：
  - 每条告警阻塞 10 秒
  - 实测峰值 193 条/60 秒 → 扫描线程最坏卡 32 分钟
  - 订阅线程卡住 = 实时推送全线停摆
（这是巡检报告 AUDIT_2026-11-22.md §P1-13）

现在改成：**入队即返回 + 后台线程限速发送 + 失败重试退避**，
推送再慢也拖不垮监测线程。告警可以丢，看板不能卡。

两条通道
--------
- **自建应用**（主用）：`im/v1/messages` + tenant_access_token，
  支持 interactive 卡片。配置 FEISHU_APP_ID / FEISHU_APP_SECRET / FEISHU_CHAT_ID。
- **群机器人 webhook**（备选）：自定义机器人 URL，配置
  STOCK_FLOW_FEISHU_WEBHOOK。沿用旧变量名，老配置不用改。

FEISHU_MODE 取值：
  auto（默认，优先 app，缺 app 凭据则退 webhook）/ app / webhook / both / off

设计原则
--------
- **导入无副作用**：不起线程、不发网络请求、不读文件。
  后台 worker 与日志文件都等第一次真正发送时才创建。
  （tools/smoke_test.py 硬约束：绝不能 import 有副作用的模块）
- **永不抛异常到调用方**：推送失败只记日志，告警主流程不受影响。
- **密钥不外泄**：日志里只出现通道名与脱敏后的 URL，token/secret 从不进字符串。

对外 API
--------
    is_configured()                  有可用通道吗（不触网）
    channel_name()                  "app" / "webhook" / "both" / "none"
    build_alert_card(...)           异动卡片（纯函数）
    build_system_card(...)          系统通知卡片（纯函数）
    enqueue_card(card) / enqueue_text(text)   非阻塞入队
    notify_alert(alert)             异动告警便捷封装
    notify_system(title, lines, level)        系统通知便捷封装
    status()                        运行状态（给 /api 用）
    shutdown(timeout)               退出前把队列发完
"""

import os
import json
import time
import threading
import logging
import logging.handlers
from queue import Queue, Empty
from datetime import datetime, timezone, timedelta

__all__ = [
    "is_configured", "channel_name", "build_alert_card", "build_system_card",
    "enqueue_card", "enqueue_text", "notify_alert", "notify_system",
    "status", "shutdown",
]

# 本机时区不是 Asia/Shanghai —— A 股相关的时间戳一律用北京时间
BJ_TZ = timezone(timedelta(hours=8))

logger = logging.getLogger("feishu_push")
# 自己的日志写 logs/feishu.log，不污染 dashboard.log（handler 惰性挂载）
logger.propagate = False

LOG_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs")

FEISHU_TOKEN_URL = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
FEISHU_MSG_URL = "https://open.feishu.cn/open-apis/im/v1/messages"

# 卡片 header 配色。飞书卡片只有具名色，red/green 是看板配色
# #F2435E(红涨) / #28BC86(绿跌) 最接近的具名。
_TEMPLATE_BY_LEVEL = {
    "ok": "green",
    "info": "grey",
    "warn": "orange",
    "error": "red",
}

# 异动类型的卡片图标（中文文案统一由 alert_monitor 的 _ALERT_REASON_LABEL 提供）
_EMOJI_BY_ALERT_TYPE = {
    "big_inflow": "📈",
    "big_outflow": "📉",
    "limit_up": "🚀",
    "limit_down": "🛑",
    "price_jump": "⚡",
    "turnover_spike": "💥",
}

# token 失效错误码 → 重新取 token 再发一次
_TOKEN_INVALID_CODES = (99991663, 99991661)
# 频率超限
_RATE_LIMIT_CODES = (230020,)
# 权限 / 配置类错误（重试无意义）
_FATAL_CODES = (230013, 230027, 232009)

_RETRY_BACKOFF = (1.0, 3.0, 9.0)   # 指数退避（秒）
_MAX_RETRY = len(_RETRY_BACKOFF)
_HTTP_TIMEOUT = 10

_SENTINEL = object()


# ============================================================
# 配置（惰性读取）
# ============================================================

class _Config:
    """一次性读进来的配置快照"""

    def __init__(self):
        self.enabled = os.environ.get("FEISHU_ENABLED", "1").strip() != "0"
        self.mode = (os.environ.get("FEISHU_MODE", "auto").strip() or "auto").lower()
        self.app_id = os.environ.get("FEISHU_APP_ID", "").strip()
        self.app_secret = os.environ.get("FEISHU_APP_SECRET", "").strip()
        self.chat_id = os.environ.get("FEISHU_CHAT_ID", "").strip()
        self.webhook = os.environ.get("STOCK_FLOW_FEISHU_WEBHOOK", "").strip()
        try:
            self.rate_per_sec = float(os.environ.get("FEISHU_RATE_PER_SEC", "1.5"))
        except ValueError:
            self.rate_per_sec = 1.5
        if self.rate_per_sec < 0:
            self.rate_per_sec = 0.0
        try:
            self.queue_max = int(os.environ.get("FEISHU_QUEUE_MAX", "1000"))
        except ValueError:
            self.queue_max = 1000
        if self.queue_max < 1:
            self.queue_max = 1000
        self.channels = self._resolve_channels()

    def _resolve_channels(self):
        """按 mode 与实际凭据算出要用的通道列表"""
        has_app = bool(self.app_id and self.app_secret and self.chat_id)
        has_hook = bool(self.webhook)
        mode = self.mode
        if mode == "off" or not self.enabled:
            return []
        if mode == "app":
            return ["app"] if has_app else []
        if mode == "webhook":
            return ["webhook"] if has_hook else []
        if mode == "both":
            return (["app"] if has_app else []) + (["webhook"] if has_hook else [])
        # auto：优先 app（卡片 + 复用已有凭据），没有则退 webhook
        if has_app:
            return ["app"]
        if has_hook:
            return ["webhook"]
        return []


_CONFIG = None
_CONFIG_LOCK = threading.Lock()


def _config():
    global _CONFIG
    with _CONFIG_LOCK:
        if _CONFIG is None:
            _CONFIG = _Config()
        return _CONFIG


def is_configured() -> bool:
    """有没有可用通道（只读配置，不触网、不起线程）"""
    return bool(_config().channels)


def channel_name() -> str:
    """当前使用的通道：app / webhook / both / none"""
    chans = _config().channels
    if not chans:
        return "none"
    if len(chans) > 1:
        return "both"
    return chans[0]


def _mask_url(url: str) -> str:
    """脱敏：只留头尾，中间用 *** 遮住（webhook URL 本身就是密钥）"""
    if not url:
        return ""
    if len(url) <= 24:
        return url[:8] + "***"
    return url[:20] + "***" + url[-6:]


# ============================================================
# 日志（惰性挂载，保证导入无副作用）
# ============================================================

_log_handler = None
_log_lock = threading.Lock()


def _ensure_file_log():
    """第一次真正发送时才建日志文件（导入阶段不碰磁盘）"""
    global _log_handler
    with _log_lock:
        if _log_handler is not None:
            return
        handler = None
        try:
            os.makedirs(LOG_DIR, exist_ok=True)
            handler = logging.handlers.RotatingFileHandler(
                os.path.join(LOG_DIR, "feishu.log"),
                maxBytes=2 * 1024 * 1024,
                backupCount=2,
                encoding="utf-8",
            )
            handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
        except Exception:
            # 落盘失败也要留痕 —— 退回 stderr（pythonw 下看不到，但至少不崩）
            handler = logging.StreamHandler()
            handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
        logger.setLevel(logging.INFO)
        logger.addHandler(handler)
        _log_handler = handler


# ============================================================
# requests 惰性导入（仓库没有 requirements.txt，不能假设它装了）
# ============================================================

_requests_mod = None


def _requests():
    global _requests_mod
    if _requests_mod is None:
        import requests
        _requests_mod = requests
    return _requests_mod


# ============================================================
# token 缓存
# ============================================================

class _TokenCache:
    """tenant_access_token 缓存（提前 300s 过期，留出刷新余量）"""

    def __init__(self):
        self._token = None
        self._expire_at = 0.0
        self._lock = threading.Lock()

    def invalidate(self):
        with self._lock:
            self._token = None
            self._expire_at = 0.0

    def get(self, force_refresh: bool = False):
        cfg = _config()
        if not (cfg.app_id and cfg.app_secret):
            return None
        with self._lock:
            if not force_refresh and self._token and time.time() < self._expire_at:
                return self._token
            try:
                r = _requests().post(
                    FEISHU_TOKEN_URL,
                    json={"app_id": cfg.app_id, "app_secret": cfg.app_secret},
                    timeout=_HTTP_TIMEOUT,
                )
                data = r.json()
            except Exception as e:
                # 取不到新 token 时，若手上还有一个没过期的旧 token 就继续用 ——
                # 免得网络抖一下就全哑火
                logger.warning("取飞书 token 失败: %s", type(e).__name__)
                if self._token and time.time() < self._expire_at:
                    return self._token
                return None
            if data.get("code") != 0:
                logger.error("取飞书 token 失败: code=%s msg=%s",
                             data.get("code"), data.get("msg"))
                if self._token and time.time() < self._expire_at:
                    return self._token
                return None
            self._token = data.get("tenant_access_token") or None
            try:
                expire = int(data.get("expire", 7200))
            except (TypeError, ValueError):
                expire = 7200
            self._expire_at = time.time() + max(60, expire - 300)
            return self._token


# ============================================================
# 卡片构造（纯函数 —— 不触网、不入队，便于冒烟测试断言）
# ============================================================

def build_alert_card(code, name, alert_type, zjl_yuan, zaf_pct, threshold,
                     emoji="", label="", ts=None):
    """构造一张异动告警卡片

    参数都是普通值（不是 Alert 对象），这样冒烟测试能用字面量调用。
      code       股票代码
      name       股票名称
      alert_type 异动类型（见 alert_monitor.AlertType）
      zjl_yuan   主力净流入（元，正=流入 负=流出）
      zaf_pct    当前涨幅（%）
      threshold  触发阈值（%）
      emoji      图标；不传则按 alert_type 取默认
      label      中文标题；不传则用 alert_type
      ts         时间戳文本；默认取北京时间 HH:MM:SS
    """
    if ts is None:
        ts = datetime.now(BJ_TZ).strftime("%H:%M:%S")
    icon = emoji or _EMOJI_BY_ALERT_TYPE.get(alert_type, "📌")
    title = f"{icon} {label or alert_type}".strip()

    try:
        zjl = float(zjl_yuan or 0)
    except (TypeError, ValueError):
        zjl = 0.0
    try:
        zaf = float(zaf_pct or 0)
    except (TypeError, ValueError):
        zaf = 0.0
    try:
        thr = float(threshold or 0)
    except (TypeError, ValueError):
        thr = 0.0

    if alert_type == "limit_up":
        template = "red"
    elif alert_type == "limit_down":
        template = "green"
    elif alert_type == "big_inflow":
        template = "red"
    elif alert_type == "big_outflow":
        template = "green"
    elif alert_type == "turnover_spike":
        template = "orange"
    else:
        template = "red" if zaf >= 0 else "green"

    zjl_yi = zjl / 1e8
    sign = "+" if zjl_yi >= 0 else ""
    flow_label = "主力净流入" if zjl_yi >= 0 else "主力净流出"

    fields = [
        {"is_short": True, "text": {"tag": "lark_md",
                                     "content": f"**{flow_label}**\n{sign}{zjl_yi:.2f}亿"}},
        {"is_short": True, "text": {"tag": "lark_md",
                                     "content": f"**涨幅**\n{zaf:+.2f}%"}},
        {"is_short": True, "text": {"tag": "lark_md",
                                     "content": f"**触发阈值**\n{thr:.2f}%"}},
        {"is_short": True, "text": {"tag": "lark_md",
                                     "content": f"**时间**\n{ts}"}},
    ]
    return {
        "msg_type": "interactive",
        "card": {
            "config": {"wide_screen_mode": True},
            "header": {
                "title": {"tag": "plain_text", "content": title},
                "template": template,
            },
            "elements": [
                {"tag": "div", "text": {"tag": "lark_md",
                                        "content": f"**{name} ({code})**"}},
                {"tag": "div", "fields": fields},
            ],
        },
    }


def build_system_card(title, lines, level="info"):
    """构造一张系统通知卡片（看板/看门狗事件用）

    level: ok / info / warn / error → 决定 header 配色
    """
    template = _TEMPLATE_BY_LEVEL.get(level, "grey")
    elements = []
    for line in (lines or []):
        if line is None:
            continue
        elements.append({"tag": "div", "text": {"tag": "lark_md", "content": str(line)}})
    if not elements:
        elements.append({"tag": "div", "text": {"tag": "lark_md", "content": "-"}})
    return {
        "msg_type": "interactive",
        "card": {
            "config": {"wide_screen_mode": True},
            "header": {
                "title": {"tag": "plain_text", "content": str(title)},
                "template": template,
            },
            "elements": elements,
        },
    }


# ============================================================
# 发送器
# ============================================================

class _Pusher:
    """队列 + 后台 worker（限速 + 重试）"""

    def __init__(self, cfg):
        self.cfg = cfg
        self.queue = Queue(maxsize=cfg.queue_max)
        self._thread = None
        self._stop = threading.Event()
        self._stopped = False
        self._token = _TokenCache()
        self._stats = {"sent": 0, "failed": 0, "dropped": 0}
        self._dropped_since_log = 0
        self._last_error = None
        self._lock = threading.Lock()

    # ---------- 生命周期 ----------

    def is_stopped(self):
        return self._stopped

    def start(self):
        with self._lock:
            if self._thread is not None and not self._stopped:
                return
            self._stopped = False
            self._thread = threading.Thread(target=self._run, name="feishu_push",
                                            daemon=True)
            self._thread.start()

    def stop(self, timeout=5.0):
        """停 worker：先把队列里剩下的发完（最多 timeout 秒）"""
        self._stopped = True
        try:
            self.queue.put_nowait(_SENTINEL)
        except Exception:
            pass
        self._stop.set()
        th = self._thread
        if th is not None and th.is_alive():
            th.join(timeout=max(0.5, timeout))

    # ---------- 入队 ----------

    def enqueue(self, msg_type, content):
        """入队一条消息（非阻塞）。队列满 → 丢弃并记日志"""
        self.start()
        try:
            self.queue.put_nowait((msg_type, content))
            return True
        except Exception:
            self._note_dropped()
            return False

    def _note_dropped(self):
        with self._lock:
            self._stats["dropped"] += 1
            self._dropped_since_log += 1
            n = self._dropped_since_log
            # 每丢 10 条汇总记一条，避免日志本身刷屏
            if n >= 10:
                self._dropped_since_log = 0
                logger.error("飞书队列已满，丢弃 %d 条（累计丢弃 %d）",
                             n, self._stats["dropped"])
            else:
                logger.warning("飞书队列已满，丢弃 1 条")

    # ---------- worker ----------

    def _run(self):
        while True:
            try:
                item = self.queue.get(timeout=0.5)
            except Empty:
                if self._stop.is_set():
                    break
                continue
            if item is _SENTINEL:
                self.queue.task_done()
                break
            try:
                self._deliver(item)
            except Exception as e:      # 兜底：worker 绝不能死
                logger.error("飞书 worker 异常: %s", type(e).__name__)
            finally:
                self.queue.task_done()
            # 限速（飞书群维度 5 QPS 是群内共享的，留足余量给群里其他人）
            if self.cfg.rate_per_sec > 0:
                if self._stop.wait(1.0 / self.cfg.rate_per_sec):
                    break

    def _deliver(self, item):
        msg_type, content = item
        ok = False
        for channel in self.cfg.channels:
            try:
                if channel == "app":
                    ok = self._send_app(msg_type, content)
                else:
                    ok = self._send_webhook(msg_type, content)
            except Exception as e:
                logger.warning("飞书发送失败(%s): %s", channel, type(e).__name__)
                self._last_error = f"{channel}: {type(e).__name__}"
                ok = False
            if ok:
                break
        with self._lock:
            if ok:
                self._stats["sent"] += 1
            else:
                self._stats["failed"] += 1
        return ok

    # ---------- 两条通道 ----------

    def _send_app(self, msg_type, content):
        """自建应用通道：im/v1/messages（content 必须是 JSON 字符串）"""
        cfg = self.cfg
        token = self._token.get()
        if not token:
            self._last_error = "取 token 失败"
            return False
        body = {
            "receive_id": cfg.chat_id,
            "msg_type": msg_type,
            "content": json.dumps(content, ensure_ascii=False),
        }
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json; charset=utf-8",
        }
        # receive_id_type 是**必填的 query 参数** —— 漏了直接
        # 400 / code=99992402 "field validation failed"（实测踩过）
        params = {"receive_id_type": "chat_id"}
        ok, data = self._post_with_retry(FEISHU_MSG_URL, headers, body, params=params)
        if ok:
            return True
        if data.get("_token_invalid"):
            # token 失效 → 强制刷新再来一次
            logger.warning("飞书 token 失效，刷新后重试")
            self._token.invalidate()
            token = self._token.get(force_refresh=True)
            if not token:
                self._last_error = "token 刷新失败"
                return False
            ok, _ = self._post_with_retry(
                FEISHU_MSG_URL,
                {"Authorization": f"Bearer {token}",
                 "Content-Type": "application/json; charset=utf-8"},
                body,
                params=params,
            )
            return ok
        self._last_error = str(data.get("_err") or data.get("msg") or "发送失败")
        return False

    def _send_webhook(self, msg_type, content):
        """群机器人 webhook 通道"""
        body = {"msg_type": msg_type, "content": content}
        ok, _ = self._post_with_retry(self.cfg.webhook, {}, body)
        return ok

    # ---------- HTTP + 重试 ----------

    def _post_with_retry(self, url, headers, body, params=None):
        """POST 并按失败类型决定重试。

        返回 (ok, data)；data 里可能带 _token_invalid / _err 供上层判断。
        """
        last_err = "未知错误"
        for attempt in range(_MAX_RETRY):
            try:
                resp = _requests().post(url, headers=headers, json=body,
                                        params=params, timeout=_HTTP_TIMEOUT)
            except Exception as e:
                last_err = f"{type(e).__name__}"
                logger.warning("飞书请求异常(第 %d 次): %s", attempt + 1, last_err)
                if attempt + 1 < _MAX_RETRY:
                    self._stop.wait(_RETRY_BACKOFF[attempt])
                    continue
                return False, {"_err": last_err}

            code = getattr(resp, "status_code", 0)
            if code == 200:
                try:
                    data = resp.json()
                except Exception:
                    data = {}
                biz = data.get("code")
                if biz in (0, None):
                    return True, data
                if biz in _TOKEN_INVALID_CODES:
                    return False, {"_token_invalid": True, "_err": f"code={biz}"}
                if biz in _RATE_LIMIT_CODES:
                    last_err = f"频率超限 code={biz}"
                    logger.warning("飞书频率超限(第 %d 次)，退避重试", attempt + 1)
                    if attempt + 1 < _MAX_RETRY:
                        self._stop.wait(_RETRY_BACKOFF[attempt])
                        continue
                    return False, {"_err": last_err}
                if biz in _FATAL_CODES:
                    logger.error("飞书配置/权限错误: code=%s msg=%s（不重试）",
                                 biz, data.get("msg"))
                    return False, {"_err": f"code={biz} {data.get('msg')}"}
                logger.error("飞书业务错误: code=%s msg=%s（不重试）",
                             biz, data.get("msg"))
                return False, {"_err": f"code={biz} {data.get('msg')}"}

            if code == 429 or code >= 500:
                last_err = f"HTTP {code}"
                logger.warning("飞书 %s(第 %d 次)，退避重试", last_err, attempt + 1)
                if attempt + 1 < _MAX_RETRY:
                    self._stop.wait(_RETRY_BACKOFF[attempt])
                    continue
                return False, {"_err": last_err}

            # 其它 4xx：配置错，重试没意义。
            # 把响应体截断记下来 —— 飞书的报错信息在 body 里（不含密钥）
            body_text = ""
            try:
                body_text = resp.text[:300]
            except Exception:
                pass
            logger.error("飞书 HTTP %s（不重试）: %s", code, body_text)
            return False, {"_err": f"HTTP {code} {body_text}"}

        return False, {"_err": last_err}

    # ---------- 状态 ----------

    def status(self):
        with self._lock:
            stats = dict(self._stats)
        return {
            "queued": self.queue.qsize(),
            "sent": stats["sent"],
            "failed": stats["failed"],
            "dropped": stats["dropped"],
            "last_error": self._last_error,
            "rate_per_sec": self.cfg.rate_per_sec,
            "queue_max": self.cfg.queue_max,
            "webhook": _mask_url(self.cfg.webhook) if self.cfg.webhook else "",
        }


_PUSHER = None
_PUSHER_LOCK = threading.Lock()


def _get_pusher():
    global _PUSHER
    with _PUSHER_LOCK:
        if _PUSHER is None or _PUSHER.is_stopped():
            # 已经停掉的 pusher 不能复用 —— 它的 worker 线程已经退出，
            # 队列里还压着 sentinel，重新 start 会立刻被 sentinel 带走
            _ensure_file_log()
            cfg = _config()
            _PUSHER = _Pusher(cfg)
            logger.info("飞书推送已初始化: 通道=%s 限速=%.1f条/秒 队列上限=%d",
                        channel_name(), cfg.rate_per_sec, cfg.queue_max)
        return _PUSHER


# ============================================================
# 对外 API
# ============================================================

def enqueue_card(card):
    """入队一张卡片（非阻塞）。返回是否入队成功"""
    if not is_configured():
        return False
    try:
        return _get_pusher().enqueue("interactive", card.get("card", card))
    except Exception as e:
        logger.error("飞书入队失败: %s", type(e).__name__)
        return False


def enqueue_text(text):
    """入队一条纯文本（非阻塞）"""
    if not is_configured():
        return False
    try:
        return _get_pusher().enqueue("text", {"text": text})
    except Exception as e:
        logger.error("飞书入队失败: %s", type(e).__name__)
        return False


def notify_alert(alert, label=""):
    """推送一条异动告警（传入 alert_monitor.Alert 或任何同形状对象）

    label 是中文标题；调用方（alert_monitor）从自己的 _ALERT_REASON_LABEL 取，
    本模块不去 import alert_monitor —— 免得两个模块互相牵扯。
    """
    try:
        card = build_alert_card(
            code=getattr(alert, "code", ""),
            name=getattr(alert, "name", ""),
            alert_type=getattr(alert, "alert_type", ""),
            zjl_yuan=getattr(alert, "current_zjl", 0),
            zaf_pct=getattr(alert, "current_zaf", 0),
            threshold=getattr(alert, "threshold", 0),
            label=label or getattr(alert, "alert_type", ""),
        )
        return enqueue_card(card)
    except Exception as e:
        logger.error("飞书异动告警构造失败: %s", type(e).__name__)
        return False


def notify_system(title, lines, level="info"):
    """推送一条系统通知（看板/看门狗事件用）"""
    try:
        card = build_system_card(title, lines, level)
        return enqueue_card(card)
    except Exception as e:
        logger.error("飞书系统通知构造失败: %s", type(e).__name__)
        return False


def status():
    """运行状态（给 /api/feishu/status 用）"""
    cfg = _config()
    pusher = _PUSHER
    out = {
        "enabled": cfg.enabled,
        "configured": is_configured(),
        "channel": channel_name(),
        "mode": cfg.mode,
        "rate_per_sec": cfg.rate_per_sec,
        "queue_max": cfg.queue_max,
        "webhook": _mask_url(cfg.webhook) if cfg.webhook else "",
        "queued": 0, "sent": 0, "failed": 0, "dropped": 0, "last_error": None,
    }
    if pusher is not None:
        out.update(pusher.status())
    return out


def shutdown(timeout=5.0):
    """退出前把队列里剩下的消息发完（必须在 os._exit 之前调用）

    注意：**不清空 _PUSHER** —— 清掉的话 status() 就查不到本次的
    发送计数了（一次性进程里这正是最想看到的）。
    下次 enqueue 时 _get_pusher() 会发现它已停止，自动换一个新的。
    """
    with _PUSHER_LOCK:
        pusher = _PUSHER
    if pusher is None:
        return
    try:
        pusher.stop(timeout)
        logger.info("飞书推送已停止")
    except Exception as e:
        logger.error("飞书推送停止异常: %s", type(e).__name__)
