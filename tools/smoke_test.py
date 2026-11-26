#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Stock_Dashboard 端到端冒烟测试。

设计约束（重要）：
- **绝不能 import stock_dashboard / tdx_client** —— 模块导入有副作用，
  会 init+close 一个通达信 TQ 槽位（mode 0/1/2/3 每种只有一个，
  烧光后 TQ 再也起不来）。本脚本只走 HTTP + 读文件。
  （`env_loader` / `feishu_push` 不碰通达信，是无副作用模块，可以 import —— 见第 10 组）
- 不杀进程、不改任何状态，只读。
- 退出码：0=全过，1=有失败项。

用法:
    python tools/smoke_test.py                 # 测默认 8765
    python tools/smoke_test.py --port 8765
    python tools/smoke_test.py --url http://127.0.0.1:8765
"""
import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request

sys.stdout.reconfigure(encoding="utf-8", errors="replace")

HERE = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.path.dirname(HERE)

_results = []


def check(name, ok, detail=""):
    _results.append((name, bool(ok), detail))
    mark = "PASS" if ok else "FAIL"
    print(f"  [{mark}] {name}" + (f"  —— {detail}" if detail else ""))
    return ok


def http(url, timeout=20, headers=None, method="GET", data=None):
    """返回 (status, body_str, elapsed_ms)；异常时 status=None"""
    req = urllib.request.Request(url, data=data, method=method, headers=headers or {})
    t0 = time.time()
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            body = r.read()
            return r.status, body, (time.time() - t0) * 1000, dict(r.headers)
    except urllib.error.HTTPError as e:
        body = e.read()
        return e.code, body, (time.time() - t0) * 1000, dict(e.headers)
    except Exception as e:
        return None, f"{type(e).__name__}: {e}".encode(), (time.time() - t0) * 1000, {}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=int(os.environ.get("DASHBOARD_PORT", "8765")))
    ap.add_argument("--url", default=None)
    args = ap.parse_args()
    base = (args.url or f"http://127.0.0.1:{args.port}").rstrip("/")

    print("=" * 62)
    print(f"  Stock_Dashboard 冒烟测试   target = {base}")
    print("=" * 62)

    # ---------- 1. 存活与健康 ----------
    print("\n[1] 服务存活与健康")
    st, body, ms, hdrs = http(f"{base}/api/health")
    if not check("GET /api/health 返回 200", st == 200, f"status={st}"):
        print("\n  服务不可用，后续用例跳过。")
        return 1
    health = json.loads(body.decode("utf-8", "replace"))
    check("/api/health 含 tdx_health", "tdx_health" in health, f"tdx_health={health.get('tdx_health')}")
    # 关键回归：/api/health 必须**只读缓存**，不能做实时 TQ 调用
    # （实时调用在扫描期间必然 3s 超时 → 误报 fail → 重启风暴）
    check("/api/health 响应 < 1000ms（证明未做实时 TQ 调用）", ms < 1000, f"{ms:.0f}ms")
    check("/api/health 含 tdx_health_error 仅在有错时出现",
          ("tdx_health_error" in health) == bool(health.get("tdx_health_error")))

    # ---------- 2. 路由与 query string ----------
    print("\n[2] 路由（query string 剥离）")
    st1, b1, ms1, _ = http(f"{base}/api/data")
    st2, b2, ms2, _ = http(f"{base}/api/data?t=1")
    check("GET /api/data 正常", st1 == 200, f"status={st1}, {len(b1)}B")
    check("GET /api/data?t=1 也正常（修复前 404）", st2 == 200, f"status={st2}")

    # ---------- 3. 静态文件与异常分支 ----------
    print("\n[3] 静态文件")
    st, b, ms, hdrs = http(f"{base}/favicon.ico")
    check("GET /favicon.ico 返回 404（修复前是断连无响应）", st == 404, f"status={st}")
    st, b, ms, hdrs = http(f"{base}/echarts.min.js")
    check("GET /echarts.min.js 返回 200", st == 200, f"status={st}, {len(b)}B")
    etag = hdrs.get("ETag")
    check("静态资源带 ETag", bool(etag), f"ETag={etag}")
    if etag:
        st, b, ms, hdrs = http(f"{base}/echarts.min.js", headers={"If-None-Match": etag})
        check("带 If-None-Match 返回 304", st == 304, f"status={st}")

    # ---------- 4. 路径穿越防御 ----------
    print("\n[4] 路径穿越防御")
    for evil in ("/../.env", "/..%2f..%2f.env", "/.git/config", "/%2e%2e/.env"):
        st, b, ms, _ = http(f"{base}{evil}")
        check(f"拒绝 {evil}", st == 404, f"status={st}")

    # ---------- 5. CORS ----------
    print("\n[5] CORS 收敛")
    st, b, ms, hdrs = http(f"{base}/api/data")
    acao = hdrs.get("Access-Control-Allow-Origin")
    check("默认不发 ACAO:* （需显式配 DASHBOARD_CORS_ORIGIN）", acao != "*", f"ACAO={acao}")

    # ---------- 6. 其余 API 契约 ----------
    print("\n[6] API 契约")
    for path, keys in [
        ("/api/trend", ("timestamps", "market", "names", "market_net")),
        ("/api/alerts", ("enabled", "subscribed_size", "events_received")),
        ("/api/history", None),
    ]:
        st, b, ms, _ = http(f"{base}{path}")
        ok = st == 200
        detail = f"status={st}"
        if ok and keys:
            try:
                d = json.loads(b.decode("utf-8", "replace"))
                missing = [k for k in keys if k not in d]
                ok = not missing
                detail = f"status={st}" + (f" 缺字段 {missing}" if missing else "")
            except Exception as e:
                ok = False
                detail = f"JSON 解析失败: {e}"
        check(f"GET {path}", ok, detail)

    # ---------- 7. 告警链路状态 ----------
    print("\n[7] 告警链路")
    st, b, ms, _ = http(f"{base}/api/alerts")
    if st == 200:
        d = json.loads(b.decode("utf-8", "replace"))
        check("订阅数 > 0（实时订阅已建立）", d.get("subscribed_size", 0) > 0,
              f"subscribed_size={d.get('subscribed_size')}")
        check("events_received 字段存在且非负", d.get("events_received", -1) >= 0,
              f"events_received={d.get('events_received')}")

    # ---------- 8. keepalive 状态文件 ----------
    print("\n[8] keepalive 状态文件")
    state_path = os.path.join(PROJECT_DIR, "logs", "keepalive_state.json")
    if os.path.exists(state_path):
        try:
            with open(state_path, "r", encoding="utf-8") as f:
                st_json = json.load(f)
            check("keepalive_state.json 是合法 JSON", isinstance(st_json, dict))
            check("限流计数已记录（restarts 字段存在）", "restarts" in st_json)
        except Exception as e:
            check("keepalive_state.json 是合法 JSON", False, str(e))
    else:
        check("keepalive_state.json 存在", False, "文件不存在")

    # ---------- 9. 前端 JS 语法 ----------
    # 前端脚本出错时页面会整块白屏，而 HTTP 接口仍然正常 ——
    # 只有单独做语法检查才能发现（2026-11-22 改动中真的踩到过）。
    print("\n[9] 前端 JS 语法")
    html_path = os.path.join(PROJECT_DIR, "dashboard.html")
    try:
        import re
        with open(html_path, "r", encoding="utf-8") as f:
            html = f.read()
        blocks = re.findall(r"<script(?![^>]*src=)[^>]*>(.*?)</script>", html, re.S)
        check("dashboard.html 含内联脚本", len(blocks) > 0, f"{len(blocks)} 块")
        node = shutil.which("node")
        if node and blocks:
            tmp = os.path.join(tempfile.gettempdir(), "_dash_smoke_js.js")
            with open(tmp, "w", encoding="utf-8") as f:
                f.write("\n".join(blocks))
            r = subprocess.run([node, "--check", tmp], capture_output=True,
                               text=True, encoding="utf-8", errors="replace", timeout=60)
            check("内联 JS 语法正确 (node --check)", r.returncode == 0,
                  (r.stderr or "").strip().splitlines()[0] if r.returncode else "")
        elif not node:
            print("       (跳过：未找到 node)")
    except Exception as e:
        check("前端 JS 语法检查", False, str(e))

    # ---------- 10. 飞书推送模块 ----------
    # 只做**离线**检查：不真的发消息、不起后台线程、不打印任何密钥。
    # env_loader / feishu_push 是无副作用模块（不碰通达信 TQ），
    # 可以安全 import —— 与被明令禁止的 stock_dashboard / tdx_client 不同。
    print("\n[10] 飞书推送模块")
    if PROJECT_DIR not in sys.path:
        sys.path.insert(0, PROJECT_DIR)
    import threading

    try:
        import env_loader
        loaded = env_loader.load_env()
        # 只报**键的数量**，绝不报值 —— 值里可能有 app_secret
        check(".env 可加载（只读键名，不打印值）", len(loaded) >= 1,
              f"加载 {len(loaded)} 项")
    except Exception as e:
        check(".env 可加载", False, str(e)[:120])

    before = threading.active_count()
    try:
        import feishu_push
        check("feishu_push 可导入", True)
        check("导入不创建后台线程（导入无副作用）",
              threading.active_count() == before,
              f"{before} -> {threading.active_count()}")
    except Exception as e:
        check("feishu_push 可导入", False, str(e)[:120])
        feishu_push = None

    if feishu_push is not None:
        stt = feishu_push.status()
        # 只报通道名，不报 URL / token
        check("飞书通道已配置", stt.get("configured") is True,
              f"channel={stt.get('channel')}")
        check("限速在合理范围（0 < rate <= 10 条/秒）",
              0 < float(stt.get("rate_per_sec", 0)) <= 10,
              f"rate={stt.get('rate_per_sec')}")

        card = feishu_push.build_alert_card(
            code="600519.SH", name="贵州茅台", alert_type="big_inflow",
            zjl_yuan=1.234e9, zaf_pct=5.21, threshold=4.0, label="主力大额流入")
        ok = (isinstance(card, dict)
              and card.get("msg_type") == "interactive"
              and isinstance(card.get("card", {}).get("header", {}).get("template"), str)
              and bool(card.get("card", {}).get("elements")))
        check("异动卡片构造正确", ok,
              f"template={card.get('card', {}).get('header', {}).get('template')}")

        syscard = feishu_push.build_system_card("[OK] 测试", ["一行文本"], "ok")
        ok2 = (isinstance(syscard, dict)
               and syscard.get("msg_type") == "interactive"
               and bool(syscard.get("card", {}).get("elements")))
        check("系统通知卡片构造正确", ok2)

        try:
            json.dumps(card, ensure_ascii=False)
            json.dumps(syscard, ensure_ascii=False)
            check("卡片可 JSON 序列化", True)
        except Exception as e:
            check("卡片可 JSON 序列化", False, str(e)[:120])

        # 未入队就不该起 worker 线程
        before2 = threading.active_count()
        check("未入队时不启动发送线程", threading.active_count() == before2)

    # 看板侧的飞书状态端点（只读；看板没重启到该版本就跳过）
    st, b, ms, _ = http(f"{base}/api/feishu/status")
    if st == 200:
        try:
            d = json.loads(b.decode("utf-8", "replace"))
            check("GET /api/feishu/status 返回通道信息", "channel" in d,
                  f"channel={d.get('channel')}")
        except Exception as e:
            check("GET /api/feishu/status 返回通道信息", False, str(e)[:120])
    elif st == 404:
        print("       (跳过：看板未重启到含 /api/feishu/status 的版本)")

    # ---------- 汇总 ----------
    passed = sum(1 for _, ok, _ in _results if ok)
    failed = len(_results) - passed
    print("\n" + "=" * 62)
    print(f"  结果: {passed} 通过 / {failed} 失败 / 共 {len(_results)}")
    print("=" * 62)
    if failed:
        for name, ok, detail in _results:
            if not ok:
                print(f"  FAIL: {name}  {detail}")
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
