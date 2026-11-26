#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
飞书推送自检工具 —— 手动验证"推送链路到底通不通"

用法:
    python tools/feishu_test.py            # 发一条系统通知卡片
    python tools/feishu_test.py --alert    # 发一张异动卡片（假数据，看样式）
    python tools/feishu_test.py --status   # 只看通道与队列状态，不发送

为什么需要它:
    异动告警只在交易时段产生，盘后没法验证推送通没通。
    这个脚本走的是**同一套** feishu_push 模块（同一通道、同一队列、同一重试逻辑），
    所以它能通 = 告警能通。

注意:
    - 一次性进程，退出前会 flush 队列（不 flush 的话守护线程随进程死，
      消息发不出去）
    - 日志里只出现通道名与脱敏 URL，密钥不会打印
"""
import argparse
import json
import os
import sys
from pathlib import Path

# 项目根目录（用 abspath 不用 resolve —— resolve 会把映射盘展开成 UNC）
PROJECT_DIR = Path(os.path.abspath(__file__)).parent.parent
sys.path.insert(0, str(PROJECT_DIR))

try:
    import env_loader
    env_loader.load_env()
except Exception:
    pass

import feishu_push  # noqa: E402


def main() -> int:
    ap = argparse.ArgumentParser(description="飞书推送自检")
    ap.add_argument("--alert", action="store_true", help="发一张异动卡片（假数据）")
    ap.add_argument("--status", action="store_true", help="只看状态，不发送")
    args = ap.parse_args()

    st = feishu_push.status()
    print("=" * 56)
    print("飞书推送自检")
    print("=" * 56)
    print(f"  通道      : {st['channel']}")
    print(f"  已配置    : {st['configured']}")
    print(f"  模式      : {st['mode']}")
    print(f"  限速      : {st['rate_per_sec']} 条/秒")
    if st.get("webhook"):
        print(f"  webhook   : {st['webhook']}（脱敏）")

    if args.status:
        print(f"  队列深度  : {st['queued']}")
        print(f"  已发/失败 : {st['sent']} / {st['failed']}")
        if st.get("last_error"):
            print(f"  最近错误  : {st['last_error']}")
        return 0

    if not st["configured"]:
        print()
        print("  [FAIL] 没有可用通道 —— 推送不会发生。")
        print("         在项目根目录建 .env，填下面任意一组：")
        print("           FEISHU_APP_ID=cli_xxx")
        print("           FEISHU_APP_SECRET=xxx")
        print("           FEISHU_CHAT_ID=oc_xxx")
        print("         或：")
        print("           STOCK_FLOW_FEISHU_WEBHOOK=https://open.feishu.cn/...")
        return 1

    if args.alert:
        card = feishu_push.build_alert_card(
            code="600519.SH", name="贵州茅台", alert_type="big_inflow",
            zjl_yuan=1.234e9, zaf_pct=5.21, threshold=4.0,
            label="主力大额流入",
        )
        queued = feishu_push.enqueue_card(card)
        what = "异动卡片"
    else:
        queued = feishu_push.notify_system(
            "✅ 飞书推送自检",
            ["这是一条来自 tools/feishu_test.py 的测试通知",
             f"通道: {st['channel']}",
             "如果你在飞书群里看到了这张卡片，说明推送链路是通的。"],
            level="ok",
        )
        what = "系统通知卡片"

    print(f"  已入队    : {what} -> {queued}")
    print()
    print("  正在发送（一次性进程，退出前会等它发完）…")

    # 一次性进程：不 flush 的话守护线程随进程退出被干掉，消息发不出去
    feishu_push.shutdown(20.0)

    after = feishu_push.status()
    print(f"  已发      : {after['sent']}")
    print(f"  失败      : {after['failed']}")
    if after.get("last_error"):
        print(f"  最近错误  : {after['last_error']}")
    print(f"  队列残留  : {after['queued']}")
    ok = after["sent"] > 0 and after["failed"] == 0
    print()
    print(f"  [{'OK' if ok else 'FAIL'}] " + ("推送成功，去飞书群里确认一下。" if ok
                                          else "推送失败 —— 看 logs/feishu.log"))
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
