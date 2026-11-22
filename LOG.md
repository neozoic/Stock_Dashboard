---
title: "Stock_Dashboard 项目日志"
description: "A 股资金流实时看板："
type: reference
tags:
  - 记录
created: 2026-09-19
updated: 2026-10-05
related:
  []
---
# Stock_Dashboard 项目日志

> 最后更新：2026-09-07
> 接管来源：`C:\Users\openclaw\` 根目录散落的 Python 脚本 + 通达信数据源

---

## 1. 项目概述

A 股资金流实时看板：

- **数据源**：通达信 TdxQuant（主源 + run_id=槽位管理）+ akshare（同花顺接口兜底）
- **前端**：单文件 ECharts 5.4.3 单页应用 + 6 张对比表 + 5 张折线图
- **后端**：`http.server.HTTPServer` + 后台线程拉数 + 环形缓冲历史

### 三层架构

```
┌─────────────────┐    ┌────────────────────┐    ┌─────────────────────┐
│ 通达信 TdxQuant  │ →  │  看板后端 (8765)   │ →  │ 浏览器 dashboard.html│
│ akshare 兜底    │    │  LATEST 缓存         │    │ ECharts 折线图 + 表  │
└─────────────────┘    └────────────────────┘    └─────────────────────┘
                              │
                              ├── daily_snapshot_loop → daily_summaries (10 天环形)
                              ├── period_background_loop → 3/5/10 日排行 (10 分钟周期)
                              └── history 时序 (盘中/盘后累积)
```

---

## 2. 文件清单

| 文件 | 大小 | 作用 |
|---|---|---|
| `stock_dashboard.py` | ~50 KB | 主后端：HTTP 路由 + 后台拉数线程 + LATEST 缓存 |
| `tdx_client.py` | ~26 KB | 通达信 TdxQuant 封装，含 8s 超时（`_call_with_timeout`）|
| `alert_monitor.py` | ~24 KB | 实时异动监测器（涨停/跌停/大单/异动），GBK emoji 已替换 |
| `dashboard.html` | ~49 KB | 前端单文件：CSS + JS + 6 张对比表 + 5 张折线图 |
| `echarts.min.js` | 1 MB | ECharts 5.4.3 本地副本（离线可用，规避 CDN 受限）|
| `start_dashboard.bat` | <2 KB | UTF-8 + Python 自检 + 设置 TDX_HOME 的启动脚本 |
| `README.md` | <6 KB | 项目说明（四件套、Python 版本、启动命令）|
| `.env.example` | <1 KB | 环境变量示例 |
| `logs/` | — | stdout/stderr 日志目录 |
| `archive/stock_dashboard_tdx.py` | — | 旧版主程序（已停用，仅作历史参考）|

---

## 3. 关键代码改造（迁移中修复的问题）

| # | 问题 | 文件:行 | 修复 |
|---|---|---|---|
| 1 | `C:\new_tdx64` 硬编码 6 处 | `tdx_client.py:41-47` + `alert_monitor.py:318-319` | 改用 `TDX_HOME` / `TDX_EXE_NAME` 环境变量（默认 `C:\new_tdx64`）|
| 2 | Python 3.12 强校验 | `tdx_client.py:69-72` | 放宽到 3.11–3.13 区间 |
| 3 | GBK 编码崩溃（✅/❌/🚀/⏰）| `stock_dashboard.py:96, 745, 747` + `alert_monitor.py:79-83, 99, 640` | 替换为 `[OK]` / `[FAIL]` / `[UP]` 等 ASCII 标签 |
| 4 | 启动时 emoji GBK 报错导致 main() 整体退出 | `stock_dashboard.py` 整体 | `start_dashboard.bat` 强制 `PYTHONIOENCODING=utf-8 PYTHONUTF8=1` |
| 5 | 代理环境强制清空 | `stock_dashboard.py:27-30` | `if DASHBOARD_KEEP_PROXY != "1"` 包一层，opt-in 退出 |
| 6 | 端口 8765 硬编码 | `stock_dashboard.py:725` | `DASHBOARD_PORT` 环境变量 |
| 7 | 绑定 `0.0.0.0` 硬编码 | `stock_dashboard.py:763` | `DASHBOARD_BIND` 环境变量 |
| 8 | TdxQuant 调用 hang | `tdx_client.py` | `_call_with_timeout()` 包装 5 个阻塞调用（get_stock_list / get_sector_list / get_stock_list_in_sector / get_market_snapshot / get_more_info），`TDX_CALL_TIMEOUT` 默认 8 秒 |
| 9 | 飞书 webhook 未配置静默 | `alert_monitor.py:33` | 启动期 WARNING 提示 |
| 10 | `alert_monitor.detect()` `zaf - prev['zaf']` TypeError（TDX 字段类型不稳定）| `alert_monitor.py:208-218` | `float()` 转换 + try/except |
| 11 | 趋势图 X 轴 `[nan, value]` | `dashboard.html` `hhmmssToMin` | `parts[0]\|\|0, parts[1]\|\|0, parts[2]\|\|0` 兼容 HH:MM 与 HH:MM:SS |
| 12 | 趋势图容器无高度 | `dashboard.html` | `<div style="width:100%;height:280px">` 显式 |
| 13 | `/echarts.min.js` 404 | `stock_dashboard.py` `do_GET` | 同目录白名单路由（`.js/.css/.html/.png/.ico`），路径穿越过滤 |
| 14 | 折线图左上角元素堆叠 | `dashboard.html` `renderDailyTopChart` | 删除 `graphic.elements` 绝对定位，改为 series `markPoint` |
| 15 | 图例堆在标题区 | `dashboard.html` | legend `orient:"vertical", right:10` 移右侧 + `type:"scroll"` |
| 16 | 行业/概念流出与流入行数不齐 | demo 数据 | 流出补到 10/15 |
| 17 | LATEST 被 fetch_once 拉到空 snapshot 时擦掉 | `stock_dashboard.py:991` | `has_data` 守卫：空 snapshot 不覆盖 |
| 18 | fetch_once 即使空 snapshot 也污染 history | `stock_dashboard.py:956` | history 追加也加 `has_data` 守卫 |
| 19 | 休市时段前端显示空表 | `dashboard.html` `fetchData()` | 实时数据空 → fetch `/api/data/latest` 取最近一日 |
| 20 | HTTPServer 单线程阻塞 | `stock_dashboard.py` `main()` | 改 `ThreadingHTTPServer`（socketserver.ThreadingMixIn + daemon_threads=True）|
| 21 | handler 同步锁 + json.dumps + wfile.write 三合一 | `stock_dashboard.py` `_serve_*` | 锁内只读快照，序列化 + 网络写出走统一 `_send_json_response()`，断连用 try/except |
| 22 | 后台线程 daemon=true 资源不清理 | `stock_dashboard.py` `main()` | 改 `daemon=False` + `stop_event` + 优雅退出 join |
| 23 | fetch_once 无总超时 watchdog | `stock_dashboard.py` `background_loop` | 用 `_call_with_timeout(fetch_once, timeout=interval-5)` 包整轮 |
| 24 | history series key 无上限 | `stock_dashboard.py` push_series | 加 `MAX_SERIES_KEYS=500` + 长期 None series 淘汰 + 用 `del` 裁剪避免 O(N) |
| 25 | path traversal 返 HTML 404 | `stock_dashboard.py` `do_GET` | 改 `_send_json_response(400, ...)` |
| 26 | window resize 无节流 | `dashboard.html` | rAF 节流 + `chart.resize({silent:true})` |
| 27 | handler 响应无 try/except | `stock_dashboard.py` `_send_json_response` | 包 BrokenPipeError / ConnectionResetError / OSError |
| 28 | `fetch_once` 函数过长、history 与 LATEST 双锁 | | 拆 `_fetch_via_tdx` / `_fetch_via_akshare_fallback` / `_compose_snapshot` / `_commit_snapshot_and_history` 四段，主函数 30 行内 |
| 29 | 错误拼接重复 `' | xxx: ...'` | `stock_dashboard.py` `_accumulate_error()` | 抽公共函数 + `_classify_error()` 给异常贴 NETWORK/TIMEOUT/AUTH/SCHEMA 标签 |
| 30 | ECharts 每 10s 全量重建 series | `dashboard.html` `_setChartOption()` |首次 notMerge=true 建模，后续 merge 模式只刷数据 |
| 31 | history series 与 LATEST 双锁更新可能撕裂 | `stock_dashboard.py` `_commit_snapshot_and_history()` | 单 DATA_LOCK 内一次性 history + LATEST 提交 |
| 32 | magic number 散落（20/15/8/5/600/240...）| `stock_dashboard.py` `DEFAULT_FETCH_*_TOP_N` 常量 | fetch_once 内显式使用常量名 |
| 33 | watchdog 仍指向旧路径 `C:\Users\openclaw\stock_dashboard_tdx.py`（已删除）| `.hermes/scripts/dashboard_watchdog.py:19` 与 `.hermes/skills/productivity/stock-dashboard-frontend/scripts/dashboard_watchdog.py` | ✅ 2026-09-05 已解决：权威版重写为 `tools/dashboard_watchdog.py`（路径自定位、env 对齐 start_dashboard.bat），两份 `.hermes` 副本改为 thin wrapper 转发调用，hermes cron 注册无需变更 |
| 34 | `stock_flow_push_tdx.py` 仍用 `datetime.now()` 本地时间 | `C:\Users\openclaw\stock_flow_push_tdx.py`（不在本看板范围）| ✅ 2026-09-05 已解决：已从 `stock_flow_push.py` 复制 `BJ_TZ` + `bj_now()`，3 处 `datetime.now()` 全部替换 |

---

## 16. 安全审查清单（公网部署前必做）

按 P3-4，部署到公网/共享网络前需逐项核验：

| # | 项目 | 现状 | 缓解 |
|---|---|---|---|
| 1 | 默认绑定 `0.0.0.0` 暴露公网 | `main()` 默认 `bind_host = "0.0.0.0"` | 启动前 `set DASHBOARD_BIND=127.0.0.1` 或 Nginx 反代 |
| 2 | 无访问控制 / 鉴权 | `Handler.do_GET` 无任何 token / IP 白名单 | 加 `Authorization` Bearer token，或 nginx basic_auth |
| 3 | 静态文件白名单路由 | 已实现，但缺 `.json` mime 与 ETag | 已加 `Cache-Control: no-store`；建议加 ETag/304 |
| 4 | 路径穿越 | `do_GET` else 分支已检查 `.. / / / \\` | ✅ 已防御 |
| 5 | subprocess / eval / exec | 全代码无 `subprocess.Popen(shell=True)` 用户输入拼接、无 `eval/exec` | ✅ 已审计 |
| 6 | 日志泄漏敏感数据 | stderr 走 `logs/`；无 token/webhook URL 写日志 | ✅ 安全 |
| 7 | 飞书 webhook URL 泄漏 | `STOCK_FLOW_FEISHU_WEBHOOK` 仅在 `alert_monitor.py` 用，不写日志 | ✅ 安全 |
| 8 | akshare SSRF | 仅访问官方公开数据源；URL 不可被外部输入控制 | ✅ 安全 |
| 9 | HTTP 路径注入 | 路径仅在 `do_GET` 静态文件路由使用，无 shell 拼接 | ✅ 安全 |
| 10 | 端口冲突 | ~~启动时未检测占用~~ ✅ 2026-09-05 已解决 | `main()` 启动前 `_is_port_in_use()` 自检，占用时打印清理命令并 exit 2 |

> 本节仅为**自检清单**，未跑实际渗透测试。公网部署前建议用 `bandit` / `safety` 等工具扫一遍 `stock_dashboard.py / tdx_client.py / alert_monitor.py`。

---

## 4. HTTP 接口

| 路径 | 方法 | 返回 | 缓存 |
|---|---|---|---|
| `/` | GET | `dashboard.html` | no-store |
| `/api/health` | GET | `{tdx_client_available, akshare_available, current_source, last_timestamp, last_error, tdx_health}` | no-store |
| `/api/data` | GET | `{timestamp, is_trading, stock_top/bottom, industry_top/bottom, concept_top/bottom, market_net, source}` | no-store |
| `/api/history` | GET | 全量 history 时序（开发用）| no-store |
| `/api/trend` | GET | `{timestamps, stock, industry, concept, market, names: {stock,industry,concept}, market_net}` | no-store |
| `/api/alerts` | GET | 异动监测状态 | no-store |
| `/api/data/period_compare` | GET | 1d/3d/5d/10d 累计流入流出（akshare 兜底）| no-store |
| `/api/data/daily_top?n=6` | GET | 最近 N 个交易日完整快照（含 industry/concept）| no-store |
| `/api/data/latest` | GET | 最近一日完整 snapshot（休市兜底）| no-store |
| `/api/minute?code=600519.SH&period=1m&count=N` | GET | 单股/板块 K线分时（tq.get_market_data，读本地已下载数据；TDX 不可用 503，无数据 200 空数组）| no-store |
| `/api/indicator?code=600519.SH&formula=KDJ&arg=9,3,3` | GET | 内置公式指标（tq.formula_zb；formula 白名单 MA/KDJ/MACD/BOLL/VOL/RSI/WR/DMA/ASI，白名单外 400，TDX 不可用 503）| no-store |
| `/<file>` | GET | 同目录白名单静态文件（`.js/.css/.html/.json/.png/.svg/.ico`）| no-store |
| `<其余路径>` | — | 404 | — |

---

## 5. 后台线程清单

| 线程 | 间隔 | 作用 | 启动 |
|---|---|---|---|
| `background_loop` | 180s | 主拉数：TDX 资金流 + akshare 兜底；写 LATEST + history | `main()` 中启动 |
| `period_background_loop` | 600s | 拉 akshare 3/5/10 日排行 | `main()` 中启动 |
| `daily_snapshot_loop` | 60s | 每日 15:30 后保存完整 snapshot 到 `LATEST["daily_summaries"]`（含 industry/concept）| `main()` 中启动 |
| `alert_monitor._scan_loop` | 60s | 全市场异动扫描（仅 alert_monitor 模块）| alert_monitor 内启动 |

---

## 6. 数据流时序

```
T+0 (启动)
  LATEST = {} （空）
  fetch_once 第一次跑
    → TDX 资金流（盘后全0）
    → akshare 兜底（通常失败）
    → snapshot 全空 → has_data=False → 不写 LATEST（保留空）
    → history 也不追加
  LATEST 仍为空，前端表格显示 "无数据"

T+30s 第二次 fetch_once
  → 继续空（盘后无数据）

第二日 9:30-11:30 / 13:00-15:00 (盘中)
  → fetch_once 拉到真实数据 → LATEST 填满
  → history 累积每日多个时间点
  → 前端表格实时更新

第二日 15:30 后
  → daily_snapshot_loop 触发 → 保存当日 LATEST 到 daily_summaries["YYYY-MM-DD"]
  → 包含 stock_top/bottom + industry_top/bottom + concept_top/bottom

第三日起
  → 盘后 LATEST 实时为空（snapshot 已被覆盖）
  → 但 daily_summaries 已累积，可从 daily_top?n=1 取最近一日
```

---

## 7. 依赖

### 外部依赖

| 依赖 | 来源 | 用途 |
|---|---|---|
| Python | 3.11 / 3.12 / 3.13 | 运行时 |
| 通达信 TdxQuant | `C:\new_tdx64\`（可改 `TDX_HOME`）| TQ 行情接口 |
| `tqcenter` Python 绑定 | 通达信客户端自带的 Python 模块 | TdxQuant 调用入口 |
| `akshare` | pip | 同花顺数据接口兜底 |
| `psutil` | pip | 进程检查（可选）|
| ECharts 5.4.3 | jsdelivr CDN（已本地化为 `echarts.min.js`）| 前端图表 |
| Python 标准库 | — | `http.server`、`threading`、`json`、`datetime`、`urllib.parse` |

### 安装命令

```bash
pip install akshare psutil
# 通达信客户端自带 tqcenter，无需额外安装（但需安装到对应 Python 版本）
```

---

## 8. 环境变量

| 变量 | 默认值 | 说明 |
|---|---|---|
| `TDX_HOME` | `C:\new_tdx64` | 通达信客户端安装路径 |
| `TDX_EXE_NAME` | `TdxW.exe` | 通达信客户端进程名 |
| `TDX_CALL_TIMEOUT` | `8` | TdxQuant 单次调用超时（秒）|
| `DASHBOARD_PORT` | `8765` | 看板 HTTP 端口 |
| `DASHBOARD_BIND` | `0.0.0.0` | 绑定地址（容器化可改 `127.0.0.1`）|
| `DASHBOARD_KEEP_PROXY` | `0` | `1`=保留系统代理；默认清掉 |
| `STOCK_FLOW_FEISHU_WEBHOOK` | （空）| 飞书机器人 webhook；不填则异动仅 stdout |
| `TDX_SEND_WARN` | `1` | 异动信号回推通达信 TQ 策略信号窗口（tq.send_warn）；`0`=关闭 |

---

## 9. 启动 / 重启 / 停止

### 启动（推荐）

```batch
cd C:\Mac\Home\Desktop\win_WorkSpace_ClaudeCode\Stock_Dashboard
start_dashboard.bat
```

### 直接启动

```batch
set PYTHONIOENCODING=utf-8
set PYTHONUTF8=1
python stock_dashboard.py
```

### 停止

```powershell
Get-NetTCPConnection -LocalPort 8765 -State Listen | ForEach-Object { Stop-Process -Id $_.OwningProcess -Force }
Get-Process | Where-Object { $_.ProcessName -eq 'python' } | Stop-Process -Force
```

### 查看实时日志

```powershell
Get-Content C:\Mac\Home\Desktop\win_WorkSpace_ClaudeCode\Stock_Dashboard\logs\dashboard.stdout.log -Wait
Get-Content C:\Mac\Home\Desktop\win_WorkSpace_ClaudeCode\Stock_Dashboard\logs\dashboard.stderr.log -Wait
```

---

## 10. 验证清单（启动后必跑）

```powershell
# 1. 进程在跑
Get-Process | Where-Object { $_.ProcessName -eq 'python' }

# 2. 端口在听
Get-NetTCPConnection -LocalPort 8765 -State Listen

# 3. 健康检查
(Invoke-WebRequest -Uri http://127.0.0.1:8765/api/health -UseBasicParsing).Content
# {"tdx_client_available": true, "akshare_available": true, "current_source": "tdx", "tdx_health": "ok"}

# 4. 数据接口
(Invoke-WebRequest -Uri http://127.0.0.1:8765/api/data -UseBasicParsing).Content
# 盘中应有非空 stock_top；盘后/未拉数时空

# 5. 趋势接口
(Invoke-WebRequest -Uri http://127.0.0.1:8765/api/trend -UseBasicParsing).Content

# 6. 浏览器
Start-Process 'http://localhost:8765/'
```

---

## 11. 问题排查指南

### Q1: 看板打开表格全部 "无数据"

**A:** 拉数周期未跑过，或 fetch_once 拉到全空。
- 检查 `logs/dashboard.stderr.log` 最后 20 行，看 `[bg] fetching` / `[bg-period] 3d done in` / `[bg-snapshot]` 等日志
- 盘中：TDX 资金流应非空；非盘中：可能只有 `current_source=mixed(tdx+akshare)` 部分有数据
- 盘后且历史未保存：浏览器顶部应显示 `⏸ 当前为非交易时段，显示最近 1 日数据`；若无此提示且仍空，检查 `/api/data/latest` 是否返回

### Q2: 启动失败 / 端口已被占用

**A:** 残留进程占用 8765。
```powershell
Get-NetTCPConnection -LocalPort 8765 -State Listen | ForEach-Object { Stop-Process -Id $_.OwningProcess -Force }
```
再重启 `start_dashboard.bat`。

### Q3: TdxQuant 初始化失败（`return ID 小于 0`）

**A:** 通达信客户端未启用 TQ 量化模块。
1. 打开通达信客户端，登录行情账号
2. 顶部菜单「设置」→「量化」勾选启用
3. 重启客户端后再启动看板

### Q4: 趋势图无折线

**A:**
- 数据问题：`/api/trend` 应返回 `timestamps` 数组 + `industry/stock/concept/market` 字典
- 容器问题：检查 `trend-stock` 等 div 是否有显式高度
- NaN 问题：检查 timestamps 是否都是 HH:MM 或 HH:MM:SS 格式（不能混用）
- ECharts 版本：本地 `echarts.min.js` 是 5.4.3；若被替换为旧版可能不支持新 API

### Q5: 折线图"近 6 个交易日"折线全 0 或显示"尚无数据"

**A:** `LATEST["daily_summaries"]` 为空（首次启动 6 个交易日内）。
- 项目刚迁移：等 6 天自动累积；或调用 `daily_snapshot_loop` 立即保存一次（手动触发）
- 检查 stderr：`[daily-snapshot] 已保存 YYYY-MM-DD` 日志是否存在

### Q6: 飞书推送没收到

**A:** `STOCK_FLOW_FEISHU_WEBHOOK` 环境变量未设或 webhook URL 失效。
- stderr 启动期会 WARNING："飞书 Webhook 未配置..."
- 设环境变量后重启：异动会立即通过 webhook 推送

### Q7: ECharts CDN 加载失败（出现 `echarts is not defined`）

**A:** 检查 `<script src="./echarts.min.js">` 是否能被服务：访问 `http://localhost:8765/echarts.min.js` 应返回 1024 KB 文件。若 404，说明 `do_GET` 静态文件白名单未启用。

### Q8: 字符乱码

**A:** 中文显示是 `???` 或 `锟斤拷`：
- 检查 stdout/stderr：PowerShell 默认 GBK
- 启动时已设 `PYTHONIOENCODING=utf-8 PYTHONUTF8=1`
- HTML 头部 `<meta charset="UTF-8">` 已设

---

## 12. 与更大项目集成的指引

### 嵌入到主项目的方案

#### 方案 A：作为独立 HTTP 服务（推荐）

主项目通过 HTTP 调用本看板接口，纳入自己的网关：

```python
# 主项目调用示例
import httpx
async def get_a_stock_fund_flow():
    async with httpx.AsyncClient() as c:
        r = await c.get("http://localhost:8765/api/data")
        return r.json()
```

#### 方案 B：作为子进程

主项目启动本看板作为子进程，通过端口暴露数据：

```python
subprocess.Popen(["start_dashboard.bat"], cwd=stock_dashboard_dir)
```

#### 方案 C：内联嵌入

直接把 `stock_dashboard.py` 中的 `LATEST` 数据结构、后台线程、HTTP handler 抽成独立模块，import 到主项目。

### 集成时需注意

1. **端口冲突**：主项目若占用 8765 需改 `DASHBOARD_PORT`
2. **时区**：所有时间戳用 `bj_now_naive()`（北京时间），不要用 `datetime.now()`
3. **线程安全**：读写 `LATEST` 必须持 `DATA_LOCK` 或 `FETCH_LOCK`
4. **依赖安装**：主项目 environment 需装 `akshare`；通达信 tqcenter 仅在 Windows + 通达信客户端环境可用
5. **dogpile effect**：如果主项目自身频繁调用 `/api/data`，建议前端缓存 30s（看板前端已设 `cache: "no-store"`，按需调整）

### 提取核心数据模块的最小代码骨架

```python
# 仅取 LATEST 缓存 + 后台拉数（去掉 HTTP 层）：
from stock_dashboard import LATEST, background_loop, DATA_LOCK

# 在自己的线程里跑
threading.Thread(target=background_loop, args=(180,), daemon=True).start()

# 读 LATEST
with DATA_LOCK:
    data = dict(LATEST)
```

### 提取静态前端

`dashboard.html` + `echarts.min.js` 可独立部署到任何静态服务器（nginx / GitHub Pages），只要配置 API 反向代理到 `localhost:8765`。

---

## 13. 已知遗留问题

| 问题 | 影响 | 解决方案 |
|---|---|---|
| ~~watchdog 路径漂移~~ ✅ 2026-09-05 已解决 | 旧副本守护已归档脚本 | 权威版重写为 `tools/dashboard_watchdog.py`（路径自定位）；`.hermes` 两份副本改为 thin wrapper 转发调用 |
| 实时拉数 BASEHTTPR equestHandler 同步阻塞 | 慢请求会阻塞其他请求 30s+ | 已改 ThreadingHTTPServer（2026-08-09）；如需更高并发可再切 aiohttp / FastAPI |
| ~~`industry/concept` 历史只有累计值~~ ✅ 前端已接入 | 趋势图是当日累积曲线，不能看盘中分时占比 | 已提供 `GET /api/minute`（tq.get_market_data 分钟线封装，tdx_client.get_minute_bars），dashboard.html 分钟线查询面板已接入 |
| 日内累积曲线 vs 当日截面对比 | T-5..T 折线图是每只股的净额，趋势图是行业/概念累计（不同语义）| 在前端 UI 区分命名 + hover 说明 |
| ~~`fetch_once` 仍可能 hang~~ ✅ 2026-09-05 已解决 | 单次 hang 会持续到下次 init | `background_loop` 已加连续超时计数：超时后下一轮前 `init(force=True)` 强制重连（init 本身包 30s 超时）|
| ~~看板挂了没人拉起~~ ✅ 2026-11-22 已解决 | 进程被杀/崩溃后服务一直停着 | `tools/dashboard_watchdog.py` 升级为 keepalive，由计划任务每 5 分钟调用；自动拉通达信+自动登录+拉看板 |
| ~~通达信每次启动都要人工点登录~~ ✅ 2026-11-22 已解决 | 客户端停在登录框 → TQ 不可用 → 看板只能走 akshare 兜底 | 新增 `tools/tdx_login.py`，用 `WM_COMMAND` 触发登录对话框默认按钮（`BM_CLICK` 对皮肤自绘的隐藏按钮无效） |
| ~~看板进程内 TQ 状态"毒化"~~ ✅ 2026-11-22 已缓解 | 进程在 TQ 未就绪时启动后，`tq.initialize` 永远失败，只能重启进程 | keepalive 拉看板前先用独立子进程探 TQ 就绪；仍失败则打标记，下一轮 TQ 就绪时重启看板 |

---

## 14. 变更历史

| 日期 | 变更 |
|---|---|
| 2026-08-08 | 从 `C:\Users\openclaw\` 根目录接管，建立 `Stock_Dashboard/` |
| 2026-08-08 | 修复 GBK emoji 崩溃、TDX 路径硬编码、静态资源 404、趋势图 NaN、折线图元素堆叠 |
| 2026-08-08 | 增加 `daily_snapshot_loop` + `period_background_loop` 后台线程 |
| 2026-08-08 | 增加 `/api/data/latest` 休市兜底 |
| 2026-08-08 | 清除所有 `_DEMO_*` 临时数据，文件清爽就绪 |
| 2026-08-08 | 编写本 LOG.md |
| 2026-08-09 | 全项目 review + P0/P1 修复：HTTPServer → ThreadingHTTPServer；handler 内统一 `_send_json_response`（锁外序列化 + 断连 try/except）；后台线程 daemon → stop_event + join；fetch_once watchdog；history series LRU 500 + `del` 裁剪；window resize rAF 节流；path traversal 返 JSON 400。|
| 2026-09-05 | 接入未用 TQ 能力：tdx_client 新增 `get_minute_bars` / `send_warn_signals` / `get_formula_indicator`（均走 `_call_with_timeout`）；看板新增 `/api/minute` 与 `/api/indicator`（400/503 语义明确）；alert_monitor 异动经 `tq.send_warn` 回推 TQ 信号窗口（`TDX_SEND_WARN` 开关，bs_flag：涨停/大单流入=0、跌停=1、其他=2）。|
| 2026-09-05 | watchdog 重写：`tools/dashboard_watchdog.py` 为权威版（路径自定位、env 对齐 start_dashboard.bat），`.hermes` 两份旧副本改 thin wrapper；`stock_flow_push_tdx.py` 时区修复（bj_now）；`main()` 启动前端口占用自检（占用时打印清理命令并 exit 2）；`background_loop` 连续超时后 `init(force=True)` 强制重连；根目录散脚本归档 `archive/tq_experiments/`；`tdx_quant.pdf` 全文提取至 `../tdx_quant.txt` 并向 `../通达信TQ策略接口列表.md` 补录 6 个遗漏接口（get_bkjy_value/get_scjy_value 等）。|
| 2026-09-07 | 盘中修复"看板内容显示不全"：① `fetch_akshare_fallback` 缺失 `return snap`（概念板块抓取代码错位成 `build_period_compare` return 后的死代码），导致 akshare 兜底恒返回 None、报错"akshare: 也不可用"——已把概念抓取 + return 移回；② akshare 1.18.x `stock_fund_flow_individual` 写死列数，10jqka 脏页混入即 ValueError——新增列数宽容版 `fetch_ths_fund_flow_individual`（复用 akshare hexin-v 签名，逐页校验列数、跳过脏页），即时 + 3/5/10/20 日排行统一走它，N 日排行改按"资金流入净额"排序；③ TDX 资金流全 0（level-1 客户端 TQ 不下发分类数据）时新增粘性跳过：连续 2 轮全 0 后 15 分钟内跳过 ~90s 的全 A 股 TDX 主拉，直接 akshare 兜底（一轮 ~30s）；④ `background_loop` watchdog 假阳性修复：`fetch_once` 正常返回也是 None，与原 `default=None` 的超时返回值不可区分，导致每轮都被误判超时、每轮强制重建 TDX 连接（此前"连续 241 轮超时"全为假阳性）——改用 `object()` 哨兵区分。akshare 顺带升级 1.18.55 → 1.18.94。|
| 2026-09-12 | 审美优化迭代（不改运行时行为）。**前端 dashboard.html 重设计**：页面重组为 5 个分区（总览/榜单/日线回看/盘中趋势/工具）并加分隔标题；大盘净流入趋势从页底提升至首屏，与 KPI×4 组成 hero 区（宽屏 400px+1fr）；行业/概念趋势宽屏并排两列；图表高度与滚动区收敛为 CSS 类（`h-trend`/`h-daily`/`scroll-h250/380`），消灭内联样式；表格加极淡斑马纹；设计令牌补 `--up-bg`/`--down-bg`；JS 清理：`toNumber`/`parseAmountToYi` 合并为 `parseYi`（无效输入 null）+ `toNumber`（兜底 0）；修复 `setMeta` 忽略形参、`renderDualIndustryTable` 死三元（恒为"行业"键）；3 个 1s setInterval 合并为单一心跳；趋势图 X/Y 轴配置抽 `TRADING_X_AXIS`/`netYAxis` 共享。**后端低风险清理 ×16**：修复 alert_monitor 独立 init 隐性 bug（`from tdx_client import TDX_EXE` → `_TDX_EXE`，原 ImportError 被内层 except 吞掉导致四种 run_mode 静默全败）；`default_watchlist` 复制循环抽 `_collect`；`detect()` 复用 `_to_float`；AlertType label 映射收敛为 `_ALERT_REASON_LABEL` 单一来源；订阅上限 `MAX_HOT_SUBSCRIBE=100`、涨跌停阈值 `LIMIT_TRIGGER_PCT/LIMIT_FULL_PCT` 具名化；tdx_client `batch_snapshot`/`batch_fund_flow` 抽 `_batch_run`、`_check_tdx_process_alive` 改用 `TDX_EXE_NAME` 常量、`_TDX_HOME` 冗余别名收敛；stock_dashboard 删 `_commit_snapshot` 死包装器与不可达 return、清陈旧注释、`YI=1e8` 常量化、`TDX_NOFLOW_*` 常量归位、`dtime` 导入上移、删三处未使用 typing/sys import。高风险区（monkey-patch、sys.argv run_mode、并发时序、print/logger）按方案不动。验证：py_compile ×3 通过、前端 JS 语法检查通过、元素 ID 完整性通过。 |
| 2026-11-22 | **自动守护(keepalive)落地**。起因：当天 07:35 看板与通达信同时哑火，此后 50 分钟无人拉起（机器 08:19 重启，两者都没自启）。**新 `tools/tdx_login.py`**：通达信每次启动都停在登录框，用 `WM_COMMAND(dlg, BN_CLICKED|IDOK, hBtn)` 触发默认按钮即可自动登录——`BM_CLICK` 对皮肤自绘的隐藏按钮无效，这是实测踩到的坑。**重写 `tools/dashboard_watchdog.py` 为 keepalive**：① 看板死了→拉通达信→自动登录→探测 TQ 就绪→拉看板；② 看板活着但 `tdx_health!=ok` 且通达信已登录→连续 3 次（15 分钟）才重启看板（进程内 TQ 状态中毒后只有重启能解）；③ 通达信未登录时不重启看板（重启无用），只提示手动登录；④ 拉看板前用独立子进程探 TQ（只试 mode=1、不显式 close，避免烧掉通达信的 mode 槽位）；⑤ 端口残留僵尸实例清理（netstat 找 PID → taskkill）；⑥ 日志滚动（>8MB 滚一代，保留 3 代）；⑦ 1 小时内最多重启 3 次的限流；⑧ 文件锁互斥（登录自启与定时任务可能撞车）；⑨ 路径用 `os.path.abspath` 而非 `Path.resolve()`——后者会把映射盘 C:\Mac\Home 展开成 UNC `\psf\Home\...`。**新增 `tools/install_keepalive.py`**：用 `schtasks.exe`（`Register-ScheduledTask` 走 WMI，普通用户会 Access denied；且 `/sc onlogon` 也需提权）注册 `Stock_Dashboard_KeepAlive`（每 5 分钟）+ `Stock_Dashboard_AutoSync`（每 5 分钟），登录自启改走 HKCU Run 注册项（无需提权）。`tools/install_task.bat` / `tools/install_keepalive.bat` 改为薄封装。README 补 keepalive 章节与行为矩阵。**后续三处修正**：① TQ 探测必须遍历所有 mode——实测通达信每个 mode(0/1/2/3) 只有一个槽位，反复 init/close 会烧掉槽位（当日 mode 1/2/3 全部耗尽只剩 mode 0），只试单个 mode 会把"可用"误判成"未就绪"并白等满 120s 超时；② 日志轮转在 Windows 上遇到文件被占用无法改名（"Device or resource busy"），原实现会静默失效——改为重试 3 次后退化成截断（顺带清掉 9 月积压的 2.8MB 旧 stderr 报错）；③ auto_sync 只在有未提交改动时才推送，手工 commit 的 commit 会被漏掉——新增 push_unpushed() 补推（空闲时每 5 分钟一次）。验证：全量故障演练（同时杀通达信+看板）→ keepalive 单次调用完成「拉起通达信→自动点登录→TQ 就绪→拉看板」，`/api/health` 恢复 `tdx_health=ok`；计划任务触发确认写入心跳日志。 |

---

## 15. 联系与维护

- 看板作者：openclaw
- 数据源版权：通达信 TdxQuant、akshare 同花顺接口
- 项目路径：`C:\Mac\Home\Desktop\win_WorkSpace_ClaudeCode\Stock_Dashboard\`
- 旧位置（已迁移）：`C:\Users\openclaw\stock_dashboard.py` 等

如需重构或与更大项目集成，请先阅读本 LOG.md 第 11/12 章。