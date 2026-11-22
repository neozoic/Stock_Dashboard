# A 股资金流实时看板（Stock_Dashboard）

[![GitHub](https://img.shields.io/badge/GitHub-neozoic%2FStock__Dashboard-181717?logo=github)](https://github.com/neozoic/Stock_Dashboard)
[![Gitee](https://img.shields.io/badge/Gitee-neozoic%2Fstock__dashboard-c71d23?logo=gitee)](https://gitee.com/neozoic/stock_-dashboard)
[![License](https://img.shields.io/badge/license-MIT-blue)](LICENSE)
[![Python](https://img.shields.io/badge/python-3.11%2B-blue?logo=python)](https://www.python.org)
[![Status](https://img.shields.io/badge/status-active-brightgreen)]()

通达信 TdxQuant 主源 + akshare 兜底的实时 A 股资金流看板，提供：

- 个股 / 行业 / 概念板块的"流入 vs 流出"对比表
- 趋势图（个股 / 行业 / 概念 / 大盘）
- 实时异动监测（大单流入、涨停、跌停、量能异动）

## 目录结构

```text
Stock_Dashboard/
├── stock_dashboard.py          # 主后端（HTTP 服务，端口默认 8765）
├── tdx_client.py               # 通达信 TdxQuant 封装
├── alert_monitor.py            # 实时异动监测
├── dashboard.html              # 前端（ECharts 5.4.3）
├── echarts.min.js              # 本地 ECharts（已预下载，断网可用）
├── start_dashboard.bat         # Windows 启动脚本
├── .env.example                # 环境变量示例
├── README.md                   # 本文件
├── logs/                       # stdout/stderr 日志输出
├── tools/
│   ├── dashboard_watchdog.py   # 看板 watchdog（权威版；.hermes 副本为 wrapper）
│   └── auto_sync.py 等          # 目录变动自动同步 GitHub/Gitee
└── archive/
    └── stock_dashboard_tdx.py  # 旧版主程序（已停用，仅作历史参考）
```

## 四件套同目录约束

`stock_dashboard.py` 通过 `os.path.dirname(__file__)` 拼出 `dashboard.html` 路径，
**`stock_dashboard.py`、`tdx_client.py`、`alert_monitor.py`、`dashboard.html` 必须放在同一目录**。
本项目已经把它们统一在 `Stock_Dashboard/` 下。

## 环境要求

| 项 | 要求 | 备注 |
|---|---|---|
| Python | 3.11–3.13 | 历史要求 3.12，本项目放宽到 3.11–3.13 区间 |
| 通达信客户端 | TdxQuant（含 `TdxW.exe` + `PYPlugins\TPythClient.dll` + `tdxrpc64.dll`） | 由 `TDX_HOME` 环境变量指定，默认 `C:\new_tdx64` |
| akshare | `pip install akshare` | 兜底数据源，需外网访问 |
| 端口 | 8765（可改） | 由 `DASHBOARD_PORT` 环境变量覆盖 |
| 时区 | 不要求系统时区为北京时间 | 所有时间逻辑内部 `BJ_TZ = timezone(+8)` |

## 启动

### Windows（推荐）

```bat
cd C:\Mac\Home\Desktop\win_WorkSpace_ClaudeCode\Stock_Dashboard
start_oneclick.bat
```

一键启动脚本：后台拉起后端 → 等待 `/api/health` 就绪（最长 40 秒）→ 自动打开浏览器。
已运行时会直接打开浏览器，不会重复启动。停止看板用 `stop_dashboard.bat`（按端口找到进程并结束）。

也可以用旧的 `start_dashboard.bat`（前台运行，日志写入 `logs\`，窗口不能关）。

启动成功后：

- 业务日志写入 `logs\dashboard.log`(由 Python 自己轮转,保留 3 代);
  `logs\dashboard.stdout.log` / `logs\dashboard.stderr.log` 只收启动横幅与未捕获异常
- 浏览器打开 `http://localhost:8765/`

> 手动启动只保证"这一次在跑"。想让看板在崩溃/重启后自动回来,见下面的
> [自动守护(keepalive)](#自动守护keepalive)—— 装好后登录即自启,
> 之后每 5 分钟体检一次。

### 直接命令行

```bat
cd C:\Mac\Home\Desktop\win_WorkSpace_ClaudeCode\Stock_Dashboard
python stock_dashboard.py
```

## 配置（环境变量）

| 环境变量 | 默认值 | 说明 |
|---|---|---|
| `TDX_HOME` | `C:\new_tdx64` | 通达信客户端安装目录 |
| `TDX_EXE_NAME` | `TdxW.exe` | 通达信客户端进程名 |
| `TDX_CALL_TIMEOUT` | `8` | TdxQuant 单次调用超时（秒）。盘中可调小（5），盘后建议放大（12） |
| `DASHBOARD_PORT` | `8765` | 看板 HTTP 端口 |
| `DASHBOARD_BIND` | `127.0.0.1` | 绑定地址；两个 .bat 启动脚本显式设为 `0.0.0.0` 以便局域网访问，公网环境务必改回 |
| `DASHBOARD_KEEP_PROXY` | `0` | `1`=保留系统代理；默认会清掉代理环境变量 |
| `STOCK_FLOW_FEISHU_WEBHOOK` | （空） | 飞书机器人 webhook；不填则异动仅输出 stdout |
| `TDX_SEND_WARN` | `1` | 异动信号回推通达信 TQ 策略信号窗口（tq.send_warn）；`0`=关闭 |

修改 `.env.example` 或在 `start_dashboard.bat` 中调整默认值。

## HTTP 接口

| 路径 | 方法 | 说明 |
|---|---|---|
| `/` / `/index.html` | GET | 看板前端页面 |
| `/api/health` | GET | 健康检查：`tdx_client_available`、`akshare_available`、`current_source`、`last_timestamp` |
| `/api/data` | GET | 最新快照：`stock_top/bottom`、`industry_top/bottom`、`concept_top/bottom` |
| `/api/trend` | GET | 趋势数据：`timestamps`、`stock`、`industry`、`concept`、`market` |
| `/api/history` | GET | 全部历史（仅开发用） |
| `/api/alerts` | GET | 实时异动监测状态 |
| `/api/minute?code=600519.SH&period=1m&count=N` | GET | 单股/板块 K线分时（TDX 本地数据；无数据返回空数组，TDX 不可用 503）|
| `/api/indicator?code=600519.SH&formula=KDJ&arg=9,3,3` | GET | 内置公式指标（MA/KDJ/MACD/BOLL/VOL/RSI/WR/DMA/ASI；白名单外 400，TDX 不可用 503）|

## 数据源策略

- **盘中（9:30-11:30, 13:00-15:00 北京时间）**：优先用通达信 TdxQuant；
  当资金流全 0 时自动 fallback 到 akshare（同花顺源）。
- **盘后 / 通达信不可用**：自动走 akshare 兜底。
- 监控 `GET /api/health` 看 `current_source` 字段（`tdx` / `akshare` / `mixed(tdx+akshare)`）。

## 浏览器打开后常见情况

- **绿点 + "交易中"**：通达信在工作，资金流正常。
- **红点 + "非交易时段"**：当前北京时间不在交易时段（盘前/午休/收盘后），
  看板表格仍会显示最近一次快照，但趋势图只有历史点。
- **"❌ 数据获取失败"**：`fetch('/api/data')` 失败，通常是后端未启动或端口冲突；
  先看 `logs\dashboard.log`(业务日志)与 `logs\dashboard.stderr.log`(未捕获异常)。

## 详细变更与维护日志

见 [`LOG.md`](LOG.md)，包含：

- 完整文件清单
- 19 + 13 + 5 项代码改造清单（含行号 + 修复）
- 11 个 HTTP 接口、4 个后台线程、数据流时序
- 7 个环境变量、3 种集成方案
- 8 个常见问题排查指南
- 安全审查清单（公网部署前必做）
- 已知遗留问题 + 技术债登记

## 自动同步到 GitHub + Gitee

`tools/auto_sync.py` 监控目录变动,debounce 5 分钟后自动 commit + push 到双 remote。

### 触发阈值(可在 `tools/auto_sync.py` 顶部修改)
- `MIN_FILES = 1`  (至少 1 个文件变动)
- `MIN_LINES = 10` (或至少 10 行 diff)
- `DEBOUNCE_SECONDS = 300` (变动后 5 分钟无新变动才提交)

### 三种使用方式

**1. 一次性手动同步**
```bash
tools\sync_once.bat
```

**2. 后台服务(本次会话内)**
```bash
tools\start_sync.bat
```
日志:`logs\auto_sync.log`

**3. Task Scheduler 开机自启(推荐)**
普通用户直接跑(不需要管理员权限):
```bash
tools\install_task.bat
```
等价于 `tools\install_keepalive.bat`,会注册下面两张表里的全部任务。
载:`python tools\install_keepalive.py --uninstall`

---

## 自动守护(keepalive)

看板是长驻服务,进程会被重启/崩溃/断网带走。`tools/dashboard_watchdog.py`
是一个幂等的"体检 + 修复"脚本,由计划任务定时调用。

| 任务/自启项 | 触发 | 作用 |
|---|---|---|
| `Stock_Dashboard_KeepAlive` | 每 5 分钟 | 体检 + 修复(见下) |
| `Stock_Dashboard_Startup` (HKCU Run) | 用户登录时 | 拉起通达信 + 看板 |
| `Stock_Dashboard_AutoSync` | 每 5 分钟 | 自动 commit/push(自带单实例锁) |
| `Stock_Dashboard_AutoSync` (HKCU Run) | 用户登录时 | 同上 |

keepalive 的行为矩阵:

| 看板(8765) | 通达信 | 动作 |
|---|---|---|
| 活 + `tdx_health=ok` | — | 静默,只在 `logs\keepalive.log` 写心跳 |
| 活 + `tdx_health=fail` | 未登录 | 不重启(重启无用),日志提示手动点登录 |
| 活 + `tdx_health=fail` | 已登录 | 连续 3 次(15 分钟)→ 重启看板进程 |
| 死 | 死 | 拉起通达信 → 自动点登录 → 等 TQ 就绪 → 拉起看板 |
| 死 | 活 | 自动点登录(若需要) → 等 TQ 就绪 → 拉起看板 |

几个关键点:

- **通达信每次启动都停在登录框**,keepalive 会用 `WM_COMMAND` 触发登录对话框的
  默认按钮(`tools/tdx_login.py`),无需人工点击。
- **看板进程在 TQ 未就绪时启动会被"毒化"** —— 进程内 `tq.initialize` 一旦失败就
  再也连不上,只能重启进程。所以 keepalive 会先用独立子进程探测 TQ 就绪,
  再拉看板。
- **限流**:15 分钟内最多重启 3 次(挡住崩溃循环的秒级重启)+ 1 小时内最多 10 次(防缓慢抖动)。
  旧版只看"1 小时内绝对次数",早上的重启记录会一直占着预算,导致看板稳定运行后
  一次正常杀进程也拉不起来。
- **TQ 槽位会被"烧掉"**:通达信每个 mode(0/1/2/3) 只有一个槽位,持有它的进程被
  强杀时槽位不释放(下次 `init` 直接返回"返回ID小于0")——每强杀一次看板就烧一个,
  4 个烧光 TQ 就再也起不来。所以重启看板走 `POST /api/shutdown`(仅本机 loopback)
  优雅退出:走 finally 分支 `tdx_client.close()` 正常归还槽位;退不掉才强杀。
  万一槽位真的耗尽(探测失败但通达信已登录),keepalive 会重启通达信客户端重置槽位。
- **人工调试也占预算**:自己手动重启看板几次后,keepalive 会判定为崩溃循环而拒绝拉起。
  用 `python tools\dashboard_watchdog.py --reset-budget` 清空限流计数并立即恢复服务。
- **日志滚动**:业务日志 `logs\dashboard.log` 由看板自己的 RotatingFileHandler 轮转
  (8MB 一代,保留 3 代)。`dashboard.stdout.log` / `dashboard.stderr.log` 是外层重定向的
  应急输出,被看板占用时 Windows 改不了名 —— 此时只告警、**不截断**:截断会让写入方
  偏移量不回退,在文件头留下一整段 NUL 空洞。

常用环境变量(改 `tools\install_keepalive.bat` 或系统环境变量):

| 变量 | 默认 | 说明 |
|---|---|---|
| `DASHBOARD_LOG_MAX_BYTES` | `8388608` | 日志滚动阈值(看板 `dashboard.log` 与 keepalive 共用) |
| `DASHBOARD_LOG_KEEP` | `3` | 保留几代日志 |
| `DASHBOARD_TDX_FAIL_THRESHOLD` | `3` | TQ 连续异常几次才重启看板 |
| `DASHBOARD_MAX_RESTARTS_BURST` | `3` | 突发窗口内最多重启次数(崩溃循环保护) |
| `DASHBOARD_RESTART_BURST_WINDOW` | `900` | 突发窗口秒数(默认 15 分钟) |
| `DASHBOARD_MAX_RESTARTS_PER_HOUR` | `10` | 每小时最多重启次数(防缓慢抖动) |
| `DASHBOARD_MAX_TDX_RESTARTS_PER_HOUR` | `1` | 槽位耗尽时重启通达信客户端的次数上限 |

### 手动控制已注册任务
```cmd
schtasks /run  /tn "Stock_Dashboard_KeepAlive"   :: 立即体检一次
schtasks /end  /tn "Stock_Dashboard_KeepAlive"   :: 停止当前实例
schtasks /delete /tn "Stock_Dashboard_KeepAlive"  :: 取消注册
python tools\install_keepalive.py --uninstall      :: 卸载全部任务与自启项
```

## 已知风险

| 风险 | 触发 | 缓解 |
|---|---|---|
| 通达信客户端不可用 | 没装 TdxW.exe | 设置 `TDX_HOME` 指向现有安装；否则看板走 akshare 兜底 |
| 通达信停在登录框 | 客户端每次启动都要点一次登录 | keepalive 会自动点(`tools/tdx_login.py`)；没装 keepalive 就手动点一下 |
| 看板进程活着但 TQ 连不上 | 进程在 TQ 未就绪时启动，状态被"毒化" | keepalive 检测到连续 3 次异常会重启看板；手动可 `stop_dashboard.bat` 后重启 |
| 8765 端口冲突 | 旧实例未清 | `Get-NetTCPConnection -LocalPort 8765 -State Listen` 查 PID 后 `Stop-Process` |
| ECharts 渲染失败 | `echarts.min.js` 缺失 | 已预下载到同目录；如需恢复 CDN 引用，编辑 `dashboard.html` 头部 `<script>` |
| 0.0.0.0 暴露公网 | 共享/容器网络 | `DASHBOARD_BIND=127.0.0.1` 或经反向代理 |
| 公司代理 | 默认清代理导致外网拉取失败 | 启动前 `set DASHBOARD_KEEP_PROXY=1` |
| 飞书推送未生效 | 未设 `STOCK_FLOW_FEISHU_WEBHOOK` | 启动时会在日志里 WARNING |

## 迁移历史

- 2026-08-08：从 `C:\Users\openclaw\` 根目录接管，迁入当前目录。
- 2026-09-05：watchdog 已重写解决路径漂移——权威版为 `tools/dashboard_watchdog.py`
  （路径自定位，env 对齐 `start_dashboard.bat`），`.hermes` 下两份旧副本改为
  thin wrapper 转发调用，hermes cron 注册无需变更。

## 不在本项目内

- 飞书推送独立系统（`stock_flow_push*.py`、`stock_flow_summary.py`、`stock_flow_rank.py`）
- 通达信客户端二进制（仍装在 `C:\new_tdx64`）
- Hermes 框架（`.hermes\`）