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
start_dashboard.bat
```

启动成功后：

- 后端日志写入 `logs\dashboard.stdout.log` / `logs\dashboard.stderr.log`
- 浏览器打开 `http://localhost:8765/`

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
| `DASHBOARD_BIND` | `0.0.0.0` | 绑定地址；容器化时改 `127.0.0.1` |
| `DASHBOARD_KEEP_PROXY` | `0` | `1`=保留系统代理；默认会清掉代理环境变量 |
| `STOCK_FLOW_FEISHU_WEBHOOK` | （空） | 飞书机器人 webhook；不填则异动仅输出 stdout |

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
  先看 `logs\dashboard.stderr.log`。

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
用管理员权限运行 cmd:
```bash
tools\install_task.bat
```
任务名:`Stock_Dashboard_AutoSync`,触发器:`onlogon`(用户登录时启动)

### 手动控制已注册任务
```cmd
schtasks /run  /tn "Stock_Dashboard_AutoSync"   :: 立即启动
schtasks /end  /tn "Stock_Dashboard_AutoSync"   :: 停止当前实例
schtasks /delete /tn "Stock_Dashboard_AutoSync"  :: 取消注册
```

## 已知风险

| 风险 | 触发 | 缓解 |
|---|---|---|
| 通达信客户端不可用 | 没装 TdxW.exe | 设置 `TDX_HOME` 指向现有安装；否则看板走 akshare 兜底 |
| 8765 端口冲突 | 旧实例未清 | `Get-NetTCPConnection -LocalPort 8765 -State Listen` 查 PID 后 `Stop-Process` |
| ECharts 渲染失败 | `echarts.min.js` 缺失 | 已预下载到同目录；如需恢复 CDN 引用，编辑 `dashboard.html` 头部 `<script>` |
| 0.0.0.0 暴露公网 | 共享/容器网络 | `DASHBOARD_BIND=127.0.0.1` 或经反向代理 |
| 公司代理 | 默认清代理导致外网拉取失败 | 启动前 `set DASHBOARD_KEEP_PROXY=1` |
| 飞书推送未生效 | 未设 `STOCK_FLOW_FEISHU_WEBHOOK` | 启动时会在日志里 WARNING |

## 迁移历史

- 2026-08-08：从 `C:\Users\openclaw\` 根目录接管，迁入当前目录。
- 旧 watchdog（`C:\Users\openclaw\.hermes\scripts\dashboard_watchdog.py` 与
  `C:\Users\openclaw\.hermes\skills\productivity\stock-dashboard-frontend\scripts\dashboard_watchdog.py`）
  仍硬编码守护 `C:\Users\openclaw\stock_dashboard_tdx.py`（已归档到 `archive/`），
  路径已漂移。**用户后续决定**：保留（指向新路径）/ 重写 / 弃用。

## 不在本项目内

- 飞书推送独立系统（`stock_flow_push*.py`、`stock_flow_summary.py`、`stock_flow_rank.py`）
- 通达信客户端二进制（仍装在 `C:\new_tdx64`）
- Hermes 框架（`.hermes\`）