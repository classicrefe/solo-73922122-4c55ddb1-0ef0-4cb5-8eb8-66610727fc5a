# 深度行情采集演示（Binance Spot 快照 + diff depth）

面向行情数据工程的断流核查演示：单进程采集端接入**本地模拟行情源**（无需账号、无需外网），
按 Binance Spot 官方规范维护本地盘口，断流/缺号时立即停止发布并自动重同步。
浏览器控制台可启动采集、查看买卖前五档与同步状态，并一键切换故障场景。

## 架构

```
浏览器 ──► collector (FastAPI, :8000) ──WS 增量──► mock_exchange (FastAPI, :8001)
              │  └ 静态控制台 /             └ GET /api/v3/depth   HTTP 深度快照
              │     REST API                └ WS  /ws/btcusdt@depth  diff depth 增量
              └ 维护本地盘口（每侧≤100档）      └ GET/POST /scenario   故障场景开关
```

- **mock_exchange**：本地模拟行情源。HTTP 快照（`lastUpdateId` + `bids/asks`）与
  WebSocket 增量（`U/u/b/a` 字段，Binance `depthUpdate` 风格），内置 5 个故障场景。
- **collector**：采集端。先缓存 WS 增量再取 HTTP 快照，对齐后发布有效报价；
  固定交易对 `BTCUSDT`，每侧最多 100 档。

## 快速开始（Docker Compose）

```bash
docker compose up --build
# 打开 http://localhost:8000  （采集控制台，浏览器入口）
# 行情源调试入口 http://localhost:8001 （可查看 /api/v3/depth 与 /scenario）
```

无 Docker 时本地运行（Python 3.11+）：

```bash
pip install -r mock_exchange/requirements.txt -r collector/requirements.txt
uvicorn app:app --port 8001 --app-dir mock_exchange &
MOCK_HTTP=http://localhost:8001 MOCK_WS=ws://localhost:8001/ws/btcusdt@depth \
  uvicorn app:app --port 8000 --app-dir collector &
```

## 盘口同步规则（与代码对照）

| 规则 | 实现 |
|---|---|
| 先缓存 WebSocket 增量，再取 HTTP 快照 | `collector/app.py` `sync_cycle()`：`pump` 先运行，`fetch_snapshot()` 并发进行 |
| 快照编号为 L，丢弃 `u ≤ L` 的增量 | `OrderBook.apply()`：`u <= L` → 计入 `dropped` 并丢弃 |
| 仅当 `U ≤ L+1 ≤ u` 时应用并令 `L = u` | `OrderBook.apply()`：`U > L+1` 抛 `GapDetected`，否则应用并推进 L |
| 数量是价位最新总量，0 表示删除 | `OrderBook._set()`：绝对值覆盖，`qty == 0` 删除价位 |
| 每侧最多 100 档 | `OrderBook._trim()`：保留最优 100 档 |
| 缺号/断线立即停止发布并自动重同步 | `collector_loop()`：异常即 `state.book = None`（停止发布），退避后重同步 |
| 快照失败不能报就绪 | `fetch_snapshot()`：`raise_for_status()`，失败周期全程 `synced=false` |
| 在途响应不得覆盖新盘口 | 断线即取消在途快照请求 + `generation` 代际号双重防护 |

## 控制台使用

1. 点击 **启动采集** —— 状态变为「同步中（缓存增量/取快照）」，对齐后变为「已同步 · 发布有效报价」，
   展示买卖前五档、`lastUpdateId (L)`、档位数与统计计数。
2. 故障场景开关（作用于行情源，即改即生效）：
   - **快照延迟**：快照 N ms 后才返回。延迟期间状态保持「同步中」，不就绪；恢复后自动对齐。
   - **快照失败**：快照返回 500。期间状态绝不变为「已同步」，报价停止发布，快照计数持续重试。
   - **重复消息**：每条增量发两遍。「丢弃（重复/过期）」计数上升，盘口不受影响。
   - **缺号注入**：周期性跳过更新编号。「缺号次数」上升，状态短暂变为「重新同步中」后自动恢复。
   - **断线注入**：周期性关闭 WS 连接。「重同步次数」上升，自动重连重对齐。

## API（采集端，:8000）

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/` | 控制台页面 |
| POST | `/api/control` | `{"action":"start"\|"stop"}` 启动/停止采集 |
| GET | `/api/state` | 运行状态、L、前五档、档位数、统计、最近异常（未同步时不含报价） |
| GET/POST | `/api/scenario` | 查询/设置行情源故障场景（透传） |

行情源（:8001）：`GET /api/v3/depth?symbol=BTCUSDT&limit=100`、`WS /ws/btcusdt@depth`、`GET/POST /scenario`。

## 测试

```bash
cd collector && python -m pytest test_order_book.py -v
```

覆盖：`u ≤ L` 丢弃、`U ≤ L+1 ≤ u` 应用并推进 L、缺号抛错、零量删除、数量为绝对值、100 档裁剪。
