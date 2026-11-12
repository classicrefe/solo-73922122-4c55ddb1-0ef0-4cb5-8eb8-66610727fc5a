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
- **单进程启停并发安全**：多个标签页同时启停时，停止受理即撤下报价，清理期间
  启动返回 409；并发停止共享同一次清理，断连/取消不影响后台收尾；每次启动只
  创建一个可追踪任务，旧停止操作不会清空新任务或覆盖其状态。

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

## 启停并发语义（多标签页竞争修复）

旧实现的停止流程直接操作共享的 `state.task` 句柄：多个标签页交错启停时，
旧停止请求会在 `await` 期间被新启动插入，其随后的 `state.task = None`
会清掉新任务句柄，导致再次停止也无法结束采集。现以「可追踪运行 +
停止协调器」修复（见 `collector/app.py` 的 `Run` / `start_collection` /
`stop_collection` / `_shutdown_collection`）：

| 语义 | 实现 |
|---|---|
| 受理停止即撤下报价 | `stop_collection()` 在第一个 `await` 前同步置 `status=stopping`、`book=None`，页面立即显示「停止中」且不再发布任何报价 |
| 清理期间启动返回 409 | `start_collection()` 发现 `stopping`/清理任务未结束即抛 `ConflictError`，HTTP 层返回 `409` 并附带当前状态 |
| 并发停止等待同一次清理 | `state.stop_lock` 串行化停止，同一 `Run` 只创建一个 `state.shutdown` 清理任务，其余停止请求 `await` 同一个任务 |
| 请求取消/断连后仍继续清理 | 清理任务经 `asyncio.ensure_future` 脱离请求生命周期，配合 `asyncio.shield` 等待；句柄保留在 `state.shutdown`，晚到的停止会接着等同一次清理 |
| 全部子任务结束才落 stopped | 取消主循环后回收 `Run.children` 中的 WebSocket 接收任务与在途 HTTP 快照任务，全部结束才置 `status=stopped` |
| 新任务不被旧操作清空 | 每次启动递增 `run_token` 并新建 `Run`；旧清理仅在 `state.run is run` 时落终态，旧循环发现 token 不匹配即退出，不再改状态 |
| 启动只建一个可追踪任务 | 已运行时启动幂等返回；`Run` 统一持有主循环与全部子任务句柄，不丢失 |

**交错操作演示**：网页「停止清理演示」面板可调停止清理延迟（也可用环境变量
`STOP_CLEANUP_DELAY_MS` 设默认值，Compose 默认 1500ms）。把延迟调到 3~5 秒后：

1. 标签页 A 点停止 —— 状态立刻变「停止中」、报价撤下，停止请求保持等待；
2. 标签页 B（或同页）点启动 —— 立即收到 409，提示等待完全停止；
3. A 点「取消等待」或直接关闭标签页 —— 本页放弃等待，但服务端清理继续，
   轮询仍可看到状态从「停止中」走到「已停止」；
4. 完全停止后再启动 —— 正常创建新任务并重新同步，再次停止可以正常结束。

## 控制台使用

1. 点击 **启动采集** —— 状态变为「同步中（缓存增量/取快照）」，对齐后变为「已同步 · 发布有效报价」，
   展示买卖前五档、`lastUpdateId (L)`、档位数与统计计数。
2. 点击 **停止采集** —— 状态立刻变为「停止中」并撤下全部报价；按钮请求会等待采集循环、
   WebSocket 接收与在途 HTTP 快照任务全部回收，完成后变为「已停止」并返回成功。
   多个标签页可同时点停止：所有请求等待同一次清理；点 **取消等待** 只放弃本页响应，
   服务端清理不受影响。停止清理延迟可在「停止清理演示」面板调整，用于制造交错操作窗口。
3. 故障场景开关（作用于行情源，即改即生效）：
   - **快照延迟**：快照 N ms 后才返回。延迟期间状态保持「同步中」，不就绪；恢复后自动对齐。
   - **快照失败**：快照返回 500。期间状态绝不变为「已同步」，报价停止发布，快照计数持续重试。
   - **重复消息**：每条增量发两遍。「丢弃（重复/过期）」计数上升，盘口不受影响。
   - **缺号注入**：周期性跳过更新编号。「缺号次数」上升，状态短暂变为「重新同步中」后自动恢复。
   - **断线注入**：周期性关闭 WS 连接。「重同步次数」上升，自动重连重对齐。

## API（采集端，:8000）

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/` | 控制台页面 |
| POST | `/api/control` | `{"action":"start"\|"stop"}` 启动/停止采集；停止清理期间启动返回 `409`，停止成功响应在全部子任务回收后返回 |
| GET | `/api/state` | 运行状态（含 `stopping` 停止中）、L、前五档、档位数、统计、最近异常、`cleanupDelayMs`（未同步/停止中不含报价） |
| GET/POST | `/api/config` | 查询/设置 `stop_cleanup_delay_ms`（0~30000，演示交错启停用） |
| GET/POST | `/api/scenario` | 查询/设置行情源故障场景（透传） |

状态机：`stopped → syncing → synced ⇄ resyncing`；任意运行态收到停止后
`→ stopping → stopped`，`stopping` 期间启动为 409。

行情源（:8001）：`GET /api/v3/depth?symbol=BTCUSDT&limit=100`、`WS /ws/btcusdt@depth`、`GET/POST /scenario`。

## 测试

```bash
cd collector && python -m pytest -v
```

- `test_order_book.py`：`u ≤ L` 丢弃、`U ≤ L+1 ≤ u` 应用并推进 L、缺号抛错、零量删除、数量为绝对值、100 档裁剪。
- `test_lifecycle.py`：启停并发语义——受理即撤报价、清理期 409、并发停止共享一次清理、
  停止请求取消后后台继续清理并回收全部子任务、旧清理不得覆盖新运行、启停幂等、
  HTTP 层 409/配置校验、停止中不发布报价。
