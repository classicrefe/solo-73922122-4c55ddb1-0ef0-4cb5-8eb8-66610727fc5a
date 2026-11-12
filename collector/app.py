"""深度行情采集服务（FastAPI 单进程）。

对齐 Binance Spot 本地盘口维护规范：
  1. 先连 WebSocket 缓存增量 depthUpdate，再取 HTTP 深度快照；
  2. 快照 lastUpdateId 记为 L，增量含起止编号 U/u；
  3. 丢弃 u <= L 的增量；仅当 U <= L+1 <= u 时应用，并令 L = u；
  4. 增量中的数量是该价位的最新总量，0 表示删除该价位；每侧最多保留 100 档；
  5. 缺号（U > L+1）或断线：立即停止发布有效报价并自动重新同步；
  6. 快照失败绝不上报就绪；重连前的在途快照响应不得覆盖新盘口（代际号防护）。

启停并发语义（多标签页同时操作也安全）：
  - 受理停止即撤下报价（book=None，status=stopping），清理期间启动返回 409；
  - 同一次停止只清理一次，并发停止请求等待同一个清理任务结束；
  - 停止请求被客户端取消/断连后，清理在后台继续：必须等采集循环、WebSocket
    接收任务与在途 HTTP 快照任务全部结束，才落 stopped 终态并返回停止成功；
  - 每次启动只创建一个可追踪的 Run（主循环 + 全部子任务句柄），旧停止操作凭
    run token / 代际号防护，不得清空新任务或覆盖新任务的状态。
"""
from __future__ import annotations

import asyncio
import json
import os
from contextlib import asynccontextmanager
from pathlib import Path

import httpx
import websockets
from fastapi import FastAPI
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel
from websockets.exceptions import WebSocketException

SYMBOL = "BTCUSDT"
MAX_LEVELS = 100
MOCK_HTTP = os.getenv("MOCK_HTTP", "http://localhost:8001")
MOCK_WS = os.getenv("MOCK_WS", "ws://localhost:8001/ws/btcusdt@depth")
SNAPSHOT_TIMEOUT = 30.0
RESYNC_BACKOFF = 1.0
# 停止清理延迟（秒）：仅用于演示交错启停，可经 /api/config 在线调整
STOP_CLEANUP_DELAY_MS = int(os.getenv("STOP_CLEANUP_DELAY_MS", "0"))
MAX_CLEANUP_DELAY_MS = 30000

STATIC_INDEX = Path(__file__).parent / "static" / "index.html"


class GapDetected(Exception):
    """增量编号与本地 L 不连续（U > L+1）。"""


class ResyncRequired(Exception):
    """连接中断等需要重新同步的情况。"""


class ConflictError(Exception):
    """清理期间收到启动请求等冲突操作（HTTP 409）。"""


class OrderBook:
    """本地盘口：数量为价位最新总量，0 删除；每侧最多 MAX_LEVELS 档。"""

    def __init__(self) -> None:
        self.bids: dict[float, float] = {}
        self.asks: dict[float, float] = {}
        self.last_update_id: int | None = None  # L

    def load_snapshot(self, snapshot: dict) -> None:
        self.bids = {float(p): float(q) for p, q in snapshot["bids"]}
        self.asks = {float(p): float(q) for p, q in snapshot["asks"]}
        self.last_update_id = int(snapshot["lastUpdateId"])
        self._trim()

    def apply(self, event: dict, stats: dict) -> None:
        U, u = int(event["U"]), int(event["u"])
        L = self.last_update_id
        assert L is not None, "快照未加载"
        if u <= L:
            stats["dropped"] += 1  # 重复/过期消息，直接丢弃
            return
        if U > L + 1:
            raise GapDetected(f"缺号: U={U} > L+1={L + 1}")
        # 满足 U <= L+1 <= u，应用并推进 L
        for p, q in event.get("b", []):
            self._set(self.bids, float(p), float(q))
        for p, q in event.get("a", []):
            self._set(self.asks, float(p), float(q))
        self.last_update_id = u
        stats["applied"] += 1
        self._trim()

    @staticmethod
    def _set(side: dict[float, float], price: float, qty: float) -> None:
        if qty == 0.0:
            side.pop(price, None)
        else:
            side[price] = qty

    def _trim(self) -> None:
        if len(self.bids) > MAX_LEVELS:
            self.bids = dict(sorted(self.bids.items(), key=lambda kv: kv[0], reverse=True)[:MAX_LEVELS])
        if len(self.asks) > MAX_LEVELS:
            self.asks = dict(sorted(self.asks.items(), key=lambda kv: kv[0])[:MAX_LEVELS])

    def top(self, n: int) -> tuple[list[list[str]], list[list[str]]]:
        bids = [[f"{p:.1f}", f"{q:.4f}"]
                for p, q in sorted(self.bids.items(), key=lambda kv: kv[0], reverse=True)[:n]]
        asks = [[f"{p:.1f}", f"{q:.4f}"]
                for p, q in sorted(self.asks.items(), key=lambda kv: kv[0])[:n]]
        return bids, asks


class Run:
    """一次采集运行的全部可追踪句柄。

    task     采集主循环（collector_loop）
    children 该运行派生的 WebSocket 接收任务与在途 HTTP 快照任务，
             停止时必须全部回收，运行结束前不得丢失句柄。
    """

    def __init__(self, token: int) -> None:
        self.token = token
        self.task: asyncio.Task | None = None
        self.children: set[asyncio.Task] = set()

    def track(self, task: asyncio.Task) -> asyncio.Task:
        self.children.add(task)
        task.add_done_callback(self.children.discard)
        return task


class CollectorState:
    def __init__(self) -> None:
        # stopped / syncing / synced / resyncing / stopping
        self.status = "stopped"
        self.book: OrderBook | None = None    # 仅在 synced 时对外发布，其余时间为 None
        self.generation = 0                   # 代际号：防止在途旧响应覆盖新盘口
        self.run: Run | None = None           # 当前运行；同一时刻至多一个
        self.run_token = 0                    # 每次启动递增，旧任务据此识别自己已过期
        self.stop_lock = asyncio.Lock()       # 串行化停止，并发停止共用同一次清理
        self.shutdown: asyncio.Task | None = None  # 正在进行的清理任务（断连也保留）
        self.cleanup_delay = STOP_CLEANUP_DELAY_MS / 1000
        self.last_error: str | None = None
        self.stats = {
            "applied": 0,            # 已应用增量
            "dropped": 0,            # 丢弃的重复/过期消息
            "gaps": 0,               # 缺号次数
            "resyncs": 0,            # 重同步次数
            "snapshotAttempts": 0,   # 快照尝试次数
            "snapshotOk": 0,         # 快照成功次数
        }

    @property
    def running(self) -> bool:
        return self.status not in ("stopped", "stopping")


state = CollectorState()


async def fetch_snapshot() -> dict:
    state.stats["snapshotAttempts"] += 1
    async with httpx.AsyncClient(timeout=SNAPSHOT_TIMEOUT) as client:
        resp = await client.get(
            f"{MOCK_HTTP}/api/v3/depth",
            params={"symbol": SYMBOL, "limit": MAX_LEVELS},
        )
        resp.raise_for_status()  # 快照失败：抛错，外层重试，期间不就绪
        data = resp.json()
    state.stats["snapshotOk"] += 1
    return data


async def sync_cycle(run: Run) -> None:
    """一轮「连接 → 缓存增量 → 取快照 → 对齐 → 直播」周期；异常交由外层重同步。"""
    state.generation += 1
    gen = state.generation
    state.status = "syncing"

    buffer: list[dict] = []
    closed = asyncio.Event()
    item_ready = asyncio.Event()

    async def pump(ws) -> None:
        try:
            async for raw in ws:
                buffer.append(json.loads(raw))
                item_ready.set()
        finally:
            closed.set()
            item_ready.set()

    async with websockets.connect(MOCK_WS, ping_interval=20, ping_timeout=20) as ws:
        # WebSocket 接收任务纳入运行句柄，停止时必须等待其结束
        pump_task = run.track(asyncio.create_task(pump(ws)))
        fetch_task: asyncio.Task | None = None
        try:
            # 步骤一：pump 已在缓存 WebSocket 增量；步骤二：并发取 HTTP 快照
            # 在途快照任务同样纳入运行句柄，停止时等待其被取消并回收
            fetch_task = run.track(asyncio.create_task(fetch_snapshot()))
            closed_task = asyncio.create_task(closed.wait())
            await asyncio.wait({fetch_task, closed_task}, return_when=asyncio.FIRST_COMPLETED)
            closed_task.cancel()
            if closed.is_set():
                # 快照在途期间断线：作废本次在途响应，直接重同步
                fetch_task.cancel()
                raise ResyncRequired("快照在途期间连接断开")
            snapshot = fetch_task.result()  # 快照失败在此抛错，绝不将就绪置真
            if run.token != state.run_token or gen != state.generation:
                return  # 在途旧响应/旧运行不得覆盖新盘口

            book = OrderBook()
            book.load_snapshot(snapshot)

            # 步骤三：对齐缓存增量，随后进入直播
            i = 0
            published = False
            while True:
                while i < len(buffer):
                    book.apply(buffer[i], state.stats)  # 缺号抛 GapDetected
                    i += 1
                if closed.is_set():
                    raise ResyncRequired("连接断开")
                if not published:
                    if run.token != state.run_token or gen != state.generation:
                        return  # 在途旧周期不得发布
                    state.book = book  # 对齐完成，开始发布有效报价
                    state.status = "synced"
                    state.last_error = None
                    published = True
                item_ready.clear()
                if i < len(buffer) or closed.is_set():
                    continue
                await item_ready.wait()
        finally:
            pump_task.cancel()
            if fetch_task is not None and not fetch_task.done():
                fetch_task.cancel()  # 停止/重同步时取消在途快照请求


async def collector_loop(run: Run) -> None:
    try:
        while True:
            try:
                await sync_cycle(run)
            except asyncio.CancelledError:
                raise
            except GapDetected as exc:
                state.stats["gaps"] += 1
                state.last_error = str(exc)
            except (ResyncRequired, WebSocketException, OSError) as exc:
                state.last_error = f"连接问题: {exc}"
            except httpx.HTTPError as exc:
                state.last_error = f"快照失败: {exc}"
            except Exception as exc:  # noqa: BLE001
                state.last_error = f"未预期错误: {exc!r}"
            # 旧运行已被新启动取代：不得再修改新运行的状态
            if run.token != state.run_token:
                break
            # 缺号/断线/快照失败：立即停止发布有效报价
            state.book = None
            state.status = "resyncing"
            state.stats["resyncs"] += 1
            await asyncio.sleep(RESYNC_BACKOFF)
    except asyncio.CancelledError:
        # 停止清理协调器负责落 stopped 终态，这里只负责安静退出
        return


async def start_collection() -> None:
    """启动采集：清理期间冲突返回 409；已在运行则幂等；每次只建一个 Run。"""
    if state.status == "stopping" or (
        state.shutdown is not None and not state.shutdown.done()
    ):
        raise ConflictError("采集正在停止清理中，请等待完全停止后再启动")
    if state.run is not None:
        return  # 已在运行：幂等返回，绝不重复创建任务

    state.run_token += 1
    run = Run(state.run_token)
    state.run = run
    state.book = None
    state.last_error = None
    state.status = "syncing"
    run.task = asyncio.create_task(collector_loop(run))


async def _shutdown_collection(run: Run) -> None:
    """一次停止的完整清理：主循环 → WS 接收 / 在途快照子任务全部回收后落终态。

    本任务由 stop_collection 以脱离请求生命周期的方式托管，请求取消或断连
    都不会中断它；finally 中的 run 身份检查保证旧操作不会覆盖之后启动的新运行。
    """
    try:
        if run.task is not None:
            run.task.cancel()
        if state.cleanup_delay > 0:
            # 演示用可调清理延迟：拉长“停止中”窗口，便于在多个标签页交错启停
            await asyncio.sleep(state.cleanup_delay)
        if run.task is not None:
            try:
                await run.task
            except asyncio.CancelledError:
                pass
            except Exception as exc:  # noqa: BLE001
                state.last_error = f"采集循环异常退出: {exc!r}"
        # 兜底回收子任务（正常路径下 sync_cycle 的 finally 已取消它们）：
        # 必须等 WebSocket 接收与在途 HTTP 快照全部结束
        while run.children:
            pending = [t for t in run.children if not t.done()]
            if not pending:
                break
            for task in pending:
                task.cancel()
            await asyncio.gather(*pending, return_exceptions=True)
    finally:
        # 代际防护：仅当当前运行仍是本次停止的运行时才落终态，
        # 旧停止操作不得清空新任务或覆盖其状态
        if state.run is run:
            state.status = "stopped"
            state.book = None
            state.run = None


async def stop_collection() -> None:
    """停止采集。

    - 受理即撤下报价并进入 stopping；同一次清理只做一次，并发停止共享同一个
      后台清理任务并等待其结束；
    - 等待期间请求被取消/断连只影响调用方，清理在后台继续，句柄保留在
      state.shutdown，下一个停止请求会接着等同一个清理。
    """
    async with state.stop_lock:
        run = state.run
        if run is None:
            # 并发停止晚到一步：清理已结束，幂等返回
            state.status = "stopped"
            state.book = None
            if state.shutdown is not None and state.shutdown.done():
                state.shutdown = None
            return

        shutdown = state.shutdown
        if shutdown is None or shutdown.done():
            # 受理停止：在第一个 await 之前同步撤下报价
            state.status = "stopping"
            state.book = None
            # 脱离请求生命周期托管：客户端断连后清理仍继续
            shutdown = asyncio.ensure_future(_shutdown_collection(run))
            state.shutdown = shutdown

        try:
            # shield：外层请求任务被取消时，内部清理任务不受影响
            await asyncio.shield(shutdown)
        except asyncio.CancelledError:
            raise  # 放弃本次响应，但 state.shutdown 仍持有正在进行的清理
        if state.shutdown is shutdown:
            state.shutdown = None


# ------------------------------- HTTP API -------------------------------

@asynccontextmanager
async def lifespan(_: FastAPI):
    yield
    # 进程退出（含容器停止）：若仍有运行中的采集，按同一套清理流程回收全部句柄
    if state.run is not None:
        await _shutdown_collection(state.run)


app = FastAPI(title="Depth Collector", lifespan=lifespan)


class ControlBody(BaseModel):
    action: str  # start / stop


class ScenarioBody(BaseModel):
    snapshot_delay_ms: int = 0
    snapshot_fail: bool = False
    duplicates: bool = False
    gap: bool = False
    disconnect: bool = False


class ConfigBody(BaseModel):
    stop_cleanup_delay_ms: int | None = None


@app.get("/")
async def index():
    return FileResponse(STATIC_INDEX)


@app.post("/api/control")
async def api_control(body: ControlBody):
    if body.action == "start":
        try:
            await start_collection()
        except ConflictError as exc:
            return JSONResponse(
                status_code=409,
                content={"error": str(exc), "status": state.status},
            )
    elif body.action == "stop":
        await stop_collection()
    else:
        return JSONResponse(status_code=400, content={"error": "action 须为 start/stop"})
    return {"ok": True, "running": state.running, "status": state.status}


@app.get("/api/state")
async def api_state():
    synced = state.status == "synced" and state.book is not None
    if synced:
        bids, asks = state.book.top(5)
        last_update_id = state.book.last_update_id
        levels = {"bids": len(state.book.bids), "asks": len(state.book.asks)}
    else:
        # 未同步/停止中：不发布任何报价，下游不得使用
        bids, asks, last_update_id, levels = [], [], None, {"bids": 0, "asks": 0}
    return {
        "running": state.running,
        "status": state.status,
        "synced": synced,
        "symbol": SYMBOL,
        "lastUpdateId": last_update_id,
        "bids": bids,
        "asks": asks,
        "levels": levels,
        "stats": state.stats,
        "lastError": state.last_error,
        "cleanupDelayMs": int(state.cleanup_delay * 1000),
        "source": {"http": MOCK_HTTP, "ws": MOCK_WS},
    }


@app.get("/api/config")
async def api_get_config():
    return {"stop_cleanup_delay_ms": int(state.cleanup_delay * 1000)}


@app.post("/api/config")
async def api_set_config(body: ConfigBody):
    delay_ms = body.stop_cleanup_delay_ms
    if delay_ms is None:
        return JSONResponse(status_code=400, content={"error": "缺少 stop_cleanup_delay_ms"})
    if not 0 <= delay_ms <= MAX_CLEANUP_DELAY_MS:
        return JSONResponse(
            status_code=400,
            content={"error": f"stop_cleanup_delay_ms 须在 0~{MAX_CLEANUP_DELAY_MS} 之间"},
        )
    state.cleanup_delay = delay_ms / 1000
    return {"ok": True, "stop_cleanup_delay_ms": delay_ms}


@app.get("/api/scenario")
async def api_get_scenario():
    try:
        async with httpx.AsyncClient(timeout=5) as client:
            resp = await client.get(f"{MOCK_HTTP}/scenario")
            resp.raise_for_status()
            return resp.json()
    except httpx.HTTPError as exc:
        return JSONResponse(status_code=502, content={"error": f"行情源不可达: {exc}"})


@app.post("/api/scenario")
async def api_set_scenario(body: ScenarioBody):
    try:
        async with httpx.AsyncClient(timeout=5) as client:
            resp = await client.post(f"{MOCK_HTTP}/scenario", json=body.model_dump())
            resp.raise_for_status()
            return resp.json()
    except httpx.HTTPError as exc:
        return JSONResponse(status_code=502, content={"error": f"行情源不可达: {exc}"})
