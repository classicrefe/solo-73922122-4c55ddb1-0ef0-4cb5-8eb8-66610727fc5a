"""本地模拟行情源（无需账号、无需外网）。

模拟 Binance Spot 的两类接口：
  - HTTP 深度快照:  GET /api/v3/depth?symbol=BTCUSDT&limit=100
  - WebSocket 增量: WS  /ws/btcusdt@depth   (depthUpdate, 字段 U/u/b/a)

内置故障场景开关（POST /scenario）：
  snapshot_delay_ms  快照响应延迟（先按请求时刻取数，再延迟返回，模拟在途慢响应）
  snapshot_fail      快照返回 HTTP 500
  duplicates         每条增量消息重复发送一次
  gap                周期性跳过更新编号（制造缺号）
  disconnect         周期性关闭全部 WebSocket 连接
"""
from __future__ import annotations

import asyncio
import random
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse
from pydantic import BaseModel

SYMBOL = "BTCUSDT"
TICK = 0.1
MAX_BAND = 150          # 模拟盘口维持在中间价 ±150 个 tick
SNAPSHOT_LIMIT = 100
TICK_INTERVAL = 0.2     # 增量推送间隔（秒）


class Scenario(BaseModel):
    snapshot_delay_ms: int = 0
    snapshot_fail: bool = False
    duplicates: bool = False
    gap: bool = False
    disconnect: bool = False


scenario = Scenario()

mid_price = 42000.0
bids: dict[float, float] = {}
asks: dict[float, float] = {}
next_update_id = 1
event_count = 0
clients: set[WebSocket] = set()
lock = asyncio.Lock()


def seed_book() -> None:
    for i in range(1, MAX_BAND + 1):
        bids[round(mid_price - i * TICK, 1)] = round(random.uniform(0.05, 8.0), 4)
        asks[round(mid_price + i * TICK, 1)] = round(random.uniform(0.05, 8.0), 4)


def mutate_book() -> tuple[list[list[str]], list[list[str]]]:
    """对模拟盘口做 1~3 笔随机修改，返回 diff depth 的 b/a 变更列表。"""
    global mid_price
    if random.random() < 0.02:  # 中间价缓慢漂移
        mid_price = round(mid_price + random.choice((-1, 1)) * TICK, 1)
        # 漂移后清理穿越档，保持买侧 < 卖侧
        for p in [p for p in bids if p >= mid_price]:
            bids.pop(p)
        for p in [p for p in asks if p <= mid_price]:
            asks.pop(p)
    b_changes: list[list[str]] = []
    a_changes: list[list[str]] = []
    for _ in range(random.randint(1, 3)):
        side = random.choice(("b", "a"))
        offset = random.randint(1, MAX_BAND)
        price = round(mid_price - offset * TICK, 1) if side == "b" else round(mid_price + offset * TICK, 1)
        book = bids if side == "b" else asks
        if random.random() < 0.12:
            qty = 0.0  # 数量为 0 表示删除该价位
            book.pop(price, None)
        else:
            qty = round(random.uniform(0.01, 8.0), 4)
            book[price] = qty  # 数量为该价位的最新总量（绝对值）
        (b_changes if side == "b" else a_changes).append([f"{price:.1f}", f"{qty:.4f}"])
    # 清理远离中间价的档位
    for p in [p for p in bids if p < mid_price - MAX_BAND * TICK]:
        bids.pop(p)
    for p in [p for p in asks if p > mid_price + MAX_BAND * TICK]:
        asks.pop(p)
    return b_changes, a_changes


async def broadcast(event: dict) -> None:
    dead = []
    for ws in list(clients):
        try:
            await ws.send_json(event)
            if scenario.duplicates:
                await ws.send_json(event)  # 重复消息场景：同一条再发一遍
        except Exception:
            dead.append(ws)
    for ws in dead:
        clients.discard(ws)


async def close_all_clients() -> None:
    for ws in list(clients):
        try:
            await ws.close(code=1011, reason="mock disconnect scenario")
        except Exception:
            pass
    clients.clear()


async def market_loop() -> None:
    global next_update_id, event_count
    while True:
        await asyncio.sleep(TICK_INTERVAL)
        async with lock:
            b_changes, a_changes = mutate_book()
            event_count += 1
            if scenario.gap and event_count % 12 == 0:
                # 缺号场景：跳过 2~5 个更新编号，采集端将看到 U > L+1
                next_update_id += random.randint(2, 5)
            event = {
                "e": "depthUpdate",
                "E": int(time.time() * 1000),
                "s": SYMBOL,
                "U": next_update_id,  # 本事件起始更新编号
                "u": next_update_id,  # 本事件结束更新编号
                "b": b_changes,
                "a": a_changes,
            }
            next_update_id += 1
            await broadcast(event)
            if scenario.disconnect and event_count % 40 == 0:
                # 断线场景：广播后关闭全部连接
                await close_all_clients()


@asynccontextmanager
async def lifespan(_: FastAPI):
    seed_book()
    task = asyncio.create_task(market_loop())
    yield
    task.cancel()


app = FastAPI(title="Mock Binance Spot Market Source", lifespan=lifespan)


@app.get("/api/v3/depth")
async def depth(symbol: str = SYMBOL, limit: int = SNAPSHOT_LIMIT):
    """Binance Spot GET /api/v3/depth 风格快照。

    先在锁内按当前时刻取数（lastUpdateId = 已生成的最大更新编号），
    再按场景配置延迟返回，用于验证采集端“先缓存增量再对齐快照”的逻辑。
    """
    async with lock:
        snapshot = {
            "lastUpdateId": next_update_id - 1,
            "bids": [[f"{p:.1f}", f"{q:.4f}"]
                     for p, q in sorted(bids.items(), key=lambda kv: kv[0], reverse=True)[:limit]],
            "asks": [[f"{p:.1f}", f"{q:.4f}"]
                     for p, q in sorted(asks.items(), key=lambda kv: kv[0])[:limit]],
        }
        delay_ms = scenario.snapshot_delay_ms
        fail = scenario.snapshot_fail
    if delay_ms > 0:
        await asyncio.sleep(delay_ms / 1000)
    if fail:
        return JSONResponse(status_code=500, content={"code": -1000, "msg": "mock snapshot failure"})
    return snapshot


@app.websocket("/ws/{stream}")
async def ws_endpoint(websocket: WebSocket, stream: str):
    await websocket.accept()
    clients.add(websocket)
    try:
        while True:
            await websocket.receive_text()  # 客户端不发送数据，仅用于感知断开
    except (WebSocketDisconnect, RuntimeError):
        pass
    finally:
        clients.discard(websocket)


@app.get("/scenario")
async def get_scenario():
    return scenario.model_dump()


@app.post("/scenario")
async def set_scenario(update: Scenario):
    global scenario
    scenario = update
    return scenario.model_dump()


@app.get("/")
async def root():
    return {
        "name": "mock-binance-spot",
        "symbol": SYMBOL,
        "snapshot": f"/api/v3/depth?symbol={SYMBOL}&limit={SNAPSHOT_LIMIT}",
        "websocket": f"/ws/{SYMBOL.lower()}@depth",
        "scenario": scenario.model_dump(),
    }
