"""启停并发语义测试（不依赖真实行情源，模拟 sync_cycle 与子任务）。

运行：cd collector && python -m pytest test_lifecycle.py -v

覆盖修复要求：
  1. 受理停止即撤下报价，进入 stopping；
  2. 清理期间启动返回 409（ConflictError，HTTP 层同样验证）；
  3. 并发停止共享同一次清理；
  4. 停止请求取消/断连后清理继续，主循环、WS 接收、在途快照全部回收才落 stopped；
  5. 后续启动只创建一个可追踪 Run，旧停止操作不得清空新任务/覆盖其状态；
  6. start/stop 幂等，/api/config 参数校验。
"""
import asyncio
from contextlib import suppress

import httpx
import pytest

import app as collector
from app import ConflictError, Run


# ----------------------------- 测试基础设施 -----------------------------

def fresh_state(monkeypatch, cleanup_delay: float = 0.0) -> collector.CollectorState:
    state = collector.CollectorState()
    state.cleanup_delay = cleanup_delay
    monkeypatch.setattr(collector, "state", state)
    return state


def install_fake_cycle(monkeypatch):
    """用假周期替换 sync_cycle：模拟 WS 接收与在途 HTTP 快照两个子任务。"""
    started = asyncio.Event()

    async def fake_child():
        await asyncio.sleep(30)  # 正常不会结束，必须由停止清理取消并回收

    async def fake_sync_cycle(run: Run):
        run.track(asyncio.create_task(fake_child()))   # 模拟 WebSocket pump
        run.track(asyncio.create_task(fake_child()))   # 模拟在途 HTTP 快照
        started.set()
        await asyncio.Event().wait()                   # 主周期等待取消

    monkeypatch.setattr(collector, "sync_cycle", fake_sync_cycle)
    return started


async def wait_for(predicate, timeout: float = 3.0):
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while loop.time() < deadline:
        if predicate():
            return
        await asyncio.sleep(0.01)
    raise AssertionError("等待条件超时")


def run(coro):
    return asyncio.run(coro)


# ------------------------------- 测试用例 -------------------------------

def test_start_creates_single_tracked_run_and_stop_reclaims_all(monkeypatch):
    async def scenario():
        state = fresh_state(monkeypatch)
        started = install_fake_cycle(monkeypatch)

        await collector.start_collection()
        await started.wait()
        assert state.running and state.status == "syncing"
        assert state.run is not None and state.run.task is not None
        assert len(state.run.children) == 2

        await collector.stop_collection()

        assert state.status == "stopped"
        assert state.run is None and state.shutdown is None
        assert state.book is None
        # 主循环与全部子任务句柄均已回收
        for task in asyncio.all_tasks():
            assert task.done() or task is asyncio.current_task()

    run(scenario())


def test_stop_withdraws_quote_before_first_await(monkeypatch):
    async def scenario():
        state = fresh_state(monkeypatch, cleanup_delay=0.2)
        started = install_fake_cycle(monkeypatch)
        await collector.start_collection()
        await started.wait()
        # 模拟已发布有效报价
        state.book = object()
        state.status = "synced"

        stop_task = asyncio.ensure_future(collector.stop_collection())
        await asyncio.sleep(0.02)  # 仅让出事件循环，不应越过受理点

        # 受理停止即撤下报价；清理尚未结束
        assert state.status == "stopping"
        assert state.book is None
        assert not stop_task.done()

        await stop_task
        assert state.status == "stopped"
        assert state.run is None

    run(scenario())


def test_concurrent_stops_share_one_cleanup(monkeypatch):
    async def scenario():
        state = fresh_state(monkeypatch, cleanup_delay=0.15)
        install_fake_cycle(monkeypatch)
        calls = {"n": 0}
        original = collector._shutdown_collection

        async def counting_shutdown(run):
            calls["n"] += 1
            return await original(run)

        monkeypatch.setattr(collector, "_shutdown_collection", counting_shutdown)

        await collector.start_collection()
        await asyncio.gather(
            collector.stop_collection(),
            collector.stop_collection(),
            collector.stop_collection(),
        )

        assert calls["n"] == 1  # 同一次停止只清理一次
        assert state.status == "stopped"
        assert state.run is None and state.shutdown is None

    run(scenario())


def test_start_during_cleanup_conflicts_then_succeeds_after(monkeypatch):
    async def scenario():
        state = fresh_state(monkeypatch, cleanup_delay=0.25)
        started = install_fake_cycle(monkeypatch)
        await collector.start_collection()
        await started.wait()
        old_run = state.run

        stop_task = asyncio.ensure_future(collector.stop_collection())
        await wait_for(lambda: state.status == "stopping")

        with pytest.raises(ConflictError):
            await collector.start_collection()

        await stop_task
        assert state.status == "stopped" and state.run is None

        await collector.start_collection()
        assert state.running
        assert state.run is not old_run  # 新启动创建新的可追踪任务
        await collector.stop_collection()
        assert state.status == "stopped"

    run(scenario())


def test_cancelled_stop_request_keeps_cleanup_running(monkeypatch):
    async def scenario():
        state = fresh_state(monkeypatch, cleanup_delay=0.2)
        started = install_fake_cycle(monkeypatch)
        await collector.start_collection()
        await started.wait()
        run_obj = state.run

        stop_task = asyncio.ensure_future(collector.stop_collection())
        await asyncio.sleep(0.02)
        stop_task.cancel()  # 模拟浏览器标签页关闭/请求断连
        with suppress(asyncio.CancelledError):
            await stop_task

        # 请求已死，清理仍在后台继续且句柄保留
        assert state.shutdown is not None
        assert state.status == "stopping"
        assert state.run is run_obj

        await state.shutdown  # 等待同一次清理自然结束
        assert state.status == "stopped"
        assert state.run is None
        assert all(t.done() for t in run_obj.children)

        # 晚到的停止请求：幂等成功，不会重复清理
        await collector.stop_collection()
        assert state.status == "stopped"

    run(scenario())


def test_late_stop_joins_the_same_inflight_cleanup(monkeypatch):
    async def scenario():
        state = fresh_state(monkeypatch, cleanup_delay=0.25)
        install_fake_cycle(monkeypatch)
        calls = {"n": 0}
        original = collector._shutdown_collection

        async def counting_shutdown(run):
            calls["n"] += 1
            return await original(run)

        monkeypatch.setattr(collector, "_shutdown_collection", counting_shutdown)

        await collector.start_collection()
        first_stop = asyncio.ensure_future(collector.stop_collection())
        await wait_for(lambda: state.status == "stopping")
        first_stop.cancel()  # 第一个停止请求断连
        with suppress(asyncio.CancelledError):
            await first_stop
        assert state.shutdown is not None and not state.shutdown.done()

        # 第二个停止请求在清理进行中到达：加入同一次清理而非另起炉灶
        await collector.stop_collection()
        assert calls["n"] == 1
        assert state.status == "stopped"
        assert state.run is None and state.shutdown is None

    run(scenario())


def test_stale_shutdown_cannot_touch_new_run(monkeypatch):
    async def scenario():
        state = fresh_state(monkeypatch)
        install_fake_cycle(monkeypatch)

        await collector.start_collection()
        new_run = state.run
        # 一个属于旧运行的清理任务（token 不匹配）此刻才收尾
        stale_run = Run(token=-1)
        await collector._shutdown_collection(stale_run)

        assert state.run is new_run          # 新任务未被清空
        assert state.status == "syncing"     # 新状态未被覆盖为 stopped

        await collector.stop_collection()
        assert state.status == "stopped"

    run(scenario())


def test_lifespan_shutdown_reclaims_inflight_run(monkeypatch):
    async def scenario():
        state = fresh_state(monkeypatch, cleanup_delay=0.05)
        install_fake_cycle(monkeypatch)

        # 进入/退出 ASGI lifespan 等价于 uvicorn 启动/进程关闭
        async with collector.app.router.lifespan_context(collector.app):
            await collector.start_collection()
            await wait_for(lambda: state.run is not None and len(state.run.children) == 2)
            run_obj = state.run
            assert state.status == "syncing"

        assert state.status == "stopped"
        assert state.run is None and state.book is None
        assert run_obj.task.done()
        assert all(t.done() for t in run_obj.children)

    run(scenario())


def test_idempotent_start_and_stop(monkeypatch):
    async def scenario():
        state = fresh_state(monkeypatch)
        install_fake_cycle(monkeypatch)

        await collector.start_collection()
        first_run = state.run
        await collector.start_collection()  # 重复启动不创建第二个任务
        assert state.run is first_run

        await collector.stop_collection()
        await collector.stop_collection()  # 重复停止幂等
        assert state.status == "stopped"
        await collector.stop_collection()
        assert state.status == "stopped"

    run(scenario())


def test_http_control_409_and_config_validation(monkeypatch):
    async def scenario():
        state = fresh_state(monkeypatch, cleanup_delay=0.25)
        install_fake_cycle(monkeypatch)
        transport = httpx.ASGITransport(app=collector.app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            r = await client.post("/api/control", json={"action": "start"})
            assert r.status_code == 200 and r.json()["running"] is True

            stop_task = asyncio.ensure_future(
                client.post("/api/control", json={"action": "stop"})
            )
            await wait_for(lambda: state.status == "stopping")

            r = await client.post("/api/control", json={"action": "start"})
            assert r.status_code == 409
            assert r.json()["status"] == "stopping"

            resp = await stop_task
            assert resp.status_code == 200 and resp.json()["status"] == "stopped"

            r = await client.post("/api/control", json={"action": "start"})
            assert r.status_code == 200
            r = await client.post("/api/control", json={"action": "stop"})
            assert r.status_code == 200

            # 清理延迟配置校验与生效
            r = await client.post("/api/config", json={"stop_cleanup_delay_ms": -1})
            assert r.status_code == 400
            r = await client.post("/api/config", json={"stop_cleanup_delay_ms": 1200})
            assert r.status_code == 200
            r = await client.get("/api/config")
            assert r.json()["stop_cleanup_delay_ms"] == 1200
            assert state.cleanup_delay == pytest.approx(1.2)

    run(scenario())


def test_state_endpoint_hides_quotes_while_stopping(monkeypatch):
    async def scenario():
        state = fresh_state(monkeypatch, cleanup_delay=0.15)
        install_fake_cycle(monkeypatch)
        transport = httpx.ASGITransport(app=collector.app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            await client.post("/api/control", json={"action": "start"})
            stop_task = asyncio.ensure_future(
                client.post("/api/control", json={"action": "stop"})
            )
            await wait_for(lambda: state.status == "stopping")
            r = await client.get("/api/state")
            body = r.json()
            assert body["status"] == "stopping"
            assert body["running"] is False and body["synced"] is False
            assert body["bids"] == [] and body["asks"] == []
            assert body["cleanupDelayMs"] >= 0
            await stop_task

    run(scenario())
