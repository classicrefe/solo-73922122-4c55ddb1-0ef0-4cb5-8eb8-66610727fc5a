"""盘口同步规则单元测试（不依赖网络，直接测试 OrderBook 纯逻辑）。

运行：cd collector && python -m pytest test_order_book.py -v
"""
import pytest

from app import GapDetected, OrderBook


def ev(U, u, b=None, a=None):
    return {"U": U, "u": u, "b": b or [], "a": a or []}


def snapshot(L=100):
    return {
        "lastUpdateId": L,
        "bids": [["100.0", "1.0000"], ["99.9", "2.0000"]],
        "asks": [["100.1", "1.5000"], ["100.2", "2.5000"]],
    }


def new_book():
    book = OrderBook()
    book.load_snapshot(snapshot())
    return book


def stats():
    return {"applied": 0, "dropped": 0}


def test_snapshot_loads_L_and_levels():
    book = new_book()
    assert book.last_update_id == 100
    assert book.bids == {100.0: 1.0, 99.9: 2.0}
    assert book.asks == {100.1: 1.5, 100.2: 2.5}


def test_drop_events_with_u_not_greater_than_L():
    book = new_book()
    s = stats()
    book.apply(ev(99, 100, b=[["100.0", "9.0000"]]), s)   # u == L，丢弃
    book.apply(ev(95, 98, b=[["100.0", "9.0000"]]), s)    # u < L，丢弃（重复消息）
    assert s["dropped"] == 2 and s["applied"] == 0
    assert book.bids[100.0] == 1.0  # 未被污染
    assert book.last_update_id == 100


def test_apply_straddling_event_and_advance_L():
    book = new_book()
    s = stats()
    book.apply(ev(99, 101, b=[["100.0", "3.0000"]]), s)   # U<=L+1<=u，应用
    assert book.bids[100.0] == 3.0
    assert book.last_update_id == 101
    book.apply(ev(102, 102, a=[["100.1", "0.0000"]]), s)  # 紧接着的下一条
    assert 100.1 not in book.asks
    assert book.last_update_id == 102


def test_gap_raises_and_book_untouched():
    book = new_book()
    s = stats()
    with pytest.raises(GapDetected):
        book.apply(ev(105, 106, b=[["100.0", "9.0000"]]), s)  # U=105 > L+1=101
    assert book.bids[100.0] == 1.0
    assert book.last_update_id == 100


def test_zero_quantity_deletes_level():
    book = new_book()
    s = stats()
    book.apply(ev(101, 101, b=[["99.9", "0.0000"]]), s)
    assert 99.9 not in book.bids
    assert book.bids[100.0] == 1.0


def test_quantity_is_absolute_not_delta():
    book = new_book()
    s = stats()
    book.apply(ev(101, 101, b=[["100.0", "5.0000"]]), s)
    book.apply(ev(102, 102, b=[["100.0", "7.0000"]]), s)
    assert book.bids[100.0] == 7.0  # 最新总量，而非累加


def test_trim_to_100_levels_per_side():
    snap = {
        "lastUpdateId": 1,
        "bids": [[f"{200.0 - i * 0.1:.1f}", "1.0000"] for i in range(150)],
        "asks": [[f"{200.1 + i * 0.1:.1f}", "1.0000"] for i in range(150)],
    }
    book = OrderBook()
    book.load_snapshot(snap)
    assert len(book.bids) == 100 and len(book.asks) == 100
    assert max(book.bids) == 200.0          # 保留最优 100 档
    assert min(book.asks) == pytest.approx(200.1)
