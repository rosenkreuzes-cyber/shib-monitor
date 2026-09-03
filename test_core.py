import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "backend"))
from orderbook import OrderBookEngine


def test_counts_and_weighted_values():
    b = OrderBookEngine(depth_pct=0.05, max_levels=100)
    b.load_snapshot([[100, 10], [99, 5], [98, 2]], [[101, 8], [102, 4], [103, 2]])
    s = b.snapshot()
    assert s["ready"] is True
    assert s["bid_levels"] == 3
    assert s["ask_levels"] == 3
    assert s["total_levels"] == 6
    assert s["weighted_bid"] > 0 and s["weighted_ask"] > 0


def test_zero_removes_level():
    b = OrderBookEngine()
    b.load_snapshot([[100, 10]], [[101, 8]])
    b.apply_diff([[100, 0]], [[101, 0]])
    assert b.best_bid() is None
    assert b.best_ask() is None
    assert b.snapshot()["total_levels"] == 0


def test_sparse_book_fallback():
    b = OrderBookEngine(depth_pct=0.005, max_levels=10)
    b.load_snapshot([[100, 10]], [[110, 8]])
    s = b.snapshot()
    assert s["weighted_bid"] > 0 and s["weighted_ask"] > 0


if __name__ == "__main__":
    test_counts_and_weighted_values(); test_zero_removes_level(); test_sparse_book_fallback(); print("v5 core tests: OK")
