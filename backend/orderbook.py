from collections import deque
from statistics import median
from time import time


class OrderBookEngine:
    """Coincheck order-book state and pressure calculations.

    The previous build used a fixed 0.5% window.  SHIB/JPY can temporarily have a
    much wider spread, so the best bid/ask could both sit outside that window and
    weighted_bid/weighted_ask became 0.  This version uses a configurable 5% window
    and falls back to the nearest levels when the window contains no rows.
    """

    def __init__(self, max_events=5000, depth_pct=0.05, max_levels=100):
        self.bids = {}
        self.asks = {}
        self.events = deque(maxlen=max_events)
        self.last_sequence = None
        self.ready = False
        self.last_snapshot_ts = None
        self.depth_pct = float(depth_pct)
        self.max_levels = int(max_levels)

    @staticmethod
    def _rows(rows):
        out = []
        for row in rows or []:
            try:
                if isinstance(row, dict):
                    p = row.get("price", row.get("rate"))
                    a = row.get("amount", row.get("quantity", row.get("size")))
                else:
                    p, a = row[0], row[1]
                p, a = float(p), float(a)
                if p > 0 and a > 0:
                    out.append((p, a))
            except (TypeError, ValueError, IndexError, KeyError):
                continue
        return out

    def load_snapshot(self, bids, asks, sequence=None):
        self.bids = dict(self._rows(bids))
        self.asks = dict(self._rows(asks))
        self.last_sequence = sequence
        self.ready = bool(self.bids and self.asks)
        self.last_snapshot_ts = time()

    def apply_diff(self, bids, asks, sequence=None):
        for p, a in self._rows_allow_zero(bids):
            old = self.bids.get(p, 0.0)
            if a <= 0:
                self.bids.pop(p, None)
            else:
                self.bids[p] = a
            self.events.append({"ts": time(), "side": "bid", "price": p,
                                "old": old, "new": a, "delta": a-old, "seq": sequence})

        for p, a in self._rows_allow_zero(asks):
            old = self.asks.get(p, 0.0)
            if a <= 0:
                self.asks.pop(p, None)
            else:
                self.asks[p] = a
            self.events.append({"ts": time(), "side": "ask", "price": p,
                                "old": old, "new": a, "delta": a-old, "seq": sequence})

        if sequence is not None:
            self.last_sequence = sequence
        self.ready = bool(self.bids and self.asks)

    @staticmethod
    def _rows_allow_zero(rows):
        out = []
        for row in rows or []:
            try:
                if isinstance(row, dict):
                    p = row.get("price", row.get("rate"))
                    a = row.get("amount", row.get("quantity", row.get("size")))
                else:
                    p, a = row[0], row[1]
                p, a = float(p), float(a)
                if p > 0:
                    out.append((p, a))
            except (TypeError, ValueError, IndexError, KeyError):
                continue
        return out

    def best_bid(self):
        return max(self.bids) if self.bids else None

    def best_ask(self):
        return min(self.asks) if self.asks else None

    def mid(self):
        b, a = self.best_bid(), self.best_ask()
        return (b + a) / 2 if b is not None and a is not None else None

    def spread_pct(self):
        b, a = self.best_bid(), self.best_ask()
        if b is None or a is None or b <= 0:
            return None
        return (a - b) / b * 100

    def _weighted_side(self, book, m, pct, levels):
        if not book or m is None or m <= 0:
            return 0.0, 0
        rows = sorted(book.items(), key=lambda x: abs(x[0] - m))
        inside = [(p, a) for p, a in rows if abs(p - m) / m <= pct]
        # Important fallback: if a sparse/wide SHIB book has no rows inside the
        # window, still use the nearest levels instead of returning zero.
        selected = inside[:levels] if inside else rows[:levels]
        if not selected:
            return 0.0, 0
        weighted = 0.0
        for p, a in selected:
            distance = abs(p - m) / m
            weight = max(0.05, 1.0 - min(distance / pct, 1.0)) if pct > 0 else 1.0
            weighted += a * weight
        return weighted, len(selected)

    def imbalance(self, pct=None, levels=None):
        pct = self.depth_pct if pct is None else float(pct)
        levels = self.max_levels if levels is None else int(levels)
        m = self.mid()
        if m is None:
            return 50.0, 0.0, 0.0, 0, 0
        b, bn = self._weighted_side(self.bids, m, pct, levels)
        a, an = self._weighted_side(self.asks, m, pct, levels)
        total = b + a
        return (b / total * 100 if total else 50.0), b, a, bn, an

    def walls(self, pct=None, multiple=6.0):
        pct = self.depth_pct if pct is None else float(pct)
        m = self.mid()
        if not m:
            return []
        out = []
        for side, book in (("buy", self.bids), ("sell", self.asks)):
            vals = [a for p, a in book.items() if abs(p-m)/m <= pct and a > 0]
            if len(vals) < 5:
                continue
            base = median(vals)
            if base <= 0:
                continue
            for p, a in book.items():
                if abs(p-m)/m <= pct and a >= base * multiple:
                    out.append({"side": side, "price": p, "amount": a,
                                "multiple": round(a/base, 1)})
        return sorted(out, key=lambda x: x["multiple"], reverse=True)[:10]

    def change_pressure(self, seconds=5):
        cut = time() - seconds
        ev = [e for e in self.events if e["ts"] >= cut]
        addb = sum(max(0, e["delta"]) for e in ev if e["side"] == "bid")
        adda = sum(max(0, e["delta"]) for e in ev if e["side"] == "ask")
        remb = sum(abs(min(0, e["delta"])) for e in ev if e["side"] == "bid")
        rema = sum(abs(min(0, e["delta"])) for e in ev if e["side"] == "ask")
        buy = addb + rema
        sell = adda + remb
        total = buy + sell
        return {"buy_pct": buy/total*100 if total else 50.0,
                "sell_pct": sell/total*100 if total else 50.0,
                "events": len(ev)}

    def snapshot(self):
        im, b, a, bn, an = self.imbalance()
        return {
            "ready": self.ready,
            "best_bid": self.best_bid(),
            "best_ask": self.best_ask(),
            "mid": self.mid(),
            "spread_pct": round(self.spread_pct(), 4) if self.spread_pct() is not None else None,
            "imbalance_pct": round(im, 1),
            "weighted_bid": round(b, 8),
            "weighted_ask": round(a, 8),
            "weighted_bid_levels": bn,
            "weighted_ask_levels": an,
            "depth_window_pct": round(self.depth_pct * 100, 2),
            "walls": self.walls(),
            "book_change": self.change_pressure(),
            "sequence": self.last_sequence,
            "snapshot_age_sec": round(time()-self.last_snapshot_ts, 1) if self.last_snapshot_ts else None,
        }
