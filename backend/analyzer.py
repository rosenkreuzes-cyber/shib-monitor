from collections import deque
from time import time
from orderbook import OrderBookEngine


class MarketAnalyzer:
    def __init__(self):
        self.book = OrderBookEngine(depth_pct=0.05, max_levels=100)
        self.last_price = None
        self.prev_price = None
        self.trades = deque(maxlen=5000)
        self.source = "unknown"
        self.ws_connected = False
        self.last_error = None
        self.last_trade_id = None
        self.absorption = {"buy": 0.0, "sell": 0.0}
        self.large_trade_threshold_jpy = 100_000.0

    def set_source(self, source): self.source = source
    def set_ws(self, connected, error=None): self.ws_connected = connected; self.last_error = error

    def load_depth(self, d):
        self.book.load_snapshot(d.get("bids", []), d.get("asks", []), d.get("sequence"), d.get("last_update_at"))

    def diff_depth(self, d):
        self.book.apply_diff(d.get("bids", []), d.get("asks", []), d.get("sequence"), d.get("last_update_at"))

    def ticker(self, d):
        last = d.get("last") or d.get("last_price")
        if last is not None:
            try:
                v = float(last)
                if v > 0:
                    self.prev_price = self.last_price
                    self.last_price = v
            except (TypeError, ValueError):
                pass

    @staticmethod
    def _side(v):
        if v is None: return None
        v = str(v).lower()
        return "buy" if v in ("buy", "bid") else "sell" if v in ("sell", "ask") else None

    def trade(self, t):
        side = self._side(t.get("side") or t.get("order_type"))
        if side is None: return
        try:
            price = float(t.get("price") or t.get("rate")); amount = float(t.get("amount"))
        except (TypeError, ValueError): return
        if price <= 0 or amount <= 0: return
        trade_id = t.get("id")
        if trade_id is not None and trade_id == self.last_trade_id:
            return
        if trade_id is not None:
            self.last_trade_id = trade_id
        raw_ts = t.get("executed_at") or t.get("created_at") or time()
        try: ts = float(raw_ts)
        except (TypeError, ValueError): ts = time()
        if ts > 10_000_000_000: ts /= 1000
        self.trades.append({"ts": ts, "side": side, "price": price, "amount": amount,
                            "notional": price * amount, "id": trade_id})
        self.prev_price = self.last_price
        self.last_price = price
        self._update_absorption(side, price, amount)

    def _update_absorption(self, trade_side, price, amount):
        # Buy trades consume asks; sell trades consume bids. We score the
        # immediately opposing book liquidity near the trade as absorption.
        m = self.book.mid()
        if not m: return
        window = max(m * 0.002, price * 0.002)
        if trade_side == "buy":
            nearby = sum(a for p, a in self.book.asks.items() if abs(p - price) <= window)
            self.absorption["buy"] = min(1_000_000.0, self.absorption["buy"] * 0.95 + min(amount, nearby))
        else:
            nearby = sum(a for p, a in self.book.bids.items() if abs(p - price) <= window)
            self.absorption["sell"] = min(1_000_000.0, self.absorption["sell"] * 0.95 + min(amount, nearby))

    def flow(self, seconds=10, large=False):
        now = time()
        rows = [t for t in self.trades if 0 <= now - t["ts"] <= seconds]
        if large:
            rows = [t for t in rows if t["notional"] >= self.large_trade_threshold_jpy]
        b = sum(t["notional"] if large else t["amount"] for t in rows if t["side"] == "buy")
        s = sum(t["notional"] if large else t["amount"] for t in rows if t["side"] == "sell")
        total = b + s
        return {"buy": b, "sell": s, "buy_pct": b / total * 100 if total else 50.0,
                "sell_pct": s / total * 100 if total else 50.0, "count": len(rows)}

    def snapshot(self):
        book = self.book.snapshot(); flow = self.flow(); large = self.flow(10, True)
        wb = sum(x["multiple"] for x in book["walls"] if x["side"] == "buy")
        ws = sum(x["multiple"] for x in book["walls"] if x["side"] == "sell")
        wall_buy = wb / (wb + ws) * 100 if wb + ws else 50.0
        # Keep components bounded and transparent: depth 40%, trades 25%,
        # order-book changes 15%, walls 10%, large trades 10%.
        score = max(0, min(100,
            .40 * book["imbalance_pct"] + .25 * flow["buy_pct"] +
            .15 * book["book_change"]["buy_pct"] + .10 * wall_buy +
            .10 * large["buy_pct"]))
        label = ("買い圧力 強" if score >= 72 else "買い優勢" if score >= 58 else
                 "拮抗" if score > 42 else "売り優勢" if score > 28 else "売り圧力 強")
        change = ((self.last_price / self.prev_price - 1) * 100
                  if self.prev_price and self.last_price else 0.0)
        return {
            "version": "v5.0",
            "pair": "SHIB/JPY", "price": self.last_price, "score": round(score, 1),
            "label": label, "price_change_pct": round(change, 4),
            "source": self.source, "ws_connected": self.ws_connected,
            "last_error": self.last_error, "book": book, "trade_flow": flow,
            "large_trade_flow": large, "absorption": {
                "buy": round(self.absorption["buy"], 8),
                "sell": round(self.absorption["sell"], 8)
            },
            "server_time": int(time() * 1000)
        }
