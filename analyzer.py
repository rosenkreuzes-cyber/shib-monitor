from collections import deque
from time import time

from orderbook import OrderBookEngine


VERSION = "v5.4"


class MarketAnalyzer:
    def __init__(self):
        self.book = OrderBookEngine(
            depth_pct=0.05,
            max_levels=100,
        )

        self.last_price = None
        self.prev_price = None

        self.trades = deque(maxlen=10000)

        self.source = "unknown"
        self.ws_connected = False
        self.last_error = None

        self.last_trade_ids = deque(maxlen=5000)
        self.last_trade_id_set = set()

        self.absorption = {
            "buy": 0.0,
            "sell": 0.0,
        }

        self.large_trade_threshold_jpy = 100_000.0

    def set_source(self, source):
        self.source = source

    def set_ws(self, connected, error=None):
        self.ws_connected = connected
        self.last_error = error

    def load_depth(self, d):
        self.book.load_snapshot(
            d.get("bids", []),
            d.get("asks", []),
            d.get("sequence"),
            d.get("last_update_at"),
        )
        self.set_source("coincheck_rest")

    def diff_depth(self, d):
        self.book.apply_diff(
            d.get("bids", []),
            d.get("asks", []),
            d.get("sequence"),
            d.get("last_update_at"),
        )
        self.set_source("coincheck_ws_orderbook")

    def ticker(self, d):
        last = d.get("last") or d.get("last_price")

        try:
            value = float(last)

            if value > 0:
                self.prev_price = self.last_price
                self.last_price = value

        except (TypeError, ValueError):
            pass

    @staticmethod
    def _side(value):
        if value is None:
            return None

        value = str(value).lower()

        if value in ("buy", "bid"):
            return "buy"

        if value in ("sell", "ask"):
            return "sell"

        return None

    def trade(self, t):
        side = self._side(
            t.get("side") or t.get("order_type")
        )

        try:
            price = float(
                t.get("price") or t.get("rate")
            )
            amount = float(t.get("amount"))

        except (TypeError, ValueError):
            return

        if side is None or price <= 0 or amount <= 0:
            return

        trade_id = t.get("id")

        if trade_id is not None:
            key = str(trade_id)

            if key in self.last_trade_id_set:
                return

            if len(self.last_trade_ids) >= self.last_trade_ids.maxlen:
                old = self.last_trade_ids.popleft()
                self.last_trade_id_set.discard(old)

            self.last_trade_ids.append(key)
            self.last_trade_id_set.add(key)

        raw_timestamp = (
            t.get("executed_at")
            or t.get("created_at")
            or time()
        )

        try:
            timestamp = float(raw_timestamp)

        except (TypeError, ValueError):
            timestamp = time()

        # Unix milliseconds -> seconds
        if timestamp > 10_000_000_000:
            timestamp /= 1000

        self.trades.append(
            {
                "ts": timestamp,
                "side": side,
                "price": price,
                "amount": amount,
                "notional": price * amount,
                "id": trade_id,
            }
        )

        self.prev_price = self.last_price
        self.last_price = price

        self._update_absorption(
            side,
            price,
            amount,
        )

    def _update_absorption(
        self,
        trade_side,
        price,
        amount,
    ):
        mid = self.book.mid()

        if not mid:
            return

        window = max(
            mid * 0.002,
            price * 0.002,
        )

        if trade_side == "buy":
            nearby = sum(
                amount_at_price
                for price_level, amount_at_price
                in self.book.asks.items()
                if abs(price_level - price) <= window
            )

            self.absorption["buy"] = min(
                1_000_000,
                self.absorption["buy"] * 0.95
                + min(amount, nearby),
            )

        else:
            nearby = sum(
                amount_at_price
                for price_level, amount_at_price
                in self.book.bids.items()
                if abs(price_level - price) <= window
            )

            self.absorption["sell"] = min(
                1_000_000,
                self.absorption["sell"] * 0.95
                + min(amount, nearby),
            )

    def flow(self, seconds=60, large=False):
        now = time()

        rows = [
            trade
            for trade in self.trades
            if 0 <= now - trade["ts"] <= seconds
        ]

        if large:
            rows = [
                trade
                for trade in rows
                if trade["notional"]
                >= self.large_trade_threshold_jpy
            ]

        if large:
            buy = sum(
                trade["notional"]
                for trade in rows
                if trade["side"] == "buy"
            )
            sell = sum(
                trade["notional"]
                for trade in rows
                if trade["side"] == "sell"
            )
        else:
            buy = sum(
                trade["amount"]
                for trade in rows
                if trade["side"] == "buy"
            )
            sell = sum(
                trade["amount"]
                for trade in rows
                if trade["side"] == "sell"
            )

        total = buy + sell

        return {
            "buy": buy,
            "sell": sell,
            "buy_pct": buy / total * 100 if total else 50.0,
            "sell_pct": sell / total * 100 if total else 50.0,
            "count": len(rows),
        }

    def _price_change(self, seconds=300):
        now = time()

        rows = [
            trade
            for trade in self.trades
            if 0 <= now - trade["ts"] <= seconds
        ]

        if len(rows) >= 2:
            first = rows[0]["price"]
            last = rows[-1]["price"]

            if first > 0:
                return (last / first - 1) * 100

        return 0.0

    def snapshot(self):
        book = self.book.snapshot()

        flow = self.flow(60)
        large = self.flow(60, True)

        buy_wall_weight = sum(
            wall["multiple"]
            for wall in book["walls"]
            if wall["side"] == "buy"
        )

        sell_wall_weight = sum(
            wall["multiple"]
            for wall in book["walls"]
            if wall["side"] == "sell"
        )

        wall_total = (
            buy_wall_weight + sell_wall_weight
        )

        wall_buy = (
            buy_wall_weight / wall_total * 100
            if wall_total
            else 50.0
        )

        freshness = book["freshness"]

        usable = (
            book["ready"]
            and freshness not in ("INVALID", "UNKNOWN")
        )

        weighted_total = (
            book["weighted_bid"]
            + book["weighted_ask"]
        )

        weighted_bid_pct = (
            book["weighted_bid"]
            / weighted_total
            * 100
            if weighted_total
            else 50.0
        )

        price_momentum = max(
            0,
            min(
                100,
                50 + self._price_change(300) * 10,
            ),
        )

        components = {
            "板のインバランス":
                0.40 * book["imbalance_pct"],

            "板の厚み比率":
                0.15 * weighted_bid_pct,

            "トレードフロー":
                0.15 * flow["buy_pct"],

            "価格モメンタム":
                0.10 * price_momentum,

            "巨大注文":
                0.10 * wall_buy,

            "大口約定":
                0.10 * large["buy_pct"],
        }

        score = (
            sum(components.values())
            if usable
            else None
        )

        if score is None:
            label = "判定停止"

        elif score >= 72:
            label = "買い圧力 強"

        elif score >= 58:
            label = "買い優勢"

        elif score > 42:
            label = "拮抗"

        elif score > 28:
            label = "売り優勢"

        else:
            label = "売り圧力 強"

        if (
            self.prev_price
            and self.last_price
            and self.prev_price > 0
        ):
            price_change = (
                self.last_price / self.prev_price - 1
            ) * 100

        else:
            price_change = 0.0

        return {
            "version": VERSION,
            "pair": "SHIB/JPY",
            "price": self.last_price,
            "score": (
                round(score, 1)
                if score is not None
                else None
            ),
            "label": label,
            "score_usable": usable,
            "price_change_pct": round(
                price_change,
                4,
            ),
            "price_change_5m_pct": round(
                self._price_change(300),
                4,
            ),
            "source": self.source,
            "ws_connected": self.ws_connected,
            "last_error": self.last_error,
            "book": book,
            "trade_flow": flow,
            "large_trade_flow": large,
            "absorption": {
                "buy": round(
                    self.absorption["buy"],
                    8,
                ),
                "sell": round(
                    self.absorption["sell"],
                    8,
                ),
            },
            "score_components": {
                key: round(value, 2)
                for key, value in components.items()
            },
            "server_time": int(time() * 1000),
        }
