from collections import deque
from time import time
from orderbook import OrderBookEngine

class MarketAnalyzer:
    def __init__(self):
        self.book = OrderBookEngine(); self.last_price = None; self.prev_price = None
        self.trades = deque(maxlen=2000); self.absorption = {"buy":0.0,"sell":0.0}
        self.large_trade_threshold = 100000.0
        self.source = "unknown"; self.ws_connected = False; self.last_error = None

    def set_source(self, source): self.source = source
    def set_ws(self, connected, error=None): self.ws_connected = connected; self.last_error = error

    def load_depth(self, d):
        self.book.load_snapshot(d.get("bids", []), d.get("asks", []), None)

    def diff_depth(self, d):
        self.book.apply_diff(d.get("bids", []), d.get("asks", []), None)

    def ticker(self, d):
        last = d.get("last") or d.get("last_price")
        if last is not None:
            self.prev_price = self.last_price
            self.last_price = float(last)

    def trade(self, t):
        side = t.get("side") or t.get("order_type")
        if side == "buy": side = "buy"
        elif side == "sell": side = "sell"
        else: return
        try:
            price = float(t.get("price") or t.get("rate")); amount = float(t.get("amount"))
        except (TypeError, ValueError): return
        if price <= 0 or amount <= 0: return
        raw_ts = t.get("executed_at") or t.get("created_at") or time()
        try: ts = float(raw_ts)
        except (TypeError, ValueError): ts = time()
        if ts > 10_000_000_000: ts /= 1000
        self.trades.append({"ts": ts, "side": side, "price": price, "amount": amount})
        self.prev_price = self.last_price
        self.last_price = price

    def flow(self, seconds=10, large=False):
        now = time(); r=[t for t in self.trades if now-t["ts"]<=seconds and (not large or t["price"]*t["amount"]>=self.large_trade_threshold)]
        b=sum(t["price"]*t["amount"] if large else t["amount"] for t in r if t["side"]=="buy")
        s=sum(t["price"]*t["amount"] if large else t["amount"] for t in r if t["side"]=="sell")
        total=b+s
        return {"buy_pct":b/total*100 if total else 50.,"sell_pct":s/total*100 if total else 50.,"count":len(r)}

    def snapshot(self):
        book=self.book.snapshot(); flow=self.flow(); large=self.flow(10,True)
        wb=sum(x["multiple"] for x in book["walls"] if x["side"]=="buy"); ws=sum(x["multiple"] for x in book["walls"] if x["side"]=="sell"); wt=wb+ws
        wall=wb/wt*100 if wt else 50.
        score=max(0,min(100,.40*book["imbalance_pct"]+.25*flow["buy_pct"]+.15*book["book_change"]["buy_pct"]+.10*wall+.10*large["buy_pct"]))
        label="買い圧力 強" if score>=72 else "買い優勢" if score>=58 else "拮抗" if score>42 else "売り優勢" if score>28 else "売り圧力 強"
        change=(self.last_price/self.prev_price-1)*100 if self.prev_price and self.last_price else 0.
        return {"pair":"SHIB/JPY","price":self.last_price,"score":round(score,1),"label":label,"price_change_pct":round(change,4),"source":self.source,"ws_connected":self.ws_connected,"last_error":self.last_error,"book":book,"trade_flow":flow,"large_trade_flow":large,"absorption":self.absorption,"server_time":int(time()*1000)}
