from collections import deque
from time import time
from orderbook import OrderBookEngine

class MarketAnalyzer:
    def __init__(self):
        self.book=OrderBookEngine(); self.last_price=None; self.prev_price=None
        self.trades=deque(maxlen=1000); self.absorption={"buy":0.,"sell":0.}
        self.large_trade_threshold=100000.

    def whole(self,d): self.book.apply_whole(d)

    def diff(self,d):
        self.book.buffer_diff(d)
        if self.book.ready and int(d["s"])>self.book.last_whole_seq:self.book.apply_diff(d)

    def trade(self,side,price,amount,executed_at=None):
        if price<=0 or amount<=0:return
        ts=(executed_at or int(time()*1000))/1000
        self.prev_price=self.last_price; self.last_price=price
        x={"ts":ts,"side":side,"price":price,"amount":amount}; self.trades.append(x)
        book=self.book.bids if side=="sell" else self.book.asks
        resting=book.get(price,0.)
        recent=[t for t in self.trades if time()-t["ts"]<=5 and t["price"]==price]
        if resting>0 and len(recent)>=3 and sum(t["amount"] for t in recent)>=resting*.5:
            self.absorption["buy" if side=="sell" else "sell"]+=sum(t["amount"] for t in recent)

    def flow(self,seconds=10):
        r=[t for t in self.trades if time()-t["ts"]<=seconds]
        b=sum(t["amount"] for t in r if t["side"]=="buy"); s=sum(t["amount"] for t in r if t["side"]=="sell"); t=b+s
        return {"buy_pct":b/t*100 if t else 50.,"sell_pct":s/t*100 if t else 50.,"count":len(r)}

    def large_flow(self,seconds=10):
        r=[t for t in self.trades if time()-t["ts"]<=seconds and t["price"]*t["amount"]>=self.large_trade_threshold]
        b=sum(t["price"]*t["amount"] for t in r if t["side"]=="buy"); s=sum(t["price"]*t["amount"] for t in r if t["side"]=="sell"); t=b+s
        return {"buy_pct":b/t*100 if t else 50.,"sell_pct":s/t*100 if t else 50.,"count":len(r)}

    def snapshot(self):
        book=self.book.snapshot(); flow=self.flow(); large=self.large_flow()
        wb=sum(x["multiple"] for x in book["walls"] if x["side"]=="buy")
        ws=sum(x["multiple"] for x in book["walls"] if x["side"]=="sell"); wt=wb+ws
        wall=wb/wt*100 if wt else 50.
        score=max(0,min(100,.40*book["imbalance_pct"]+.25*flow["buy_pct"]+
                         .15*book["book_change"]["buy_pct"]+.10*wall+.10*large["buy_pct"]))
        label="買い圧力 強" if score>=72 else "買い優勢" if score>=58 else "拮抗" if score>42 else "売り優勢" if score>28 else "売り圧力 強"
        change=(self.last_price/self.prev_price-1)*100 if self.prev_price and self.last_price else 0.
        return {"pair":"SHIB/JPY","price":self.last_price,"score":round(score,1),"label":label,
                "price_change_pct":round(change,4),"book":book,"trade_flow":flow,
                "large_trade_flow":large,"absorption":self.absorption,"server_time":int(time()*1000)}
