from collections import deque
from time import time
from orderbook import OrderBookEngine

VERSION="v5.5-okj-tradefix"

class MarketAnalyzer:
    def __init__(self):
        self.book=OrderBookEngine(); self.last_price=None; self.prev_price=None
        self.trades=deque(maxlen=10000); self.source="unknown"
        self.ws_connected=False; self.last_error=None
        self.ids=set(); self.idq=deque(maxlen=5000)

    def set_source(self,s): self.source=s
    def set_ws(self,c,error=None): self.ws_connected=c; self.last_error=error

    def ticker(self,d):
        try:
            v=float(d.get("last") or d.get("last_price"))
            if v>0: self.prev_price,self.last_price=self.last_price,v
        except (TypeError,ValueError): pass

    def trade(self,t):
        side=str(t.get("side") or "").lower()
        if side not in ("buy","sell"): return
        try: p=float(t.get("price")); q=float(t.get("amount"))
        except (TypeError,ValueError): return
        if p<=0 or q<=0: return
        tid=t.get("id")
        if tid is not None:
            k=str(tid)
            if k in self.ids: return
            if len(self.idq)>=self.idq.maxlen: self.ids.discard(self.idq.popleft())
            self.idq.append(k); self.ids.add(k)
        ts=float(t.get("executed_at") or time())
        if ts>1e10: ts/=1000
        self.trades.append({"ts":ts,"side":side,"price":p,"amount":q,"notional":p*q,"id":tid})
        self.prev_price,self.last_price=self.last_price,p

    def flow(self):
        now=time(); r=[x for x in self.trades if 0<=now-x["ts"]<=60]
        buy=sum(x["amount"] for x in r if x["side"]=="buy"); sell=sum(x["amount"] for x in r if x["side"]=="sell")
        total=buy+sell
        return {"buy":buy,"sell":sell,"buy_pct":buy/total*100 if total else 50,"sell_pct":sell/total*100 if total else 50,"count":len(r)}

    def snapshot(self):
        b=self.book.snapshot(); f=self.flow()
        usable=b["ready"] and b["freshness"] not in ("INVALID","UNKNOWN")
        score=(b["imbalance_pct"]*.70+f["buy_pct"]*.30) if usable else None
        label="判定停止" if score is None else "買い優勢" if score>=58 else "売り優勢" if score<=42 else "拮抗"
        return {"version":VERSION,"pair":"SHIB/JPY","price":self.last_price,
                "score":round(score,1) if score is not None else None,"label":label,
                "score_usable":usable,"source":self.source,"ws_connected":self.ws_connected,
                "last_error":self.last_error,"book":b,"trade_flow":f,"server_time":int(time()*1000)}
