from collections import deque
from statistics import median
from time import time

class OrderBookEngine:
    def __init__(self, max_events=5000):
        self.bids={}; self.asks={}; self.events=deque(maxlen=max_events); self.last_sequence=None; self.ready=False; self.last_snapshot_ts=None
    def load_snapshot(self,bids,asks,sequence=None):
        self.bids={float(p):float(a) for p,a in bids if float(a)>0}; self.asks={float(p):float(a) for p,a in asks if float(a)>0}
        self.last_sequence=sequence; self.ready=bool(self.bids and self.asks); self.last_snapshot_ts=time()
    def apply_diff(self,bids,asks,sequence=None):
        for p,a in bids:
            p=float(p); a=float(a); old=self.bids.get(p,0.0)
            if a<=0:self.bids.pop(p,None)
            else:self.bids[p]=a
            self.events.append({"ts":time(),"side":"bid","price":p,"old":old,"new":a,"delta":a-old,"seq":sequence})
        for p,a in asks:
            p=float(p); a=float(a); old=self.asks.get(p,0.0)
            if a<=0:self.asks.pop(p,None)
            else:self.asks[p]=a
            self.events.append({"ts":time(),"side":"ask","price":p,"old":old,"new":a,"delta":a-old,"seq":sequence})
        self.ready=bool(self.bids and self.asks)
    def best_bid(self): return max(self.bids) if self.bids else None
    def best_ask(self): return min(self.asks) if self.asks else None
    def mid(self):
        b,a=self.best_bid(),self.best_ask(); return (b+a)/2 if b is not None and a is not None else None
    def imbalance(self,pct=.005,levels=100):
        m=self.mid()
        if m is None:return 50.,0.,0.
        def total(book):
            rows=sorted(book.items(),key=lambda x:abs(x[0]-m))[:levels]
            return sum(a*max(0,1-abs(p-m)/m/pct) for p,a in rows if abs(p-m)/m<=pct)
        b,a=total(self.bids),total(self.asks); t=b+a; return (b/t*100 if t else 50.),b,a
    def walls(self,pct=.005,multiple=6.):
        m=self.mid()
        if not m:return []
        out=[]
        for side,book in (("buy",self.bids),("sell",self.asks)):
            vals=[a for p,a in book.items() if abs(p-m)/m<=pct and a>0]
            if len(vals)<5:continue
            base=median(vals)
            for p,a in book.items():
                if abs(p-m)/m<=pct and base>0 and a>=base*multiple: out.append({"side":side,"price":p,"amount":a,"multiple":round(a/base,1)})
        return sorted(out,key=lambda x:x["multiple"],reverse=True)[:10]
    def change_pressure(self,seconds=5):
        cut=time()-seconds; ev=[e for e in self.events if e["ts"]>=cut]
        addb=sum(max(0,e["delta"]) for e in ev if e["side"]=="bid"); adda=sum(max(0,e["delta"]) for e in ev if e["side"]=="ask")
        remb=sum(abs(min(0,e["delta"])) for e in ev if e["side"]=="bid"); rema=sum(abs(min(0,e["delta"])) for e in ev if e["side"]=="ask")
        buy=addb+rema; sell=adda+remb; t=buy+sell
        return {"buy_pct":buy/t*100 if t else 50.,"sell_pct":sell/t*100 if t else 50.,"events":len(ev)}
    def snapshot(self):
        im,b,a=self.imbalance()
        return {"ready":self.ready,"best_bid":self.best_bid(),"best_ask":self.best_ask(),"mid":self.mid(),"imbalance_pct":round(im,1),"weighted_bid":b,"weighted_ask":a,"walls":self.walls(),"book_change":self.change_pressure(),"sequence":self.last_sequence,"snapshot_age_sec":round(time()-self.last_snapshot_ts,1) if self.last_snapshot_ts else None}
