from collections import deque
from statistics import median
from time import time

class OrderBookEngine:
    def __init__(self, max_events=5000):
        self.bids={}; self.asks={}; self.pending=[]; self.last_whole_seq=None
        self.last_seq=None; self.ready=False; self.events=deque(maxlen=max_events)

    def buffer_diff(self,d):
        self.pending.append((int(d["s"]),d))
        self.pending.sort(key=lambda x:x[0])

    def apply_whole(self,d):
        self.bids={float(p):float(a) for p,a in d.get("bids",[]) if float(a)>0}
        self.asks={float(p):float(a) for p,a in d.get("asks",[]) if float(a)>0}
        self.last_whole_seq=int(d["sequenceId"]); self.last_seq=self.last_whole_seq
        for seq,diff in self.pending:
            if seq>self.last_whole_seq: self.apply_diff(diff,False)
        self.pending=[]; self.ready=True

    def apply_diff(self,d,record=True):
        seq=int(d["s"])
        for p,a in d.get("b",[]):
            p=float(p); a=float(a); old=self.bids.get(p,0)
            if a==0: self.bids.pop(p,None)
            else: self.bids[p]=a
            if record:self._event("bid",p,old,a,seq)
        for p,a in d.get("a",[]):
            p=float(p); a=float(a); old=self.asks.get(p,0)
            if a==0:self.asks.pop(p,None)
            else:self.asks[p]=a
            if record:self._event("ask",p,old,a,seq)
        self.last_seq=max(self.last_seq or seq,seq)

    def _event(self,side,p,old,new,seq):
        self.events.append({"ts":time(),"side":side,"price":p,"old":old,"new":new,"delta":new-old,"seq":seq})

    def best_bid(self): return max(self.bids) if self.bids else None
    def best_ask(self): return min(self.asks) if self.asks else None
    def mid(self):
        b,a=self.best_bid(),self.best_ask()
        return (b+a)/2 if b is not None and a is not None else None

    def imbalance(self,pct=.005,levels=100):
        m=self.mid()
        if not m:return 50.,0.,0.
        def total(book):
            s=0.
            for p,a in sorted(book.items(),key=lambda x:abs(x[0]-m))[:levels]:
                d=abs(p-m)/m
                if d<=pct:s+=a*(1-d/pct)
            return s
        b,a=total(self.bids),total(self.asks); t=b+a
        return (b/t*100 if t else 50.),b,a

    def walls(self,pct=.005,multiple=6.):
        m=self.mid()
        if not m:return []
        out=[]
        for side,book in (("buy",self.bids),("sell",self.asks)):
            vals=[a for p,a in book.items() if abs(p-m)/m<=pct and a>0]
            if len(vals)<5:continue
            base=median(vals)
            for p,a in book.items():
                if abs(p-m)/m<=pct and a>=base*multiple:
                    out.append({"side":side,"price":p,"amount":a,"multiple":round(a/base,1)})
        return sorted(out,key=lambda x:x["multiple"],reverse=True)[:10]

    def change_pressure(self,seconds=5):
        cut=time()-seconds; ev=[e for e in self.events if e["ts"]>=cut]
        addb=sum(max(0,e["delta"]) for e in ev if e["side"]=="bid")
        adda=sum(max(0,e["delta"]) for e in ev if e["side"]=="ask")
        remb=sum(abs(min(0,e["delta"])) for e in ev if e["side"]=="bid")
        rema=sum(abs(min(0,e["delta"])) for e in ev if e["side"]=="ask")
        buy=addb+rema; sell=adda+remb; t=buy+sell
        return {"buy_pct":buy/t*100 if t else 50.,"sell_pct":sell/t*100 if t else 50.,"events":len(ev)}

    def snapshot(self):
        im,b,a=self.imbalance()
        return {"ready":self.ready,"best_bid":self.best_bid(),"best_ask":self.best_ask(),
                "mid":self.mid(),"imbalance_pct":round(im,1),"weighted_bid":b,"weighted_ask":a,
                "walls":self.walls(),"book_change":self.change_pressure(),"sequence":self.last_seq}
