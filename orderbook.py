from time import time

class OrderBookEngine:
    def __init__(self, depth_pct=0.05, max_levels=100):
        self.bids={}; self.asks={}; self.last_sequence=None
        self.ready=False; self.last_update_ts=None
        self.last_data_received_ts=None; self.last_data_source=None
        self.depth_pct=depth_pct; self.max_levels=max_levels

    @staticmethod
    def rows(rows, zero=False):
        out=[]
        for r in rows or []:
            try:
                p,a=float(r[0]),float(r[1])
                if p>0 and (a>=0 if zero else a>0): out.append((p,a))
            except (TypeError,ValueError,IndexError): pass
        return out

    def mark(self, source, ts=None):
        self.last_data_received_ts=time()
        self.last_data_source=source
        try:
            v=float(ts); self.last_update_ts=v/1000 if v>1e10 else v
        except (TypeError,ValueError): self.last_update_ts=self.last_data_received_ts

    def load_snapshot(self,bids,asks,sequence=None,ts=None):
        self.bids=dict(self.rows(bids)); self.asks=dict(self.rows(asks))
        self.last_sequence=sequence; self.ready=bool(self.bids and self.asks)
        self.mark("rest_snapshot" if sequence is None else "ws_snapshot",ts)

    def apply_diff(self,bids,asks,sequence=None,ts=None):
        for rows,book in ((bids,self.bids),(asks,self.asks)):
            for p,a in self.rows(rows,True):
                if a==0: book.pop(p,None)
                else: book[p]=a
        if sequence is not None: self.last_sequence=sequence
        self.ready=bool(self.bids and self.asks); self.mark("ws_diff",ts)

    def best_bid(self): return max(self.bids) if self.bids else None
    def best_ask(self): return min(self.asks) if self.asks else None
    def mid(self):
        b,a=self.best_bid(),self.best_ask()
        return (b+a)/2 if b is not None and a is not None else None

    def snapshot(self):
        b,a=self.best_bid(),self.best_ask()
        age=time()-self.last_data_received_ts if self.last_data_received_ts else None
        fresh="UNKNOWN" if age is None else "LIVE" if age<=3 else "CAUTION" if age<=10 else "STALE" if age<=30 else "INVALID"
        bids=sorted(self.bids.items(),reverse=True)[:10]
        asks=sorted(self.asks.items())[:10]
        total=sum(x[1] for x in self.bids.items())+sum(x[1] for x in self.asks.items())
        bidqty=sum(x[1] for x in self.bids.items())
        return {"ready":self.ready,"best_bid":b,"best_ask":a,"mid":self.mid(),
                "spread_pct":((a-b)/b*100 if b and a else None),
                "imbalance_pct":bidqty/total*100 if total else 50,
                "weighted_bid":bidqty,"weighted_ask":total-bidqty,
                "bid_levels":len(self.bids),"ask_levels":len(self.asks),
                "total_levels":len(self.bids)+len(self.asks),
                "bids_top10":[{"price":p,"amount":q,"notional":p*q} for p,q in bids],
                "asks_top10":[{"price":p,"amount":q,"notional":p*q} for p,q in asks],
                "walls":[],"book_change":{"events":0},"sequence":self.last_sequence,
                "last_update_ts":self.last_update_ts,"last_data_received_ts":self.last_data_received_ts,
                "last_data_source":self.last_data_source,"snapshot_age_sec":age,"freshness":fresh}
