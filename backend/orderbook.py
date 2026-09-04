from collections import deque
from statistics import median
from time import time

class OrderBookEngine:
    """Coincheck order-book state with explicit data-receipt freshness.

    Important v5.1 fix: snapshot_age_sec is based on the most recent RECEIVED
    order-book message, not only the initial REST snapshot. This prevents a
    healthy WebSocket stream from being reported as hundreds of seconds old.
    """
    def __init__(self, max_events=10000, depth_pct=0.05, max_levels=100):
        self.bids={}; self.asks={}; self.events=deque(maxlen=max_events)
        self.last_sequence=None; self.ready=False
        self.last_snapshot_ts=None; self.last_update_ts=None
        self.last_data_received_ts=None; self.last_data_source=None
        self.depth_pct=float(depth_pct); self.max_levels=int(max_levels)

    @staticmethod
    def _parse_rows(rows, allow_zero=False):
        out=[]
        for row in rows or []:
            try:
                if isinstance(row,dict):
                    p=row.get('price',row.get('rate')); a=row.get('amount',row.get('quantity',row.get('size')))
                else: p,a=row[0],row[1]
                p,a=float(p),float(a)
                if p>0 and (a>=0 if allow_zero else a>0): out.append((p,a))
            except (TypeError,ValueError,IndexError,KeyError): continue
        return out

    @staticmethod
    def _parse_ts(value):
        if value is None:return None
        try:
            v=float(value)
            if v>10_000_000_000:v/=1000
            return v
        except (TypeError,ValueError):return None

    def _mark_received(self, source, update_ts=None):
        now=time(); self.last_data_received_ts=now; self.last_data_source=source
        parsed=self._parse_ts(update_ts)
        if parsed is not None:self.last_update_ts=parsed
        elif self.last_update_ts is None or source=='ws_diff':self.last_update_ts=now

    def load_snapshot(self,bids,asks,sequence=None,update_ts=None):
        self.bids=dict(self._parse_rows(bids)); self.asks=dict(self._parse_rows(asks))
        self.last_sequence=sequence; self.ready=bool(self.bids and self.asks)
        self.last_snapshot_ts=time(); self.events.clear(); self._mark_received('rest_snapshot',update_ts)

    def apply_diff(self,bids,asks,sequence=None,update_ts=None):
        changed=0; now=time()
        for side,rows,book in (('bid',bids,self.bids),('ask',asks,self.asks)):
            for p,a in self._parse_rows(rows,allow_zero=True):
                old=book.get(p,0.0)
                if a<=0:book.pop(p,None)
                else:book[p]=a
                if old!=a:
                    changed+=1; self.events.append({'ts':now,'side':side,'price':p,'old':old,'new':a,'delta':a-old,'seq':sequence})
        if sequence is not None:self.last_sequence=sequence
        # Every valid WS orderbook message refreshes RECEIPT freshness, even
        # when Coincheck omits last_update_at.
        self._mark_received('ws_diff',update_ts)
        self.ready=bool(self.bids and self.asks)
        return changed

    def best_bid(self):return max(self.bids) if self.bids else None
    def best_ask(self):return min(self.asks) if self.asks else None
    def mid(self):
        b,a=self.best_bid(),self.best_ask(); return (b+a)/2 if b is not None and a is not None else None
    def spread_pct(self):
        b,a=self.best_bid(),self.best_ask()
        return max(0.0,(a-b)/b*100) if b is not None and a is not None and b>0 else None

    def data_age_sec(self):return round(time()-self.last_data_received_ts,3) if self.last_data_received_ts else None

    def freshness(self):
        age=self.data_age_sec()
        if age is None:return 'UNKNOWN'
        if age<=3:return 'LIVE'
        if age<=10:return 'CAUTION'
        if age<=30:return 'STALE'
        return 'INVALID'

    def _selected(self,book,m,pct,levels):
        if not book or m is None or m<=0:return []
        rows=sorted(book.items(),key=lambda x:abs(x[0]-m))
        inside=[(p,a) for p,a in rows if abs(p-m)/m<=pct]
        return inside[:levels] if inside else rows[:levels]

    def _weighted(self,selected,m,pct):
        qty=notional=0.0
        for p,a in selected:
            distance=abs(p-m)/m; weight=max(0.05,1-min(distance/pct,1)) if pct>0 else 1
            qty+=a*weight; notional+=p*a*weight
        return qty,notional

    def depth_stats(self,pct=None,levels=None):
        pct=self.depth_pct if pct is None else max(.0001,float(pct)); levels=self.max_levels if levels is None else max(1,int(levels)); m=self.mid()
        if m is None:return {'imbalance_pct':50.0,'weighted_bid':0.0,'weighted_ask':0.0,'weighted_bid_notional':0.0,'weighted_ask_notional':0.0,'bid_levels':0,'ask_levels':0,'bid_qty':0.0,'ask_qty':0.0,'bid_notional':0.0,'ask_notional':0.0,'bids_top10':[],'asks_top10':[]}
        bs=self._selected(self.bids,m,pct,levels); a_s=self._selected(self.asks,m,pct,levels); bq,bn=self._weighted(bs,m,pct); aq,an=self._weighted(a_s,m,pct); total=bq+aq
        return {'imbalance_pct':bq/total*100 if total else 50.0,'weighted_bid':bq,'weighted_ask':aq,'weighted_bid_notional':bn,'weighted_ask_notional':an,'bid_levels':len(bs),'ask_levels':len(a_s),'bid_qty':sum(a for _,a in bs),'ask_qty':sum(a for _,a in a_s),'bid_notional':sum(p*a for p,a in bs),'ask_notional':sum(p*a for p,a in a_s),'bids_top10':[{'price':p,'amount':a,'notional':p*a} for p,a in sorted(self.bids.items(),reverse=True)[:10]],'asks_top10':[{'price':p,'amount':a,'notional':p*a} for p,a in sorted(self.asks.items())[:10]]}

    def walls(self,pct=None,multiple=6.0):
        pct=self.depth_pct if pct is None else float(pct); m=self.mid()
        if not m:return []
        out=[]
        for side,book in (('buy',self.bids),('sell',self.asks)):
            vals=[a for p,a in book.items() if abs(p-m)/m<=pct and a>0]
            if len(vals)<5:continue
            base=median(vals)
            if base<=0:continue
            for p,a in book.items():
                if abs(p-m)/m<=pct and a>=base*multiple:out.append({'side':side,'price':p,'amount':a,'notional':p*a,'multiple':round(a/base,1)})
        return sorted(out,key=lambda x:x['multiple'],reverse=True)[:10]

    def change_pressure(self,seconds=5):
        cut=time()-seconds; ev=[e for e in self.events if e['ts']>=cut]
        addb=sum(max(0,e['delta']) for e in ev if e['side']=='bid'); adda=sum(max(0,e['delta']) for e in ev if e['side']=='ask'); remb=sum(abs(min(0,e['delta'])) for e in ev if e['side']=='bid'); rema=sum(abs(min(0,e['delta'])) for e in ev if e['side']=='ask')
        buy=addb+rema; sell=adda+remb; total=buy+sell
        return {'buy_qty':buy,'sell_qty':sell,'buy_pct':buy/total*100 if total else 50.0,'sell_pct':sell/total*100 if total else 50.0,'events':len(ev)}

    def snapshot(self):
        d=self.depth_stats(); spread=self.spread_pct(); age=self.data_age_sec()
        return {'ready':self.ready,'best_bid':self.best_bid(),'best_ask':self.best_ask(),'mid':self.mid(),'spread_pct':round(spread,6) if spread is not None else None,'imbalance_pct':round(d['imbalance_pct'],2),'weighted_bid':round(d['weighted_bid'],8),'weighted_ask':round(d['weighted_ask'],8),'weighted_bid_notional':round(d['weighted_bid_notional'],4),'weighted_ask_notional':round(d['weighted_ask_notional'],4),'bid_qty':round(d['bid_qty'],8),'ask_qty':round(d['ask_qty'],8),'bid_notional':round(d['bid_notional'],4),'ask_notional':round(d['ask_notional'],4),'bid_levels':d['bid_levels'],'ask_levels':d['ask_levels'],'total_levels':d['bid_levels']+d['ask_levels'],'depth_window_pct':round(self.depth_pct*100,2),'max_levels':self.max_levels,'bids_top10':d['bids_top10'],'asks_top10':d['asks_top10'],'walls':self.walls(),'book_change':self.change_pressure(),'sequence':self.last_sequence,'last_update_ts':self.last_update_ts,'last_data_received_ts':self.last_data_received_ts,'last_data_source':self.last_data_source,'snapshot_age_sec':age,'freshness':self.freshness()}
