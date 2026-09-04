from collections import deque
from time import time
from orderbook import OrderBookEngine

class MarketAnalyzer:
    def __init__(self):
        self.book=OrderBookEngine(depth_pct=.05,max_levels=100); self.last_price=None; self.prev_price=None
        self.trades=deque(maxlen=10000); self.source='unknown'; self.ws_connected=False; self.last_error=None
        self.last_trade_ids=deque(maxlen=5000); self.last_trade_id_set=set(); self.absorption={'buy':0.0,'sell':0.0}; self.large_trade_threshold_jpy=100_000.0
    def set_source(self,source):self.source=source
    def set_ws(self,connected,error=None):self.ws_connected=connected; self.last_error=error
    def load_depth(self,d):self.book.load_snapshot(d.get('bids',[]),d.get('asks',[]),d.get('sequence'),d.get('last_update_at'))
    def diff_depth(self,d):self.book.apply_diff(d.get('bids',[]),d.get('asks',[]),d.get('sequence'),d.get('last_update_at'))
    def ticker(self,d):
        last=d.get('last') or d.get('last_price')
        try:
            v=float(last)
            if v>0:self.prev_price=self.last_price; self.last_price=v
        except (TypeError,ValueError):pass
    @staticmethod
    def _side(v):
        if v is None:return None
        v=str(v).lower(); return 'buy' if v in ('buy','bid') else 'sell' if v in ('sell','ask') else None
    def trade(self,t):
        side=self._side(t.get('side') or t.get('order_type'))
        try:price=float(t.get('price') or t.get('rate')); amount=float(t.get('amount'))
        except (TypeError,ValueError):return
        if side is None or price<=0 or amount<=0:return
        trade_id=t.get('id')
        if trade_id is not None:
            key=str(trade_id)
            if key in self.last_trade_id_set:return
            if len(self.last_trade_ids)>=self.last_trade_ids.maxlen:
                old=self.last_trade_ids.popleft(); self.last_trade_id_set.discard(old)
            self.last_trade_ids.append(key); self.last_trade_id_set.add(key)
        raw=t.get('executed_at') or t.get('created_at') or time()
        try:ts=float(raw)
        except (TypeError,ValueError):ts=time()
        if ts>10_000_000_000:ts/=1000
        self.trades.append({'ts':ts,'side':side,'price':price,'amount':amount,'notional':price*amount,'id':trade_id}); self.prev_price=self.last_price; self.last_price=price; self._update_absorption(side,price,amount)
    def _update_absorption(self,trade_side,price,amount):
        m=self.book.mid()
        if not m:return
        window=max(m*.002,price*.002)
        if trade_side=='buy':nearby=sum(a for p,a in self.book.asks.items() if abs(p-price)<=window); self.absorption['buy']=min(1_000_000,self.absorption['buy']*.95+min(amount,nearby))
        else:nearby=sum(a for p,a in self.book.bids.items() if abs(p-price)<=window); self.absorption['sell']=min(1_000_000,self.absorption['sell']*.95+min(amount,nearby))
    def flow(self,seconds=60,large=False):
        now=time(); rows=[t for t in self.trades if 0<=now-t['ts']<=seconds]
        if large:rows=[t for t in rows if t['notional']>=self.large_trade_threshold_jpy]
        b=sum(t['notional'] if large else t['amount'] for t in rows if t['side']=='buy'); s=sum(t['notional'] if large else t['amount'] for t in rows if t['side']=='sell'); total=b+s
        return {'buy':b,'sell':s,'buy_pct':b/total*100 if total else 50.0,'sell_pct':s/total*100 if total else 50.0,'count':len(rows)}
    def _price_change(self,seconds=300):
        now=time(); rows=[t for t in self.trades if 0<=now-t['ts']<=seconds]
        if len(rows)>=2:return (rows[-1]['price']/rows[0]['price']-1)*100
        return 0.0
    def snapshot(self):
        book=self.book.snapshot(); flow=self.flow(60); large=self.flow(60,True); wb=sum(x['multiple'] for x in book['walls'] if x['side']=='buy'); ws=sum(x['multiple'] for x in book['walls'] if x['side']=='sell'); wall_buy=wb/(wb+ws)*100 if wb+ws else 50.0
        freshness=book['freshness']; usable=book['ready'] and freshness not in ('INVALID','UNKNOWN')
        price_momentum=max(0,min(100,50+self._price_change(300)*10)); components={'板のインバランス':.40*book['imbalance_pct'],'板の厚み比率':.15*(book['weighted_bid']/(book['weighted_bid']+book['weighted_ask'])*100 if book['weighted_bid']+book['weighted_ask'] else 50),'トレードフロー':.15*flow['buy_pct'],'価格モメンタム':.10*price_momentum,'巨大注文':.10*wall_buy,'大口約定':.10*large['buy_pct']}
        score=sum(components.values()) if usable else None
        label=('買い圧力 強' if score is not None and score>=72 else '買い優勢' if score is not None and score>=58 else '拮抗' if score is not None and score>42 else '売り優勢' if score is not None and score>28 else '売り圧力 強') if score is not None else '判定停止'
        change=((self.last_price/self.prev_price-1)*100 if self.prev_price and self.last_price else 0.0)
        return {'version':'v5.1','pair':'SHIB/JPY','price':self.last_price,'score':round(score,1) if score is not None else None,'label':label,'score_usable':usable,'price_change_pct':round(change,4),'price_change_5m_pct':round(self._price_change(300),4),'source':self.source,'ws_connected':self.ws_connected,'last_error':self.last_error,'book':book,'trade_flow':flow,'large_trade_flow':large,'absorption':{'buy':round(self.absorption['buy'],8),'sell':round(self.absorption['sell'],8)},'score_components':{k:round(v,2) for k,v in components.items()},'server_time':int(time()*1000)}
