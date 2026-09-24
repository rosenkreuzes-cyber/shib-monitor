import asyncio,json
from time import time
import aiohttp

WS="wss://ws.okj.com:443/ws/v5/public"
REST="https://api.okj.com"

class OKJStream:
    def __init__(self,pair,analyzer,broadcast=None):
        self.inst_id="SHIB-JPY"; self.a=analyzer; self.broadcast=broadcast
        self.connected=False; self.ws_transport_connected=False; self.ws_subscribed=False
        self.ws_data_state="DISCONNECTED"; self.ws_last_event=None; self.last_error=None
        self.ws_messages=0; self.ws_orderbook_messages=0; self.ws_trade_messages=0
        self.ws_ticker_messages=0; self.ws_subscribe_messages=0; self.ws_error_messages=0
        self.ws_reconnects=0; self.last_ws_trade_ts=None; self.last_ws_ticker_ts=None
        self.last_ws_orderbook_ts=None; self.last_ws_message_ts=None
        self.last_seq_id=None; self.last_prev_seq_id=None; self.last_checksum=None; self.last_action=None
        self.ws_raw_preview=None; self._stop=False

    async def emit(self):
        if self.broadcast:
            try: await self.broadcast()
            except Exception: pass

    async def bootstrap(self):
        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15)) as s:
                async with s.get(f"{REST}/api/v5/market/books",params={"instId":self.inst_id,"sz":"400"}) as r:
                    p=await r.json()
                x=(p.get("data") or [{}])[0]
                self.a.book.load_snapshot(x.get("bids",[]),x.get("asks",[]),None,x.get("ts"))
                async with s.get(f"{REST}/api/v5/market/ticker",params={"instId":self.inst_id}) as r:
                    p=await r.json()
                x=(p.get("data") or [{}])[0]; self.a.ticker({"last":x.get("last")})
                self.a.set_source("okj_rest"); await self.emit()
        except Exception as e: self.last_error=str(e)

    async def subscribe(self,ws):
        m={"op":"subscribe","args":[{"channel":"books","instId":self.inst_id},{"channel":"trades","instId":self.inst_id},{"channel":"tickers","instId":self.inst_id}]}
        await ws.send_str(json.dumps(m,separators=(",",":"))); self.ws_subscribe_messages=3
        self.ws_subscribed=True; self.ws_last_event="subscribe_sent"

    async def handle(self,raw):
        if raw=="pong": return
        m=json.loads(raw); self.ws_messages+=1; self.last_ws_message_ts=time(); self.ws_raw_preview=raw[:3000]
        if m.get("event")=="subscribe": self.ws_subscribed=True; self.ws_data_state="SUBSCRIBED"; return
        if m.get("event")=="error": self.ws_error_messages+=1; self.last_error=f'{m.get("code")}: {m.get("msg")}'; return
        ch=(m.get("arg") or {}).get("channel"); data=m.get("data") or []
        if ch=="books":
            x=data[0]; self.last_action=m.get("action"); self.last_seq_id=x.get("seqId"); self.last_prev_seq_id=x.get("prevSeqId"); self.last_checksum=x.get("checksum")
            if m.get("action")=="snapshot": self.a.book.load_snapshot(x.get("bids",[]),x.get("asks",[]),x.get("seqId"),x.get("ts"))
            else:
                local=self.a.book.last_sequence
                if local is not None and x.get("prevSeqId") is not None and int(x["prevSeqId"])!=int(local): raise RuntimeError("orderbook sequence gap")
                self.a.book.apply_diff(x.get("bids",[]),x.get("asks",[]),x.get("seqId"),x.get("ts"))
            self.ws_orderbook_messages+=1; self.last_ws_orderbook_ts=time(); self.ws_data_state="LIVE"; self.a.set_source("okj_ws_orderbook"); await self.emit()
        elif ch=="trades":
            for x in data:
                self.a.trade({"id":x.get("tradeId"),"price":x.get("px"),"amount":x.get("sz"),"side":x.get("side"),"executed_at":x.get("ts")})
                self.ws_trade_messages+=1; self.last_ws_trade_ts=time()
            self.a.set_source("okj_ws_trade"); await self.emit()
        elif ch=="tickers":
            for x in data: self.a.ticker({"last":x.get("last")})
            self.ws_ticker_messages+=len(data); self.last_ws_ticker_ts=time(); await self.emit()

    async def once(self):
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=None)) as s:
            async with s.ws_connect(WS,heartbeat=None,autoping=True,receive_timeout=None) as ws:
                self.connected=self.ws_transport_connected=True; self.ws_data_state="CONNECTED_NO_DATA"; self.a.set_ws(True)
                await self.subscribe(ws)
                while not self._stop:
                    try: m=await asyncio.wait_for(ws.receive(),15)
                    except asyncio.TimeoutError: await ws.send_str("ping"); continue
                    if m.type==aiohttp.WSMsgType.TEXT: await self.handle(m.data)
                    elif m.type in (aiohttp.WSMsgType.CLOSED,aiohttp.WSMsgType.ERROR): raise RuntimeError("websocket closed")

    async def run(self):
        await self.bootstrap()
        while not self._stop:
            try: await self.once()
            except asyncio.CancelledError: raise
            except Exception as e:
                self.last_error=str(e); self.connected=False; self.ws_transport_connected=False; self.a.set_ws(False,self.last_error)
                self.ws_reconnects+=1; await asyncio.sleep(min(10,1+self.ws_reconnects))
    def stop(self): self._stop=True
