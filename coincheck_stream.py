import asyncio
import json
import logging
from time import time
import aiohttp

LOG=logging.getLogger(__name__)
REST_BASE="https://coincheck.com/api"
WS_URL="wss://ws-api.coincheck.com/"
REST_INTERVAL=5.0
RECONNECT_INITIAL=2.0
RECONNECT_MAX=30.0
WS_STALE_SECONDS=30.0
WS_IDLE_SECONDS=10.0

class CoincheckStream:
    def __init__(self,pair,analyzer,broadcast=None):
        self.pair=pair; self.analyzer=analyzer; self._broadcast=broadcast; self._stopped=False
        self.connected=False; self.transport_connected=False; self.subscribed=False
        self.last_error=None; self.ws_last_error=None
        self.ws_messages=0; self.ws_orderbook_messages=0; self.ws_trade_messages=0
        self.last_ws_message_ts=None; self.last_ws_orderbook_ts=None; self.last_ws_trade_ts=None
        self.ws_subscribe_sent_ts=None; self.ws_subscribe_ack_ts=None; self.ws_subscribe_error_ts=None
        self.ws_last_event=None; self.ws_last_channel=None; self.ws_raw_preview_type=None; self.ws_raw_preview=None
        self.ws_close_code=None; self.ws_close_reason=None; self.ws_exception_type=None; self.ws_exception_message=None
        self.ws_trade_raw_preview=None; self.ws_trade_parse_failures=0; self.ws_nontrade_list_messages=0
        self.ws_last_data_state="NEVER"
        self.rest_refresh_count=0; self.last_rest_refresh_ts=None; self.rest_fail_count=0
    def now(self): return time()
    async def broadcast(self):
        if self._broadcast is None:return
        try:
            r=self._broadcast()
            if asyncio.iscoroutine(r): await r
        except Exception: LOG.exception("broadcast failed")
    async def rest_refresh(self,session):
        try:
            async with session.get(f"{REST_BASE}/order_books",params={"pair":self.pair}) as r:
                r.raise_for_status(); d=await r.json(content_type=None)
            bids=d.get("bids") or []; asks=d.get("asks") or []
            if not bids or not asks: raise RuntimeError(f"empty orderbook bids={len(bids)} asks={len(asks)}")
            self.analyzer.load_depth(d)
            try:
                async with session.get(f"{REST_BASE}/ticker",params={"pair":self.pair}) as r:
                    r.raise_for_status(); t=await r.json(content_type=None)
                if isinstance(t,dict): self.analyzer.ticker(t)
            except Exception as e: LOG.warning("ticker refresh failed: %s",e)
            try:
                async with session.get(f"{REST_BASE}/trades",params={"pair":self.pair,"limit":20}) as r:
                    r.raise_for_status(); tp=await r.json(content_type=None)
                for row in (tp.get("data",[]) if isinstance(tp,dict) else []):
                    if isinstance(row,dict): self.analyzer.trade({"executed_at":row.get("created_at",row.get("executed_at")),"id":row.get("id"),"pair":row.get("pair",self.pair),"price":row.get("price",row.get("rate")),"amount":row.get("amount"),"side":row.get("side",row.get("order_type"))})
            except Exception as e: LOG.warning("trades refresh failed: %s",e)
            self.last_error=None; self.analyzer.last_error=None; self.rest_refresh_count+=1; self.last_rest_refresh_ts=self.now()
            await self.broadcast()
        except asyncio.CancelledError: raise
        except Exception as e:
            self.last_error=str(e); self.analyzer.last_error=self.last_error; self.rest_fail_count+=1
            LOG.warning("REST refresh failed: %s",e)
    async def rest_loop(self,session):
        while not self._stopped:
            await self.rest_refresh(session); await asyncio.sleep(REST_INTERVAL)
    async def subscribe(self,ws,ch):
        await ws.send_str(json.dumps({"type":"subscribe","channel":ch}))
        self.ws_subscribe_sent_ts=self.now(); self.ws_last_event=f"subscribe_sent:{ch}"
    async def handle(self,text):
        try: d=json.loads(text)
        except Exception: return
        self.ws_messages+=1; self.last_ws_message_ts=self.now(); self.ws_raw_preview_type=type(d).__name__; self.ws_raw_preview=json.dumps(d,ensure_ascii=False)[:1000]
        if isinstance(d,dict):
            typ=d.get("type")
            if typ in ("subscribed","subscribe"):
                self.ws_subscribe_ack_ts=self.now(); self.ws_last_event=f"subscribed:{d.get('channel')}"; self.subscribed=True; self.ws_last_channel=d.get("channel"); return
            if typ in ("error","subscribe_error"):
                self.ws_subscribe_error_ts=self.now(); self.ws_last_event="subscribe_error"; self.ws_last_error=str(d); self.analyzer.ws_last_error=self.ws_last_error; return
        if isinstance(d,list) and len(d)>=2 and d[0]==self.pair and isinstance(d[1],dict):
            p=d[1]
            if "bids" in p or "asks" in p:
                self.ws_orderbook_messages+=1; self.last_ws_orderbook_ts=self.now(); self.ws_last_channel=f"{self.pair}-orderbook"; self.ws_last_event="orderbook_received"
                self.analyzer.diff_depth(p); self.analyzer.set_ws(True,None); self.ws_last_error=None; await self.broadcast()
            return
        if isinstance(d,list):
            changed=False
            matched_rows=0
            for row in d:
                if isinstance(row,list) and len(row)>=6 and row[2]==self.pair:
                    matched_rows += 1
                    self.ws_trade_raw_preview=json.dumps(row,ensure_ascii=False)[:1000]
                    side=row[5]
                    if side not in ("buy","sell"):
                        self.ws_trade_parse_failures += 1
                        self.ws_last_event="trade_parse_error"
                        continue
                    try:
                        float(row[3]); float(row[4]); float(row[0])
                    except (TypeError,ValueError):
                        self.ws_trade_parse_failures += 1
                        self.ws_last_event="trade_parse_error"
                        continue
                    self.ws_trade_messages+=1; self.last_ws_trade_ts=self.now(); self.ws_last_channel=f"{self.pair}-trades"; self.ws_last_event="trade_received"
                    self.analyzer.trade({"executed_at":row[0],"id":row[1],"pair":row[2],"price":row[3],"amount":row[4],"side":row[5]}); changed=True
            if matched_rows == 0:
                self.ws_nontrade_list_messages += 1
            if changed: self.analyzer.set_ws(True,None); self.ws_last_error=None; await self.broadcast()
    async def ws_session(self,session):
        async with session.ws_connect(WS_URL,heartbeat=20,autoping=True,autoclose=True,receive_timeout=None,timeout=15) as ws:
            self.transport_connected=True; self.connected=True; self.ws_last_event="transport_connected"; self.analyzer.set_ws(True,None)
            await self.subscribe(ws,f"{self.pair}-orderbook"); await self.subscribe(ws,f"{self.pair}-trades")
            while not self._stopped:
                m=await ws.receive()
                if m.type==aiohttp.WSMsgType.TEXT: await self.handle(m.data)
                elif m.type==aiohttp.WSMsgType.BINARY: self.ws_messages+=1; self.last_ws_message_ts=self.now()
                elif m.type==aiohttp.WSMsgType.PING: await ws.pong()
                elif m.type==aiohttp.WSMsgType.PONG: pass
                elif m.type in (aiohttp.WSMsgType.CLOSE,aiohttp.WSMsgType.CLOSED,aiohttp.WSMsgType.ERROR): raise RuntimeError(f"websocket closed type={m.type}")
    async def ws_loop(self,session):
        delay=RECONNECT_INITIAL
        while not self._stopped:
            try:
                LOG.info("Coincheck WS connecting %s pair=%s",WS_URL,self.pair)
                await self.ws_session(session); delay=RECONNECT_INITIAL
            except asyncio.CancelledError: raise
            except Exception as e:
                self.connected=False; self.transport_connected=False; self.subscribed=False; self.ws_exception_type=type(e).__name__; self.ws_exception_message=str(e); self.ws_last_error=str(e); self.analyzer.set_ws(False,self.ws_last_error)
                LOG.warning("Coincheck WS disconnected: %s",e); await self.broadcast(); await asyncio.sleep(delay); delay=min(delay*2,RECONNECT_MAX)
            finally:
                self.connected=False; self.transport_connected=False; self.subscribed=False
    async def run(self):
        timeout=aiohttp.ClientTimeout(total=None,connect=15,sock_connect=15,sock_read=None)
        async with aiohttp.ClientSession(timeout=timeout) as s:
            rt=asyncio.create_task(self.rest_loop(s)); wt=asyncio.create_task(self.ws_loop(s))
            try: await asyncio.gather(rt,wt)
            finally:
                self._stopped=True; rt.cancel(); wt.cancel()
                for t in (rt,wt):
                    try: await t
                    except asyncio.CancelledError: pass
    def health(self):
        n=self.now(); age=lambda x: round(n-x,3) if x else None
        msg_age=age(self.last_ws_message_ts)
        ob_age=age(self.last_ws_orderbook_ts)
        trade_age=age(self.last_ws_trade_ts)
        if not self.connected:
            data_state="DISCONNECTED"
        elif ob_age is None and trade_age is None:
            data_state="CONNECTED_NO_DATA"
        elif ob_age is not None and ob_age <= WS_IDLE_SECONDS:
            data_state="LIVE"
        elif trade_age is not None and trade_age <= WS_IDLE_SECONDS:
            data_state="LIVE_TRADE"
        elif ob_age is not None and ob_age <= WS_STALE_SECONDS:
            data_state="IDLE"
        else:
            data_state="STALE"
        self.ws_last_data_state=data_state
        return {"ws_transport_connected":self.transport_connected,"ws_connected":self.connected,"ws_subscribed":self.subscribed,"ws_age_sec":msg_age,"ws_orderbook_age_sec":ob_age,"ws_trade_age_sec":trade_age,"ws_data_state":data_state,"ws_subscribe_sent_ts":self.ws_subscribe_sent_ts,"ws_subscribe_ack_ts":self.ws_subscribe_ack_ts,"ws_subscribe_error_ts":self.ws_subscribe_error_ts,"ws_last_event":self.ws_last_event,"ws_last_channel":self.ws_last_channel,"ws_raw_preview_type":self.ws_raw_preview_type,"ws_raw_preview":self.ws_raw_preview,"ws_close_code":self.ws_close_code,"ws_close_reason":self.ws_close_reason,"ws_exception_type":self.ws_exception_type,"ws_exception_message":self.ws_exception_message,"ws_messages":self.ws_messages,"ws_orderbook_messages":self.ws_orderbook_messages,"ws_trade_messages":self.ws_trade_messages,"ws_trade_parse_failures":self.ws_trade_parse_failures,"ws_nontrade_list_messages":self.ws_nontrade_list_messages,"ws_trade_raw_preview":self.ws_trade_raw_preview,"last_ws_message_ts":self.last_ws_message_ts,"last_ws_orderbook_ts":self.last_ws_orderbook_ts,"last_ws_trade_ts":self.last_ws_trade_ts,"rest_refresh_count":self.rest_refresh_count,"last_rest_refresh_ts":self.last_rest_refresh_ts,"rest_fail_count":self.rest_fail_count,"rest_last_error":self.last_error,"ws_last_error":self.ws_last_error}
