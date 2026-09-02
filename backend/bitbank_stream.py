import asyncio,json,logging,aiohttp
LOG=logging.getLogger("bitbank")
WS_URL="wss://stream.bitbank.cc/socket.io/?EIO=4&transport=websocket"

class BitbankStream:
    def __init__(self,pair,analyzer,broadcast):
        self.pair=pair; self.analyzer=analyzer; self.broadcast=broadcast
    async def rest_snapshot(self,session):
        async with session.get(f"https://public.bitbank.cc/{self.pair}/depth",timeout=10) as r:
            r.raise_for_status(); j=await r.json()
        if j.get("success")!=1: raise RuntimeError(f"depth API error: {j}")
        self.analyzer.load_depth(j["data"]); await self.broadcast()
    async def run(self):
        while True:
            try:
                async with aiohttp.ClientSession() as session:
                    await self.rest_snapshot(session)
                    async with session.ws_connect(WS_URL,heartbeat=20,autoping=True) as ws:
                        await ws.send_str("40"); await asyncio.sleep(.5)
                        for room in (f"depth_whole_{self.pair}",f"depth_diff_{self.pair}",f"transactions_{self.pair}",f"ticker_{self.pair}"):
                            await ws.send_str("42[\"join-room\",\""+room+"\"]")
                        LOG.info("connected to bitbank stream for %s",self.pair)
                        async for msg in ws:
                            if msg.type==aiohttp.WSMsgType.TEXT: await self.handle(msg.data)
                            elif msg.type in (aiohttp.WSMsgType.CLOSED,aiohttp.WSMsgType.ERROR): break
            except Exception as e:
                LOG.exception("stream disconnected: %s",e); await asyncio.sleep(5)
    async def handle(self,text):
        if text in ("2","3","40","41") or not text.startswith("42"): return
        try: payload=json.loads(text[2:])
        except Exception:return
        if not isinstance(payload,list) or len(payload)<2:return
        event,data=payload[0],payload[1]
        if event!="message" or not isinstance(data,dict):return
        room=data.get("room_name",""); msg=data.get("message",{})
        if isinstance(msg,dict) and "data" in msg:msg=msg["data"]
        if room.startswith("depth_whole_"):self.analyzer.load_depth(msg)
        elif room.startswith("depth_diff_"):self.analyzer.diff_depth(msg)
        elif room.startswith("ticker_"):self.analyzer.ticker(msg)
        elif room.startswith("transactions_"):
            for t in (msg.get("transactions",[]) if isinstance(msg,dict) else []):self.analyzer.trade(t)
        await self.broadcast()
