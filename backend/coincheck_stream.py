import asyncio, json, logging, time
import aiohttp

LOG = logging.getLogger("coincheck")
REST = "https://coincheck.com"
WS = "wss://ws-api.coincheck.com/"

class CoincheckStream:
    def __init__(self, pair, analyzer, broadcast):
        self.pair = pair
        self.analyzer = analyzer
        self.broadcast = broadcast
        self.connected = False
        self.last_error = None

    async def rest_snapshot(self, session):
        async with session.get(f"{REST}/api/order_books", params={"pair": self.pair}, timeout=10) as r:
            r.raise_for_status()
            j = await r.json(content_type=None)
        if not j.get("asks") and not j.get("bids"):
            raise RuntimeError(f"empty order book: {j}")
        self.analyzer.load_depth(j)
        async with session.get(f"{REST}/api/ticker", params={"pair": self.pair}, timeout=10) as r:
            r.raise_for_status()
            t = await r.json(content_type=None)
        self.analyzer.ticker(t)
        self.analyzer.set_source("coincheck_rest")
        await self.broadcast()

    async def run(self):
        while True:
            try:
                async with aiohttp.ClientSession() as session:
                    await self.rest_snapshot(session)
                    async with session.ws_connect(WS, heartbeat=20, autoping=True, timeout=15) as ws:
                        self.connected = True
                        self.last_error = None
                        self.analyzer.set_ws(True, None)
                        await ws.send_json({"type": "subscribe", "channel": f"{self.pair}-orderbook"})
                        await ws.send_json({"type": "subscribe", "channel": f"{self.pair}-trades"})
                        LOG.info("connected to Coincheck public WS for %s", self.pair)
                        await self.broadcast()
                        async for msg in ws:
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                await self.handle(msg.data)
                            elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                                break
            except Exception as e:
                self.connected = False
                self.last_error = f"{type(e).__name__}: {e}"
                self.analyzer.set_ws(False, self.last_error)
                LOG.exception("Coincheck stream disconnected: %s", e)
                await self.broadcast()
                await asyncio.sleep(5)
            finally:
                self.connected = False

    async def handle(self, text):
        try:
            data = json.loads(text)
        except Exception:
            return
        if isinstance(data, list) and len(data) == 2 and data[0] == self.pair and isinstance(data[1], dict):
            payload = data[1]
            if "bids" in payload or "asks" in payload:
                self.analyzer.diff_depth(payload)
                await self.broadcast()
            return
        if isinstance(data, list):
            # trades: [[timestamp,id,pair,rate,amount,side,...], ...]
            for row in data:
                if isinstance(row, list) and len(row) >= 6 and row[2] == self.pair:
                    self.analyzer.trade({
                        "executed_at": row[0], "id": row[1], "pair": row[2],
                        "price": row[3], "amount": row[4], "side": row[5]
                    })
            if data:
                await self.broadcast()
