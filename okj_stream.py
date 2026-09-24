import asyncio
import json
from time import time

import aiohttp

WS_PUBLIC = "wss://ws.okj.com:443/ws/v5/public"
WS_BUSINESS = "wss://ws.okj.com:443/ws/v5/business"
REST = "https://api.okj.com"


class OKJStream:
    def __init__(self, pair, analyzer, broadcast=None):
        self.inst_id = "SHIB-JPY"
        self.a = analyzer
        self.broadcast = broadcast

        self.connected = False
        self.ws_transport_connected = False
        self.ws_business_connected = False
        self.ws_subscribed = False
        self.ws_trade_subscribed = False
        self.ws_data_state = "DISCONNECTED"
        self.ws_last_event = None
        self.last_error = None

        self.ws_messages = 0
        self.ws_orderbook_messages = 0
        self.ws_trade_messages = 0
        self.ws_ticker_messages = 0
        self.ws_subscribe_messages = 0
        self.ws_error_messages = 0
        self.ws_reconnects = 0
        self.ws_trade_reconnects = 0

        self.last_ws_trade_ts = None
        self.last_ws_ticker_ts = None
        self.last_ws_orderbook_ts = None
        self.last_ws_message_ts = None
        self.last_pong_ts = None
        self.last_subscribe_ts = None

        self.last_seq_id = None
        self.last_prev_seq_id = None
        self.last_checksum = None
        self.last_action = None
        self.ws_raw_preview = None
        self._stop = False

    async def emit(self):
        if self.broadcast:
            try:
                await self.broadcast()
            except Exception:
                pass

    async def bootstrap(self):
        try:
            timeout = aiohttp.ClientTimeout(total=15)
            async with aiohttp.ClientSession(timeout=timeout) as s:
                async with s.get(
                    f"{REST}/api/v5/market/books",
                    params={"instId": self.inst_id, "sz": "400"},
                ) as r:
                    p = await r.json()
                x = (p.get("data") or [{}])[0]
                self.a.book.load_snapshot(
                    x.get("bids", []), x.get("asks", []), None, x.get("ts")
                )

                async with s.get(
                    f"{REST}/api/v5/market/ticker",
                    params={"instId": self.inst_id},
                ) as r:
                    p = await r.json()
                x = (p.get("data") or [{}])[0]
                self.a.ticker({"last": x.get("last")})

                # Seed recent trades so trade flow is not empty after startup.
                async with s.get(
                    f"{REST}/api/v5/market/trades",
                    params={"instId": self.inst_id, "limit": "100"},
                ) as r:
                    p = await r.json()
                for t in p.get("data") or []:
                    self.a.trade(
                        {
                            "id": t.get("tradeId"),
                            "price": t.get("px"),
                            "amount": t.get("sz"),
                            "side": t.get("side"),
                            "executed_at": t.get("ts"),
                        }
                    )

                self.a.set_source("okj_rest")
                await self.emit()
        except Exception as e:
            self.last_error = str(e)

    async def subscribe_public(self, ws):
        msg = {
            "op": "subscribe",
            "args": [
                {"channel": "books", "instId": self.inst_id},
                {"channel": "tickers", "instId": self.inst_id},
            ],
        }
        await ws.send_str(json.dumps(msg, separators=(",", ":")))
        self.ws_subscribe_messages += 2
        self.last_subscribe_ts = time()
        self.ws_last_event = "public_subscribe_sent"

    async def subscribe_business(self, ws):
        msg = {
            "op": "subscribe",
            "args": [{"channel": "trades-all", "instId": self.inst_id}],
        }
        await ws.send_str(json.dumps(msg, separators=(",", ":")))
        self.ws_subscribe_messages += 1
        self.last_subscribe_ts = time()
        self.ws_last_event = "business_trade_subscribe_sent"

    async def handle_public(self, raw):
        if raw == "pong":
            self.last_pong_ts = time()
            return

        msg = json.loads(raw)
        self.ws_messages += 1
        self.last_ws_message_ts = time()
        self.ws_raw_preview = raw[:3000]

        if msg.get("event") == "subscribe":
            self.ws_subscribed = True
            self.ws_data_state = "SUBSCRIBED"
            return

        if msg.get("event") == "error":
            self.ws_error_messages += 1
            self.last_error = f'{msg.get("code")}: {msg.get("msg")}'
            return

        channel = (msg.get("arg") or {}).get("channel")
        data = msg.get("data") or []

        if channel == "books" and data:
            x = data[0]
            self.last_action = msg.get("action")
            self.last_seq_id = x.get("seqId")
            self.last_prev_seq_id = x.get("prevSeqId")
            self.last_checksum = x.get("checksum")

            if msg.get("action") == "snapshot":
                self.a.book.load_snapshot(
                    x.get("bids", []),
                    x.get("asks", []),
                    x.get("seqId"),
                    x.get("ts"),
                )
            else:
                local = self.a.book.last_sequence
                prev = x.get("prevSeqId")
                if local is not None and prev is not None and int(prev) != int(local):
                    raise RuntimeError(
                        f"orderbook sequence gap: local={local} prev={prev} seq={x.get('seqId')}"
                    )
                self.a.book.apply_diff(
                    x.get("bids", []),
                    x.get("asks", []),
                    x.get("seqId"),
                    x.get("ts"),
                )

            self.ws_orderbook_messages += 1
            self.last_ws_orderbook_ts = time()
            self.ws_data_state = "LIVE"
            self.a.set_source("okj_ws_orderbook")
            await self.emit()

        elif channel == "tickers":
            for x in data:
                self.a.ticker({"last": x.get("last")})
            self.ws_ticker_messages += len(data)
            self.last_ws_ticker_ts = time()
            await self.emit()

    async def handle_business(self, raw):
        if raw == "pong":
            self.last_pong_ts = time()
            return

        msg = json.loads(raw)
        self.ws_messages += 1
        self.last_ws_message_ts = time()
        self.ws_raw_preview = raw[:3000]

        if msg.get("event") == "subscribe":
            self.ws_trade_subscribed = True
            self.ws_last_event = "business_trade_subscribed"
            return

        if msg.get("event") == "error":
            self.ws_error_messages += 1
            self.last_error = f'{msg.get("code")}: {msg.get("msg")}'
            return

        channel = (msg.get("arg") or {}).get("channel")
        if channel != "trades-all":
            return

        for x in msg.get("data") or []:
            self.a.trade(
                {
                    "id": x.get("tradeId"),
                    "price": x.get("px"),
                    "amount": x.get("sz"),
                    "side": x.get("side"),
                    "executed_at": x.get("ts"),
                }
            )
            self.ws_trade_messages += 1
            self.last_ws_trade_ts = time()

        self.a.set_source("okj_ws_trade")
        await self.emit()

    async def public_once(self):
        timeout = aiohttp.ClientTimeout(total=None)
        async with aiohttp.ClientSession(timeout=timeout) as s:
            async with s.ws_connect(
                WS_PUBLIC,
                heartbeat=None,
                autoping=True,
                receive_timeout=None,
            ) as ws:
                self.connected = True
                self.ws_transport_connected = True
                self.a.set_ws(True, self.last_error)
                await self.subscribe_public(ws)

                while not self._stop:
                    try:
                        m = await asyncio.wait_for(ws.receive(), 15)
                    except asyncio.TimeoutError:
                        await ws.send_str("ping")
                        continue

                    if m.type == aiohttp.WSMsgType.TEXT:
                        await self.handle_public(m.data)
                    elif m.type in (
                        aiohttp.WSMsgType.CLOSED,
                        aiohttp.WSMsgType.ERROR,
                    ):
                        raise RuntimeError("public websocket closed")

    async def business_once(self):
        timeout = aiohttp.ClientTimeout(total=None)
        async with aiohttp.ClientSession(timeout=timeout) as s:
            async with s.ws_connect(
                WS_BUSINESS,
                heartbeat=None,
                autoping=True,
                receive_timeout=None,
            ) as ws:
                self.ws_business_connected = True
                await self.subscribe_business(ws)

                while not self._stop:
                    try:
                        m = await asyncio.wait_for(ws.receive(), 15)
                    except asyncio.TimeoutError:
                        await ws.send_str("ping")
                        continue

                    if m.type == aiohttp.WSMsgType.TEXT:
                        await self.handle_business(m.data)
                    elif m.type in (
                        aiohttp.WSMsgType.CLOSED,
                        aiohttp.WSMsgType.ERROR,
                    ):
                        raise RuntimeError("business websocket closed")

    async def public_runner(self):
        while not self._stop:
            try:
                await self.public_once()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.last_error = str(e)
                self.connected = False
                self.ws_transport_connected = False
                self.a.set_ws(False, self.last_error)
                self.ws_reconnects += 1
                await asyncio.sleep(min(10, 1 + self.ws_reconnects))

    async def business_runner(self):
        while not self._stop:
            try:
                await self.business_once()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.last_error = str(e)
                self.ws_business_connected = False
                self.ws_trade_subscribed = False
                self.ws_trade_reconnects += 1
                await asyncio.sleep(min(10, 1 + self.ws_trade_reconnects))

    async def run(self):
        await self.bootstrap()
        await asyncio.gather(
            self.public_runner(),
            self.business_runner(),
        )

    def stop(self):
        self._stop = True
