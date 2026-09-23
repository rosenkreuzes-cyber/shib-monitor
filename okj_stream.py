import asyncio
import json
import logging
import time
from typing import Optional

import aiohttp

LOG = logging.getLogger("okj")

REST = "https://api.okj.com"
WS = "wss://ws.okj.com:443/ws/v5/public"
INST_ID = "SHIB-JPY"
PAIR = "shib_jpy"
VERSION = "5.5-okj"

RECONNECT_INITIAL = 1.0
RECONNECT_MAX = 20.0
NO_DATA_RECONNECT = 35.0


class OKJStream:
    """OKJ public market-data stream for SHIB-JPY.

    Design:
      - WebSocket is the primary market-data source.
      - books gives a full snapshot then incremental updates.
      - trades gives aggregated public trades.
      - tickers keeps last/best bid/ask current.
      - REST is only a recovery/bootstrap fallback.
      - Sequence/checksum metadata is retained for diagnostics.
    """

    def __init__(self, pair, analyzer, broadcast=None):
        self.pair = pair
        self.inst_id = INST_ID
        self.analyzer = analyzer
        self._broadcast = broadcast

        self.connected = False
        self.subscribed = False
        self.last_error = None

        self.ws_messages = 0
        self.ws_orderbook_messages = 0
        self.ws_trade_messages = 0
        self.ws_ticker_messages = 0
        self.ws_subscribe_messages = 0
        self.ws_error_messages = 0
        self.ws_reconnects = 0

        self.last_ws_message_ts = None
        self.last_ws_orderbook_ts = None
        self.last_ws_trade_ts = None
        self.last_ws_ticker_ts = None
        self.last_pong_ts = None
        self.last_subscribe_ts = None
        self.last_snapshot_ts = None
        self.last_seq_id = None
        self.last_prev_seq_id = None
        self.last_checksum = None
        self.last_action = None
        self.last_raw_preview = None
        self.last_ws_error = None
        self.ws_close_code = None
        self.ws_close_reason = None

    async def broadcast(self):
        if self._broadcast is None:
            return
        try:
            result = self._broadcast()
            if asyncio.iscoroutine(result):
                await result
        except Exception:
            LOG.exception("broadcast failed")

    async def rest_bootstrap(self, session):
        """Bootstrap from OKJ REST if WS snapshot has not arrived yet."""
        try:
            async with session.get(
                f"{REST}/api/v5/market/books",
                params={"instId": self.inst_id, "sz": "400"},
            ) as resp:
                resp.raise_for_status()
                payload = await resp.json(content_type=None)

            rows = payload.get("data") or []
            if rows:
                book = rows[0]
                self.analyzer.load_okj_book(book)
                self.last_snapshot_ts = time.time()
                LOG.info(
                    "OKJ REST bootstrap: bids=%d asks=%d",
                    len(book.get("bids") or []),
                    len(book.get("asks") or []),
                )

            async with session.get(
                f"{REST}/api/v5/market/ticker",
                params={"instId": self.inst_id},
            ) as resp:
                resp.raise_for_status()
                payload = await resp.json(content_type=None)

            rows = payload.get("data") or []
            if rows:
                self.analyzer.ticker_okj(rows[0])

            self.analyzer.set_source("okj_rest_bootstrap")
            await self.broadcast()
        except Exception as exc:
            self.last_error = str(exc)
            LOG.warning("OKJ REST bootstrap failed: %s", exc)

    async def _subscribe(self, ws):
        args = [
            {"channel": "books", "instId": self.inst_id},
            {"channel": "trades", "instId": self.inst_id},
            {"channel": "tickers", "instId": self.inst_id},
        ]
        await ws.send_json({"op": "subscribe", "args": args})
        self.subscribed = True
        self.last_subscribe_ts = time.time()
        LOG.info("OKJ subscribe sent: %s", args)

    async def _ping_loop(self, ws):
        while True:
            await asyncio.sleep(15)
            if ws.closed:
                return
            await ws.send_str("ping")
            self.last_action = "ping"

    async def run(self):
        delay = RECONNECT_INITIAL

        while True:
            session = None
            try:
                timeout = aiohttp.ClientTimeout(
                    total=None,
                    connect=15,
                    sock_connect=15,
                    sock_read=None,
                )
                session = aiohttp.ClientSession(timeout=timeout)
                await self.rest_bootstrap(session)

                LOG.info("Connecting OKJ WS: %s", WS)
                async with session.ws_connect(
                    WS,
                    heartbeat=None,
                    autoping=False,
                    timeout=15,
                    max_msg_size=8 * 1024 * 1024,
                ) as ws:
                    self.connected = True
                    self.subscribed = False
                    self.last_error = None
                    self.last_ws_error = None
                    self.ws_close_code = None
                    self.ws_close_reason = None
                    self.analyzer.set_ws(True, None)
                    delay = RECONNECT_INITIAL

                    await self._subscribe(ws)
                    ping_task = asyncio.create_task(self._ping_loop(ws))
                    try:
                        async for msg in ws:
                            self.last_ws_message_ts = time.time()
                            self.ws_messages += 1

                            if msg.type == aiohttp.WSMsgType.TEXT:
                                text = msg.data
                                if text == "pong":
                                    self.last_pong_ts = time.time()
                                    continue
                                await self.handle(text)

                            elif msg.type == aiohttp.WSMsgType.BINARY:
                                try:
                                    await self.handle(msg.data.decode())
                                except Exception:
                                    LOG.warning("OKJ binary frame could not be decoded")

                            elif msg.type == aiohttp.WSMsgType.PING:
                                await ws.pong()
                                self.last_pong_ts = time.time()

                            elif msg.type == aiohttp.WSMsgType.PONG:
                                self.last_pong_ts = time.time()

                            elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                                self.ws_close_code = ws.close_code
                                self.ws_close_reason = str(msg.extra)
                                break

                            if self.last_ws_message_ts and (
                                time.time() - self.last_ws_message_ts > NO_DATA_RECONNECT
                            ):
                                break
                    finally:
                        ping_task.cancel()
                        try:
                            await ping_task
                        except asyncio.CancelledError:
                            pass

            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.last_error = str(exc)
                self.last_ws_error = str(exc)
                LOG.exception("OKJ stream error; reconnect in %.1fs", delay)
            finally:
                self.connected = False
                self.subscribed = False
                self.analyzer.set_ws(False, self.last_error)
                self.ws_reconnects += 1
                await self.broadcast()
                if session is not None:
                    await session.close()

            await asyncio.sleep(delay)
            delay = min(delay * 2, RECONNECT_MAX)

    async def handle(self, text):
        try:
            data = json.loads(text)
        except Exception as exc:
            self.last_ws_error = f"invalid JSON: {exc}"
            return

        self.last_raw_preview = text[:1000]

        if not isinstance(data, dict):
            return

        event = data.get("event")
        if event == "subscribe":
            self.ws_subscribe_messages += 1
            LOG.info("OKJ subscribe ACK: %s", data.get("arg"))
            return

        if event == "error":
            self.ws_error_messages += 1
            self.last_ws_error = f"{data.get('code')}: {data.get('msg')}"
            LOG.error("OKJ WS subscription error: %s", self.last_ws_error)
            return

        arg = data.get("arg") or {}
        channel = arg.get("channel")
        rows = data.get("data") or []
        if not rows:
            return

        now = time.time()

        if channel == "books":
            book = rows[0]
            if not isinstance(book, dict):
                return
            self.ws_orderbook_messages += 1
            self.last_ws_orderbook_ts = now
            self.last_seq_id = book.get("seqId")
            self.last_prev_seq_id = book.get("prevSeqId")
            self.last_checksum = book.get("checksum")
            self.last_action = data.get("action")

            if data.get("action") == "snapshot" or self.last_snapshot_ts is None:
                self.analyzer.load_okj_book(book)
                self.last_snapshot_ts = now
            else:
                self.analyzer.diff_okj_book(book)

            self.analyzer.set_source("okj_ws_orderbook")
            if self.ws_orderbook_messages <= 3:
                LOG.info(
                    "OKJ book #%d action=%s bids=%d asks=%d seq=%s prev=%s",
                    self.ws_orderbook_messages,
                    data.get("action"),
                    len(book.get("bids") or []),
                    len(book.get("asks") or []),
                    book.get("seqId"),
                    book.get("prevSeqId"),
                )
            await self.broadcast()
            return

        if channel == "trades":
            changed = False
            for row in rows:
                if not isinstance(row, dict):
                    continue
                before = len(self.analyzer.trades)
                self.analyzer.trade_okj(row)
                changed = changed or len(self.analyzer.trades) > before
            if changed:
                self.ws_trade_messages += len(rows)
                self.last_ws_trade_ts = now
                await self.broadcast()
            return

        if channel == "tickers":
            for row in rows:
                if isinstance(row, dict):
                    self.analyzer.ticker_okj(row)
            self.ws_ticker_messages += len(rows)
            self.last_ws_ticker_ts = now
            await self.broadcast()
