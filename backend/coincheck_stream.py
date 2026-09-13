import asyncio
import json
import logging
from time import time

import aiohttp

LOG = logging.getLogger(__name__)

REST = "https://coincheck.com"
WS = "wss://ws-api.coincheck.com/"
PAIR = "shib_jpy"
VERSION = "5.4"

from analyzer import MarketAnalyzer


class CoincheckStream:
    def __init__(self, analyzer: MarketAnalyzer):
        self.analyzer = analyzer
        self.connected = False
        self.last_error = None
        self.ws_messages = 0
        self.ws_orderbook_messages = 0
        self.ws_trade_messages = 0
        self.last_ws_message_ts = None
        self.last_ws_orderbook_ts = None
        self._debug_raw_messages = 0
        self._debug_raw_limit = 10
        self._broadcast = None

    def set_broadcast(self, callback):
        self._broadcast = callback

    def _now(self):
        return time()

    async def broadcast(self):
        if self._broadcast is None:
            return
        try:
            result = self._broadcast()
            if asyncio.iscoroutine(result):
                await result
        except Exception:
            LOG.exception("broadcast failed")

    async def rest_snapshot(self):
        timeout = aiohttp.ClientTimeout(total=15)
        try:
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(f"{REST}/api/order_books") as resp:
                    resp.raise_for_status()
                    data = await resp.json()

                self.analyzer.book.load_snapshot(
                    data.get("bids", []),
                    data.get("asks", []),
                )

                try:
                    async with session.get(
                        f"{REST}/api/ticker?pair={PAIR}"
                    ) as resp:
                        resp.raise_for_status()
                        ticker = await resp.json()
                    if isinstance(ticker, dict):
                        last = ticker.get("last")
                        if last is not None:
                            self.analyzer.price = float(last)
                except Exception:
                    LOG.exception("ticker REST request failed")

                self.analyzer.set_source("coincheck_rest")
                await self.broadcast()

                LOG.info(
                    "REST snapshot loaded pair=%s bids=%d asks=%d",
                    PAIR,
                    len(data.get("bids", [])),
                    len(data.get("asks", [])),
                )
        except Exception as exc:
            self.last_error = str(exc)
            LOG.exception("REST snapshot failed")

    async def _subscribe(self, ws, channel):
        message = json.dumps({"type": "subscribe", "channel": channel})
        await ws.send_str(message)
        LOG.info("Coincheck subscribe sent: %s", channel)

    async def _watchdog(self, ws):
        while not ws.closed:
            await asyncio.sleep(5)
            now = self._now()
            ws_age = (
                now - self.last_ws_message_ts
                if self.last_ws_message_ts is not None else None
            )
            orderbook_age = (
                now - self.last_ws_orderbook_ts
                if self.last_ws_orderbook_ts is not None else None
            )

            LOG.debug(
                "watchdog pair=%s ws_age=%s orderbook_age=%s "
                "ws_messages=%d orderbook_messages=%d trade_messages=%d",
                PAIR, ws_age, orderbook_age,
                self.ws_messages, self.ws_orderbook_messages,
                self.ws_trade_messages,
            )

            if orderbook_age is None:
                if ws_age is not None and ws_age > 20:
                    LOG.warning(
                        "websocket connected but no orderbook message "
                        "received for %.1fs; reconnecting", ws_age
                    )
                    await ws.close(code=1012, message=b"no orderbook data")
                    return
                continue

            if orderbook_age > 30:
                LOG.warning(
                    "orderbook websocket stale %.1fs; forcing websocket reconnect",
                    orderbook_age
                )
                await ws.close(code=1012, message=b"orderbook stale")
                return

    async def _handle_orderbook(self, payload):
        if not isinstance(payload, dict):
            LOG.warning(
                "orderbook payload is not dict: type=%s payload=%r",
                type(payload).__name__, payload
            )
            return

        bids = payload.get("bids", [])
        asks = payload.get("asks", [])

        self.ws_orderbook_messages += 1
        self.last_ws_orderbook_ts = self._now()

        changed = self.analyzer.diff_depth(payload)
        self.analyzer.set_source("coincheck_ws_orderbook")

        LOG.info(
            "orderbook websocket received #%d pair=%s "
            "bids=%d asks=%d changed=%s last_update_at=%s",
            self.ws_orderbook_messages, PAIR, len(bids), len(asks),
            changed, payload.get("last_update_at")
        )
        await self.broadcast()

    async def _handle_trade(self, payload):
        if not isinstance(payload, list):
            LOG.warning(
                "trade payload is not list: type=%s payload=%r",
                type(payload).__name__, payload
            )
            return

        before_count = len(self.analyzer.trades)
        try:
            self.analyzer.trade(payload)
        except Exception:
            LOG.exception("trade payload handling failed")
            return

        after_count = len(self.analyzer.trades)
        if after_count != before_count:
            self.ws_trade_messages += 1
            await self.broadcast()

    async def run(self):
        while True:
            try:
                timeout = aiohttp.ClientTimeout(total=None)

                async with aiohttp.ClientSession(timeout=timeout) as session:
                    LOG.info("connecting Coincheck WebSocket: %s", WS)

                    async with session.ws_connect(
                        WS,
                        heartbeat=20,
                        autoping=True,
                        receive_timeout=15,
                    ) as ws:
                        self.connected = True
                        self.last_error = None
                        self.last_ws_message_ts = None
                        self.last_ws_orderbook_ts = None
                        self._debug_raw_messages = 0

                        LOG.info("Coincheck WebSocket connected")

                        await self._subscribe(
                            ws, f"{PAIR}-orderbook"
                        )
                        await asyncio.sleep(0.2)
                        await self._subscribe(
                            ws, f"{PAIR}-trades"
                        )

                        watchdog_task = asyncio.create_task(
                            self._watchdog(ws)
                        )

                        try:
                            async for msg in ws:
                                self.ws_messages += 1
                                self.last_ws_message_ts = self._now()

                                if self._debug_raw_messages < self._debug_raw_limit:
                                    self._debug_raw_messages += 1
                                    raw = msg.data
                                    if isinstance(raw, bytes):
                                        raw = raw.decode("utf-8", "replace")
                                    LOG.warning(
                                        "WS RAW #%d type=%s data=%s",
                                        self._debug_raw_messages,
                                        msg.type,
                                        raw,
                                    )

                                if msg.type == aiohttp.WSMsgType.TEXT:
                                    try:
                                        data = json.loads(msg.data)
                                    except json.JSONDecodeError:
                                        LOG.warning(
                                            "WS JSON decode failed: %r",
                                            msg.data
                                        )
                                        continue

                                    if (
                                        isinstance(data, list)
                                        and len(data) == 2
                                        and data[0] == f"{PAIR}-orderbook"
                                    ):
                                        await self._handle_orderbook(data[1])
                                        continue

                                    if (
                                        isinstance(data, list)
                                        and len(data) == 2
                                        and data[0] == f"{PAIR}-trades"
                                    ):
                                        payload = data[1]
                                        if isinstance(payload, list):
                                            for trade in payload:
                                                if isinstance(trade, list):
                                                    await self._handle_trade(trade)
                                        else:
                                            LOG.warning(
                                                "trade channel payload unexpected: %r",
                                                payload
                                            )
                                        continue

                                    LOG.warning(
                                        "WS unrecognized message: %r", data
                                    )

                                elif msg.type == aiohttp.WSMsgType.ERROR:
                                    LOG.error(
                                        "Coincheck WebSocket error: %s",
                                        ws.exception()
                                    )
                                    break

                                elif msg.type in (
                                    aiohttp.WSMsgType.CLOSED,
                                    aiohttp.WSMsgType.CLOSE,
                                    aiohttp.WSMsgType.CLOSING,
                                ):
                                    LOG.warning(
                                        "Coincheck WebSocket closed type=%s",
                                        msg.type
                                    )
                                    break

                        finally:
                            watchdog_task.cancel()
                            try:
                                await watchdog_task
                            except asyncio.CancelledError:
                                pass

            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.last_error = str(exc)
                LOG.exception(
                    "Coincheck WebSocket loop failed: %s", exc
                )
            finally:
                self.connected = False

            LOG.warning(
                "Coincheck WebSocket disconnected; reconnecting in 3 seconds"
            )
            await asyncio.sleep(3)


async def start_coincheck_stream(analyzer: MarketAnalyzer, broadcast=None):
    stream = CoincheckStream(analyzer)
    if broadcast is not None:
        stream.set_broadcast(broadcast)
    await stream.rest_snapshot()
    task = asyncio.create_task(stream.run())
    return stream, task
