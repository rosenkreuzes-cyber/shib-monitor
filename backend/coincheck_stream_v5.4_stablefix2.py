import asyncio
import json
import logging
from time import time

import aiohttp

LOG = logging.getLogger(__name__)

REST = "https://coincheck.com"
WS = "wss://ws-api.coincheck.com"
VERSION = "5.4"


class CoincheckStream:
    """
    Coincheck public market-data stream for the v5.4 FastAPI app.

    Compatible with main.py:
        CoincheckStream(PAIR, analyzer, broadcast)
        await stream.run()
    """

    def __init__(self, pair, analyzer, broadcast=None):
        self.pair = pair
        self.analyzer = analyzer
        self._broadcast = broadcast

        self.connected = False
        self.last_error = None

        self.ws_messages = 0
        self.ws_orderbook_messages = 0
        self.ws_trade_messages = 0

        self.last_ws_message_ts = None
        self.last_ws_orderbook_ts = None

        # First 10 raw frames are logged for diagnosis.
        self._debug_raw_messages = 0
        self._debug_raw_limit = 10

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
        """Load an initial order-book snapshot from Coincheck REST."""
        timeout = aiohttp.ClientTimeout(total=15)

        try:
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(
                    f"{REST}/api/order_books",
                    params={"pair": self.pair},
                ) as resp:
                    resp.raise_for_status()
                    data = await resp.json()

                bids = data.get("bids", [])
                asks = data.get("asks", [])

                self.analyzer.book.load_snapshot(bids, asks)

                try:
                    async with session.get(
                        f"{REST}/api/ticker",
                        params={"pair": self.pair},
                    ) as resp:
                        resp.raise_for_status()
                        ticker = await resp.json()

                    if isinstance(ticker, dict):
                        last = ticker.get("last")
                        if last is not None:
                            # Keep compatibility with existing analyzer versions.
                            try:
                                self.analyzer.last_price = float(last)
                            except Exception:
                                pass
                except Exception:
                    LOG.exception("ticker REST request failed")

                self.analyzer.set_source("coincheck_rest")
                await self.broadcast()

                LOG.info(
                    "REST snapshot loaded pair=%s bids=%d asks=%d",
                    self.pair,
                    len(bids),
                    len(asks),
                )

        except Exception as exc:
            self.last_error = str(exc)
            LOG.exception("REST snapshot failed")

    async def _subscribe(self, ws, channel):
        message = json.dumps({
            "type": "subscribe",
            "channel": channel,
        })
        await ws.send_str(message)
        LOG.info("Coincheck subscribe sent: %s", channel)

    async def _watchdog(self, ws):
        # Diagnostic mode: do not force-close a healthy WebSocket merely
        # because orderbook updates are temporarily sparse. aiohttp heartbeat
        # handles the connection liveness; the main receive loop handles
        # actual close/error events.
        while not ws.closed:
            await asyncio.sleep(15)
            LOG.debug(
                "watchdog pair=%s ws_age=%s orderbook_age=%s "
                "ws_messages=%d orderbook_messages=%d trade_messages=%d",
                self.pair,
                (
                    self._now() - self.last_ws_message_ts
                    if self.last_ws_message_ts is not None else None
                ),
                (
                    self._now() - self.last_ws_orderbook_ts
                    if self.last_ws_orderbook_ts is not None else None
                ),
                self.ws_messages,
                self.ws_orderbook_messages,
                self.ws_trade_messages,
            )


    async def _handle_orderbook(self, payload):
        if not isinstance(payload, dict):
            LOG.warning(
                "orderbook payload is not dict: type=%s payload=%r",
                type(payload).__name__,
                payload,
            )
            return

        bids = payload.get("bids", [])
        asks = payload.get("asks", [])

        self.ws_orderbook_messages += 1
        self.last_ws_orderbook_ts = self._now()

        try:
            changed = self.analyzer.diff_depth(payload)
        except Exception:
            LOG.exception("orderbook diff handling failed")
            return

        self.analyzer.set_source("coincheck_ws_orderbook")

        LOG.info(
            "orderbook websocket received #%d pair=%s "
            "bids=%d asks=%d changed=%s last_update_at=%s",
            self.ws_orderbook_messages,
            self.pair,
            len(bids),
            len(asks),
            changed,
            payload.get("last_update_at"),
        )

        await self.broadcast()

    async def _handle_trade(self, payload):
        if not isinstance(payload, list):
            LOG.warning(
                "trade payload is not list: type=%s payload=%r",
                type(payload).__name__,
                payload,
            )
            return

        before_count = len(self.analyzer.trades)

        try:
            # Coincheck trades channel may contain multiple trade rows.
            for trade in payload:
                if isinstance(trade, list):
                    self.analyzer.trade(trade)
        except Exception:
            LOG.exception("trade payload handling failed")
            return

        after_count = len(self.analyzer.trades)

        if after_count != before_count:
            self.ws_trade_messages += 1
            # Do NOT change analyzer source to trades. The orderbook source
            # controls orderbook freshness/decision validity.
            await self.broadcast()

    async def run(self):
        """
        Reconnecting Coincheck WebSocket loop.
        Compatible with main.py's await stream.run().
        """
        while True:
            try:
                timeout = aiohttp.ClientTimeout(total=None)

                async with aiohttp.ClientSession(timeout=timeout) as session:
                    LOG.info(
                        "connecting Coincheck WebSocket: %s",
                        WS,
                    )

                    async with session.ws_connect(
                        WS,
                        heartbeat=20,
                        autoping=True,
                        receive_timeout=None,
                    ) as ws:
                        self.connected = True
                        self.last_error = None
                        self.last_ws_message_ts = None
                        self.last_ws_orderbook_ts = None
                        self._debug_raw_messages = 0

                        LOG.info("Coincheck WebSocket connected")

                        # Load REST snapshot before consuming WebSocket diffs.
                        # This initializes the local book even when the first WS
                        # frame is a partial/difference update.
                        await self.rest_snapshot()

                        # Coincheck public API uses one subscribe command
                        # per channel.
                        await self._subscribe(
                            ws,
                            f"{self.pair}-orderbook",
                        )

                        # Give orderbook subscription a small head start.
                        await asyncio.sleep(0.2)

                        await self._subscribe(
                            ws,
                            f"{self.pair}-trades",
                        )

                        LOG.info(
                            "Coincheck subscriptions complete: %s-orderbook, %s-trades",
                            self.pair,
                            self.pair,
                        )

                        watchdog_task = asyncio.create_task(
                            self._watchdog(ws)
                        )

                        try:
                            async for msg in ws:
                                self.ws_messages += 1
                                self.last_ws_message_ts = self._now()

                                # Diagnostic: expose the first frames exactly as
                                # received. This is the key test for the current
                                # "connected but 0 messages" problem.
                                if (
                                    self._debug_raw_messages
                                    < self._debug_raw_limit
                                ):
                                    self._debug_raw_messages += 1
                                    raw = msg.data

                                    if isinstance(raw, bytes):
                                        raw = raw.decode(
                                            "utf-8",
                                            "replace",
                                        )

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
                                            msg.data,
                                        )
                                        continue

                                    # Coincheck may deliver the orderbook using
                                    # the pair name itself (for example
                                    # ["shib_jpy", {"bids": [...], "asks": [...]}])
                                    # rather than the documented -orderbook
                                    # suffix. Accept both forms.
                                    if (
                                        isinstance(data, list)
                                        and len(data) == 2
                                        and data[0] in (
                                            self.pair,
                                            f"{self.pair}-orderbook",
                                        )
                                        and isinstance(data[1], dict)
                                        and (
                                            "bids" in data[1]
                                            or "asks" in data[1]
                                        )
                                    ):
                                        LOG.info(
                                            "recognized orderbook channel=%s",
                                            data[0],
                                        )
                                        await self._handle_orderbook(data[1])
                                        continue

                                    # Official Coincheck trades shape:
                                    # ["shib_jpy-trades", [[...], ...]]
                                    if (
                                        isinstance(data, list)
                                        and len(data) == 2
                                        and data[0]
                                        == f"{self.pair}-trades"
                                    ):
                                        await self._handle_trade(data[1])
                                        continue

                                    # Subscription acknowledgements/errors and
                                    # unexpected frames must remain visible.
                                    LOG.warning(
                                        "WS unrecognized message: %r",
                                        data,
                                    )

                                elif msg.type == aiohttp.WSMsgType.ERROR:
                                    self.last_error = repr(ws.exception())
                                    LOG.error(
                                        "Coincheck WebSocket ERROR "
                                        "type=%s exception=%r close_code=%r",
                                        type(ws.exception()).__name__
                                        if ws.exception() else None,
                                        ws.exception(),
                                        ws.close_code,
                                    )
                                    break

                                elif msg.type in (
                                    aiohttp.WSMsgType.CLOSED,
                                    aiohttp.WSMsgType.CLOSE,
                                    aiohttp.WSMsgType.CLOSING,
                                ):
                                    LOG.warning(
                                        "Coincheck WebSocket CLOSED "
                                        "type=%s close_code=%r exception=%r",
                                        msg.type,
                                        ws.close_code,
                                        ws.exception(),
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
                self.last_error = f"{type(exc).__name__}: {exc!r}"
                LOG.exception(
                    "Coincheck WebSocket loop failed "
                    "type=%s repr=%r",
                    type(exc).__name__,
                    exc,
                )

            finally:
                self.connected = False
                # Keep a useful reason even when the server closes the socket
                # cleanly (which does not raise an exception in aiohttp).
                try:
                    if "ws" in locals():
                        LOG.warning(
                            "Coincheck WebSocket session ended "
                            "closed=%s close_code=%s exception=%r "
                            "last_error=%r",
                            ws.closed,
                            ws.close_code,
                            ws.exception(),
                            self.last_error,
                        )
                        if self.last_error is None and ws.closed:
                            self.last_error = (
                                f"WebSocketClosed: code={ws.close_code!r} "
                                f"exception={ws.exception()!r}"
                            )
                    else:
                        LOG.warning(
                            "Coincheck WebSocket session ended before ws object "
                            "was created; last_error=%r",
                            self.last_error,
                        )
                except Exception:
                    LOG.exception("failed to inspect WebSocket close state")

            LOG.warning(
                "Coincheck WebSocket disconnected; reconnecting in 3 seconds"
            )
            await asyncio.sleep(3)
