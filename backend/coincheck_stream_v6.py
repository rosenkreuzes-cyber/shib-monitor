import asyncio
import json
import logging
import time

import aiohttp

LOG = logging.getLogger("coincheck")
REST = "https://coincheck.com"
WS = "wss://ws-api.coincheck.com"

STALE_SECONDS = 30
WATCHDOG_INTERVAL = 2
RECONNECT_INITIAL = 2
RECONNECT_MAX = 30


class CoincheckStream:
    """Coincheck public WebSocket stream with REST snapshot + WS diffs."""

    def __init__(self, pair, analyzer, broadcast):
        self.pair = pair
        self.analyzer = analyzer
        self.broadcast = broadcast
        self.connected = False
        self.last_error = None

        # WebSocket diagnostics
        self.ws_messages = 0
        self.ws_orderbook_messages = 0
        self.ws_trade_messages = 0
        self.last_ws_message_ts = None
        self.last_ws_orderbook_ts = None

    async def rest_snapshot(self, session):
        async with session.get(
            f"{REST}/api/order_books",
            params={"pair": self.pair},
            timeout=aiohttp.ClientTimeout(total=10),
        ) as r:
            r.raise_for_status()
            book = await r.json(content_type=None)

        if not book.get("asks") or not book.get("bids"):
            raise RuntimeError(f"empty order book: {book}")

        self.analyzer.load_depth(book)

        # Ticker is supplementary; a ticker failure must not discard a valid
        # order-book snapshot.
        try:
            async with session.get(
                f"{REST}/api/ticker",
                params={"pair": self.pair},
                timeout=aiohttp.ClientTimeout(total=10),
            ) as r:
                r.raise_for_status()
                ticker = await r.json(content_type=None)
            self.analyzer.ticker(ticker)
        except Exception as e:
            LOG.warning("Coincheck ticker REST failed: %s", e)

        self.analyzer.set_source("coincheck_rest+ws")
        await self.broadcast()

    def _mark_ws_orderbook_received(self, payload):
        """Explicitly refresh WS receipt freshness after a valid orderbook message."""
        ts = payload.get("last_update_at")
        book = getattr(self.analyzer, "book", None)
        if book is None:
            return

        # Prefer a public method if one exists in a future version.
        marker = getattr(book, "mark_received", None)
        if callable(marker):
            marker("ws_diff", ts)
            return

        # Current v5.1 OrderBookEngine exposes _mark_received().
        marker = getattr(book, "_mark_received", None)
        if callable(marker):
            marker("ws_diff", ts)

    async def _watchdog(self, ws):
        while True:
            await asyncio.sleep(WATCHDOG_INTERVAL)

            book = getattr(self.analyzer, "book", None)
            age = book.data_age_sec() if book is not None else None

            if age is not None and age > STALE_SECONDS:
                msg = f"orderbook stale {age:.1f}s; forcing reconnect"
                LOG.warning(msg)
                self.last_error = msg

                if not ws.closed:
                    await ws.close(code=1012, message=b"orderbook stale")
                return

    async def run(self):
        delay = RECONNECT_INITIAL

        while True:
            try:
                timeout = aiohttp.ClientTimeout(
                    total=None,
                    connect=15,
                    sock_connect=15,
                    sock_read=None,
                )

                async with aiohttp.ClientSession(timeout=timeout) as session:
                    # Rebuild the local book from REST before every WS session.
                    await self.rest_snapshot(session)

                    LOG.info(
                        "Coincheck WS connecting: endpoint=%s pair=%s",
                        WS,
                        self.pair,
                    )

                    async with session.ws_connect(
                        WS,
                        heartbeat=20,
                        autoping=True,
                        timeout=15,
                    ) as ws:
                        self.connected = True
                        self.last_error = None
                        self.analyzer.set_ws(True, None)
                        delay = RECONNECT_INITIAL

                        await ws.send_json({
                            "type": "subscribe",
                            "channel": f"{self.pair}-orderbook",
                        })
                        await ws.send_json({
                            "type": "subscribe",
                            "channel": f"{self.pair}-trades",
                        })

                        LOG.info(
                            "Coincheck WS subscribed: %s-orderbook, %s-trades",
                            self.pair,
                            self.pair,
                        )
                        await self.broadcast()

                        watchdog = asyncio.create_task(self._watchdog(ws))

                        try:
                            async for msg in ws:
                                self.last_ws_message_ts = time.time()
                                self.ws_messages += 1

                                if msg.type == aiohttp.WSMsgType.TEXT:
                                    await self.handle(msg.data)

                                elif msg.type == aiohttp.WSMsgType.BINARY:
                                    LOG.warning(
                                        "Coincheck WS binary message received: %d bytes",
                                        len(msg.data or b""),
                                    )

                                elif msg.type == aiohttp.WSMsgType.PING:
                                    await ws.pong()

                                elif msg.type == aiohttp.WSMsgType.PONG:
                                    continue

                                elif msg.type in (
                                    aiohttp.WSMsgType.CLOSED,
                                    aiohttp.WSMsgType.ERROR,
                                ):
                                    LOG.warning(
                                        "Coincheck WS closed/error: type=%s extra=%s",
                                        msg.type,
                                        msg.extra,
                                    )
                                    break
                        finally:
                            watchdog.cancel()
                            try:
                                await watchdog
                            except asyncio.CancelledError:
                                pass

            except asyncio.CancelledError:
                raise

            except Exception as e:
                self.connected = False
                self.last_error = f"{type(e).__name__}: {e}"
                self.analyzer.set_ws(False, self.last_error)

                LOG.exception(
                    "Coincheck stream disconnected; reconnect in %ss: %s",
                    delay,
                    e,
                )

                await self.broadcast()
                await asyncio.sleep(delay)
                delay = min(delay * 2, RECONNECT_MAX)

            finally:
                self.connected = False

    async def handle(self, text):
        try:
            data = json.loads(text)
        except (TypeError, ValueError) as e:
            LOG.warning("Coincheck WS invalid JSON: %s", e)
            return

        # Official orderbook format:
        # ["shib_jpy", {"bids": [...], "asks": [...], "last_update_at": "..."}]
        if (
            isinstance(data, list)
            and len(data) == 2
            and data[0] == self.pair
            and isinstance(data[1], dict)
        ):
            payload = data[1]

            if "bids" in payload or "asks" in payload:
                self.ws_orderbook_messages += 1
                self.last_ws_orderbook_ts = time.time()

                try:
                    self.analyzer.diff_depth(payload)

                    # Receipt time is independent of Coincheck's exchange timestamp.
                    self._mark_ws_orderbook_received(payload)

                    if self.ws_orderbook_messages <= 3:
                        LOG.info(
                            "Coincheck orderbook received #%d: bids=%d asks=%d last_update_at=%s",
                            self.ws_orderbook_messages,
                            len(payload.get("bids") or []),
                            len(payload.get("asks") or []),
                            payload.get("last_update_at"),
                        )

                    await self.broadcast()
                except Exception:
                    LOG.exception("Coincheck orderbook processing failed")

            return

        # Official trades format is a 2-dimensional array.
        if isinstance(data, list):
            changed = False

            for row in data:
                if (
                    isinstance(row, list)
                    and len(row) >= 6
                    and row[2] == self.pair
                ):
                    self.analyzer.trade({
                        "executed_at": row[0],
                        "id": row[1],
                        "pair": row[2],
                        "price": row[3],
                        "amount": row[4],
                        "side": row[5],
                    })
                    self.ws_trade_messages += 1
                    changed = True

            if changed:
                await self.broadcast()
            return

        LOG.debug("Coincheck WS unhandled message: %s", data)
