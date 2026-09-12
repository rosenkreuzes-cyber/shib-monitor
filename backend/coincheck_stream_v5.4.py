import asyncio
import json
import logging

import aiohttp


LOG = logging.getLogger("coincheck")

REST = "https://coincheck.com"
WS = "wss://ws-api.coincheck.com/"

VERSION = "5.4"


class CoincheckStream:
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
        """Load the initial order book and ticker from REST."""

        async with session.get(
            f"{REST}/api/order_books",
            params={"pair": self.pair},
            timeout=10,
        ) as response:
            response.raise_for_status()
            book = await response.json(content_type=None)

        if not book.get("asks") or not book.get("bids"):
            raise RuntimeError(
                f"empty order book: {book}"
            )

        self.analyzer.load_depth(book)

        async with session.get(
            f"{REST}/api/ticker",
            params={"pair": self.pair},
            timeout=10,
        ) as response:
            response.raise_for_status()
            ticker = await response.json(content_type=None)

        self.analyzer.ticker(ticker)
        self.analyzer.set_source("coincheck_rest")

        LOG.info(
            "REST snapshot loaded pair=%s bids=%d asks=%d",
            self.pair,
            len(book.get("bids", [])),
            len(book.get("asks", [])),
        )

        await self.broadcast()

    async def run(self):
        delay = 2

        while True:
            try:
                async with aiohttp.ClientSession() as session:

                    # 1. Initial REST snapshot
                    await self.rest_snapshot(session)

                    # 2. WebSocket connection
                    LOG.info(
                        "connecting Coincheck public websocket: %s",
                        WS,
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

                        self.ws_messages = 0
                        self.ws_orderbook_messages = 0
                        self.ws_trade_messages = 0
                        self.last_ws_message_ts = None
                        self.last_ws_orderbook_ts = None

                        delay = 2

                        LOG.info(
                            "Coincheck websocket connected pair=%s",
                            self.pair,
                        )

                        # 3. Subscribe separately.
                        # Coincheck Public WebSocket accepts one channel
                        # per subscribe command.
                        orderbook_channel = (
                            f"{self.pair}-orderbook"
                        )
                        trades_channel = (
                            f"{self.pair}-trades"
                        )

                        await ws.send_json(
                            {
                                "type": "subscribe",
                                "channel": orderbook_channel,
                            }
                        )

                        LOG.info(
                            "subscribe sent: %s",
                            orderbook_channel,
                        )

                        await ws.send_json(
                            {
                                "type": "subscribe",
                                "channel": trades_channel,
                            }
                        )

                        LOG.info(
                            "subscribe sent: %s",
                            trades_channel,
                        )

                        await self.broadcast()

                        # 4. Watchdog.
                        async def watchdog():
                            while True:
                                await asyncio.sleep(5)

                                age = (
                                    self.analyzer.book.data_age_sec()
                                )

                                if age is None:
                                    continue

                                if age > 30:
                                    LOG.warning(
                                        "orderbook stale %.1fs; "
                                        "forcing websocket reconnect",
                                        age,
                                    )

                                    await ws.close(
                                        code=1012,
                                        message=b"orderbook stale",
                                    )
                                    return

                        watchdog_task = asyncio.create_task(
                            watchdog()
                        )

                        try:
                            async for message in ws:

                                # Any incoming WebSocket frame.
                                self.ws_messages += 1
                                self.last_ws_message_ts = (
                                    self._now()
                                )

                                if (
                                    message.type
                                    == aiohttp.WSMsgType.TEXT
                                ):
                                    LOG.debug(
                                        "websocket TEXT #%d: %s",
                                        self.ws_messages,
                                        message.data[:500],
                                    )

                                    await self.handle(
                                        message.data
                                    )

                                elif (
                                    message.type
                                    == aiohttp.WSMsgType.BINARY
                                ):
                                    LOG.warning(
                                        "unexpected websocket "
                                        "BINARY frame"
                                    )

                                elif (
                                    message.type
                                    == aiohttp.WSMsgType.ERROR
                                ):
                                    LOG.error(
                                        "websocket error: %s",
                                        ws.exception(),
                                    )
                                    break

                                elif (
                                    message.type
                                    == aiohttp.WSMsgType.CLOSED
                                ):
                                    LOG.warning(
                                        "websocket closed"
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
                self.connected = False

                self.last_error = (
                    f"{type(exc).__name__}: {exc}"
                )

                self.analyzer.set_ws(
                    False,
                    self.last_error,
                )

                LOG.exception(
                    "Coincheck stream disconnected: %s",
                    exc,
                )

                await self.broadcast()

                await asyncio.sleep(delay)

                delay = min(delay * 2, 30)

            finally:
                self.connected = False

    async def handle(self, text):
        """Handle Coincheck Public WebSocket messages."""

        try:
            data = json.loads(text)

        except Exception:
            LOG.warning(
                "invalid websocket JSON: %r",
                text[:500],
            )
            return

        # --------------------------------------------------
        # Coincheck order book:
        #
        # [
        #   "shib_jpy",
        #   {
        #       "bids": [...],
        #       "asks": [...],
        #       "last_update_at": "..."
        #   }
        # ]
        # --------------------------------------------------
        if (
            isinstance(data, list)
            and len(data) == 2
            and data[0] == self.pair
            and isinstance(data[1], dict)
        ):
            payload = data[1]

            if (
                "bids" in payload
                or "asks" in payload
            ):
                self.ws_orderbook_messages += 1
                self.last_ws_orderbook_ts = self._now()

                LOG.info(
                    "orderbook websocket received "
                    "#%d pair=%s bids=%d asks=%d "
                    "last_update_at=%s",
                    self.ws_orderbook_messages,
                    self.pair,
                    len(payload.get("bids", [])),
                    len(payload.get("asks", [])),
                    payload.get("last_update_at"),
                )

                self.analyzer.diff_depth(payload)
                self.analyzer.set_source(
                    "coincheck_ws_orderbook"
                )

                await self.broadcast()

            return

        # --------------------------------------------------
        # Coincheck trades:
        #
        # [
        #   [
        #       timestamp,
        #       id,
        #       pair,
        #       price,
        #       amount,
        #       side,
        #       ...
        #   ]
        # ]
        # --------------------------------------------------
        if isinstance(data, list):
            changed = False

            for row in data:
                if not isinstance(row, list):
                    continue

                if len(row) < 6:
                    continue

                if row[2] != self.pair:
                    continue

                self.ws_trade_messages += 1

                self.analyzer.trade(
                    {
                        "executed_at": row[0],
                        "id": row[1],
                        "pair": row[2],
                        "price": row[3],
                        "amount": row[4],
                        "side": row[5],
                    }
                )

                changed = True

            if changed:
                LOG.info(
                    "trade websocket received "
                    "#%d pair=%s",
                    self.ws_trade_messages,
                    self.pair,
                )

                self.analyzer.set_source(
                    "coincheck_ws_trades"
                )

                await self.broadcast()

    @staticmethod
    def _now():
        from time import time

        return time()
