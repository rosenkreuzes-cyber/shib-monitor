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

        # デバッグ用
        self.orderbook_messages = 0
        self.trade_messages = 0

    async def rest_snapshot(self, session):
        """
        RESTから初期板情報とTickerを取得する。
        WebSocket接続直後の初期状態を作るために使用。
        """

        # -------------------------
        # Order Book
        # -------------------------
        async with session.get(
            f"{REST}/api/order_books",
            params={"pair": self.pair},
            timeout=10,
        ) as r:
            r.raise_for_status()
            book = await r.json(content_type=None)

        if not book.get("asks") or not book.get("bids"):
            raise RuntimeError(f"empty order book: {book}")

        self.analyzer.load_depth(book)

        # -------------------------
        # Ticker
        # -------------------------
        async with session.get(
            f"{REST}/api/ticker",
            params={"pair": self.pair},
            timeout=10,
        ) as r:
            r.raise_for_status()
            ticker = await r.json(content_type=None)

        self.analyzer.ticker(ticker)

        # REST初期値であることを明示
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

                    # --------------------------------
                    # 1. REST初期スナップショット
                    # --------------------------------
                    await self.rest_snapshot(session)

                    # --------------------------------
                    # 2. WebSocket接続
                    # --------------------------------
                    LOG.info("connecting websocket: %s", WS)

                    async with session.ws_connect(
                        WS,
                        heartbeat=20,
                        autoping=True,
                        timeout=15,
                    ) as ws:

                        self.connected = True
                        self.last_error = None

                        self.analyzer.set_ws(True, None)

                        delay = 2

                        self.orderbook_messages = 0
                        self.trade_messages = 0

                        LOG.info(
                            "websocket connected pair=%s",
                            self.pair,
                        )

                        # --------------------------------
                        # 3. Subscribe
                        # --------------------------------
                        orderbook_channel = f"{self.pair}-orderbook"
                        trades_channel = f"{self.pair}-trades"

                        await ws.send_json(
                            {
                                "type": "subscribe",
                                "channel": orderbook_channel,
                            }
                        )

                        await ws.send_json(
                            {
                                "type": "subscribe",
                                "channel": trades_channel,
                            }
                        )

                        LOG.info(
                            "subscribed: %s",
                            orderbook_channel,
                        )

                        LOG.info(
                            "subscribed: %s",
                            trades_channel,
                        )

                        await self.broadcast()

                        # --------------------------------
                        # 4. Watchdog
                        # --------------------------------
                        async def watchdog():
                            while True:
                                await asyncio.sleep(5)

                                age = self.analyzer.book.data_age_sec()

                                if age is None:
                                    continue

                                if age > 30:
                                    LOG.warning(
                                        "orderbook stale %.1fs; forcing reconnect",
                                        age,
                                    )

                                    await ws.close(
                                        code=1012,
                                        message=b"orderbook stale",
                                    )

                                    return

                        wd = asyncio.create_task(watchdog())

                        try:
                            async for msg in ws:

                                if msg.type == aiohttp.WSMsgType.TEXT:
                                    await self.handle(msg.data)

                                elif msg.type == aiohttp.WSMsgType.ERROR:
                                    LOG.error(
                                        "websocket error: %s",
                                        ws.exception(),
                                    )
                                    break

                                elif msg.type == aiohttp.WSMsgType.CLOSED:
                                    LOG.warning(
                                        "websocket closed"
                                    )
                                    break

                        finally:
                            wd.cancel()

                            try:
                                await wd
                            except asyncio.CancelledError:
                                pass

            except asyncio.CancelledError:
                raise

            except Exception as e:

                self.connected = False

                self.last_error = (
                    f"{type(e).__name__}: {e}"
                )

                self.analyzer.set_ws(
                    False,
                    self.last_error,
                )

                LOG.exception(
                    "Coincheck stream disconnected: %s",
                    e,
                )

                await self.broadcast()

                await asyncio.sleep(delay)

                delay = min(delay * 2, 30)

            finally:
                self.connected = False

    async def handle(self, text):
        """
        Coincheck Public WebSocketの受信処理。
        """

        try:
            data = json.loads(text)

        except Exception:
            LOG.warning(
                "invalid websocket json: %r",
                text[:300],
            )
            return

        # ==========================================
        # Order Book
        #
        # Coincheck:
        # ["shib_jpy", {...}]
        # ==========================================
        if (
            isinstance(data, list)
            and len(data) == 2
            and data[0] == self.pair
            and isinstance(data[1], dict)
        ):

            payload = data[1]

            if "bids" in payload or "asks" in payload:

                self.orderbook_messages += 1

                LOG.debug(
                    "orderbook websocket #%d pair=%s",
                    self.orderbook_messages,
                    self.pair,
                )

                # WebSocketの板差分を反映
                self.analyzer.diff_depth(payload)

                # ★ ここが重要
                # RESTではなくWebSocketから最新板を取得したことを明示
                self.analyzer.set_source(
                    "coincheck_ws_orderbook"
                )

                await self.broadcast()

            return

        # ==========================================
        # Trades
        #
        # [
        #   [
        #     timestamp,
        #     id,
        #     pair,
        #     price,
        #     amount,
        #     side,
        #     ...
        #   ]
        # ]
        # ==========================================
        if isinstance(data, list):

            changed = False

            for row in data:

                if not isinstance(row, list):
                    continue

                if len(row) < 6:
                    continue

                if row[2] != self.pair:
                    continue

                self.trade_messages += 1

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

                self.analyzer.set_source(
                    "coincheck_ws_trades"
                )

                await self.broadcast()
