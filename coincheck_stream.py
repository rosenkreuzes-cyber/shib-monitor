import asyncio
import json
import logging
from time import time

import aiohttp

LOG = logging.getLogger(__name__)

REST = "https://coincheck.com"
WS = "wss://ws-api.coincheck.com"
VERSION = "5.4-renderfix6"


class CoincheckStream:
    """
    Coincheck public market-data stream for the v5.4 FastAPI app.

    RenderFix2:
      - Uses explicit ws.receive() instead of async-for iteration.
      - Counts every received application frame immediately.
      - Handles TEXT / BINARY / PING / PONG / CLOSE / ERROR explicitly.
      - Accepts both the documented orderbook channel name
        ("shib_jpy-orderbook") and the actual pair-name form observed in logs
        ("shib_jpy").
      - Reconnects when the transport stays open but no market-data frame
        arrives.
      - Keeps REST snapshot fallback.
    """

    def __init__(self, pair, analyzer, broadcast=None):
        self.pair = pair
        self.analyzer = analyzer
        self._broadcast = broadcast

        self.connected = False
        self.transport_connected = False
        self.subscribed = False
        self.last_error = None
        self.last_ws_event = None
        self.last_ws_raw_preview = None
        self.subscribe_sent_ts = None

        self.ws_messages = 0
        self.ws_orderbook_messages = 0
        self.ws_trade_messages = 0

        self.last_ws_message_ts = None
        self.last_ws_orderbook_ts = None

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

                if not isinstance(data, dict):
                    raise RuntimeError(
                        f"unexpected order_books response: {type(data).__name__}"
                    )

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
        message = json.dumps(
            {
                "type": "subscribe",
                "channel": channel,
            },
            separators=(",", ":"),
        )
        await ws.send_str(message)
        if channel.endswith("-orderbook"):
            self.subscribe_sent_ts = self._now()
        LOG.info("Coincheck subscribe sent: %s", channel)

    def _log_raw(self, msg):
        """Log the first few frames exactly as received."""
        if self._debug_raw_messages >= self._debug_raw_limit:
            return

        self._debug_raw_messages += 1
        raw = msg.data

        if isinstance(raw, bytes):
            raw = raw.decode("utf-8", "replace")

        preview = str(raw)
        if len(preview) > 500:
            preview = preview[:500] + "...[truncated]"
        self.last_ws_raw_preview = preview
        LOG.warning(
            "WS RAW #%d type=%s data=%s",
            self._debug_raw_messages,
            msg.type,
            preview,
        )

    async def _handle_text(self, raw_text):
        try:
            data = json.loads(raw_text)
        except (json.JSONDecodeError, TypeError) as exc:
            LOG.warning(
                "WS JSON decode failed: %s data=%r",
                exc,
                raw_text,
            )
            return

        # Coincheck orderbook response observed in production:
        # ["shib_jpy", {"bids":[...], "asks":[...], "last_update_at":"..."}]
        #
        # Official documentation also describes:
        # ["shib_jpy-orderbook", {...}]
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
            self.connected = True
            self.subscribed = True
            self.last_ws_event = "orderbook"
            await self._handle_orderbook(data[1])
            return

        # Official Coincheck trades response:
        # ["shib_jpy-trades", [[...], ...]]
        if (
            isinstance(data, list)
            and len(data) == 2
            and data[0] == f"{self.pair}-trades"
        ):
            self.connected = True
            self.subscribed = True
            self.last_ws_event = "trade"
            await self._handle_trade(data[1])
            return

        # ACK/error frames are diagnostic only; they do not prove market-data
        # reception.
        if isinstance(data, dict) and data.get("type") in ("subscribe", "subscribed", "ack", "error"):
            self.last_ws_event = str(data.get("type"))
        LOG.warning("WS unrecognized message: %r", data)

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

        # Count and timestamp immediately after recognizing a valid orderbook
        # frame, before analyzer processing. This prevents a parser-side error
        # from making the health endpoint falsely report zero WS messages.
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
            for trade in payload:
                if isinstance(trade, list):
                    self.analyzer.trade(trade)
        except Exception:
            LOG.exception("trade payload handling failed")
            return

        after_count = len(self.analyzer.trades)

        if after_count != before_count:
            self.ws_trade_messages += 1
            # The orderbook source controls orderbook freshness/decision validity.
            await self.broadcast()

    async def _receive_loop(self, ws):
        """
        Explicit receive loop.

        aiohttp's async-for is normally fine, but explicit receive() gives us
        deterministic handling and logging for Render/proxy close conditions.
        """
        consecutive_timeouts = 0
        last_receive_monotonic = time()

        while not ws.closed:
            try:
                # Do not let a quiet socket sit forever. A normal market stream
                # should produce frames, but heartbeat/proxy behavior can vary.
                msg = await ws.receive(timeout=35)

            except asyncio.TimeoutError:
                consecutive_timeouts += 1

                age = time() - last_receive_monotonic

                LOG.warning(
                    "Coincheck WebSocket receive timeout #%d "
                    "age=%.1fs orderbook_messages=%d ws_messages=%d",
                    consecutive_timeouts,
                    age,
                    self.ws_orderbook_messages,
                    self.ws_messages,
                )

                # One timeout is tolerated. On the second, force a clean
                # reconnect so Render cannot remain in a half-open state.
                if consecutive_timeouts >= 2:
                    LOG.warning(
                        "Coincheck WebSocket no frame for %.1fs; "
                        "forcing reconnect",
                        age,
                    )
                    try:
                        await ws.close(
                            code=1012,
                            message=b"receive timeout",
                        )
                    except Exception:
                        LOG.exception("failed to close timed-out websocket")
                    return

                try:
                    pong_waiter = await ws.ping()
                    await asyncio.wait_for(pong_waiter, timeout=10)
                    LOG.info("Coincheck WebSocket ping/pong OK")
                except Exception as exc:
                    LOG.warning(
                        "Coincheck WebSocket ping/pong failed: %s",
                        exc,
                    )
                    try:
                        await ws.close(
                            code=1012,
                            message=b"ping failed",
                        )
                    except Exception:
                        pass
                    return

                continue

            except asyncio.CancelledError:
                raise

            except Exception as exc:
                self.last_error = str(exc)
                LOG.exception(
                    "Coincheck WebSocket receive failed: %s",
                    exc,
                )
                return

            last_receive_monotonic = time()
            consecutive_timeouts = 0

            # Application-level message counter. PING/PONG are deliberately
            # not counted as market-data frames.
            if msg.type in (
                aiohttp.WSMsgType.TEXT,
                aiohttp.WSMsgType.BINARY,
            ):
                self.ws_messages += 1
                self.last_ws_message_ts = self._now()
                self.last_ws_event = "application_frame"

            self._log_raw(msg)

            if msg.type == aiohttp.WSMsgType.TEXT:
                await self._handle_text(msg.data)
                continue

            if msg.type == aiohttp.WSMsgType.BINARY:
                raw = msg.data
                try:
                    text = raw.decode("utf-8", "replace")
                except Exception:
                    LOG.warning(
                        "WS binary frame could not be decoded: %r",
                        raw,
                    )
                    continue

                await self._handle_text(text)
                continue

            if msg.type == aiohttp.WSMsgType.PING:
                LOG.info("Coincheck WebSocket PING received")
                try:
                    await ws.pong(msg.data)
                except Exception:
                    LOG.exception("Coincheck WebSocket PONG failed")
                    return
                continue

            if msg.type == aiohttp.WSMsgType.PONG:
                LOG.info("Coincheck WebSocket PONG received")
                continue

            if msg.type == aiohttp.WSMsgType.CLOSED:
                LOG.warning(
                    "Coincheck WebSocket CLOSED "
                    "closed=%s close_code=%s exception=%s",
                    ws.closed,
                    ws.close_code,
                    ws.exception(),
                )
                return

            if msg.type == aiohttp.WSMsgType.CLOSING:
                LOG.warning(
                    "Coincheck WebSocket CLOSING "
                    "close_code=%s exception=%s",
                    ws.close_code,
                    ws.exception(),
                )
                return

            if msg.type == aiohttp.WSMsgType.ERROR:
                exc = ws.exception()
                self.last_error = str(exc) if exc else None
                LOG.error(
                    "Coincheck WebSocket ERROR exception=%s "
                    "close_code=%s",
                    exc,
                    ws.close_code,
                )
                return

            LOG.warning(
                "Coincheck WebSocket unhandled message type=%s data=%r",
                msg.type,
                msg.data,
            )

    async def run(self):
        """Reconnect forever until the application task is cancelled."""
        while True:
            try:
                timeout = aiohttp.ClientTimeout(
                    total=None,
                    sock_connect=20,
                    sock_read=None,
                )

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
                        max_msg_size=4 * 1024 * 1024,
                    ) as ws:
                        self.transport_connected = True
                        self.connected = False
                        self.subscribed = False
                        self.last_error = None
                        self.last_ws_message_ts = None
                        self.last_ws_orderbook_ts = None
                        self.last_ws_event = "transport_connected"
                        self.last_ws_raw_preview = None
                        self.subscribe_sent_ts = None
                        self._debug_raw_messages = 0

                        LOG.info(
                            "Coincheck WebSocket transport connected "
                            "closed=%s close_code=%s",
                            ws.closed,
                            ws.close_code,
                        )

                        # REST snapshot initializes the local book before WS
                        # orderbook differences are applied.
                        await self.rest_snapshot()

                        # One subscribe command per public channel.
                        await self._subscribe(
                            ws,
                            f"{self.pair}-orderbook",
                        )

                        # Give the orderbook subscription a short head start.
                        await asyncio.sleep(0.2)

                        await self._subscribe(
                            ws,
                            f"{self.pair}-trades",
                        )

                        LOG.info(
                            "Coincheck WebSocket subscriptions sent "
                            "pair=%s",
                            self.pair,
                        )

                        await self._receive_loop(ws)

            except asyncio.CancelledError:
                raise

            except Exception as exc:
                self.last_error = str(exc)
                LOG.exception(
                    "Coincheck WebSocket loop failed: %s",
                    exc,
                )

            finally:
                self.connected = False
                self.subscribed = False
                self.transport_connected = False

            LOG.warning(
                "Coincheck WebSocket session ended; "
                "closed/reconnect in 3 seconds",
            )
            await asyncio.sleep(3)
