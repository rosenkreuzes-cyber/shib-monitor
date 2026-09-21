import asyncio
import json
import logging
from time import time

import aiohttp

LOG = logging.getLogger(__name__)

REST = "https://coincheck.com"
WS = "wss://ws-api.coincheck.com"
VERSION = "5.4-renderfix12"


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
        self.transport_connected = False
        self.last_error = None

        # RenderFix7 subscription/receive diagnostics.
        self.ws_subscribed = False
        self.ws_subscribe_sent_ts = None
        self.ws_subscribe_ack_ts = None
        self.ws_subscribe_error_ts = None
        self.ws_last_event = None
        self.ws_raw_preview = None
        self.ws_raw_preview_type = None
        self.ws_last_channel = None
        self.ws_close_code = None
        self.ws_close_reason = None
        self.ws_exception_type = None
        self.ws_exception_message = None
        self.ws_receive_timeout_count = 0
        self.ws_ping_count = 0
        self.ws_pong_count = 0
        self.ws_data_stale_seconds = 30
        self.ws_reconnect_seconds = 60

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
        }, separators=(",", ":"))
        self.ws_subscribe_sent_ts = self._now()
        self.ws_last_event = f"subscribe_sent:{channel}"
        await ws.send_str(message)
        LOG.info("Coincheck subscribe sent channel=%s payload=%s", channel, message)

    def _set_raw_preview(self, msg):
        raw = msg.data
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8", "replace")
        self.ws_raw_preview_type = str(msg.type)
        text = str(raw)
        self.ws_raw_preview = text[:500]

    def _mark_subscription_frame(self, data):
        # Coincheck's public docs do not document a separate subscribe ACK.
        # Therefore receipt of a valid channel frame is the reliable positive signal.
        if isinstance(data, list) and len(data) == 2:
            channel = data[0]
            if channel in (self.pair, f"{self.pair}-orderbook", f"{self.pair}-trades"):
                self.ws_subscribed = True
                self.ws_subscribe_ack_ts = self._now()
                self.ws_last_channel = channel
                self.ws_last_event = f"channel_data:{channel}"
                return
        if isinstance(data, dict):
            typ = data.get("type")
            channel = data.get("channel")
            success = data.get("success")
            if typ in ("subscribed", "subscribe") and success is not False:
                self.ws_subscribed = True
                self.ws_subscribe_ack_ts = self._now()
                self.ws_last_event = "subscribe_ack"
            elif success is False or typ in ("error", "subscribe_error"):
                self.ws_subscribe_error_ts = self._now()
                self.ws_last_event = "subscribe_error"
                err = data.get("error") or data.get("message") or repr(data)
                self.last_error = str(err)
            if channel:
                self.ws_last_channel = str(channel)

    async def _watchdog(self, ws):
        # Transport heartbeat can remain healthy even when the application
        # channel has stopped delivering orderbook frames.  In that case a
        # reconnect is required; otherwise the app can sit on a stale socket
        # forever while REST fallback masks the problem.
        while not ws.closed:
            await asyncio.sleep(15)
            now = self._now()
            orderbook_age = (
                now - self.last_ws_orderbook_ts
                if self.last_ws_orderbook_ts is not None else None
            )
            LOG.debug(
                "watchdog pair=%s ws_age=%s orderbook_age=%s "
                "ws_messages=%d orderbook_messages=%d trade_messages=%d",
                self.pair,
                (now - self.last_ws_message_ts if self.last_ws_message_ts is not None else None),
                orderbook_age,
                self.ws_messages,
                self.ws_orderbook_messages,
                self.ws_trade_messages,
            )
            if orderbook_age is not None and orderbook_age > self.ws_reconnect_seconds:
                self.connected = False
                self.analyzer.set_ws(False, "WS orderbook data stale; reconnecting")
                self.last_error = "WS orderbook data stale; reconnecting"
                self.ws_last_event = "orderbook_stale_reconnect"
                LOG.warning(
                    "Coincheck WS orderbook stale for %.1fs; closing socket for reconnect",
                    orderbook_age,
                )
                await ws.close(code=1000, message=b"orderbook stale")
                return


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

        self.connected = True
        self.transport_connected = True
        self.analyzer.set_ws(True, None)
        self.last_error = None
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

        # Coincheck Public WebSocket trade rows are:
        # [timestamp, trade_id, pair, rate, amount, side, taker_id, maker_id, itayose_id]
        for row in payload:
            if not isinstance(row, list) or len(row) < 6:
                continue

            trade = {
                "executed_at": row[0],
                "id": row[1],
                "pair": row[2],
                "rate": row[3],
                "amount": row[4],
                "order_type": row[5],
                "taker_id": row[6] if len(row) > 6 else None,
                "maker_id": row[7] if len(row) > 7 else None,
                "itayose_id": row[8] if len(row) > 8 else None,
            }

            try:
                self.analyzer.trade(trade)
            except Exception:
                LOG.exception("trade row handling failed row=%r", row)

        after_count = len(self.analyzer.trades)

        if after_count != before_count:
            self.ws_trade_messages += 1
            LOG.info(
                "trade websocket received #%d pair=%s rows=%d accepted=%d",
                self.ws_trade_messages,
                self.pair,
                len(payload),
                after_count - before_count,
            )
            await self.broadcast()

    async def _rest_fallback_loop(self):
        """Refresh REST data only when WebSocket data is unavailable/stale."""
        while True:
            try:
                await asyncio.sleep(5)
                now = self._now()
                ws_age = (
                    now - self.last_ws_orderbook_ts
                    if self.last_ws_orderbook_ts is not None
                    else None
                )
                if (not self.transport_connected) or ws_age is None or ws_age > self.ws_data_stale_seconds:
                    if ws_age is not None and ws_age > self.ws_data_stale_seconds:
                        self.connected = False
                        self.analyzer.set_ws(False, "WS orderbook data stale")
                    elif ws_age is None:
                        self.connected = False
                        self.analyzer.set_ws(False, "WS awaiting orderbook data")
                    LOG.warning(
                        "REST fallback refresh pair=%s connected=%s ws_orderbook_age=%s",
                        self.pair, self.connected, ws_age,
                    )
                    await self.rest_snapshot()
            except asyncio.CancelledError:
                raise
            except Exception:
                LOG.exception("REST fallback loop failed")

    async def run(self):
        """
        Reconnecting Coincheck WebSocket loop.
        Compatible with main.py's await stream.run().
        """
        fallback_task = asyncio.create_task(self._rest_fallback_loop())
        try:
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
                            max_msg_size=4 * 1024 * 1024,
                        ) as ws:
                            self.transport_connected = True
                            # Transport is open, but logical market-data connection is
                            # not considered LIVE until a valid orderbook frame arrives.
                            self.connected = False
                            self.analyzer.set_ws(False, "WS awaiting orderbook data")
                            self.last_error = None
                            # Keep the previous timestamps across reconnects for diagnostics.
                            # Clearing them made a fresh socket look healthy even before data arrived.
                            self._debug_raw_messages = 0
                            self.ws_subscribed = False
                            self.ws_subscribe_sent_ts = None
                            self.ws_subscribe_ack_ts = None
                            self.ws_subscribe_error_ts = None
                            self.ws_last_event = "transport_connected"
                            self.ws_raw_preview = None
                            self.ws_raw_preview_type = None
                            self.ws_last_channel = None
                            self.ws_close_code = None
                            self.ws_close_reason = None
                            self.ws_exception_type = None
                            self.ws_exception_message = None
                            self.ws_receive_timeout_count = 0
                            self.ws_ping_count = 0
                            self.ws_pong_count = 0

                            LOG.info("Coincheck WebSocket transport connected pair=%s", self.pair)

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
                                # Explicit receive timeout is intentional in Fix8:
                                # it distinguishes "socket is open but absolutely no
                                # frame arrives" from a normal async-for wait.
                                while True:
                                    try:
                                        msg = await ws.receive(timeout=45)
                                    except asyncio.TimeoutError:
                                        self.ws_receive_timeout_count += 1
                                        self.ws_last_event = "receive_timeout_45s"
                                        LOG.warning(
                                            "Coincheck WS receive timeout "
                                            "count=%d transport_connected=%s "
                                            "subscribed=%s messages=%d",
                                            self.ws_receive_timeout_count,
                                            self.transport_connected,
                                            self.ws_subscribed,
                                            self.ws_messages,
                                        )
                                        # Do not silently reconnect here yet; leave the
                                        # connection alive so the next health check can
                                        # show whether frames eventually arrive.
                                        continue

                                    self.ws_messages += 1
                                    self.last_ws_message_ts = self._now()
                                    self.connected = True
                                    self.transport_connected = True
                                    self.analyzer.set_ws(True, None)
                                    self._set_raw_preview(msg)

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
                                            self._mark_subscription_frame(data)
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
                                            self.ws_subscribed = True
                                            self.ws_last_channel = str(data[0])
                                            self.ws_last_event = f"orderbook:{data[0]}"
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
                                            self.ws_subscribed = True
                                            self.ws_last_channel = str(data[0])
                                            self.ws_last_event = f"trade:{data[0]}"
                                            await self._handle_trade(data[1])
                                            continue

                                        # Subscription acknowledgements/errors and
                                        # unexpected frames must remain visible.
                                        self.ws_last_event = "unrecognized_text"
                                        LOG.warning(
                                            "WS unrecognized message: %r",
                                            data,
                                        )

                                    elif msg.type == aiohttp.WSMsgType.PING:
                                        self.ws_ping_count += 1
                                        self.ws_last_event = "ping_received"
                                        try:
                                            await ws.pong()
                                        except Exception as exc:
                                            self.last_error = f"{type(exc).__name__}: {exc!r}"
                                            LOG.exception("Coincheck WS pong failed")

                                    elif msg.type == aiohttp.WSMsgType.PONG:
                                        self.ws_pong_count += 1
                                        self.ws_last_event = "pong_received"

                                    elif msg.type == aiohttp.WSMsgType.ERROR:
                                        self.ws_exception_type = (
                                            type(ws.exception()).__name__
                                            if ws.exception() else None
                                        )
                                        self.ws_exception_message = (
                                            str(ws.exception())
                                            if ws.exception() else None
                                        )
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
                                        self.ws_close_code = ws.close_code
                                        self.ws_close_reason = getattr(ws, "close_reason", None)
                                        self.ws_exception_type = (
                                            type(ws.exception()).__name__
                                            if ws.exception() else None
                                        )
                                        self.ws_exception_message = (
                                            str(ws.exception())
                                            if ws.exception() else None
                                        )
                                        self.ws_last_event = "socket_closed"
                                        LOG.warning(
                                            "Coincheck WebSocket CLOSED "
                                            "type=%s close_code=%r close_reason=%r exception=%r",
                                            msg.type,
                                            ws.close_code,
                                            getattr(ws, "close_reason", None),
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
                    self.transport_connected = False
                    self.analyzer.set_ws(False, self.last_error)
                    self.ws_last_event = self.ws_last_event or "transport_disconnected"
                    # Keep a useful reason even when the server closes the socket
                    # cleanly (which does not raise an exception in aiohttp).
                    try:
                        if "ws" in locals():
                            self.ws_close_code = ws.close_code
                            self.ws_close_reason = getattr(ws, "close_reason", None)
                            if ws.exception():
                                self.ws_exception_type = type(ws.exception()).__name__
                                self.ws_exception_message = str(ws.exception())
                            LOG.warning(
                                "Coincheck WebSocket session ended "
                                "closed=%s close_code=%s close_reason=%r exception=%r "
                                "last_error=%r",
                                ws.closed,
                                ws.close_code,
                                getattr(ws, "close_reason", None),
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
        finally:
            fallback_task.cancel()
            try:
                await fallback_task
            except asyncio.CancelledError:
                pass
