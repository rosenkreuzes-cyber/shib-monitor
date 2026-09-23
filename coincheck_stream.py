import asyncio
import json
import logging
from time import time

import aiohttp

LOG = logging.getLogger(__name__)

REST_BASE = "https://coincheck.com/api"
WS_URL = "wss://ws-api.coincheck.com/"
REST_INTERVAL = 5.0
RECONNECT_INITIAL = 2.0
RECONNECT_MAX = 30.0
WS_STALE_SECONDS = 30.0
WS_IDLE_SECONDS = 10.0
WS_NO_DATA_RECONNECT_SECONDS = 25.0
WS_CONNECT_GRACE_SECONDS = 8.0
WS_RECEIVE_PREVIEW_LIMIT = 1000


class CoincheckStream:
    """REST-primary market stream with two independent Coincheck WS channels."""

    def __init__(self, pair, analyzer, broadcast=None):
        self.pair = pair
        self.analyzer = analyzer
        self._broadcast = broadcast
        self._stopped = False

        self.connected = False
        self.transport_connected = False
        self.subscribed = False
        self.last_error = None
        self.ws_last_error = None

        self.ws_messages = 0
        self.ws_orderbook_messages = 0
        self.ws_trade_messages = 0
        self.last_ws_message_ts = None
        self.last_ws_orderbook_ts = None
        self.last_ws_trade_ts = None

        # Channel-specific diagnostics. An ACK is optional; Coincheck's public
        # docs do not require a subscribe ACK, so *_active means market data arrived.
        self.ws_orderbook_connected = False
        self.ws_orderbook_transport_connected = False
        self.ws_orderbook_active = False
        self.ws_orderbook_subscribe_sent_ts = None
        self.ws_orderbook_subscribe_ack_ts = None
        self.ws_orderbook_subscribe_error_ts = None
        self.ws_orderbook_last_event = None
        self.ws_orderbook_last_error = None
        self.ws_orderbook_exception_type = None
        self.ws_orderbook_exception_message = None
        self.ws_orderbook_close_code = None
        self.ws_orderbook_close_reason = None
        self.ws_orderbook_reconnects = 0
        self.ws_orderbook_connection_started_ts = None
        self.ws_orderbook_receive_type = None
        self.ws_orderbook_receive_data_preview = None
        self.ws_orderbook_receive_extra_preview = None
        self.ws_orderbook_last_close_type = None
        self.ws_orderbook_last_close_message = None
        self.ws_orderbook_receive_wait_started_ts = None
        self.ws_orderbook_receive_wait_seconds = None

        self.ws_trade_connected = False
        self.ws_trade_transport_connected = False
        self.ws_trade_active = False
        self.ws_trade_subscribe_sent_ts = None
        self.ws_trade_subscribe_ack_ts = None
        self.ws_trade_subscribe_error_ts = None
        self.ws_trade_last_event = None
        self.ws_trade_last_error = None
        self.ws_trade_exception_type = None
        self.ws_trade_exception_message = None
        self.ws_trade_close_code = None
        self.ws_trade_close_reason = None
        self.ws_trade_reconnects = 0
        self.ws_trade_connection_started_ts = None
        self.ws_trade_receive_type = None
        self.ws_trade_receive_data_preview = None
        self.ws_trade_receive_extra_preview = None
        self.ws_trade_last_close_type = None
        self.ws_trade_last_close_message = None
        self.ws_trade_receive_wait_started_ts = None
        self.ws_trade_receive_wait_seconds = None

        self.ws_subscribe_sent_ts = None
        self.ws_subscribe_ack_ts = None
        self.ws_subscribe_error_ts = None
        self.ws_last_event = None
        self.ws_last_channel = None
        self.ws_raw_preview_type = None
        self.ws_raw_preview = None
        self.ws_close_code = None
        self.ws_close_reason = None
        self.ws_exception_type = None
        self.ws_exception_message = None
        self.ws_receive_type = None
        self.ws_receive_data_preview = None
        self.ws_receive_extra_preview = None
        self.ws_last_close_type = None
        self.ws_last_close_message = None
        self.ws_receive_wait_started_ts = None
        self.ws_receive_wait_seconds = None

        self.ws_trade_raw_preview = None
        self.ws_trade_parse_failures = 0
        self.ws_nontrade_list_messages = 0
        self.ws_last_data_state = "NEVER"

        self.rest_refresh_count = 0
        self.last_rest_refresh_ts = None
        self.rest_fail_count = 0

    def now(self):
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

    async def rest_refresh(self, session):
        try:
            async with session.get(
                f"{REST_BASE}/order_books",
                params={"pair": self.pair},
            ) as response:
                response.raise_for_status()
                data = await response.json(content_type=None)

            bids = data.get("bids") or []
            asks = data.get("asks") or []
            if not bids or not asks:
                raise RuntimeError(
                    f"empty orderbook bids={len(bids)} asks={len(asks)}"
                )

            self.analyzer.load_depth(data)

            try:
                async with session.get(
                    f"{REST_BASE}/ticker",
                    params={"pair": self.pair},
                ) as response:
                    response.raise_for_status()
                    ticker = await response.json(content_type=None)
                if isinstance(ticker, dict):
                    self.analyzer.ticker(ticker)
            except Exception as exc:
                LOG.warning("ticker refresh failed: %s", exc)

            try:
                async with session.get(
                    f"{REST_BASE}/trades",
                    params={"pair": self.pair, "limit": 20},
                ) as response:
                    response.raise_for_status()
                    trades = await response.json(content_type=None)

                for row in (
                    trades.get("data", []) if isinstance(trades, dict) else []
                ):
                    if isinstance(row, dict):
                        self.analyzer.trade(
                            {
                                "executed_at": row.get(
                                    "created_at", row.get("executed_at")
                                ),
                                "id": row.get("id"),
                                "pair": row.get("pair", self.pair),
                                "price": row.get("price", row.get("rate")),
                                "amount": row.get("amount"),
                                "side": row.get("side", row.get("order_type")),
                            }
                        )
            except Exception as exc:
                LOG.warning("trades refresh failed: %s", exc)

            self.last_error = None
            self.analyzer.last_error = None
            self.rest_refresh_count += 1
            self.last_rest_refresh_ts = self.now()
            await self.broadcast()

        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self.last_error = str(exc)
            self.analyzer.last_error = self.last_error
            self.rest_fail_count += 1
            LOG.warning("REST refresh failed: %s", exc)

    async def rest_loop(self, session):
        while not self._stopped:
            await self.rest_refresh(session)
            await asyncio.sleep(REST_INTERVAL)

    async def _subscribe(self, ws, channel, kind):
        await ws.send_str(
            json.dumps({"type": "subscribe", "channel": channel})
        )
        ts = self.now()
        self.ws_subscribe_sent_ts = ts
        self.ws_last_event = f"subscribe_sent:{channel}"
        self.ws_last_channel = channel
        if kind == "orderbook":
            self.ws_orderbook_subscribe_sent_ts = ts
            self.ws_orderbook_last_event = "subscribe_sent"
        else:
            self.ws_trade_subscribe_sent_ts = ts
            self.ws_trade_last_event = "subscribe_sent"

    async def _handle_control(self, data, kind):
        if not isinstance(data, dict):
            return False

        channel = data.get("channel")
        typ = data.get("type")
        if typ in ("subscribed", "subscribe"):
            ts = self.now()
            self.ws_subscribe_ack_ts = ts
            self.ws_last_event = f"subscribed:{channel}"
            self.ws_last_channel = channel
            if kind == "orderbook":
                self.ws_orderbook_subscribe_ack_ts = ts
                self.ws_orderbook_last_event = "subscribed_ack"
            else:
                self.ws_trade_subscribe_ack_ts = ts
                self.ws_trade_last_event = "subscribed_ack"
            return True

        if typ in ("error", "subscribe_error"):
            ts = self.now()
            error = json.dumps(data, ensure_ascii=False)[:1000]
            self.ws_subscribe_error_ts = ts
            self.ws_last_event = "subscribe_error"
            self.ws_last_error = error
            self.analyzer.ws_last_error = error
            if kind == "orderbook":
                self.ws_orderbook_subscribe_error_ts = ts
                self.ws_orderbook_last_error = error
                self.ws_orderbook_last_event = "subscribe_error"
            else:
                self.ws_trade_subscribe_error_ts = ts
                self.ws_trade_last_error = error
                self.ws_trade_last_event = "subscribe_error"
            return True

        return False

    async def handle_orderbook(self, text):
        try:
            data = json.loads(text)
        except Exception:
            return False

        self.ws_messages += 1
        self.last_ws_message_ts = self.now()
        self.ws_raw_preview_type = type(data).__name__
        self.ws_raw_preview = json.dumps(
            data, ensure_ascii=False
        )[:1000]

        if await self._handle_control(data, "orderbook"):
            return False

        if (
            isinstance(data, list)
            and len(data) >= 2
            and data[0] == self.pair
            and isinstance(data[1], dict)
            and ("bids" in data[1] or "asks" in data[1])
        ):
            ts = self.now()
            self.ws_orderbook_messages += 1
            self.last_ws_orderbook_ts = ts
            self.ws_orderbook_active = True
            self.ws_last_channel = f"{self.pair}-orderbook"
            self.ws_last_event = "orderbook_received"
            self.ws_orderbook_last_event = "data_received"
            self.ws_orderbook_last_error = None
            self.ws_last_error = None
            self.analyzer.diff_depth(data[1])
            self.analyzer.set_ws(True, None)
            await self.broadcast()
            return True

        return False

    async def handle_trade(self, text):
        try:
            data = json.loads(text)
        except Exception:
            return False

        self.ws_messages += 1
        self.last_ws_message_ts = self.now()
        self.ws_raw_preview_type = type(data).__name__
        self.ws_raw_preview = json.dumps(
            data, ensure_ascii=False
        )[:1000]

        if await self._handle_control(data, "trade"):
            return False

        if not isinstance(data, list):
            return False

        changed = False
        matched = 0
        for row in data:
            if not (
                isinstance(row, list)
                and len(row) >= 6
                and row[2] == self.pair
            ):
                continue

            matched += 1
            self.ws_trade_raw_preview = json.dumps(
                row, ensure_ascii=False
            )[:1000]
            side = row[5]
            try:
                float(row[0])
                float(row[3])
                float(row[4])
            except (TypeError, ValueError):
                self.ws_trade_parse_failures += 1
                self.ws_trade_last_event = "trade_parse_error"
                continue

            if side not in ("buy", "sell"):
                self.ws_trade_parse_failures += 1
                self.ws_trade_last_event = "trade_parse_error"
                continue

            ts = self.now()
            self.ws_trade_messages += 1
            self.last_ws_trade_ts = ts
            self.ws_trade_active = True
            self.ws_last_channel = f"{self.pair}-trades"
            self.ws_last_event = "trade_received"
            self.ws_trade_last_event = "data_received"
            self.ws_trade_last_error = None
            self.ws_last_error = None
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

        if matched == 0:
            self.ws_nontrade_list_messages += 1
        if changed:
            self.analyzer.set_ws(True, None)
            await self.broadcast()
        return changed

    async def _watchdog(self, ws, kind, get_last_data):
        try:
            while not self._stopped:
                await asyncio.sleep(5)
                last = get_last_data()
                if last is None:
                    started = (
                        self.ws_orderbook_connection_started_ts
                        if kind == "orderbook"
                        else self.ws_trade_connection_started_ts
                    )
                    elapsed = self.now() - (started or self.now())
                else:
                    elapsed = self.now() - last
                if elapsed >= WS_NO_DATA_RECONNECT_SECONDS:
                    message = (
                        f"{kind} websocket market-data idle for "
                        f"{elapsed:.1f}s; reconnecting"
                    )
                    if kind == "orderbook":
                        self.ws_orderbook_last_event = "watchdog_reconnect"
                        self.ws_orderbook_last_error = message
                    else:
                        self.ws_trade_last_event = "watchdog_reconnect"
                        self.ws_trade_last_error = message
                    LOG.warning(message)
                    await ws.close(code=1000, message=b"market data idle")
                    return
        except asyncio.CancelledError:
            raise

    async def _ws_channel_session(self, session, kind):
        channel = f"{self.pair}-{kind}"
        async with session.ws_connect(
            WS_URL,
            heartbeat=20,
            autoping=True,
            autoclose=True,
            receive_timeout=None,
            timeout=15,
        ) as ws:
            started_ts = self.now()
            if kind == "orderbook":
                self.ws_orderbook_connection_started_ts = started_ts
                self.ws_orderbook_connected = True
                self.ws_orderbook_transport_connected = True
                self.ws_orderbook_last_event = "transport_connected"
            else:
                self.ws_trade_connection_started_ts = started_ts
                self.ws_trade_connected = True
                self.ws_trade_transport_connected = True
                self.ws_trade_last_event = "transport_connected"

            await self._subscribe(ws, channel, kind)

            if kind == "orderbook":
                get_last = lambda: self.last_ws_orderbook_ts
            else:
                get_last = lambda: self.last_ws_trade_ts
            watchdog = asyncio.create_task(
                self._watchdog(ws, kind, get_last)
            )

            try:
                while not self._stopped:
                    receive_started = self.now()
                    if kind == "orderbook":
                        self.ws_orderbook_receive_wait_started_ts = receive_started
                    else:
                        self.ws_trade_receive_wait_started_ts = receive_started
                    self.ws_receive_wait_started_ts = receive_started

                    message = await ws.receive()
                    receive_elapsed = round(self.now() - receive_started, 3)
                    if kind == "orderbook":
                        self.ws_orderbook_receive_wait_seconds = receive_elapsed
                    else:
                        self.ws_trade_receive_wait_seconds = receive_elapsed
                    self.ws_receive_wait_seconds = receive_elapsed

                    type_name = getattr(message.type, "name", str(message.type))
                    data = getattr(message, "data", None)
                    extra = getattr(message, "extra", None)
                    data_preview = repr(data)[:WS_RECEIVE_PREVIEW_LIMIT] if data is not None else None
                    extra_preview = repr(extra)[:WS_RECEIVE_PREVIEW_LIMIT] if extra is not None else None

                    if kind == "orderbook":
                        self.ws_orderbook_receive_type = type_name
                        self.ws_orderbook_receive_data_preview = data_preview or None
                        self.ws_orderbook_receive_extra_preview = extra_preview or None
                    else:
                        self.ws_trade_receive_type = type_name
                        self.ws_trade_receive_data_preview = data_preview or None
                        self.ws_trade_receive_extra_preview = extra_preview or None
                    self.ws_receive_type = type_name
                    self.ws_receive_data_preview = data_preview or None
                    self.ws_receive_extra_preview = extra_preview or None

                    if message.type == aiohttp.WSMsgType.TEXT:
                        if kind == "orderbook":
                            await self.handle_orderbook(message.data)
                        else:
                            await self.handle_trade(message.data)
                    elif message.type == aiohttp.WSMsgType.BINARY:
                        self.ws_messages += 1
                        self.last_ws_message_ts = self.now()
                    elif message.type == aiohttp.WSMsgType.PING:
                        await ws.pong()
                    elif message.type == aiohttp.WSMsgType.PONG:
                        pass
                    elif message.type in (aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSING, aiohttp.WSMsgType.CLOSED):
                        # aiohttp can report CLOSING/CLOSED with message.data=None and
                        # close_code still unset. Do not turn that diagnostic event into
                        # a misleading RuntimeError; capture the complete socket state
                        # and let the context manager close/reconnect normally.
                        close_code = getattr(ws, "close_code", None)
                        close_reason = getattr(ws, "close_reason", None)
                        ws_closed = getattr(ws, "closed", None)
                        ws_exception = ws.exception()
                        close_detail = {
                            "message_type": type_name,
                            "message_data": data_preview,
                            "message_extra": extra_preview,
                            "ws_closed": ws_closed,
                            "ws_close_code": close_code,
                            "ws_close_reason": close_reason,
                            "ws_exception": repr(ws_exception) if ws_exception else None,
                            "receive_wait_seconds": receive_elapsed,
                        }
                        detail = json.dumps(close_detail, ensure_ascii=False)
                        if kind == "orderbook":
                            self.ws_orderbook_last_close_type = type_name
                            self.ws_orderbook_last_close_message = detail
                            self.ws_orderbook_close_code = close_code
                            self.ws_orderbook_close_reason = close_reason
                            self.ws_orderbook_last_event = "closed"
                            self.ws_orderbook_last_error = f"orderbook websocket closed: {detail}"
                        else:
                            self.ws_trade_last_close_type = type_name
                            self.ws_trade_last_close_message = detail
                            self.ws_trade_close_code = close_code
                            self.ws_trade_close_reason = close_reason
                            self.ws_trade_last_event = "closed"
                            self.ws_trade_last_error = f"trades websocket closed: {detail}"
                        self.ws_last_close_type = type_name
                        self.ws_last_close_message = detail
                        self.ws_close_code = close_code
                        self.ws_close_reason = close_reason
                        self.ws_last_event = "closed"
                        self.ws_last_channel = channel
                        self.ws_last_error = None if ws_exception is None else repr(ws_exception)
                        self.ws_exception_type = None if ws_exception is None else type(ws_exception).__name__
                        self.ws_exception_message = None if ws_exception is None else str(ws_exception)
                        LOG.warning("Coincheck WS channel=%s close event: %s", kind, detail)
                        return
                    elif message.type == aiohttp.WSMsgType.ERROR:
                        err = ws.exception()
                        detail = f"{kind} websocket ERROR type={type_name} data={data_preview!r} extra={extra_preview!r} exception={err!r}"
                        if kind == "orderbook":
                            self.ws_orderbook_last_event = "receive_error"
                            self.ws_orderbook_last_error = detail
                        else:
                            self.ws_trade_last_event = "receive_error"
                            self.ws_trade_last_error = detail
                        raise RuntimeError(detail)
            finally:
                watchdog.cancel()
                try:
                    await watchdog
                except asyncio.CancelledError:
                    pass

                self.ws_close_code = getattr(ws, "close_code", None)
                self.ws_close_reason = getattr(ws, "close_reason", None)

    async def ws_channel_loop(self, session, kind):
        delay = RECONNECT_INITIAL
        while not self._stopped:
            try:
                LOG.info(
                    "Coincheck WS connecting channel=%s pair=%s",
                    kind,
                    self.pair,
                )
                before = self.now()
                await self._ws_channel_session(session, kind)
                # A clean return with no market data is not a healthy session.
                # Keep backoff instead of hammering the endpoint.
                if kind == "orderbook":
                    active = self.ws_orderbook_active
                else:
                    active = self.ws_trade_active
                if active or self.now() - before >= WS_CONNECT_GRACE_SECONDS:
                    delay = RECONNECT_INITIAL
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                error = str(exc)
                if kind == "orderbook":
                    self.ws_orderbook_connected = False
                    self.ws_orderbook_transport_connected = False
                    self.ws_orderbook_exception_type = type(exc).__name__
                    self.ws_orderbook_exception_message = error
                    self.ws_orderbook_last_error = error
                    self.ws_orderbook_reconnects += 1
                else:
                    self.ws_trade_connected = False
                    self.ws_trade_transport_connected = False
                    self.ws_trade_exception_type = type(exc).__name__
                    self.ws_trade_exception_message = error
                    self.ws_trade_last_error = error
                    self.ws_trade_reconnects += 1

                self.ws_last_error = error
                self.ws_exception_type = type(exc).__name__
                self.ws_exception_message = error
                LOG.warning(
                    "Coincheck WS channel=%s disconnected: %s",
                    kind,
                    exc,
                )
                await self.broadcast()
                await asyncio.sleep(delay)
                delay = min(delay * 2, RECONNECT_MAX)
            finally:
                if kind == "orderbook":
                    self.ws_orderbook_connected = False
                    self.ws_orderbook_transport_connected = False
                else:
                    self.ws_trade_connected = False
                    self.ws_trade_transport_connected = False

    async def run(self):
        timeout = aiohttp.ClientTimeout(
            total=None,
            connect=15,
            sock_connect=15,
            sock_read=None,
        )
        async with aiohttp.ClientSession(timeout=timeout) as session:
            tasks = [
                asyncio.create_task(self.rest_loop(session)),
                asyncio.create_task(self.ws_channel_loop(session, "orderbook")),
                asyncio.create_task(self.ws_channel_loop(session, "trades")),
            ]
            try:
                await asyncio.gather(*tasks)
            finally:
                self._stopped = True
                for task in tasks:
                    task.cancel()
                for task in tasks:
                    try:
                        await task
                    except asyncio.CancelledError:
                        pass

    def health(self):
        now = self.now()

        def age(value):
            return round(now - value, 3) if value else None

        msg_age = age(self.last_ws_message_ts)
        ob_age = age(self.last_ws_orderbook_ts)
        trade_age = age(self.last_ws_trade_ts)

        any_connected = (
            self.ws_orderbook_connected or self.ws_trade_connected
        )
        any_live = (
            ob_age is not None and ob_age <= WS_IDLE_SECONDS
        ) or (
            trade_age is not None and trade_age <= WS_IDLE_SECONDS
        )

        if not any_connected:
            data_state = "DISCONNECTED"
        elif any_live:
            data_state = "LIVE"
        elif ob_age is None and trade_age is None:
            data_state = "CONNECTED_NO_DATA"
        elif (
            ob_age is not None and ob_age <= WS_STALE_SECONDS
        ) or (
            trade_age is not None and trade_age <= WS_STALE_SECONDS
        ):
            data_state = "IDLE"
        else:
            data_state = "STALE"

        self.ws_last_data_state = data_state
        self.connected = any_connected
        self.transport_connected = (
            self.ws_orderbook_transport_connected
            or self.ws_trade_transport_connected
        )
        self.subscribed = self.ws_orderbook_active or self.ws_trade_active

        return {
            "ws_transport_connected": self.transport_connected,
            "ws_connected": self.connected,
            "ws_subscribed": self.subscribed,
            "ws_age_sec": msg_age,
            "ws_orderbook_age_sec": ob_age,
            "ws_trade_age_sec": trade_age,
            "ws_data_state": data_state,
            "ws_subscribe_sent_ts": self.ws_subscribe_sent_ts,
            "ws_subscribe_ack_ts": self.ws_subscribe_ack_ts,
            "ws_subscribe_error_ts": self.ws_subscribe_error_ts,
            "ws_last_event": self.ws_last_event,
            "ws_last_channel": self.ws_last_channel,
            "ws_raw_preview_type": self.ws_raw_preview_type,
            "ws_raw_preview": self.ws_raw_preview,
            "ws_close_code": self.ws_close_code,
            "ws_close_reason": self.ws_close_reason,
            "ws_receive_wait_started_ts": self.ws_receive_wait_started_ts,
            "ws_receive_wait_seconds": self.ws_receive_wait_seconds,
            "ws_exception_type": self.ws_exception_type,
            "ws_exception_message": self.ws_exception_message,
            "ws_receive_type": self.ws_receive_type,
            "ws_receive_data_preview": self.ws_receive_data_preview,
            "ws_receive_extra_preview": self.ws_receive_extra_preview,
            "ws_last_close_type": self.ws_last_close_type,
            "ws_last_close_message": self.ws_last_close_message,
            "ws_messages": self.ws_messages,
            "ws_orderbook_messages": self.ws_orderbook_messages,
            "ws_trade_messages": self.ws_trade_messages,
            "ws_trade_parse_failures": self.ws_trade_parse_failures,
            "ws_nontrade_list_messages": self.ws_nontrade_list_messages,
            "ws_trade_raw_preview": self.ws_trade_raw_preview,
            "last_ws_message_ts": self.last_ws_message_ts,
            "last_ws_orderbook_ts": self.last_ws_orderbook_ts,
            "last_ws_trade_ts": self.last_ws_trade_ts,
            "ws_orderbook_connected": self.ws_orderbook_connected,
            "ws_orderbook_transport_connected": self.ws_orderbook_transport_connected,
            "ws_orderbook_active": self.ws_orderbook_active,
            "ws_orderbook_subscribe_sent_ts": self.ws_orderbook_subscribe_sent_ts,
            "ws_orderbook_subscribe_ack_ts": self.ws_orderbook_subscribe_ack_ts,
            "ws_orderbook_subscribe_error_ts": self.ws_orderbook_subscribe_error_ts,
            "ws_orderbook_last_event": self.ws_orderbook_last_event,
            "ws_orderbook_last_error": self.ws_orderbook_last_error,
            "ws_orderbook_exception_type": self.ws_orderbook_exception_type,
            "ws_orderbook_exception_message": self.ws_orderbook_exception_message,
            "ws_orderbook_reconnects": self.ws_orderbook_reconnects,
            "ws_orderbook_connection_started_ts": self.ws_orderbook_connection_started_ts,
            "ws_orderbook_receive_type": self.ws_orderbook_receive_type,
            "ws_orderbook_receive_data_preview": self.ws_orderbook_receive_data_preview,
            "ws_orderbook_receive_extra_preview": self.ws_orderbook_receive_extra_preview,
            "ws_orderbook_last_close_type": self.ws_orderbook_last_close_type,
            "ws_orderbook_last_close_message": self.ws_orderbook_last_close_message,
            "ws_orderbook_receive_wait_started_ts": self.ws_orderbook_receive_wait_started_ts,
            "ws_orderbook_receive_wait_seconds": self.ws_orderbook_receive_wait_seconds,
            "ws_orderbook_close_code": self.ws_orderbook_close_code,
            "ws_orderbook_close_reason": self.ws_orderbook_close_reason,
            "ws_trade_connected": self.ws_trade_connected,
            "ws_trade_transport_connected": self.ws_trade_transport_connected,
            "ws_trade_active": self.ws_trade_active,
            "ws_trade_subscribe_sent_ts": self.ws_trade_subscribe_sent_ts,
            "ws_trade_subscribe_ack_ts": self.ws_trade_subscribe_ack_ts,
            "ws_trade_subscribe_error_ts": self.ws_trade_subscribe_error_ts,
            "ws_trade_last_event": self.ws_trade_last_event,
            "ws_trade_last_error": self.ws_trade_last_error,
            "ws_trade_exception_type": self.ws_trade_exception_type,
            "ws_trade_exception_message": self.ws_trade_exception_message,
            "ws_trade_reconnects": self.ws_trade_reconnects,
            "ws_trade_connection_started_ts": self.ws_trade_connection_started_ts,
            "ws_trade_receive_type": self.ws_trade_receive_type,
            "ws_trade_receive_data_preview": self.ws_trade_receive_data_preview,
            "ws_trade_receive_extra_preview": self.ws_trade_receive_extra_preview,
            "ws_trade_last_close_type": self.ws_trade_last_close_type,
            "ws_trade_last_close_message": self.ws_trade_last_close_message,
            "ws_trade_receive_wait_started_ts": self.ws_trade_receive_wait_started_ts,
            "ws_trade_receive_wait_seconds": self.ws_trade_receive_wait_seconds,
            "ws_trade_close_code": self.ws_trade_close_code,
            "ws_trade_close_reason": self.ws_trade_close_reason,
            "rest_refresh_count": self.rest_refresh_count,
            "last_rest_refresh_ts": self.last_rest_refresh_ts,
            "rest_fail_count": self.rest_fail_count,
            "rest_last_error": self.last_error,
            "ws_last_error": self.ws_last_error,
        }
