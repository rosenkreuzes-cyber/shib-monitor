import asyncio
import json
import logging
import time

import aiohttp

LOG = logging.getLogger("coincheck")
REST = "https://coincheck.com"
WS = "wss://ws-api.coincheck.com/"
VERSION = "5.4-renderfix23"
REST_INTERVAL = 5
RECONNECT_INITIAL = 2
RECONNECT_MAX = 30
RECEIVE_TIMEOUT = 5
NO_DATA_RECONNECT = 35


class CoincheckStream:
    """Coincheck REST+WebSocket stream for SHIB/JPY.

    fix23 goals:
      - REST polling is independent from WebSocket health.
      - WebSocket uses a real receive timeout and explicit PING/PONG probe.
      - Upgrade/close/error/subscription diagnostics are retained.
      - One WebSocket carries both official orderbook and trades channels.
      - A silent market-data connection is periodically recycled without
        making the REST path stale.
    """

    def __init__(self, pair, analyzer, broadcast):
        self.pair = pair
        self.analyzer = analyzer
        self.broadcast = broadcast

        self.connected = False
        self.transport_connected = False
        self.last_error = None
        self.last_ws_error = None

        self.ws_messages = 0
        self.ws_orderbook_messages = 0
        self.ws_trade_messages = 0
        self.ws_nontrade_list_messages = 0
        self.ws_trade_parse_failures = 0

        self.last_ws_message_ts = None
        self.last_ws_orderbook_ts = None
        self.last_ws_trade_ts = None
        self.last_ws_pong_ts = None
        self.last_subscribe_ts = None
        self.ws_subscribed = False
        self.ws_data_state = "DISCONNECTED"
        self.ws_last_event = None
        self.ws_close_code = None
        self.ws_close_reason = None
        self.ws_upgrade_status = None
        self.ws_upgrade_headers = {}
        self.ws_receive_timeouts = 0
        self.ws_reconnects = 0
        self.ws_ping_sent = 0
        self.ws_pong_received = 0
        self.ws_raw_preview = None
        self.ws_trade_raw_preview = None

    def _now(self):
        return time.time()

    async def _broadcast(self):
        try:
            result = self.broadcast()
            if asyncio.iscoroutine(result):
                await result
        except Exception:
            LOG.exception("broadcast failed")

    async def rest_snapshot(self, session):
        try:
            async with session.get(
                f"{REST}/api/order_books",
                params={"pair": self.pair},
                timeout=aiohttp.ClientTimeout(total=10),
            ) as r:
                r.raise_for_status()
                book = await r.json(content_type=None)

            if not isinstance(book, dict) or not book.get("asks") or not book.get("bids"):
                raise RuntimeError(f"empty/invalid order book: {book}")

            self.analyzer.load_depth(book)

            try:
                async with session.get(
                    f"{REST}/api/ticker",
                    params={"pair": self.pair},
                    timeout=aiohttp.ClientTimeout(total=10),
                ) as r:
                    r.raise_for_status()
                    ticker = await r.json(content_type=None)
                self.analyzer.ticker(ticker)
            except Exception as exc:
                LOG.warning("ticker REST failed: %s", exc)

            self.analyzer.set_source("coincheck_rest")
            self.last_error = None
            await self._broadcast()
            LOG.info(
                "REST snapshot loaded pair=%s bids=%d asks=%d",
                self.pair,
                len(book.get("bids", [])),
                len(book.get("asks", [])),
            )
            return True
        except Exception as exc:
            self.last_error = f"REST: {type(exc).__name__}: {exc}"
            LOG.exception("REST snapshot failed")
            return False

    async def _rest_loop(self):
        timeout = aiohttp.ClientTimeout(total=15)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            while True:
                await self.rest_snapshot(session)
                await asyncio.sleep(REST_INTERVAL)

    async def _send_subscriptions(self, ws):
        for channel in (f"{self.pair}-orderbook", f"{self.pair}-trades"):
            payload = {"type": "subscribe", "channel": channel}
            raw = json.dumps(payload, separators=(",", ":"))
            await ws.send_str(raw)
            self.last_subscribe_ts = self._now()
            self.ws_last_event = f"subscribe_sent:{channel}"
            self.ws_subscribed = True
            LOG.info("Coincheck subscribe sent: %s", raw)

    async def _ws_session(self):
        timeout = aiohttp.ClientTimeout(total=None, connect=15, sock_connect=15, sock_read=None)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            self.ws_last_event = "connecting"
            async with session.ws_connect(
                WS,
                heartbeat=None,
                autoping=False,
                timeout=15,
                compress=0,
                headers={"User-Agent": "shib-monitor-renderfix23/1.0"},
            ) as ws:
                self.transport_connected = True
                self.connected = True
                self.ws_upgrade_status = getattr(ws._response, "status", None)
                self.ws_upgrade_headers = {
                    k: v for k, v in ws._response.headers.items()
                    if k.lower() in {
                        "connection", "upgrade", "server", "date", "content-type",
                        "sec-websocket-accept", "uwebsockets",
                    }
                }
                self.ws_close_code = None
                self.ws_close_reason = None
                self.last_ws_error = None
                self.ws_data_state = "CONNECTED_NO_DATA"
                self.ws_last_event = "transport_connected"
                self.analyzer.set_ws(True, None)
                await self._broadcast()

                await self._send_subscriptions(ws)
                subscribe_deadline = self._now() + 3
                first_data = False
                silent_since = self._now()

                while not ws.closed:
                    try:
                        msg = await ws.receive(timeout=RECEIVE_TIMEOUT)
                    except asyncio.TimeoutError:
                        self.ws_receive_timeouts += 1
                        # Control-frame ping is deliberately explicit in fix23.
                        self.ws_ping_sent += 1
                        self.ws_last_event = "ping_sent"
                        await ws.ping(b"renderfix23")

                        try:
                            pong = await ws.receive(timeout=3)
                            if pong.type == aiohttp.WSMsgType.PONG:
                                self.ws_pong_received += 1
                                self.last_ws_pong_ts = self._now()
                                self.ws_last_event = "pong_received"
                            elif pong.type == aiohttp.WSMsgType.PING:
                                await ws.pong(pong.data)
                            elif pong.type == aiohttp.WSMsgType.TEXT:
                                await self.handle(pong.data)
                                first_data = self.ws_messages > 0
                            elif pong.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                                break
                        except asyncio.TimeoutError:
                            self.ws_last_event = "pong_timeout"

                        if self._now() - silent_since >= NO_DATA_RECONNECT:
                            self.ws_last_event = "no_market_data_reconnect"
                            if not ws.closed:
                                await ws.close(code=1012, message=b"no market data")
                            break
                        continue

                    if msg.type == aiohttp.WSMsgType.TEXT:
                        self.ws_messages += 1
                        self.last_ws_message_ts = self._now()
                        self.ws_raw_preview = msg.data[:500]
                        self.ws_last_event = "text_received"
                        await self.handle(msg.data)
                        first_data = True
                        if self.ws_orderbook_messages or self.ws_trade_messages:
                            self.ws_data_state = "RECEIVING_DATA"
                            silent_since = self._now()

                    elif msg.type == aiohttp.WSMsgType.BINARY:
                        self.ws_messages += 1
                        self.last_ws_message_ts = self._now()
                        self.ws_last_event = "binary_received"
                        self.ws_raw_preview = repr(msg.data[:200])

                    elif msg.type == aiohttp.WSMsgType.PING:
                        await ws.pong(msg.data)
                        self.ws_last_event = "ping_received"

                    elif msg.type == aiohttp.WSMsgType.PONG:
                        self.ws_pong_received += 1
                        self.last_ws_pong_ts = self._now()
                        self.ws_last_event = "pong_received"

                    elif msg.type == aiohttp.WSMsgType.CLOSE:
                        self.ws_close_code = ws.close_code
                        self.ws_close_reason = msg.extra
                        self.ws_last_event = "close_received"
                        break

                    elif msg.type == aiohttp.WSMsgType.CLOSED:
                        self.ws_close_code = ws.close_code
                        self.ws_close_reason = msg.extra
                        self.ws_last_event = "closed"
                        break

                    elif msg.type == aiohttp.WSMsgType.ERROR:
                        self.ws_close_code = ws.close_code
                        self.ws_last_error = str(ws.exception())
                        self.ws_last_event = "receive_error"
                        break

                    if first_data:
                        silent_since = self._now()
                    elif self._now() >= subscribe_deadline:
                        # Keep the connection alive: absence of a subscribe ACK
                        # is not considered an error because Coincheck's public
                        # API does not document an ACK requirement.
                        self.ws_data_state = "CONNECTED_NO_DATA"

                self.ws_close_code = ws.close_code

    async def _ws_loop(self):
        delay = RECONNECT_INITIAL
        while True:
            try:
                self.ws_subscribed = False
                self.ws_data_state = "CONNECTING"
                await self._ws_session()
                self.connected = False
                self.transport_connected = False
                await self._broadcast()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.connected = False
                self.transport_connected = False
                self.last_ws_error = f"{type(exc).__name__}: {exc}"
                self.last_error = self.last_ws_error
                self.ws_last_event = "exception"
                self.analyzer.set_ws(False, self.last_ws_error)
                LOG.exception("Coincheck WS error; reconnect in %ss", delay)
                await self._broadcast()
            finally:
                self.connected = False
                self.transport_connected = False

            self.ws_reconnects += 1
            await asyncio.sleep(delay)
            delay = min(delay * 2, RECONNECT_MAX)

    async def run(self):
        rest_task = asyncio.create_task(self._rest_loop())
        ws_task = asyncio.create_task(self._ws_loop())
        try:
            await asyncio.gather(rest_task, ws_task)
        finally:
            for task in (rest_task, ws_task):
                task.cancel()
            await asyncio.gather(rest_task, ws_task, return_exceptions=True)

    async def handle(self, text):
        try:
            data = json.loads(text)
        except (TypeError, ValueError):
            return

        if (
            isinstance(data, list)
            and len(data) == 2
            and data[0] == self.pair
            and isinstance(data[1], dict)
        ):
            payload = data[1]
            if "bids" in payload or "asks" in payload:
                self.ws_orderbook_messages += 1
                self.last_ws_orderbook_ts = self._now()
                self.analyzer.diff_depth(payload)
                self.analyzer.set_source("coincheck_ws_orderbook")
                await self._broadcast()
            return

        if isinstance(data, list):
            matched = 0
            for row in data:
                if not isinstance(row, list) or len(row) < 6:
                    continue
                if row[2] != self.pair:
                    continue
                matched += 1
                self.ws_trade_messages += 1
                self.last_ws_trade_ts = self._now()
                self.ws_trade_raw_preview = repr(row)[:500]
                before = len(self.analyzer.trades)
                self.analyzer.trade({
                    "executed_at": row[0],
                    "id": row[1],
                    "pair": row[2],
                    "price": row[3],
                    "amount": row[4],
                    "side": row[5],
                })
                if len(self.analyzer.trades) == before:
                    self.ws_trade_parse_failures += 1
            if matched == 0:
                self.ws_nontrade_list_messages += 1
            if matched:
                await self._broadcast()

    def health(self):
        return {
            "ws_connected": self.connected,
            "ws_transport_connected": self.transport_connected,
            "ws_data_state": self.ws_data_state,
            "ws_subscribed": self.ws_subscribed,
            "ws_last_event": self.ws_last_event,
            "ws_upgrade_status": self.ws_upgrade_status,
            "ws_upgrade_headers": self.ws_upgrade_headers,
            "ws_close_code": self.ws_close_code,
            "ws_close_reason": self.ws_close_reason,
            "ws_messages": self.ws_messages,
            "ws_orderbook_messages": self.ws_orderbook_messages,
            "ws_trade_messages": self.ws_trade_messages,
            "ws_nontrade_list_messages": self.ws_nontrade_list_messages,
            "ws_trade_parse_failures": self.ws_trade_parse_failures,
            "last_ws_message_ts": self.last_ws_message_ts,
            "last_ws_orderbook_ts": self.last_ws_orderbook_ts,
            "last_ws_trade_ts": self.last_ws_trade_ts,
            "last_ws_pong_ts": self.last_ws_pong_ts,
            "last_subscribe_ts": self.last_subscribe_ts,
            "ws_receive_timeouts": self.ws_receive_timeouts,
            "ws_ping_sent": self.ws_ping_sent,
            "ws_pong_received": self.ws_pong_received,
            "ws_reconnects": self.ws_reconnects,
            "ws_raw_preview": self.ws_raw_preview,
            "ws_trade_raw_preview": self.ws_trade_raw_preview,
            "last_ws_error": self.last_ws_error,
        }
