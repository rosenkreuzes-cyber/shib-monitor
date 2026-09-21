import asyncio
import json
import logging
from time import time

import aiohttp

LOG = logging.getLogger(__name__)
REST = "https://coincheck.com"
WS = "wss://ws-api.coincheck.com"
VERSION = "5.4-renderfix13"
RECONNECT_DELAY = 3
RECEIVE_TIMEOUT = 35
ACK_GRACE = 5


class CoincheckStream:
    def __init__(self, pair, analyzer, broadcast=None):
        self.pair = pair
        self.analyzer = analyzer
        self._broadcast = broadcast
        self.connected = False
        self.transport_connected = False
        self.ws_subscribed = False
        self.ws_stale = True
        self.last_error = None
        self.ws_messages = 0
        self.ws_orderbook_messages = 0
        self.ws_trade_messages = 0
        self.last_ws_message_ts = None
        self.last_ws_orderbook_ts = None
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
        self.ws_receive_timeout_count = 0
        self.ws_ping_count = 0
        self.ws_pong_count = 0
        self._debug_raw_messages = 0
        self._debug_raw_limit = 12

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

    async def rest_snapshot(self, session):
        try:
            async with session.get(f"{REST}/api/order_books", params={"pair": self.pair}) as resp:
                resp.raise_for_status()
                data = await resp.json()
            if not isinstance(data, dict):
                raise RuntimeError(f"unexpected order_books response: {type(data).__name__}")
            bids, asks = data.get("bids", []), data.get("asks", [])
            self.analyzer.book.load_snapshot(bids, asks, data.get("sequence"), data.get("last_update_at"))
            try:
                async with session.get(f"{REST}/api/ticker", params={"pair": self.pair}) as resp:
                    resp.raise_for_status()
                    ticker = await resp.json()
                if isinstance(ticker, dict):
                    self.analyzer.ticker(ticker)
            except Exception:
                LOG.exception("ticker REST request failed")
            self.analyzer.set_source("coincheck_rest")
            await self.broadcast()
            LOG.info("REST snapshot loaded pair=%s bids=%d asks=%d", self.pair, len(bids), len(asks))
        except Exception as exc:
            self.last_error = str(exc)
            LOG.exception("REST snapshot failed")

    async def _subscribe(self, ws, channel):
        await ws.send_json({"type": "subscribe", "channel": channel})
        self.ws_subscribe_sent_ts = self._now()
        self.ws_last_event = f"subscribe_sent:{channel}"
        LOG.info("Coincheck subscribe sent: %s", channel)

    def _preview(self, msg):
        raw = msg.data
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8", "replace")
        self.ws_raw_preview_type = str(msg.type)
        self.ws_raw_preview = str(raw)[:500]
        if self._debug_raw_messages < self._debug_raw_limit:
            self._debug_raw_messages += 1
            LOG.warning("WS RAW #%d type=%s data=%s", self._debug_raw_messages, msg.type, raw)

    def _mark_ack_or_error(self, data):
        text = json.dumps(data, ensure_ascii=False) if not isinstance(data, str) else data
        low = text.lower()
        if any(x in low for x in ("error", "failed", "invalid", "denied")):
            self.ws_subscribe_error_ts = self._now()
            self.ws_last_event = "subscribe_error"
            self.last_error = f"WS subscribe response: {text[:500]}"
            return
        if isinstance(data, dict):
            if data.get("type") in ("subscribed", "subscribe", "ack", "success") or "channel" in data:
                channel = data.get("channel")
                self.ws_last_channel = channel
                self.ws_subscribe_ack_ts = self._now()
                self.ws_subscribed = True
                self.ws_last_event = f"subscribe_ack:{channel or 'unknown'}"
                return
        LOG.warning("WS control/unrecognized message: %r", data)

    async def _handle_text(self, raw_text):
        try:
            data = json.loads(raw_text)
        except (json.JSONDecodeError, TypeError) as exc:
            LOG.warning("WS JSON decode failed: %s data=%r", exc, raw_text[:500])
            return

        if isinstance(data, dict):
            self._mark_ack_or_error(data)
            return

        if isinstance(data, list) and len(data) == 2:
            channel, payload = data
            if channel in (self.pair, f"{self.pair}-orderbook") and isinstance(payload, dict) and ("bids" in payload or "asks" in payload):
                self.ws_subscribed = True
                self.ws_subscribe_ack_ts = self.ws_subscribe_ack_ts or self._now()
                self.ws_last_channel = channel
                self.ws_last_event = f"orderbook_received:{channel}"
                await self._handle_orderbook(payload)
                return
            if channel == f"{self.pair}-trades":
                self.ws_subscribed = True
                self.ws_subscribe_ack_ts = self.ws_subscribe_ack_ts or self._now()
                self.ws_last_channel = channel
                self.ws_last_event = f"trade_received:{channel}"
                await self._handle_trade(payload)
                return

        if isinstance(data, list):
            # Some Coincheck trade payloads are a bare list of rows.
            await self._handle_trade(data)
            return

        self._mark_ack_or_error(data)

    async def _handle_orderbook(self, payload):
        self.ws_orderbook_messages += 1
        self.last_ws_orderbook_ts = self._now()
        self.ws_stale = False
        try:
            changed = self.analyzer.diff_depth(payload)
        except Exception as exc:
            self.last_error = f"WS orderbook parse: {exc}"
            LOG.exception("orderbook diff handling failed")
            return
        self.analyzer.set_source("coincheck_ws_orderbook")
        LOG.info("orderbook WS #%d pair=%s bids=%d asks=%d changed=%s", self.ws_orderbook_messages, self.pair, len(payload.get("bids", [])), len(payload.get("asks", [])), changed)
        await self.broadcast()

    async def _handle_trade(self, payload):
        if not isinstance(payload, list):
            return
        before = len(self.analyzer.trades)
        for row in payload:
            if isinstance(row, list) and len(row) >= 6:
                try:
                    self.analyzer.trade({"executed_at": row[0], "id": row[1], "pair": row[2], "price": row[3], "amount": row[4], "side": row[5]})
                except Exception:
                    LOG.exception("trade row handling failed")
        if len(self.analyzer.trades) > before:
            self.ws_trade_messages += 1
            await self.broadcast()

    async def _receive_loop(self, ws):
        consecutive_timeouts = 0
        while not ws.closed:
            try:
                msg = await ws.receive(timeout=RECEIVE_TIMEOUT)
            except asyncio.TimeoutError:
                self.ws_receive_timeout_count += 1
                consecutive_timeouts += 1
                LOG.warning("WS receive timeout #%d", self.ws_receive_timeout_count)
                try:
                    self.ws_ping_count += 1
                    waiter = await ws.ping()
                    await asyncio.wait_for(waiter, timeout=10)
                    self.ws_pong_count += 1
                    LOG.info("WS ping/pong OK")
                except Exception as exc:
                    self.last_error = f"WS ping failed: {exc}"
                    return
                if consecutive_timeouts >= 2:
                    self.last_error = "WS receive timeout; reconnecting"
                    return
                continue
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.ws_exception_type = type(exc).__name__
                self.ws_exception_message = str(exc)
                self.last_error = str(exc)
                LOG.exception("WS receive failed")
                return

            consecutive_timeouts = 0
            self.ws_last_event = f"received:{msg.type}"
            self._preview(msg)
            if msg.type in (aiohttp.WSMsgType.TEXT, aiohttp.WSMsgType.BINARY):
                self.ws_messages += 1
                self.last_ws_message_ts = self._now()
            if msg.type == aiohttp.WSMsgType.TEXT:
                await self._handle_text(msg.data)
            elif msg.type == aiohttp.WSMsgType.BINARY:
                await self._handle_text(msg.data.decode("utf-8", "replace"))
            elif msg.type == aiohttp.WSMsgType.PING:
                self.ws_ping_count += 1
                await ws.pong(msg.data)
                self.ws_pong_count += 1
            elif msg.type == aiohttp.WSMsgType.PONG:
                self.ws_pong_count += 1
            elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.CLOSING, aiohttp.WSMsgType.ERROR):
                self.ws_close_code = ws.close_code
                self.ws_close_reason = str(msg.extra)
                return

    async def run(self):
        while True:
            try:
                timeout = aiohttp.ClientTimeout(total=None, connect=15, sock_connect=15, sock_read=None)
                async with aiohttp.ClientSession(timeout=timeout) as session:
                    await self.rest_snapshot(session)
                    async with session.ws_connect(WS, heartbeat=20, autoping=True, receive_timeout=None, max_msg_size=4 * 1024 * 1024) as ws:
                        self.transport_connected = True
                        self.connected = True
                        self.ws_subscribed = False
                        self.ws_stale = True
                        self.ws_subscribe_ack_ts = None
                        self.ws_subscribe_error_ts = None
                        self.ws_close_code = None
                        self.ws_close_reason = None
                        self.ws_exception_type = None
                        self.ws_exception_message = None
                        self._debug_raw_messages = 0
                        self.analyzer.set_ws(True, None)
                        LOG.info("Coincheck WebSocket transport connected")

                        # Critical fix: send both subscriptions immediately, then
                        # enter receive loop. Never wait synchronously for ACK.
                        await self._subscribe(ws, f"{self.pair}-orderbook")
                        await self._subscribe(ws, f"{self.pair}-trades")
                        await self.broadcast()

                        await self._receive_loop(ws)

            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.ws_exception_type = type(exc).__name__
                self.ws_exception_message = str(exc)
                self.last_error = str(exc)
                LOG.exception("Coincheck WebSocket session failed")
            finally:
                self.connected = False
                self.transport_connected = False
                self.ws_subscribed = False
                self.analyzer.set_ws(False, self.last_error)
                await self.broadcast()

            LOG.warning("Coincheck WebSocket disconnected; reconnecting in %ss", RECONNECT_DELAY)
            await asyncio.sleep(RECONNECT_DELAY)
