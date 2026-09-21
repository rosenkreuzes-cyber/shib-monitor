import asyncio
import json
import logging
import time
from typing import Callable, Optional

import aiohttp
import websockets


LOG = logging.getLogger("coincheck")
REST_BASE = "https://coincheck.com/api"
WS_URL = "wss://ws-api.coincheck.com/"
VERSION = "5.4-renderfix14"


class CoincheckStream:
    """
    Coincheck public market stream.

    fix14:
    - REST polling is independent from WebSocket.
    - Subscribe to BOTH orderbook and trades.
    - Never wait for a subscription ACK; Coincheck's documented flow does
      not require an ACK message before market data starts arriving.
    - Enter recv() immediately after subscriptions are sent.
    - websockets handles protocol ping/pong automatically.
    - Reconnect on timeout/close/error.
    """

    def __init__(self, pair: str, analyzer, broadcast: Optional[Callable] = None):
        self.pair = pair
        self.analyzer = analyzer
        self.broadcast = broadcast

        self.ws_transport_connected = False
        self.ws_subscribed = False
        self.ws_stale = True
        self.ws_messages = 0
        self.ws_orderbook_messages = 0
        self.ws_trade_messages = 0
        self.last_ws_message_ts = None
        self.last_ws_orderbook_ts = None
        self.last_ws_trade_ts = None
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
        self.last_error = None

        self._stop = asyncio.Event()
        self._tasks = []

    def status(self):
        return {
            "ws_connected": bool(self.ws_transport_connected and not self.ws_stale),
            "ws_stale": self.ws_stale,
            "ws_transport_connected": self.ws_transport_connected,
            "ws_subscribed": self.ws_subscribed,
            "ws_subscribe_sent_ts": self.ws_subscribe_sent_ts,
            "ws_subscribe_ack_ts": self.ws_subscribe_ack_ts,
            "ws_subscribe_error_ts": self.ws_subscribe_error_ts,
            "ws_last_event": self.ws_last_event,
            "ws_last_channel": self.ws_last_channel,
            "ws_raw_preview_type": self.ws_raw_preview_type,
            "ws_raw_preview": self.ws_raw_preview,
            "ws_close_code": self.ws_close_code,
            "ws_close_reason": self.ws_close_reason,
            "ws_exception_type": self.ws_exception_type,
            "ws_exception_message": self.ws_exception_message,
            "ws_receive_timeout_count": self.ws_receive_timeout_count,
            "ws_ping_count": self.ws_ping_count,
            "ws_pong_count": self.ws_pong_count,
            "ws_messages": self.ws_messages,
            "ws_orderbook_messages": self.ws_orderbook_messages,
            "ws_trade_messages": self.ws_trade_messages,
            "last_ws_message_ts": self.last_ws_message_ts,
            "last_ws_orderbook_ts": self.last_ws_orderbook_ts,
            "last_ws_trade_ts": self.last_ws_trade_ts,
            "last_error": self.last_error,
        }

    async def start(self):
        self._stop.clear()
        self._tasks = [
            asyncio.create_task(self._rest_loop(), name="coincheck-rest"),
            asyncio.create_task(self._ws_loop(), name="coincheck-ws"),
        ]

    async def stop(self):
        self._stop.set()
        for task in self._tasks:
            task.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks = []

    async def _rest_loop(self):
        timeout = aiohttp.ClientTimeout(total=8)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            while not self._stop.is_set():
                started = time.time()
                try:
                    await self._rest_refresh(session)
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    self.last_error = f"REST {type(exc).__name__}: {exc}"
                    self.analyzer.set_ws(
                        self.analyzer.ws_connected,
                        self.last_error,
                    )
                    LOG.warning("REST refresh failed: %s", self.last_error)

                elapsed = time.time() - started
                await asyncio.sleep(max(0.5, 2.0 - elapsed))

    async def _rest_refresh(self, session):
        # Order book is the authoritative fallback for score usability.
        async with session.get(
            f"{REST_BASE}/order_books",
            params={"pair": self.pair},
        ) as resp:
            resp.raise_for_status()
            payload = await resp.json(content_type=None)

        if not payload.get("success", True):
            raise RuntimeError(f"order_books response unsuccessful: {payload}")

        data = payload.get("data") or {}
        if data.get("bids") and data.get("asks"):
            self.analyzer.load_depth(data)

        # Public trades gives us a current price even when WS is unavailable.
        async with session.get(
            f"{REST_BASE}/trades",
            params={"pair": self.pair, "limit": 1},
        ) as resp:
            resp.raise_for_status()
            payload = await resp.json(content_type=None)

        rows = payload.get("data") or []
        if rows:
            row = rows[0]
            # Coincheck REST: id, amount, rate, pair, order_type, created_at
            if isinstance(row, dict):
                trade = {
                    "id": row.get("id"),
                    "amount": row.get("amount"),
                    "price": row.get("rate"),
                    "side": row.get("order_type"),
                    "created_at": row.get("created_at"),
                }
                self.analyzer.ticker({"last": row.get("rate")})
                self.analyzer.trade(trade)

        self.analyzer.set_source("coincheck_rest")

    async def _ws_loop(self):
        backoff = 1.0

        while not self._stop.is_set():
            try:
                self.ws_last_event = "connecting"
                self.ws_exception_type = None
                self.ws_exception_message = None

                async with websockets.connect(
                    WS_URL,
                    ping_interval=20,
                    ping_timeout=10,
                    close_timeout=5,
                    open_timeout=10,
                    max_size=2 * 1024 * 1024,
                ) as ws:
                    self.ws_transport_connected = True
                    self.ws_close_code = None
                    self.ws_close_reason = None
                    self.ws_stale = True
                    self.ws_last_event = "transport_connected"
                    self.last_error = None

                    channels = (
                        f"{self.pair}-orderbook",
                        f"{self.pair}-trades",
                    )

                    for channel in channels:
                        message = json.dumps(
                            {"type": "subscribe", "channel": channel},
                            separators=(",", ":"),
                        )
                        await ws.send(message)
                        self.ws_subscribe_sent_ts = time.time()
                        self.ws_last_event = f"subscribe_sent:{channel}"
                        # There is no ACK wait here by design.
                        await asyncio.sleep(0.05)

                    self.ws_subscribed = True
                    self.ws_last_event = "subscribed:orderbook,trades"

                    # Critical fix: receive starts immediately.
                    while not self._stop.is_set():
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=15)
                        except asyncio.TimeoutError:
                            self.ws_receive_timeout_count += 1
                            self.ws_last_event = "receive_timeout"
                            self.ws_stale = True
                            self.last_error = "WS receive timeout; reconnecting"
                            break

                        if raw is None:
                            self.ws_last_event = "recv_none"
                            break

                        self._handle_message(raw)

                        if self.last_ws_message_ts:
                            self.ws_stale = (
                                time.time() - self.last_ws_message_ts > 15
                            )

                    try:
                        await ws.close()
                    except Exception:
                        pass

            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.ws_exception_type = type(exc).__name__
                self.ws_exception_message = str(exc)
                self.last_error = f"WS {type(exc).__name__}: {exc}"
                self.ws_last_event = "ws_exception"
                LOG.warning("WS error: %s", self.last_error)
            finally:
                self.ws_transport_connected = False
                self.ws_subscribed = False
                self.ws_stale = True
                self.analyzer.set_ws(False, self.last_error)

            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 10.0)

    def _handle_message(self, raw):
        now = time.time()
        self.ws_messages += 1
        self.last_ws_message_ts = now
        self.ws_raw_preview_type = type(raw).__name__

        if isinstance(raw, bytes):
            raw = raw.decode("utf-8", errors="replace")

        preview = str(raw)
        self.ws_raw_preview = preview[:500]

        try:
            message = json.loads(raw)
        except Exception:
            self.ws_last_event = "non_json_message"
            return

        # Coincheck public WS sends:
        # orderbook -> ["shib_jpy", {"bids": [...], "asks": [...], ...}]
        # trades    -> ["timestamp", "id", "pair", "rate", "amount", "side", ...]
        if isinstance(message, list) and len(message) >= 2:
            if (
                isinstance(message[0], str)
                and message[0] == self.pair
                and isinstance(message[1], dict)
            ):
                body = message[1]
                if "bids" in body or "asks" in body:
                    self.ws_orderbook_messages += 1
                    self.last_ws_orderbook_ts = now
                    self.ws_last_channel = f"{self.pair}-orderbook"
                    self.ws_last_event = "orderbook_received"
                    self.analyzer.diff_depth(body)
                    self.analyzer.set_ws(True, None)
                    self.ws_stale = False
                    return

            # Trade array format from official docs:
            # [timestamp, id, pair, rate, amount, side, ...]
            if len(message) >= 6 and message[2] == self.pair:
                self.ws_trade_messages += 1
                self.last_ws_trade_ts = now
                self.ws_last_channel = f"{self.pair}-trades"
                self.ws_last_event = "trade_received"

                trade = {
                    "executed_at": message[0],
                    "id": message[1],
                    "pair": message[2],
                    "price": message[3],
                    "amount": message[4],
                    "side": message[5],
                }
                self.analyzer.trade(trade)
                self.analyzer.set_ws(True, None)
                self.ws_stale = False
                return

        # Some websocket gateways can return JSON control/error objects.
        if isinstance(message, dict):
            mtype = message.get("type")
            if mtype in ("error", "unsubscribe_error", "subscribe_error"):
                self.ws_subscribe_error_ts = now
                self.ws_last_event = "subscribe_error"
                self.last_error = str(message)[:500]
                return

            if mtype in ("subscribed", "subscribe"):
                self.ws_subscribe_ack_ts = now
                self.ws_last_event = "subscribe_message"
                return

        self.ws_last_event = "unknown_message"
