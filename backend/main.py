import asyncio
import logging
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware

from analyzer import MarketAnalyzer
from coincheck_stream import CoincheckStream


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)

PAIR = "shib_jpy"
VERSION = "5.4-renderfix1"

analyzer = MarketAnalyzer()
clients = set()
stream = None


def health_payload():
    s = analyzer.snapshot()
    b = s.get("book", {})
    freshness = b.get("freshness")
    ready = bool(b.get("ready"))
    fresh = freshness in ("LIVE", "CAUTION")
    ws_connected = bool(s.get("ws_connected"))
    transport = bool(stream and getattr(stream, "connected", False))
    price = s.get("price")
    bid = b.get("best_bid")
    ask = b.get("best_ask")
    price_in_book = bool(price is None or (bid is not None and ask is not None and bid <= price <= ask))
    depth_sufficient = bool((b.get("bid_levels") or 0) >= 2 and (b.get("ask_levels") or 0) >= 2)
    spread_ok = bool(bid is not None and ask is not None and bid <= ask)
    integrity_ok = bool(ready and fresh and spread_ok and price_in_book and depth_sufficient)
    if not ready:
        reason = "orderbook not ready"
    elif not spread_ok:
        reason = "invalid spread"
    elif not price_in_book:
        reason = "ticker last outside orderbook range"
    elif not depth_sufficient:
        reason = "insufficient orderbook depth (need 2+ levels per side)"
    else:
        reason = "ok"
    return {
        "status": "ok" if ready and fresh else "degraded",
        "service": "shib-monitor-api",
        "version": VERSION,
        "pair": PAIR,
        "book_ready": ready,
        "fresh": fresh,
        "freshness": freshness,
        "ws_connected": ws_connected,
        "ws_transport_connected": transport,
        "ws_stale": b.get("snapshot_age_sec") is None or b.get("snapshot_age_sec") > STALE_SECONDS,
        "ws_age_sec": b.get("snapshot_age_sec"),
        "snapshot_age_sec": b.get("snapshot_age_sec"),
        "last_data_received_ts": b.get("last_data_received_ts"),
        "last_data_source": b.get("last_data_source"),
        "price": price,
        "best_bid": bid,
        "best_ask": ask,
        "bid_levels": b.get("bid_levels"),
        "ask_levels": b.get("ask_levels"),
        "total_levels": b.get("total_levels"),
        "sequence": b.get("sequence"),
        "source": s.get("source"),
        "last_error": s.get("last_error"),
        "price_in_book": price_in_book,
        "ticker_book_consistent": price_in_book,
        "depth_sufficient": depth_sufficient,
        "integrity_ok": integrity_ok,
        "integrity_reason": reason,
        "score_usable": bool(s.get("score_usable")) and integrity_ok,
        "score": s.get("score") if integrity_ok else None,
        "label": s.get("label") if integrity_ok else "判定停止",
        "ws_messages": getattr(stream, "ws_messages", 0) if stream else 0,
        "ws_orderbook_messages": getattr(stream, "ws_orderbook_messages", 0) if stream else 0,
        "ws_trade_messages": getattr(stream, "ws_trade_messages", 0) if stream else 0,
        "last_ws_message_ts": getattr(stream, "last_ws_message_ts", None) if stream else None,
        "last_ws_orderbook_ts": getattr(stream, "last_ws_orderbook_ts", None) if stream else None,
        "server_time": datetime.now(timezone.utc).isoformat(),
    }


async def broadcast():
    data = analyzer.snapshot()
    dead = []

    for ws in list(clients):
        try:
            await ws.send_json(data)
        except Exception:
            dead.append(ws)

    for ws in dead:
        clients.discard(ws)


async def stream_runner():
    global stream

    stream = CoincheckStream(
        PAIR,
        analyzer,
        broadcast,
    )

    await stream.run()


@asynccontextmanager
async def lifespan(app):
    task = asyncio.create_task(stream_runner())

    try:
        yield
    finally:
        task.cancel()

        try:
            await task
        except asyncio.CancelledError:
            pass


app = FastAPI(
    title="SHIB Monitor OrderFlow",
    version=VERSION,
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
async def root():
    return {
        "app": "SHIB Monitor OrderFlow",
        "version": VERSION,
        "pair": PAIR,
        "status": "running",
        "api": [
            "/health",
            "/api/health",
            "/api/orderbook",
            "/api/analysis",
            "/ws",
        ],
    }


@app.get("/health")
async def health():
    return health_payload()


@app.get("/api/health")
async def api_health():
    return health_payload()


@app.get("/api/analysis")
async def analysis():
    return analyzer.snapshot()


@app.get("/api/orderbook")
async def orderbook():
    data = analyzer.snapshot()
    book = data.get("book", {})

    freshness = book.get("freshness")
    ready = bool(book.get("ready"))
    fresh = freshness in ("LIVE", "CAUTION")

    return {
        "ok": bool(ready and fresh),
        "pair": PAIR,
        "ready": ready,
        "fresh": fresh,
        "age_seconds": book.get("snapshot_age_sec"),
        "server_time": datetime.now(timezone.utc).isoformat(),

        "ticker": {
            "last": data.get("price"),
        },

        "book": book,

        "price": data.get("price"),
        "price_change_pct": data.get("price_change_pct"),
        "price_change_5m_pct": data.get("price_change_5m_pct"),

        "score": data.get("score"),
        "label": data.get("label"),
        "score_usable": data.get("score_usable"),

        "trade_flow": data.get("trade_flow"),
        "large_trade_flow": data.get("large_trade_flow"),

        "absorption": data.get("absorption"),

        "score_components": data.get("score_components"),

        "source": data.get("source"),
        "ws_connected": data.get("ws_connected"),
        "error": data.get("last_error"),

        "rules": {
            "stale_seconds": 30,
            "decision_allowed": bool(ready and fresh),
        },
    }


@app.websocket("/ws")
async def websocket(socket: WebSocket):
    await socket.accept()
    clients.add(socket)

    try:
        await socket.send_json(analyzer.snapshot())

        while True:
            await socket.receive_text()

    except WebSocketDisconnect:
        pass

    except Exception:
        pass

    finally:
        clients.discard(socket)
