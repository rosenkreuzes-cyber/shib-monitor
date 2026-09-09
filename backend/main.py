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
VERSION = "5.3"

analyzer = MarketAnalyzer()
clients = set()
stream = None


def health_payload():
    s = analyzer.snapshot()
    b = s.get("book", {})

    freshness = b.get("freshness")
    ready = bool(b.get("ready"))
    fresh = freshness in ("LIVE", "CAUTION")

    return {
        "ok": bool(ready and fresh),
        "version": VERSION,
        "pair": PAIR,
        "source": s.get("source"),
        "book_ready": ready,
        "fresh": fresh,
        "freshness": freshness,
        "snapshot_age_sec": b.get("snapshot_age_sec"),
        "last_data_received_ts": b.get("last_data_received_ts"),
        "price": s.get("price"),
        "best_bid": b.get("best_bid"),
        "best_ask": b.get("best_ask"),
        "bid_levels": b.get("bid_levels"),
        "ask_levels": b.get("ask_levels"),
        "total_levels": b.get("total_levels"),
        "sequence": b.get("sequence"),
        "ws_connected": s.get("ws_connected"),
        "last_error": s.get("last_error"),
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
