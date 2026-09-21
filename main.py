import asyncio
import logging
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse

from analyzer import MarketAnalyzer
from coincheck_stream import CoincheckStream

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

PAIR = "shib_jpy"
VERSION = "5.4-renderfix13"
STALE_SECONDS = 30.0

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
        "status": "ok" if ready and fresh else "degraded",
        "service": "shib-monitor-api",
        "version": VERSION,
        "pair": PAIR,
        "book_ready": ready,
        "fresh": fresh,
        "freshness": freshness,
        "ws_connected": bool(getattr(stream, "connected", False)) if stream else False,
        "ws_stale": bool(getattr(stream, "ws_stale", True)) if stream else True,
        "ws_age_sec": b.get("snapshot_age_sec") if getattr(stream, "ws_orderbook_messages", 0) else None,
        "snapshot_age_sec": b.get("snapshot_age_sec"),
        "last_data_received_ts": b.get("last_data_received_ts"),
        "last_data_source": b.get("last_data_source"),
        "price": s.get("price"),
        "best_bid": b.get("best_bid"),
        "best_ask": b.get("best_ask"),
        "bid_levels": b.get("bid_levels", 0),
        "ask_levels": b.get("ask_levels", 0),
        "total_levels": b.get("total_levels", 0),
        "sequence": b.get("sequence"),
        "source": s.get("source"),
        "last_error": getattr(stream, "last_error", None) or s.get("last_error"),
        "score": s.get("score"),
        "label": s.get("label"),
        "score_usable": s.get("score_usable"),
        "ws_messages": getattr(stream, "ws_messages", 0) if stream else 0,
        "ws_orderbook_messages": getattr(stream, "ws_orderbook_messages", 0) if stream else 0,
        "ws_trade_messages": getattr(stream, "ws_trade_messages", 0) if stream else 0,
        "last_ws_message_ts": getattr(stream, "last_ws_message_ts", None) if stream else None,
        "last_ws_orderbook_ts": getattr(stream, "last_ws_orderbook_ts", None) if stream else None,
        "ws_transport_connected": getattr(stream, "transport_connected", False) if stream else False,
        "ws_subscribed": getattr(stream, "ws_subscribed", False) if stream else False,
        "ws_subscribe_sent_ts": getattr(stream, "ws_subscribe_sent_ts", None) if stream else None,
        "ws_subscribe_ack_ts": getattr(stream, "ws_subscribe_ack_ts", None) if stream else None,
        "ws_subscribe_error_ts": getattr(stream, "ws_subscribe_error_ts", None) if stream else None,
        "ws_last_event": getattr(stream, "ws_last_event", None) if stream else None,
        "ws_last_channel": getattr(stream, "ws_last_channel", None) if stream else None,
        "ws_raw_preview_type": getattr(stream, "ws_raw_preview_type", None) if stream else None,
        "ws_raw_preview": getattr(stream, "ws_raw_preview", None) if stream else None,
        "ws_close_code": getattr(stream, "ws_close_code", None) if stream else None,
        "ws_close_reason": getattr(stream, "ws_close_reason", None) if stream else None,
        "ws_exception_type": getattr(stream, "ws_exception_type", None) if stream else None,
        "ws_exception_message": getattr(stream, "ws_exception_message", None) if stream else None,
        "ws_receive_timeout_count": getattr(stream, "ws_receive_timeout_count", 0) if stream else 0,
        "ws_ping_count": getattr(stream, "ws_ping_count", 0) if stream else 0,
        "ws_pong_count": getattr(stream, "ws_pong_count", 0) if stream else 0,
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
    stream = CoincheckStream(PAIR, analyzer, broadcast)
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


HTML_PAGE = """<!doctype html><html lang='ja'><head><meta charset='utf-8'><meta name='viewport' content='width=device-width,initial-scale=1'><title>SHIB/JPY OrderFlow</title><style>body{margin:0;background:#070b12;color:#eef2f7;font-family:system-ui,sans-serif}main{max-width:760px;margin:auto;padding:18px}.card{background:#0d1421;border:1px solid #202938;border-radius:14px;padding:14px;margin:10px 0}.price{font-size:30px;font-weight:800}.muted{color:#94a3b8;font-size:12px}.ok{color:#6ee7a8}.warn{color:#fcd34d}.bad{color:#fda4af}button{padding:10px 14px;border:0;border-radius:9px}</style></head><body><main><h2>SHIB/JPY OrderFlow</h2><div class='card'><div id='state'>接続中…</div><div id='price' class='price'>--</div><div id='detail' class='muted'></div></div><button onclick='load()'>更新</button></main><script>const API=location.origin;let ws;function render(d){document.getElementById('price').textContent=d.price==null?'--':Number(d.price).toFixed(6)+' JPY';document.getElementById('state').textContent=(d.ws_connected?'WS CONNECTED':'REST FALLBACK')+' / '+(d.book?.freshness||'UNKNOWN');document.getElementById('detail').textContent='bid '+(d.best_bid??'--')+' / ask '+(d.best_ask??'--')+' / WS messages '+(d.ws_messages??0); }async function load(){try{render(await (await fetch(API+'/api/analysis')).json())}catch(e){}}function connect(){ws=new WebSocket(API.replace(/^http/,'ws')+'/ws');ws.onmessage=e=>{try{render(JSON.parse(e.data))}catch(_){}};ws.onclose=()=>setTimeout(connect,3000);ws.onerror=()=>ws.close()}connect();load();</script></body></html>"""

app = FastAPI(title="SHIB Monitor OrderFlow", version=VERSION, lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])


@app.get("/", response_class=HTMLResponse)
async def root():
    return HTML_PAGE


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
    fresh = book.get("freshness") in ("LIVE", "CAUTION")
    return {**data, "ok": bool(book.get("ready") and fresh), "age_seconds": book.get("snapshot_age_sec"), "rules": {"stale_seconds": STALE_SECONDS, "decision_allowed": bool(book.get("ready") and fresh)}}


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
