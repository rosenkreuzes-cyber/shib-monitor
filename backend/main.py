import asyncio
import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from analyzer import MarketAnalyzer
from coincheck_stream import CoincheckStream

logging.basicConfig(level=logging.INFO)
PAIR = "shib_jpy"
analyzer = MarketAnalyzer(); clients = set(); stream = None

async def broadcast():
    data = analyzer.snapshot(); dead = []
    for ws in list(clients):
        try: await ws.send_json(data)
        except Exception: dead.append(ws)
    for ws in dead: clients.discard(ws)

async def stream_runner():
    global stream
    stream = CoincheckStream(PAIR, analyzer, broadcast)
    await stream.run()

@asynccontextmanager
async def lifespan(app):
    task = asyncio.create_task(stream_runner())
    yield
    task.cancel()
    try: await task
    except asyncio.CancelledError: pass

app = FastAPI(title="SHIB Monitor OrderFlow v5.0", version="5.0.0", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True,
                   allow_methods=["*"], allow_headers=["*"])

@app.get("/")
async def root():
    return {"app": "SHIB Monitor OrderFlow", "version": "v5.0", "pair": PAIR}

@app.get("/health")
async def health():
    s = analyzer.snapshot()
    return {"ok": True, "version": "v5.0", "pair": PAIR, "source": s["source"],
            "book_ready": s["book"]["ready"], "price": s["price"],
            "bid_levels": s["book"]["bid_levels"], "ask_levels": s["book"]["ask_levels"],
            "total_levels": s["book"]["total_levels"], "sequence": s["book"]["sequence"],
            "ws_connected": s["ws_connected"], "last_error": s["last_error"]}

@app.get("/api/analysis")
async def analysis(): return analyzer.snapshot()

@app.websocket("/ws")
async def websocket(socket: WebSocket):
    await socket.accept(); clients.add(socket)
    try:
        await socket.send_json(analyzer.snapshot())
        while True: await socket.receive_text()
    except (WebSocketDisconnect, Exception):
        clients.discard(socket)
