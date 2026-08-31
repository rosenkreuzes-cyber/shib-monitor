import asyncio
from contextlib import asynccontextmanager
from fastapi import FastAPI,WebSocket,WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from analyzer import MarketAnalyzer
from bitbank_stream import BitbankStream

PAIR="shib_jpy"; analyzer=MarketAnalyzer(); clients=set()

async def broadcast():
    d=analyzer.snapshot()
    for ws in list(clients):
        try: await ws.send_json(d)
        except Exception: clients.discard(ws)

async def handle(room,msg):
    d=(msg or {}).get("data",{}); room=room or ""
    if room.startswith("depth_diff_"): analyzer.diff(d); await broadcast()
    elif room.startswith("depth_whole_"): analyzer.whole(d); await broadcast()
    elif room.startswith("transactions_"):
        for t in d.get("transactions",[]): analyzer.trade(t.get("side",""),float(t.get("price",0)),float(t.get("amount",0)),t.get("executed_at"))
        await broadcast()
    elif room.startswith("ticker_") and d.get("last"):
        analyzer.prev_price=analyzer.last_price; analyzer.last_price=float(d["last"]); await broadcast()

async def runner():
    while True:
        try: await BitbankStream(PAIR,handle).run()
        except Exception as e: print("reconnect",repr(e)); await asyncio.sleep(5)

@asynccontextmanager
async def lifespan(app):
    task=asyncio.create_task(runner()); yield; task.cancel()

app=FastAPI(title="SHIB Monitor OrderFlow v3",lifespan=lifespan)
app.add_middleware(CORSMiddleware,allow_origins=["*"],allow_credentials=True,allow_methods=["*"],allow_headers=["*"])

@app.get("/health")
async def health(): return JSONResponse({"ok":True})
@app.get("/api/analysis")
async def analysis(): return analyzer.snapshot()
@app.websocket("/ws")
async def ws(socket:WebSocket):
    await socket.accept(); clients.add(socket)
    try:
        await socket.send_json(analyzer.snapshot())
        while True: await socket.receive_text()
    except (WebSocketDisconnect,Exception): clients.discard(socket)
