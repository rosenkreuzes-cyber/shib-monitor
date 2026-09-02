import asyncio,logging
from contextlib import asynccontextmanager
from fastapi import FastAPI,WebSocket,WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from analyzer import MarketAnalyzer
from bitbank_stream import BitbankStream
logging.basicConfig(level=logging.INFO)
PAIR="shib_jpy"; analyzer=MarketAnalyzer(); clients=set()
async def broadcast():
    data=analyzer.snapshot(); dead=[]
    for ws in list(clients):
        try: await ws.send_json(data)
        except Exception: dead.append(ws)
    for ws in dead: clients.discard(ws)
async def stream_runner(): await BitbankStream(PAIR,analyzer,broadcast).run()
@asynccontextmanager
async def lifespan(app):
    task=asyncio.create_task(stream_runner()); yield; task.cancel()
app=FastAPI(title="SHIB Monitor OrderFlow v4",lifespan=lifespan)
app.add_middleware(CORSMiddleware,allow_origins=["*"],allow_credentials=True,allow_methods=["*"],allow_headers=["*"])
@app.get("/health")
async def health():
    s=analyzer.snapshot()
    return {"ok":True,"pair":PAIR,"book_ready":s["book"]["ready"],"price":s["price"],"sequence":s["book"]["sequence"]}
@app.get("/api/analysis")
async def analysis(): return analyzer.snapshot()
@app.websocket("/ws")
async def websocket(socket:WebSocket):
    await socket.accept(); clients.add(socket)
    try:
        await socket.send_json(analyzer.snapshot())
        while True: await socket.receive_text()
    except (WebSocketDisconnect,Exception): clients.discard(socket)
