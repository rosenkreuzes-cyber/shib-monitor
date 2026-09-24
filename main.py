import asyncio
from contextlib import asynccontextmanager
from datetime import datetime,timezone
from fastapi import FastAPI,WebSocket,WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from analyzer import MarketAnalyzer
from okj_stream import OKJStream

VERSION="5.5-okj-tradefix2"; PAIR="SHIB-JPY"
a=MarketAnalyzer(); clients=set(); stream=None

async def broadcast():
    d=a.snapshot()
    for w in list(clients):
        try: await w.send_json(d)
        except Exception: clients.discard(w)

def health():
    s=a.snapshot(); b=s["book"]; age=b["snapshot_age_sec"]
    return {"status":"ok" if b["ready"] and b["freshness"] in ("LIVE","CAUTION") else "degraded",
    "service":"shib-monitor-api","version":VERSION,"pair":PAIR,"book_ready":b["ready"],
    "fresh":b["freshness"] in ("LIVE","CAUTION"),"freshness":b["freshness"],
    "ws_connected":s["ws_connected"],"ws_transport_connected":getattr(stream,"ws_transport_connected",False),"ws_business_connected":getattr(stream,"ws_business_connected",False),"ws_trade_subscribed":getattr(stream,"ws_trade_subscribed",False),
    "ws_stale":age is None or age>30,"ws_age_sec":age,"snapshot_age_sec":age,
    "last_data_received_ts":b["last_data_received_ts"],"last_data_source":b["last_data_source"],
    "price":s["price"],"best_bid":b["best_bid"],"best_ask":b["best_ask"],
    "bid_levels":b["bid_levels"],"ask_levels":b["ask_levels"],"total_levels":b["total_levels"],
    "sequence":b["sequence"],"source":s["source"],"last_error":s["last_error"],
    "ws_data_state":getattr(stream,"ws_data_state",None),"ws_subscribed":getattr(stream,"ws_subscribed",False),
    "ws_last_event":getattr(stream,"ws_last_event",None),"ws_messages":getattr(stream,"ws_messages",0),
    "ws_orderbook_messages":getattr(stream,"ws_orderbook_messages",0),"ws_trade_messages":getattr(stream,"ws_trade_messages",0),
    "ws_ticker_messages":getattr(stream,"ws_ticker_messages",0),"ws_error_messages":getattr(stream,"ws_error_messages",0),
    "ws_reconnects":getattr(stream,"ws_reconnects",0),"ws_trade_reconnects":getattr(stream,"ws_trade_reconnects",0),"last_ws_trade_ts":getattr(stream,"last_ws_trade_ts",None),
    "last_ws_ticker_ts":getattr(stream,"last_ws_ticker_ts",None),"last_seq_id":getattr(stream,"last_seq_id",None),
    "last_prev_seq_id":getattr(stream,"last_prev_seq_id",None),"last_checksum":getattr(stream,"last_checksum",None),
    "last_action":getattr(stream,"last_action",None),"ws_raw_preview":getattr(stream,"ws_raw_preview",None),
    "server_time":datetime.now(timezone.utc).isoformat()}

async def runner():
    global stream
    stream=OKJStream(PAIR,a,broadcast); await stream.run()

@asynccontextmanager
async def lifespan(app):
    task=asyncio.create_task(runner())
    try: yield
    finally:
        if stream: stream.stop()
        task.cancel()
        try: await task
        except asyncio.CancelledError: pass

app=FastAPI(title="SHIB Monitor OrderFlow",version=VERSION,lifespan=lifespan)
app.add_middleware(CORSMiddleware,allow_origins=["*"],allow_credentials=True,allow_methods=["*"],allow_headers=["*"])

@app.get("/")
async def root(): return {"app":"SHIB Monitor OrderFlow","version":VERSION,"pair":PAIR,"status":"running"}
@app.get("/health")
async def health_endpoint(): return health()
@app.get("/api/health")
async def api_health(): return health()
@app.get("/api/analysis")
async def analysis(): return a.snapshot()
@app.get("/api/orderbook")
async def orderbook(): return a.snapshot()["book"]
@app.websocket("/ws")
async def ws_endpoint(ws:WebSocket):
    await ws.accept(); clients.add(ws)
    try:
        await ws.send_json(a.snapshot())
        while True: await ws.receive_text()
    except (WebSocketDisconnect,Exception): pass
    finally: clients.discard(ws)
