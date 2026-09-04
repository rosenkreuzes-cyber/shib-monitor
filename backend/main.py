import asyncio,logging
from contextlib import asynccontextmanager
from fastapi import FastAPI,WebSocket,WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from analyzer import MarketAnalyzer
from coincheck_stream import CoincheckStream
logging.basicConfig(level=logging.INFO)
PAIR='shib_jpy';analyzer=MarketAnalyzer();clients=set();stream=None
async def broadcast():
    data=analyzer.snapshot();dead=[]
    for ws in list(clients):
        try:await ws.send_json(data)
        except Exception:dead.append(ws)
    for ws in dead:clients.discard(ws)
async def stream_runner():
    global stream;stream=CoincheckStream(PAIR,analyzer,broadcast);await stream.run()
@asynccontextmanager
async def lifespan(app):
    task=asyncio.create_task(stream_runner());yield;task.cancel()
    try:await task
    except asyncio.CancelledError:pass
app=FastAPI(title='SHIB Monitor OrderFlow v5.1',version='5.1.0',lifespan=lifespan)
app.add_middleware(CORSMiddleware,allow_origins=['*'],allow_credentials=True,allow_methods=['*'],allow_headers=['*'])
@app.get('/')
async def root():return {'app':'SHIB Monitor OrderFlow','version':'v5.1','pair':PAIR,'api':['/health','/api/analysis','/ws']}
@app.get('/health')
async def health():
    s=analyzer.snapshot();b=s['book'];return {'ok':True,'version':'v5.1','pair':PAIR,'source':s['source'],'book_ready':b['ready'],'freshness':b['freshness'],'snapshot_age_sec':b['snapshot_age_sec'],'last_data_received_ts':b['last_data_received_ts'],'price':s['price'],'best_bid':b['best_bid'],'best_ask':b['best_ask'],'bid_levels':b['bid_levels'],'ask_levels':b['ask_levels'],'total_levels':b['total_levels'],'sequence':b['sequence'],'ws_connected':s['ws_connected'],'last_error':s['last_error']}
@app.get('/api/analysis')
async def analysis():return analyzer.snapshot()
@app.websocket('/ws')
async def websocket(socket:WebSocket):
    await socket.accept();clients.add(socket)
    try:
        await socket.send_json(analyzer.snapshot())
        while True:await socket.receive_text()
    except (WebSocketDisconnect,Exception):clients.discard(socket)
