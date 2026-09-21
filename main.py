import asyncio
import os
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from fastapi import FastAPI, WebSocket
from fastapi.responses import HTMLResponse, JSONResponse

from analyzer import MarketAnalyzer, VERSION as ANALYZER_VERSION
from coincheck_stream import CoincheckStream, VERSION as STREAM_VERSION


VERSION = "5.4-renderfix14"
PAIR = os.getenv("PAIR", "shib_jpy")

analyzer = MarketAnalyzer()
stream = CoincheckStream(PAIR, analyzer)


@asynccontextmanager
async def lifespan(app: FastAPI):
    await stream.start()
    yield
    await stream.stop()


app = FastAPI(title="SHIB/JPY Monitor", version=VERSION, lifespan=lifespan)


def health_payload():
    book = analyzer.book.snapshot()
    ws = stream.status()
    age = book.get("snapshot_age_sec")

    # Price comes from the latest REST/WS trade; fallback to book midpoint.
    price = analyzer.last_price
    if price is None:
        price = book.get("mid")

    fresh = (
        book["ready"]
        and book["freshness"] not in ("INVALID", "UNKNOWN")
    )

    status = "ok" if fresh else "degraded"

    result = {
        "status": status,
        "service": "shib-monitor-api",
        "version": VERSION,
        "pair": PAIR,
        "book_ready": book["ready"],
        "fresh": fresh,
        "freshness": book["freshness"],
        "ws_connected": ws["ws_connected"],
        "ws_stale": ws["ws_stale"],
        "ws_age_sec": (
            round(
                __import__("time").time() - ws["last_ws_message_ts"], 3
            )
            if ws["last_ws_message_ts"] else None
        ),
        "snapshot_age_sec": age,
        "last_data_received_ts": book["last_data_received_ts"],
        "last_data_source": book["last_data_source"],
        "price": price,
        "best_bid": book["best_bid"],
        "best_ask": book["best_ask"],
        "bid_levels": book["bid_levels"],
        "ask_levels": book["ask_levels"],
        "total_levels": book["total_levels"],
        "sequence": book["sequence"],
        "source": analyzer.source,
        "last_error": ws["last_error"],
        "score": analyzer.snapshot().get("score"),
        "label": analyzer.snapshot().get("label"),
        "score_usable": analyzer.snapshot().get("score_usable"),
        **ws,
        "server_time": datetime.now(timezone.utc).isoformat(),
    }
    return result


@app.get("/")
async def root():
    return HTMLResponse("""
<!doctype html>
<html lang="ja">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>SHIB/JPY Monitor fix14</title>
<style>
body{font-family:system-ui,sans-serif;background:#111;color:#eee;margin:0;padding:16px}
.card{max-width:720px;margin:auto;background:#1c1c1c;border-radius:16px;padding:18px}
h1{font-size:20px;margin:0 0 14px}
pre{white-space:pre-wrap;word-break:break-word;background:#080808;padding:14px;border-radius:12px}
button{padding:10px 14px;border:0;border-radius:10px;cursor:pointer}
.ok{color:#7cff9b}.bad{color:#ff7777}
</style>
</head>
<body>
<div class="card">
<h1>SHIB/JPY Monitor — renderfix14</h1>
<div id="summary">読み込み中…</div>
<pre id="data">---</pre>
<button onclick="load()">更新</button>
</div>
<script>
async function load(){
  try{
    const r=await fetch('/api/health',{cache:'no-store'});
    const d=await r.json();
    document.getElementById('summary').innerHTML =
      `<b class="${d.status==='ok'?'ok':'bad'}">${d.status}</b>
      　${d.price ?? '-'}　板:${d.freshness}
      　WS:${d.ws_connected?'LIVE':'待機'}`;
    document.getElementById('data').textContent=JSON.stringify(d,null,2);
  }catch(e){
    document.getElementById('summary').textContent='通信エラー: '+e;
  }
}
load();
setInterval(load,2000);
</script>
</body>
</html>
""")


@app.get("/health")
async def health():
    return JSONResponse(health_payload())


@app.get("/api/health")
async def api_health():
    return JSONResponse(health_payload())


@app.get("/api/analysis")
async def api_analysis():
    return JSONResponse(analyzer.snapshot())


@app.get("/api/orderbook")
async def api_orderbook():
    return JSONResponse(analyzer.book.snapshot())


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    try:
        while True:
            await websocket.send_json(analyzer.snapshot())
            await asyncio.sleep(1)
    except Exception:
        pass
