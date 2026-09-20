import asyncio
import logging
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware

from analyzer import MarketAnalyzer
from coincheck_stream import CoincheckStream

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)

PAIR = "shib_jpy"
VERSION = "5.4-renderfix7"

STALE_SECONDS = 30

analyzer = MarketAnalyzer()
clients = set()
stream = None


def health_payload():
    s = analyzer.snapshot()
    b = s.get("book", {})

    freshness = b.get("freshness")
    ready = bool(b.get("ready"))
    ws_connected = bool(s.get("ws_connected"))

    # LIVE/CAUTION means the local order book has received recent data.
    fresh = freshness in ("LIVE", "CAUTION")
    ws_stale = (
        b.get("snapshot_age_sec") is None
        or b.get("snapshot_age_sec") > STALE_SECONDS
    )

    return {
        "status": "ok" if ready and fresh else "degraded",
        "service": "shib-monitor-api",
        "version": VERSION,
        "pair": PAIR,

        "book_ready": ready,
        "fresh": fresh,
        "freshness": freshness,
        "ws_connected": ws_connected,
        "ws_stale": ws_stale,

        "ws_age_sec": b.get("snapshot_age_sec"),
        "snapshot_age_sec": b.get("snapshot_age_sec"),
        "last_data_received_ts": b.get("last_data_received_ts"),
        "last_data_source": b.get("last_data_source"),

        "price": s.get("price"),
        "best_bid": b.get("best_bid"),
        "best_ask": b.get("best_ask"),
        "bid_levels": b.get("bid_levels"),
        "ask_levels": b.get("ask_levels"),
        "total_levels": b.get("total_levels"),
        "sequence": b.get("sequence"),

        "source": s.get("source"),
        "last_error": s.get("last_error"),

        # Diagnostics from CoincheckStream when available.
        "ws_messages": getattr(stream, "ws_messages", 0) if stream else 0,
        "ws_orderbook_messages": (
            getattr(stream, "ws_orderbook_messages", 0) if stream else 0
        ),
        "ws_trade_messages": (
            getattr(stream, "ws_trade_messages", 0) if stream else 0
        ),
        "last_ws_message_ts": (
            getattr(stream, "last_ws_message_ts", None) if stream else None
        ),
        "last_ws_orderbook_ts": (
            getattr(stream, "last_ws_orderbook_ts", None) if stream else None
        ),
        "ws_transport_connected": (
            getattr(stream, "transport_connected", False) if stream else False
        ),
        "ws_subscribed": (
            getattr(stream, "ws_subscribed", False) if stream else False
        ),
        "ws_subscribe_sent_ts": (
            getattr(stream, "ws_subscribe_sent_ts", None) if stream else None
        ),
        "ws_subscribe_ack_ts": (
            getattr(stream, "ws_subscribe_ack_ts", None) if stream else None
        ),
        "ws_subscribe_error_ts": (
            getattr(stream, "ws_subscribe_error_ts", None) if stream else None
        ),
        "ws_last_event": (
            getattr(stream, "ws_last_event", None) if stream else None
        ),
        "ws_last_channel": (
            getattr(stream, "ws_last_channel", None) if stream else None
        ),
        "ws_raw_preview_type": (
            getattr(stream, "ws_raw_preview_type", None) if stream else None
        ),
        "ws_raw_preview": (
            getattr(stream, "ws_raw_preview", None) if stream else None
        ),

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


HTML_PAGE = '<!doctype html>\n<html lang="ja"><head>\n<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover">\n<meta name="theme-color" content="#0b1220"><title>SHIB/JPY OrderFlow Monitor</title>\n<style>\n*{box-sizing:border-box}body{margin:0;background:#070b12;color:#eef2f7;font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif}\nheader{position:sticky;top:0;z-index:5;background:#0b1220ee;padding:12px 14px;border-bottom:1px solid #202938;backdrop-filter:blur(10px)}\nh1{font-size:19px;margin:0 0 3px}.sub,.muted{font-size:12px;color:#94a3b8}main{max-width:760px;margin:auto;padding:8px 10px 28px}\n.card{background:#0d1421;border:1px solid #202938;border-radius:14px;padding:13px;margin:9px 0}.row{display:flex;gap:9px}.row>*{flex:1}\n.price{font-size:30px;font-weight:800}.badge{display:inline-block;border-radius:999px;padding:5px 9px;font-size:12px;font-weight:700}\n.live{background:#123a2a;color:#6ee7a8}.caution{background:#3b3010;color:#fcd34d}.off{background:#3b1520;color:#fda4af}\n.verdict{text-align:center;padding:17px 10px}.label{font-size:27px;font-weight:900;margin:7px 0}.score{font-size:46px;font-weight:900}\n.bar{height:11px;background:#202938;border-radius:8px;overflow:hidden;margin:9px 0}.bar i{display:block;height:100%;transition:width .3s}\n.buybar{background:#35d07f}.split{display:grid;grid-template-columns:1fr 1fr;gap:8px}.metric{background:#111a29;border-radius:11px;padding:10px}.metric b{display:block;font-size:17px;margin-top:3px}\n.book{display:grid;grid-template-columns:1fr 1fr;gap:8px}h3{font-size:14px;margin:0 0 6px}table{width:100%;border-collapse:collapse;font-size:12px}\ntd{padding:5px 3px;border-bottom:1px solid #1d2635}td:last-child{text-align:right}.buy td:first-child{color:#6ee7a8}.sell td:first-child{color:#fda4af}\n.kv{display:grid;grid-template-columns:1fr 1fr;gap:8px;font-size:12px}button{width:100%;padding:11px;border:0;border-radius:10px;background:#1d293a;color:white;font-weight:700}\n@media(max-width:430px){main{padding:7px}.price{font-size:27px}.label{font-size:24px}}\n</style></head><body>\n<header><h1>SHIB/JPY OrderFlow</h1><div class="sub">売買板監視・買い/売り強弱判定</div></header>\n<main>\n<section class="card"><div class="row">\n<div><div class="muted">現在価格</div><div id="price" class="price">--</div></div>\n<div style="text-align:right"><div class="muted">状態</div><div id="state" class="badge off">取得中</div><div id="updated" class="muted" style="margin-top:7px">--</div></div>\n</div><div class="split" style="margin-top:10px">\n<div class="metric"><span class="muted">最良買い</span><b id="bid">--</b></div><div class="metric"><span class="muted">最良売り</span><b id="ask">--</b></div>\n</div></section>\n<section class="card verdict"><div class="muted">総合判定</div><div id="label" class="label">判定中</div><div id="score" class="score">--</div><div class="muted">買い強度スコア / 100</div><div class="bar"><i id="scorebar" class="buybar"></i></div></section>\n<section class="card"><div class="muted">板のバランス</div><div class="split" style="margin-top:8px">\n<div class="metric"><span class="muted">買い板</span><b id="buyPct">--%</b><div id="buyQty" class="muted">--</div></div>\n<div class="metric"><span class="muted">売り板</span><b id="sellPct">--%</b><div id="sellQty" class="muted">--</div></div>\n</div><div class="bar"><i id="imbbar" class="buybar"></i></div><div class="kv">\n<div><span class="muted">板インバランス</span><br><b id="imb">--</b></div><div><span class="muted">スプレッド</span><br><b id="spread">--</b></div>\n<div><span class="muted">買いレベル</span><br><b id="blv">--</b></div><div><span class="muted">売りレベル</span><br><b id="alv">--</b></div>\n</div></section>\n<section class="card"><div class="muted">売買板（上位10段）</div><div class="book" style="margin-top:8px">\n<div class="buy"><h3>買い板</h3><table><tbody id="bids"></tbody></table></div><div class="sell"><h3>売り板</h3><table><tbody id="asks"></tbody></table></div>\n</div></section>\n<section class="card"><details><summary>判定材料・通信状態</summary><div class="kv" style="margin-top:10px">\n<div><span class="muted">板のインバランス</span><br><b id="c1">--</b></div><div><span class="muted">板の厚み比率</span><br><b id="c2">--</b></div>\n<div><span class="muted">トレードフロー</span><br><b id="c3">--</b></div><div><span class="muted">価格モメンタム</span><br><b id="c4">--</b></div>\n<div><span class="muted">巨大注文</span><br><b id="c5">--</b></div><div><span class="muted">大口約定</span><br><b id="c6">--</b></div>\n<div><span class="muted">データ源</span><br><b id="source">--</b></div><div><span class="muted">WebSocket</span><br><b id="ws">--</b></div>\n</div></details></section>\n<section class="card"><button onclick="load()">今すぐ更新</button></section>\n</main>\n<script>\nconst $=id=>document.getElementById(id), fmt=(n,d=8)=>n==null?\'--\':Number(n).toFixed(d);\nconst qty=n=>n==null?\'--\':Number(n).toLocaleString(\'ja-JP\',{maximumFractionDigits:4});\nfunction rows(a){return (Array.isArray(a)?a:[]).slice(0,10).map(x=>`<tr><td>${fmt(x.price,8)}</td><td>${qty(x.amount)}</td></tr>`).join(\'\')||\'<tr><td colspan="2">--</td></tr>\'}\nfunction state(f,a){let e=$(\'state\');e.className=\'badge \'+(f===\'LIVE\'?\'live\':f===\'CAUTION\'?\'caution\':\'off\');e.textContent=f||\'UNKNOWN\';$(\'updated\').textContent=a==null?\'--\':`更新 ${Number(a).toFixed(1)}秒前`}\nasync function load(){try{let r=await fetch(\'/api/orderbook?ts=\'+Date.now(),{cache:\'no-store\'}),d=await r.json(),b=d.book||{},c=d.score_components||{};\n$(\'price\').textContent=fmt(d.price);$(\'bid\').textContent=fmt(b.best_bid);$(\'ask\').textContent=fmt(b.best_ask);$(\'label\').textContent=d.label||\'判定中\';$(\'score\').textContent=d.score==null?\'--\':Number(d.score).toFixed(1);\n$(\'scorebar\').style.width=Math.max(0,Math.min(100,Number(d.score)||0))+\'%\';let buy=Number(b.imbalance_pct??50),sell=100-buy;\n$(\'buyPct\').textContent=buy.toFixed(1)+\'%\';$(\'sellPct\').textContent=sell.toFixed(1)+\'%\';$(\'buyQty\').textContent=\'数量 \'+qty(b.bid_qty);$(\'sellQty\').textContent=\'数量 \'+qty(b.ask_qty);$(\'imbbar\').style.width=Math.max(0,Math.min(100,buy))+\'%\';\n$(\'imb\').textContent=(buy-50>=0?\'+\':\'\')+(buy-50).toFixed(1)+\'%\';$(\'spread\').textContent=b.spread_pct==null?\'--\':Number(b.spread_pct).toFixed(4)+\'%\';$(\'blv\').textContent=b.bid_levels??\'--\';$(\'alv\').textContent=b.ask_levels??\'--\';\n$(\'bids\').innerHTML=rows(b.bids_top10);$(\'asks\').innerHTML=rows(b.asks_top10);$(\'c1\').textContent=fmt(c[\'板のインバランス\'],2);$(\'c2\').textContent=fmt(c[\'板の厚み比率\'],2);$(\'c3\').textContent=fmt(c[\'トレードフロー\'],2);$(\'c4\').textContent=fmt(c[\'価格モメンタム\'],2);$(\'c5\').textContent=fmt(c[\'巨大注文\'],2);$(\'c6\').textContent=fmt(c[\'大口約定\'],2);$(\'source\').textContent=d.source||\'--\';$(\'ws\').textContent=d.ws_connected?\'接続中\':\'REST更新\';state(d.freshness,b.snapshot_age_sec)\n}catch(e){$(\'state\').className=\'badge off\';$(\'state\').textContent=\'通信エラー\'}}load();setInterval(load,2000);\n</script></body></html>'

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

    freshness = book.get("freshness")
    ready = bool(book.get("ready"))
    fresh = freshness in ("LIVE", "CAUTION")

    return {
        "ok": bool(ready and fresh),
        "pair": PAIR,
        "ready": ready,
        "fresh": fresh,
        "freshness": freshness,
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
            "stale_seconds": STALE_SECONDS,
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
