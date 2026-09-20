# SHIB Monitor API v5.4 RenderFix2

Coincheck `shib_jpy` orderbook monitor for Render.

## GitHub / Render

Repository root should contain:

- `main.py` — FastAPI API and health endpoint
- `coincheck_stream.py` — Coincheck WebSocket receive/reconnect loop (RenderFix2)
- `orderbook.py` — orderbook state, freshness and depth calculations
- `analyzer.py` — flow/score analysis
- `requirements.txt` — Python dependencies
- `render.yaml` — Render service definition
- `index.html` — browser monitor UI source
- `frontend/` — optional mobile-display reference files

Do **not** commit `__pycache__/` or `*.pyc`.

## Render

Build command:

```text
pip install -r requirements.txt
```

Start command:

```text
uvicorn main:app --host 0.0.0.0 --port $PORT
```

## Health check

```text
/api/health
```

Useful fields:

- `book_ready`
- `fresh`
- `freshness`
- `ws_transport_connected`
- `ws_connected`
- `ws_messages`
- `ws_orderbook_messages`
- `last_ws_orderbook_ts`
- `best_bid`
- `best_ask`

A healthy live stream should show `book_ready=true`, `fresh=true`, `freshness=LIVE`, and increasing WebSocket counters.

## RenderFix2 receive-loop changes

`coincheck_stream.py` uses explicit `ws.receive()` handling instead of relying on async iteration. It handles TEXT/BINARY/PING/PONG/CLOSE/ERROR frames, counts application frames immediately, accepts the observed Coincheck orderbook array format, and reconnects after repeated receive timeouts.
