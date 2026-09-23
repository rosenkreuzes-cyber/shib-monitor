# SHIB Monitor OrderFlow — renderfix23

SHIB/JPY (`shib_jpy`) monitor for Render/FastAPI.

## fix23 changes
- REST orderbook/ticker refresh runs independently every 5 seconds.
- Coincheck public WebSocket uses the documented `shib_jpy-orderbook` and `shib_jpy-trades` channels.
- WebSocket receive loop has explicit timeouts.
- Explicit WebSocket PING/PONG diagnostics are recorded.
- WebSocket upgrade status, close code/reason, reconnect count, raw preview and receive counters are exposed at `/api/health`.
- A silent WS connection is recycled after about 35 seconds while REST continues normally.
- No `frontend/`, `backend/`, or `app.py` is required.

## Render
Build command:
`pip install -r requirements.txt`

Start command:
`python -m uvicorn main:app --host 0.0.0.0 --port $PORT`

Health check:
`/api/health`
