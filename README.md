# SHIB Monitor — renderfix13

Render deployment root contains only the files needed by the FastAPI service.

## Deploy
- Build: `pip install -r requirements.txt`
- Start: `python -m uvicorn main:app --host 0.0.0.0 --port $PORT`
- Health: `/api/health`

## WebSocket fix
- Explicit `ws.receive()` loop
- Both orderbook/trades subscriptions sent without blocking on ACK
- ACK/error diagnostics
- TEXT/BINARY/PING/PONG/CLOSE handling
- receive timeout + ping/pong recovery
- automatic reconnect
- REST snapshot fallback
