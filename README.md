# SHIB Monitor API — RenderFix18

SHIB/JPY monitor using Coincheck REST as the primary data source and two independent WebSocket connections for realtime overlays.

## Root files

- `main.py`
- `coincheck_stream.py`
- `analyzer.py`
- `orderbook.py`
- `requirements.txt`
- `render.yaml`
- `README.md`

No `frontend/`, `backend/`, `app.py`, or cache folders are required.

## Render

Build command:

```text
pip install -r requirements.txt
```

Start command:

```text
python -m uvicorn main:app --host 0.0.0.0 --port $PORT
```

Health check:

```text
/api/health
```

## Fix18 changes

- REST polling remains the primary source and runs every 5 seconds.
- `shib_jpy-orderbook` and `shib_jpy-trades` now use separate WebSocket connections.
- Each WS reconnects independently with exponential backoff.
- A channel with no market-data message for 25 seconds is proactively closed and reconnected.
- Subscription ACK is diagnostic only; lack of an ACK is not treated as proof of failure.
- Health output exposes channel-specific connection, activity, error, and reconnect counters.
- REST health/decision status remains independent of WebSocket status.


Fix21 adds channel-specific WebSocket upgrade status/headers, exact subscribe payload diagnostics, subscribe send timing, and staggers trades by 2 seconds after orderbook connection for clearer isolation. REST remains primary.
