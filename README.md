# SHIB Monitor API — RenderFix22

SHIB/JPY monitor using Coincheck REST as the primary source. Fix22 is a diagnostic build for isolating the Coincheck WebSocket failure seen in RenderFix21.

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

## Fix22 diagnostic

For each WebSocket channel, Fix22 deliberately performs two phases:

1. **Phase A — connect only:** complete the WebSocket upgrade, send nothing, and wait 12 seconds while handling ping/pong.
2. **Phase B — subscribe:** if Phase A survives, send the normal Coincheck subscribe payload and continue receiving.

This distinguishes:
- `CLOSED_BEFORE_SUBSCRIBE` → the socket is being closed even without a subscribe request.
- `SURVIVED_NO_SUBSCRIBE` followed by closure after subscribe → the subscribe/channel path is implicated.
- market-data frames after subscribe → the Coincheck WebSocket path is working and the earlier failure was intermittent.

REST remains unchanged and continues every 5 seconds, so the app remains usable while the WebSocket is diagnosed.

## Why Coincheck?

Coincheck currently documents public WebSocket support for `shib_jpy`, including orderbook and trades, with no authentication required. The app therefore does not need to change exchanges merely to monitor SHIB/JPY. However, another exchange can be used if its SHIB/JPY market and API behavior are more suitable.
