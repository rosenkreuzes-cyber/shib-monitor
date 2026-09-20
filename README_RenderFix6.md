# SHIB/JPY OrderFlow Monitor v5.4 RenderFix6

Render start command:
`uvicorn main:app --host 0.0.0.0 --port $PORT`

This build separates WebSocket transport from actual market-data reception.
`ws_connected` becomes true only after a recognized orderbook/trade frame.
`/api/health` exposes transport/subscription/event/raw-frame diagnostics.
REST snapshot fallback and scoring behavior are retained.
