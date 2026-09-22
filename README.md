# SHIB Monitor API — renderfix16

Runtime files: main.py, coincheck_stream.py, analyzer.py, orderbook.py, requirements.txt, render.yaml.

Fix16: REST is the primary 5-second data path; WebSocket is an independent realtime overlay with immediate receive loop and automatic reconnect. WS errors do not invalidate REST readiness.

Render start command: python -m uvicorn main:app --host 0.0.0.0 --port $PORT

frontend/, backend/, and old app.py are not required.


## renderfix17
- REST order book remains the primary freshness/decision source.
- WebSocket freshness is separated from REST freshness.
- `ws_data_state` distinguishes `LIVE`, `LIVE_TRADE`, `IDLE`, `STALE`, `CONNECTED_NO_DATA`, and `DISCONNECTED`.
- Added trade diagnostics: `ws_trade_age_sec`, `ws_trade_parse_failures`, `ws_nontrade_list_messages`, and `ws_trade_raw_preview`.
- A connected WebSocket that has simply stopped sending market updates no longer makes the main REST health state degraded.
- Official Coincheck public WebSocket trade format is supported as a 2-dimensional array.
