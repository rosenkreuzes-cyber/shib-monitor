# SHIB/JPY Monitor — renderfix14

## 構成
- main.py
- coincheck_stream.py
- analyzer.py
- orderbook.py
- requirements.txt
- render.yaml

`frontend/` / `backend/` は不要です。
旧 `app.py` も現在の FastAPI 構成では不要です。

## fix14 の主な修正
1. Coincheck WS の `shib_jpy-orderbook` と `shib_jpy-trades` を両方 subscribe。
2. subscribe ACK を待たず、送信直後に `recv()` ループへ入る。
3. REST orderbook/ticker 更新を WS と完全分離。WS停止でもRESTで板を更新。
4. `websockets` の ping/pong と timeout/reconnect を有効化。
5. WS受信なしを `ws_connected=true` と誤表示しない。
6. Render起動は `python -m uvicorn` を使用。

## Render
Build:
`pip install -r requirements.txt`

Start:
`python -m uvicorn main:app --host 0.0.0.0 --port $PORT`

Health:
`/api/health`

## 確認ポイント
正常時は次のようになります。
- `book_ready: true`
- `fresh: true`
- `freshness: LIVE` または `CAUTION`
- `ws_transport_connected: true`
- `ws_subscribed: true`
- `ws_orderbook_messages` が増える
- `ws_trade_messages` が増える
- `ws_last_event: orderbook_received` または `trade_received`

Coincheck Public WebSocket は認証不要で、
`wss://ws-api.coincheck.com/` に接続し、
pairごとに `-orderbook` と `-trades` を subscribe します。
