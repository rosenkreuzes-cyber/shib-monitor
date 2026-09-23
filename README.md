# SHIB/JPY Monitor — OKJ WebSocket

OKJ Public WebSocket を主データ源にした SHIB-JPY リアルタイム監視版です。

- WebSocket: `wss://ws.okj.com:443/ws/v5/public`
- Instrument: `SHIB-JPY`
- 板: `books`（初回snapshot + 差分更新）
- 約定: `trades`
- ticker: `tickers`
- REST: 起動時のbootstrap/recovery用
- Render start: `python -m uvicorn main:app --host 0.0.0.0 --port $PORT`

`/api/health` で `ws_orderbook_messages`, `ws_trade_messages`, `ws_subscribe_messages`, `last_seq_id`, `last_action` を確認できます。

OKJの公開API仕様では `books` は初回400レベルのsnapshot後、差分を100ms間隔で配信し、`trades` は約定発生時に配信されます。
