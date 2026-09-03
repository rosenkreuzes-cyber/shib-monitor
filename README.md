# SHIB Monitor OrderFlow v5.0

SHIB/JPYのCoincheck公開REST + 公開WebSocketを監視し、板・約定・大口約定・板変化・巨大注文から買い/売り圧力を0-100で表示するスマホ向けPWAです。

## v5で直した点
- **板件数を正しく分離**: `bid_levels` / `ask_levels` / `total_levels` は実在する板レベル数。weighted計算の内部件数と混同しません。
- **板数量を明示**: `bid_qty` / `ask_qty`、JPY建て `bid_notional` / `ask_notional`、weighted値を別々に返します。
- **0数量の差分を削除として処理**: Coincheckの板差分で数量0の価格レベルを確実に削除。
- **5%窓 + 近傍レベルfallback**: 広いスプレッドでもweighted値が0にならないようにします。
- **板更新時刻を保持**: `last_update_at` をAPIレスポンスに保存。
- **約定重複ガード**: trade IDを記録し、同じ取引を二重加算しません。
- **再接続バックオフ**: WS切断時に2→4→8…最大30秒で再接続。
- **吸収候補**: 約定側と反対側の近傍板流動性を使った候補値を表示。
- **/health強化**: 板レベル数、WS状態、エラーを確認できます。
- **PWA通知を維持**: スコア急上昇時の買い/売り通知。

## Render設定
Root Directory: `backend`
Build Command: `pip install -r requirements.txt`
Start Command: `uvicorn main:app --host 0.0.0.0 --port $PORT`

## フロントエンド
`frontend/app.js` の `BACKEND_URL` をRenderのAPI URLに合わせます。

## API
- `GET /health`
- `GET /api/analysis`
- `WS /ws`

## ローカルテスト
```bash
python tests/test_core.py
```

Coincheckの公式APIでは `shib_jpy` が取引ペアとして公開され、RESTのorder bookは `asks` / `bids`、WebSocketのorderbookは差分形式、tradesは配列形式で提供されています。
