# SHIB Monitor v5.1 完全版

## v5.1修正
- 板鮮度を「最後のRESTスナップショット」ではなく「最後に受信した板メッセージ」で計測。
- WS差分を受信するたび `snapshot_age_sec` を更新。
- 30秒超でINVALID、スコア判定停止。
- 価格とBID/ASKの整合性を監視。
- TOP10 BID/ASKをAPIレスポンスに追加。
- 直近1分の約定フローを表示。
- スコア内訳をAPI化。
- WSの板更新が30秒止まった場合、自動的に再接続してRESTスナップショットを再取得。

## Render Backend
Root Directory: `backend`
Build: `pip install -r requirements.txt`
Start: `uvicorn main:app --host 0.0.0.0 --port $PORT`

## Frontend
`frontend/` を静的ホスティングへ配置。`app.js` のAPI URLは画面から変更可能。

## API
GET `/health` / GET `/api/analysis` / WebSocket `/ws`
