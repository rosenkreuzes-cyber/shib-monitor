# SHIB Monitor v5.1 Dashboard

`index.html` をブラウザで開くとダッシュボードが表示されます。

- API初期値: https://shib-monitor-api.onrender.com/
- 1秒ごとにAPIを取得
- 板鮮度: 0-3秒 LIVE / 3-10秒 CAUTION / 10-30秒 STALE / 30秒超 INVALID
- 30秒超または価格とBID/ASKが不整合の場合、スコア判定を停止
- API URLは画面から変更可能
- API側のJSON項目が存在する範囲で自動表示
- CORSがAPI側で許可されている必要があります

Render Static Site等へそのまま配置できます。
