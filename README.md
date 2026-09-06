# SHIB Monitor API v5.3

SHIB/JPY の公開板情報を Coincheck から取得し、スマートフォン縦画面で
「買い板（BID）」「価格」「売り板（ASK）」を左右に並べ、数量を横バーで比較する監視アプリです。

## 構成

Coincheck REST API
→ Flask バックエンド
→ `/api/orderbook`
→ スマホ向け HTML/CSS/JS

v5.3 はまず REST 1秒ポーリングを安定動作させる構成です。
30秒以上データ更新がない場合は `STALE` とし、判定利用を停止します。

## Render

1. GitHub にこのフォルダをアップロード
2. Render で Web Service を作成
3. Build Command:
   `pip install -r requirements.txt`
4. Start Command:
   `gunicorn app:app --bind 0.0.0.0:$PORT --workers 1 --threads 4 --timeout 30`

`render.yaml` を使う場合は Blueprint として読み込めます。

## API

- `/` スマホ監視画面
- `/api/orderbook` 正規化済み板データ
- `/api/health` ヘルスチェック

## 板バランス

TOP10の板数量から、

`(買い板数量 - 売り板数量) / (買い板数量 + 売り板数量) × 100`

で算出します。

これは板の需給傾向を表示するための指標で、価格予測や売買を保証するものではありません。
