# SHIB Monitor v5.4 RenderFix5

- Start: `uvicorn main:app --host 0.0.0.0 --port $PORT`
- Health: `/api/health`
- Analysis: `/api/analysis`
- Orderbook UI data: `/api/orderbook`
- Manual REST refresh: `POST /api/refresh` (GET also accepted)

RenderFix5 keeps REST fallback, exposes transport-vs-subscription WS state, and marks WebSocket connected explicitly after subscription/valid orderbook receipt.
