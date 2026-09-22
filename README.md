# SHIB Monitor API — renderfix16

Runtime files: main.py, coincheck_stream.py, analyzer.py, orderbook.py, requirements.txt, render.yaml.

Fix16: REST is the primary 5-second data path; WebSocket is an independent realtime overlay with immediate receive loop and automatic reconnect. WS errors do not invalidate REST readiness.

Render start command: python -m uvicorn main:app --host 0.0.0.0 --port $PORT

frontend/, backend/, and old app.py are not required.
