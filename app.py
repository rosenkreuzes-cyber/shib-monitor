import os
import time
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from flask import Flask, jsonify, render_template
import requests

APP_VERSION = "5.3"
PAIR = "shib_jpy"
DISPLAY_PAIR = "SHIB/JPY"
COINCHECK_BASE = "https://coincheck.com/api"
POLL_SECONDS = float(os.getenv("POLL_SECONDS", "1.0"))
TOP_N = int(os.getenv("TOP_N", "10"))
STALE_SECONDS = float(os.getenv("STALE_SECONDS", "30"))

app = Flask(__name__)

session = requests.Session()
session.headers.update({
    "User-Agent": "SHIB-Monitor/5.3",
    "Accept": "application/json",
})

state_lock = threading.Lock()
state = {
    "ready": False,
    "last_success": None,
    "last_error": None,
    "last_fetch_ms": None,
    "book": None,
    "ticker": None,
    "history": [],
    "events": [],
    "previous_book": None,
    "previous_price": None,
    "started_at": time.time(),
}

def now_iso():
    return datetime.now(timezone.utc).isoformat()

def to_float(v, default=None):
    try:
        return float(v)
    except (TypeError, ValueError):
        return default

def normalize_levels(rows, side):
    out = []
    if not isinstance(rows, list):
        return out
    for row in rows:
        if not isinstance(row, (list, tuple)) or len(row) < 2:
            continue
        price = to_float(row[0])
        amount = to_float(row[1])
        if price is None or amount is None or price <= 0 or amount < 0:
            continue
        out.append({"price": price, "amount": amount})
    reverse = side == "bids"
    out.sort(key=lambda x: x["price"], reverse=reverse)
    return out

def calc_book(bids, asks):
    bids = bids[:TOP_N]
    asks = asks[:TOP_N]
    bid_total = sum(x["amount"] for x in bids)
    ask_total = sum(x["amount"] for x in asks)
    total = bid_total + ask_total
    imbalance = ((bid_total - ask_total) / total * 100.0) if total else 0.0

    best_bid = bids[0]["price"] if bids else None
    best_ask = asks[0]["price"] if asks else None
    if best_bid is not None and best_ask is not None:
        mid = (best_bid + best_ask) / 2.0
        spread = best_ask - best_bid
        spread_pct = spread / mid * 100.0 if mid else None
    else:
        mid = spread = spread_pct = None

    return {
        "bids": bids,
        "asks": asks,
        "bid_total": bid_total,
        "ask_total": ask_total,
        "imbalance_pct": imbalance,
        "best_bid": best_bid,
        "best_ask": best_ask,
        "mid": mid,
        "spread": spread,
        "spread_pct": spread_pct,
    }

def fetch_json(path, params):
    r = session.get(
        f"{COINCHECK_BASE}{path}",
        params=params,
        timeout=5,
    )
    r.raise_for_status()
    data = r.json()
    if isinstance(data, dict) and data.get("success") is False:
        raise RuntimeError(data.get("error") or f"Coincheck API error: {path}")
    return data

def fetch_snapshot():
    started = time.perf_counter()
    with ThreadPoolExecutor(max_workers=2) as ex:
        f_book = ex.submit(fetch_json, "/order_books", {"pair": PAIR})
        f_ticker = ex.submit(fetch_json, "/ticker", {"pair": PAIR})
        raw_book = f_book.result()
        raw_ticker = f_ticker.result()

    bids = normalize_levels(raw_book.get("bids", []), "bids")
    asks = normalize_levels(raw_book.get("asks", []), "asks")
    book = calc_book(bids, asks)

    ticker = {
        "last": to_float(raw_ticker.get("last")),
        "bid": to_float(raw_ticker.get("bid")),
        "ask": to_float(raw_ticker.get("ask")),
        "high": to_float(raw_ticker.get("high")),
        "low": to_float(raw_ticker.get("low")),
        "volume": to_float(raw_ticker.get("volume")),
        "timestamp": raw_ticker.get("timestamp"),
    }

    elapsed_ms = round((time.perf_counter() - started) * 1000, 1)
    return book, ticker, elapsed_ms

def build_events(previous, current):
    events = []
    if not previous:
        return events

    for side_name, label in (("bids", "買い板"), ("asks", "売り板")):
        old = {round(x["price"], 12): x["amount"] for x in previous.get(side_name, [])}
        new = {round(x["price"], 12): x["amount"] for x in current.get(side_name, [])}
        for price in set(old) | set(new):
            delta = new.get(price, 0.0) - old.get(price, 0.0)
            if abs(delta) < 1e-12:
                continue
            events.append({
                "time": datetime.now().strftime("%H:%M:%S"),
                "side": "buy" if side_name == "bids" else "sell",
                "label": label + ("増加" if delta > 0 else "減少"),
                "price": price,
                "delta": delta,
            })

    events.sort(key=lambda x: abs(x["delta"]), reverse=True)
    return events[:6]

def poll_once():
    try:
        book, ticker, elapsed_ms = fetch_snapshot()
        with state_lock:
            previous_book = state["book"]
            state["previous_book"] = previous_book
            state["book"] = book
            state["ticker"] = ticker
            state["previous_price"] = (
                state["ticker"]["last"] if state["ticker"] else None
            )
            state["last_success"] = time.time()
            state["last_error"] = None
            state["last_fetch_ms"] = elapsed_ms
            state["ready"] = bool(book["bids"] and book["asks"])

            last = ticker.get("last")
            if last is not None:
                hist = state["history"]
                hist.append({
                    "ts": time.time(),
                    "time": datetime.now().strftime("%H:%M:%S"),
                    "price": last,
                    "imbalance": book["imbalance_pct"],
                })
                cutoff = time.time() - 3600
                state["history"] = [x for x in hist if x["ts"] >= cutoff][-3600:]

            if previous_book:
                new_events = build_events(previous_book, book)
                state["events"] = (new_events + state["events"])[:30]
    except Exception as exc:
        with state_lock:
            state["last_error"] = str(exc)
            state["ready"] = False if not state["book"] else state["ready"]

def poll_loop():
    while True:
        started = time.time()
        poll_once()
        elapsed = time.time() - started
        time.sleep(max(0.05, POLL_SECONDS - elapsed))

@app.get("/")
def index():
    return render_template("index.html", version=APP_VERSION)

@app.get("/api/health")
def health():
    with state_lock:
        age = None
        if state["last_success"]:
            age = round(time.time() - state["last_success"], 3)
        return jsonify({
            "ok": bool(state["ready"] and age is not None and age <= STALE_SECONDS),
            "version": APP_VERSION,
            "pair": DISPLAY_PAIR,
            "age_seconds": age,
            "last_error": state["last_error"],
            "poll_seconds": POLL_SECONDS,
        })

@app.get("/api/orderbook")
def orderbook_api():
    with state_lock:
        book = state["book"]
        ticker = state["ticker"]
        last_success = state["last_success"]
        age = (time.time() - last_success) if last_success else None
        fresh = age is not None and age <= STALE_SECONDS

        if not book or not ticker:
            return jsonify({
                "pair": DISPLAY_PAIR,
                "ready": False,
                "fresh": False,
                "error": state["last_error"] or "板情報を取得中です",
                "book": None,
                "ticker": None,
            }), 503

        current_last = ticker.get("last")
        prev_price = state["history"][-2]["price"] if len(state["history"]) >= 2 else None
        price_change_pct = (
            (current_last - prev_price) / prev_price * 100
            if current_last is not None and prev_price
            else 0.0
        )

        return jsonify({
            "pair": DISPLAY_PAIR,
            "ready": True,
            "fresh": fresh,
            "age_seconds": round(age, 3) if age is not None else None,
            "updated_at": datetime.fromtimestamp(
                last_success, tz=timezone.utc
            ).isoformat() if last_success else None,
            "server_time": now_iso(),
            "fetch_ms": state["last_fetch_ms"],
            "error": state["last_error"],
            "ticker": ticker,
            "book": book,
            "price_change_pct": price_change_pct,
            "history": state["history"][-300:],
            "events": state["events"][:12],
            "rules": {
                "stale_seconds": STALE_SECONDS,
                "top_n": TOP_N,
                "decision_allowed": fresh and state["ready"],
            },
        })

@app.get("/api")
def api_root():
    return orderbook_api()

poll_thread = threading.Thread(target=poll_loop, daemon=True)
poll_thread.start()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "8080")))
