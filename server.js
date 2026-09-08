import express from "express";
import http from "http";
import WebSocket, { WebSocketServer } from "ws";

const PORT = process.env.PORT || 10000;
const PAIR = "SHIB/JPY";
const REST_URL = "https://coincheck.com/api/order_books?pair=shib_jpy";
const WS_URL = process.env.COINCHECK_WS_URL || "wss://ws-api.coincheck.com/";
const REST_POLL_MS = 5000;
const WS_STALE_MS = 15000;
const RECONNECT_MS = 3000;

const app = express();
app.use(express.static("public"));

let state = {
  version: "v5.3",
  pair: PAIR,
  price: null,
  best_bid: null,
  best_ask: null,
  mid: null,
  spread_pct: null,
  bids: [],
  asks: [],
  source: "none",
  ws_connected: false,
  freshness: "OFFLINE",
  last_data_received_ts: null,
  last_ws_data_received_ts: null,
  ws_stale: false,
  reconnect_count: 0,
  last_error: null
};

function cleanLevels(levels, side) {
  return (Array.isArray(levels) ? levels : [])
    .map(x => {
      const p = Number(x?.[0]);
      const q = Number(x?.[1]);
      return Number.isFinite(p) && Number.isFinite(q) && p > 0 && q > 0
        ? { price: p, amount: q, notional: p * q, side }
        : null;
    })
    .filter(Boolean)
    .sort((a, b) => side === "bid" ? b.price - a.price : a.price - b.price)
    .slice(0, 10);
}

function applyBook(bids, asks, source) {
  state.bids = cleanLevels(bids, "bid");
  state.asks = cleanLevels(asks, "ask");
  state.best_bid = state.bids[0]?.price ?? null;
  state.best_ask = state.asks[0]?.price ?? null;
  state.mid = state.best_bid != null && state.best_ask != null
    ? (state.best_bid + state.best_ask) / 2 : null;
  state.price = state.mid;
  state.spread_pct = state.best_bid && state.best_ask
    ? ((state.best_ask - state.best_bid) / state.best_bid) * 100 : null;
  state.source = source;
  state.last_data_received_ts = Date.now();

  if (source === "coincheck_ws") {
    state.last_ws_data_received_ts = state.last_data_received_ts;
    state.ws_stale = false;
    state.freshness = "LIVE";
  } else if (!state.ws_connected) {
    state.freshness = "CAUTION";
  }
}

async function loadRest(source = "rest_poll") {
  try {
    const r = await fetch(REST_URL, { signal: AbortSignal.timeout(8000) });
    if (!r.ok) throw new Error(`REST ${r.status}`);
    const d = await r.json();
    if (!Array.isArray(d.bids) || !Array.isArray(d.asks)) {
      throw new Error("REST orderbook payload invalid");
    }
    applyBook(d.bids, d.asks, source);
    if (!state.ws_connected) state.freshness = "CAUTION";
    state.last_error = null;
  } catch (e) {
    state.last_error = e?.message || String(e);
    if (!state.last_data_received_ts) state.freshness = "OFFLINE";
  }
}

const server = http.createServer(app);
const wss = new WebSocketServer({ server, path: "/ws" });

// Render health check: process health must stay HTTP 200 even if Coincheck
// temporarily disconnects. Market-data health is exposed in the JSON body.
app.get("/api/health", (_req, res) => {
  const now = Date.now();
  const wsAgeSec = state.last_ws_data_received_ts
    ? Math.max(0, (now - state.last_ws_data_received_ts) / 1000)
    : null;

  res.status(200).json({
    status: "ok",
    service: "shib-monitor-api",
    version: state.version,
    pair: state.pair,
    ws_connected: state.ws_connected,
    ws_stale: state.ws_stale,
    freshness: state.freshness,
    ws_age_sec: wsAgeSec,
    last_data_received_ts: state.last_data_received_ts,
    reconnect_count: state.reconnect_count,
    last_error: state.last_error
  });
});

function broadcast() {
  const msg = JSON.stringify(state);
  for (const c of wss.clients) {
    if (c.readyState === WebSocket.OPEN) c.send(msg);
  }
}

wss.on("connection", ws => {
  ws.send(JSON.stringify(state));
});

let reconnectTimer = null;
let coincheckWs = null;

function scheduleReconnect() {
  if (reconnectTimer) return;
  reconnectTimer = setTimeout(() => {
    reconnectTimer = null;
    connectCoincheck();
  }, RECONNECT_MS);
}

function connectCoincheck() {
  if (coincheckWs && (coincheckWs.readyState === WebSocket.OPEN || coincheckWs.readyState === WebSocket.CONNECTING)) {
    return;
  }

  const ws = new WebSocket(WS_URL);
  coincheckWs = ws;

  ws.on("open", () => {
    state.ws_connected = true;
    state.ws_stale = false;
    state.last_error = null;

    // Official Coincheck public channel for SHIB/JPY order-book updates.
    ws.send(JSON.stringify({
      type: "subscribe",
      channel: "shib_jpy-orderbook"
    }));

    // Immediately refresh a REST snapshot so the book is populated while
    // waiting for the first WebSocket update.
    loadRest("rest_snapshot").then(broadcast);
    broadcast();
  });

  ws.on("message", raw => {
    try {
      const msg = JSON.parse(raw.toString());
      const payload = Array.isArray(msg) ? msg[1] : msg;
      const bids = payload?.bids ?? payload?.data?.bids;
      const asks = payload?.asks ?? payload?.data?.asks;

      if (Array.isArray(bids) && Array.isArray(asks)) {
        applyBook(bids, asks, "coincheck_ws");
        broadcast();
      }
    } catch (e) {
      state.last_error = `WS parse error: ${e?.message || e}`;
    }
  });

  ws.on("close", () => {
    if (coincheckWs === ws) coincheckWs = null;
    state.ws_connected = false;
    state.ws_stale = false;
    if (state.last_data_received_ts) state.freshness = "CAUTION";
    state.reconnect_count += 1;
    broadcast();
    scheduleReconnect();
  });

  ws.on("error", err => {
    state.last_error = `WS error: ${err?.message || err}`;
    broadcast();
  });
}

// WS watchdog: if the connection remains open but no order-book message arrives,
// terminate it so the close handler performs a clean reconnect. This fixes the
// "orderbook stale ... forcing reconnect" condition instead of merely logging it.
setInterval(() => {
  if (!state.ws_connected || !state.last_ws_data_received_ts) return;

  const ageMs = Date.now() - state.last_ws_data_received_ts;
  if (ageMs > WS_STALE_MS) {
    state.ws_stale = true;
    state.freshness = state.last_data_received_ts ? "CAUTION" : "OFFLINE";
    state.last_error = `orderbook stale ${Math.round(ageMs / 1000)}s; reconnecting`;
    broadcast();

    if (coincheckWs && coincheckWs.readyState === WebSocket.OPEN) {
      coincheckWs.terminate();
    }
  }
}, 1000);

// REST is a safety net, not the primary real-time feed.
setInterval(() => {
  loadRest("rest_poll").then(broadcast);
}, REST_POLL_MS);

setInterval(() => {
  if (state.last_data_received_ts) {
    const age = (Date.now() - state.last_data_received_ts) / 1000;
    if (state.ws_connected && !state.ws_stale && age <= 10) state.freshness = "LIVE";
    else if (age <= 30) state.freshness = "CAUTION";
    else state.freshness = "OFFLINE";
    broadcast();
  }
}, 1000);

loadRest("rest_startup").then(broadcast);
connectCoincheck();

server.listen(PORT, "0.0.0.0", () => {
  console.log(`SHIB Monitor v5.3 listening on ${PORT}`);
});
