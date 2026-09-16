import express from "express";
import http from "http";
import WebSocket, { WebSocketServer } from "ws";

const PORT = Number(process.env.PORT || 10000);
const PAIR = "shib_jpy";
const DISPLAY_PAIR = "SHIB/JPY";
const REST_BOOK_URL = `https://coincheck.com/api/order_books?pair=${PAIR}`;
const REST_TICKER_URL = `https://coincheck.com/api/ticker?pair=${PAIR}`;
const WS_URL = process.env.COINCHECK_WS_URL || "wss://ws-api.coincheck.com/";
const REST_POLL_MS = 5000;
const WS_STALE_MS = 15000;
const RECONNECT_MS = 3000;
const MAX_LEVELS = 10;

const app = express();
app.use((req, res, next) => {
  res.setHeader("Access-Control-Allow-Origin", "*");
  next();
});
app.use(express.static("public"));

let state = {
  status: "ok",
  service: "shib-monitor-api",
  version: "5.4",
  pair: DISPLAY_PAIR,
  price: null,
  price_change_pct: null,
  source: "unknown",
  ws_connected: false,
  ws_stale: false,
  freshness: "UNKNOWN",
  last_data_received_ts: null,
  last_ws_data_received_ts: null,
  last_ws_orderbook_ts: null,
  last_update_ts: null,
  reconnect_count: 0,
  last_error: null,
  sequence: null,
  book: {
    ready: false,
    best_bid: null,
    best_ask: null,
    mid: null,
    spread_pct: null,
    imbalance_pct: null,
    weighted_bid: 0,
    weighted_ask: 0,
    bid_levels: 0,
    ask_levels: 0,
    bids_top10: [],
    asks_top10: [],
    last_data_received_ts: null,
    last_update_ts: null,
    snapshot_age_sec: null,
    price_in_book: null,
    ticker_book_consistent: null
  },
  trade_flow: {
    buy_pct: null,
    sell_pct: null,
    buy: 0,
    sell: 0
  },
  score_usable: false,
  score: null,
  label: "判定停止",
  score_components: {}
};

let rawBook = { bids: new Map(), asks: new Map() };
let lastTicker = null;
let coincheckWs = null;
let reconnectTimer = null;

function finiteNumber(v) {
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

function cleanLevels(levels, side) {
  if (!Array.isArray(levels)) return [];
  return levels
    .map(x => {
      const p = finiteNumber(x?.[0]);
      const q = finiteNumber(x?.[1]);
      if (p == null || q == null || p <= 0 || q <= 0) return null;
      return { price: p, amount: q, notional: p * q, side };
    })
    .filter(Boolean)
    .sort((a, b) => side === "bid" ? b.price - a.price : a.price - b.price);
}

function mapsFromSnapshot(bids, asks) {
  const next = { bids: new Map(), asks: new Map() };
  for (const x of cleanLevels(bids, "bid")) next.bids.set(x.price, x.amount);
  for (const x of cleanLevels(asks, "ask")) next.asks.set(x.price, x.amount);
  return next;
}

function applyDelta(sideMap, levels) {
  if (!Array.isArray(levels)) return;
  for (const x of levels) {
    const p = finiteNumber(x?.[0]);
    const q = finiteNumber(x?.[1]);
    if (p == null || q == null || p <= 0 || q < 0) continue;
    if (q === 0) sideMap.delete(p);
    else sideMap.set(p, q);
  }
}

function topLevels(map, side) {
  return [...map.entries()]
    .map(([price, amount]) => ({ price, amount, notional: price * amount, side }))
    .sort((a, b) => side === "bid" ? b.price - a.price : a.price - b.price)
    .slice(0, MAX_LEVELS);
}

function computeBook() {
  const bids = topLevels(rawBook.bids, "bid");
  const asks = topLevels(rawBook.asks, "ask");
  const bestBid = bids[0]?.price ?? null;
  const bestAsk = asks[0]?.price ?? null;
  const validSpread = bestBid != null && bestAsk != null && bestBid < bestAsk;
  const mid = validSpread ? (bestBid + bestAsk) / 2 : null;
  const spreadPct = validSpread ? ((bestAsk - bestBid) / bestBid) * 100 : null;
  const bidWeight = bids.reduce((s, x) => s + x.notional, 0);
  const askWeight = asks.reduce((s, x) => s + x.notional, 0);
  const totalWeight = bidWeight + askWeight;
  const imbalancePct = totalWeight > 0 ? (bidWeight / totalWeight) * 100 : null;

  state.book.best_bid = bestBid;
  state.book.best_ask = bestAsk;
  state.book.mid = mid;
  state.book.spread_pct = spreadPct;
  state.book.imbalance_pct = imbalancePct;
  state.book.weighted_bid = bidWeight;
  state.book.weighted_ask = askWeight;
  state.book.bid_levels = bids.length;
  state.book.ask_levels = asks.length;
  state.book.bids_top10 = bids;
  state.book.asks_top10 = asks;
  state.book.ready = validSpread;

  return { bids, asks, bestBid, bestAsk, mid, validSpread };
}

function updateConsistency() {
  const b = state.book;
  const price = state.price;
  const hasBook = b.best_bid != null && b.best_ask != null && b.best_bid < b.best_ask;
  const hasPrice = price != null;
  b.price_in_book = hasBook && hasPrice ? price >= b.best_bid && price <= b.best_ask : null;

  if (lastTicker && finiteNumber(lastTicker.bid) != null && finiteNumber(lastTicker.ask) != null && hasBook) {
    const tb = finiteNumber(lastTicker.bid);
    const ta = finiteNumber(lastTicker.ask);
    b.ticker_book_consistent = tb <= ta && tb > 0 && ta > 0;
  } else {
    b.ticker_book_consistent = null;
  }
}

function calculateScore() {
  const b = state.book;
  const fresh = state.freshness === "LIVE" || state.freshness === "CAUTION";
  const enoughBook = b.ready && b.bid_levels > 0 && b.ask_levels > 0;
  const priceOK = b.price_in_book === true;

  if (!fresh || !enoughBook || !priceOK || b.imbalance_pct == null) {
    state.score_usable = false;
    state.score = null;
    state.label = "判定停止";
    state.score_components = {};
    return;
  }

  // 0-100: book imbalance is the primary measurable component.
  // The remaining components are intentionally small and transparent.
  const imbalance = Math.max(0, Math.min(100, b.imbalance_pct));
  const spreadComponent = b.spread_pct == null ? 50 : Math.max(0, Math.min(100, 100 - b.spread_pct * 100));
  const freshnessComponent = state.freshness === "LIVE" ? 100 : 70;
  const score = imbalance * 0.70 + spreadComponent * 0.10 + freshnessComponent * 0.20;

  state.score_usable = true;
  state.score = Math.max(0, Math.min(100, score));
  state.score_components = {
    "板バランス": imbalance * 0.70,
    "スプレッド": spreadComponent * 0.10,
    "鮮度": freshnessComponent * 0.20
  };
  state.label = state.score >= 58 ? "買い優勢" : state.score <= 42 ? "売り優勢" : "拮抗";
}

function refreshDerived() {
  computeBook();
  updateConsistency();
  calculateScore();
  if (state.book.last_data_received_ts) {
    state.book.snapshot_age_sec = Math.max(0, (Date.now() - state.book.last_data_received_ts) / 1000);
  }
}

async function fetchJson(url) {
  const r = await fetch(url, { signal: AbortSignal.timeout(8000) });
  if (!r.ok) throw new Error(`HTTP ${r.status} ${url}`);
  return await r.json();
}

async function loadRest(source = "rest_poll") {
  try {
    const [bookData, tickerData] = await Promise.all([
      fetchJson(REST_BOOK_URL),
      fetchJson(REST_TICKER_URL)
    ]);

    if (!Array.isArray(bookData?.bids) || !Array.isArray(bookData?.asks)) {
      throw new Error("REST orderbook payload invalid");
    }

    const next = mapsFromSnapshot(bookData.bids, bookData.asks);
    const testBids = topLevels(next.bids, "bid");
    const testAsks = topLevels(next.asks, "ask");
    const testBid = testBids[0]?.price ?? null;
    const testAsk = testAsks[0]?.price ?? null;

    // Never replace a good book with a crossed/inverted snapshot.
    if (testBid == null || testAsk == null || testBid >= testAsk) {
      throw new Error(`REST book integrity error: bid=${testBid} ask=${testAsk}`);
    }

    rawBook = next;
    lastTicker = tickerData || null;
    const last = finiteNumber(tickerData?.last);
    state.price = last;
    state.source = "coincheck_rest";
    state.last_data_received_ts = Date.now();
    state.book.last_data_received_ts = state.last_data_received_ts;
    state.book.last_update_ts = tickerData?.timestamp ? Number(tickerData.timestamp) * 1000 : state.last_data_received_ts;
    state.last_update_ts = state.book.last_update_ts;
    state.sequence = null;
    state.last_error = null;

    if (source === "rest_snapshot" && state.ws_connected) {
      // WS remains primary when healthy; REST just seeds/reconciles the book.
    }
    refreshDerived();
    updateFreshness();
    calculateScore();
  } catch (e) {
    state.last_error = e?.message || String(e);
    updateFreshness();
  }
}

function applyWsBook(payload) {
  const bids = payload?.bids;
  const asks = payload?.asks;
  if (!Array.isArray(bids) && !Array.isArray(asks)) return false;

  // Coincheck documents these as order-book differences, so merge them into
  // the current book instead of replacing the whole book on every message.
  applyDelta(rawBook.bids, bids || []);
  applyDelta(rawBook.asks, asks || []);

  const d = computeBook();
  if (!d.validSpread) return false;

  state.source = "coincheck_ws";
  state.last_data_received_ts = Date.now();
  state.last_ws_data_received_ts = state.last_data_received_ts;
  state.last_ws_orderbook_ts = payload?.last_update_at ? Number(payload.last_update_at) * 1000 : state.last_data_received_ts;
  state.book.last_data_received_ts = state.last_data_received_ts;
  state.book.last_update_ts = state.last_ws_orderbook_ts;
  state.ws_stale = false;
  state.last_error = null;
  refreshDerived();
  updateFreshness();
  calculateScore();
  return true;
}

function updateFreshness() {
  const now = Date.now();
  const age = state.last_data_received_ts ? (now - state.last_data_received_ts) / 1000 : null;
  const wsAge = state.last_ws_data_received_ts ? (now - state.last_ws_data_received_ts) / 1000 : null;

  if (state.ws_connected && wsAge != null && wsAge <= WS_STALE_MS / 1000) state.freshness = "LIVE";
  else if (age != null && age <= 10) state.freshness = "CAUTION";
  else if (age != null && age <= 30) state.freshness = "STALE";
  else if (age != null) state.freshness = "INVALID";
  else state.freshness = "UNKNOWN";

  state.ws_stale = state.ws_connected && (wsAge == null || wsAge > WS_STALE_MS / 1000);
  if (state.book.last_data_received_ts) {
    state.book.snapshot_age_sec = Math.max(0, (now - state.book.last_data_received_ts) / 1000);
  }
}

function publicState() {
  updateFreshness();
  refreshDerived();
  return {
    ...state,
    snapshot_age_sec: state.book.snapshot_age_sec,
    ws_age_sec: state.last_ws_data_received_ts ? Math.max(0, (Date.now() - state.last_ws_data_received_ts) / 1000) : null,
    bid_levels: state.book.bid_levels,
    ask_levels: state.book.ask_levels,
    total_levels: state.book.bid_levels + state.book.ask_levels,
    best_bid: state.book.best_bid,
    best_ask: state.book.best_ask,
    mid: state.book.mid,
    spread_pct: state.book.spread_pct,
    ws_messages: state.ws_connected ? 1 : 0,
    ws_orderbook_messages: state.last_ws_orderbook_ts ? 1 : 0,
    ws_trade_messages: 0,
    server_time: new Date().toISOString()
  };
}

const server = http.createServer(app);
const wss = new WebSocketServer({ server, path: "/ws" });

app.get("/api/state", (_req, res) => res.status(200).json(publicState()));
app.get("/api/health", (_req, res) => {
  const s = publicState();
  res.status(200).json({
    status: "ok",
    service: "shib-monitor-api",
    version: state.version,
    pair: "shib_jpy",
    book_ready: state.book.ready,
    fresh: state.freshness === "LIVE",
    freshness: state.freshness,
    ws_connected: state.ws_connected,
    ws_stale: state.ws_stale,
    ws_age_sec: s.ws_age_sec,
    snapshot_age_sec: s.snapshot_age_sec,
    last_data_received_ts: state.last_data_received_ts,
    last_data_source: state.source === "coincheck_ws" ? "coincheck_ws" : state.source === "coincheck_rest" ? "rest_snapshot" : null,
    price: state.price,
    best_bid: state.book.best_bid,
    best_ask: state.book.best_ask,
    bid_levels: state.book.bid_levels,
    ask_levels: state.book.ask_levels,
    total_levels: state.book.bid_levels + state.book.ask_levels,
    sequence: state.sequence,
    source: state.source,
    last_error: state.last_error,
    ws_messages: state.last_ws_data_received_ts ? 1 : 0,
    ws_orderbook_messages: state.last_ws_orderbook_ts ? 1 : 0,
    ws_trade_messages: 0,
    last_ws_message_ts: state.last_ws_data_received_ts,
    last_ws_orderbook_ts: state.last_ws_orderbook_ts,
    price_in_book: state.book.price_in_book,
    server_time: new Date().toISOString()
  });
});

function broadcast() {
  const msg = JSON.stringify(publicState());
  for (const c of wss.clients) if (c.readyState === WebSocket.OPEN) c.send(msg);
}

wss.on("connection", ws => ws.send(JSON.stringify(publicState())));

function scheduleReconnect() {
  if (reconnectTimer) return;
  reconnectTimer = setTimeout(() => {
    reconnectTimer = null;
    connectCoincheck();
  }, RECONNECT_MS);
}

function connectCoincheck() {
  if (coincheckWs && (coincheckWs.readyState === WebSocket.OPEN || coincheckWs.readyState === WebSocket.CONNECTING)) return;
  const ws = new WebSocket(WS_URL);
  coincheckWs = ws;

  ws.on("open", () => {
    state.ws_connected = true;
    state.ws_stale = false;
    state.last_error = null;
    ws.send(JSON.stringify({ type: "subscribe", channel: `${PAIR}-orderbook` }));
    loadRest("rest_snapshot").then(broadcast);
    broadcast();
  });

  ws.on("message", raw => {
    try {
      const msg = JSON.parse(raw.toString());
      const payload = Array.isArray(msg) ? msg[1] : msg;
      if (applyWsBook(payload)) broadcast();
    } catch (e) {
      state.last_error = `WS parse error: ${e?.message || e}`;
      broadcast();
    }
  });

  ws.on("close", () => {
    if (coincheckWs === ws) coincheckWs = null;
    state.ws_connected = false;
    state.ws_stale = false;
    state.reconnect_count += 1;
    updateFreshness();
    broadcast();
    scheduleReconnect();
  });

  ws.on("error", err => {
    state.last_error = `WS error: ${err?.message || err}`;
    broadcast();
  });
}

setInterval(() => {
  updateFreshness();
  if (state.ws_connected && state.last_ws_data_received_ts) {
    const age = Date.now() - state.last_ws_data_received_ts;
    if (age > WS_STALE_MS) {
      state.ws_stale = true;
      state.last_error = `orderbook stale ${Math.round(age / 1000)}s; reconnecting`;
      broadcast();
      if (coincheckWs?.readyState === WebSocket.OPEN) coincheckWs.terminate();
    }
  }
  broadcast();
}, 1000);

setInterval(() => loadRest("rest_poll").then(broadcast), REST_POLL_MS);

loadRest("rest_startup").then(broadcast);
connectCoincheck();

server.listen(PORT, "0.0.0.0", () => {
  console.log(`SHIB Monitor v5.4 integrity server listening on ${PORT}`);
});
