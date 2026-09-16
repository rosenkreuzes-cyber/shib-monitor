const $ = id => document.getElementById(id);
const DEFAULT = location.origin;
let ws = null;
let reconnectTimer = null;

$('apiUrl').value = localStorage.getItem('shibApiUrl') || DEFAULT;
$('save').onclick = () => {
  localStorage.setItem('shibApiUrl', $('apiUrl').value.replace(/\/$/, ''));
  connect();
};

const fmt = (v, d = 8) => v == null || !Number.isFinite(Number(v)) ? '--' : Number(v).toFixed(d).replace(/0+$/, '').replace(/\.$/, '');
const qty = v => v == null || !Number.isFinite(Number(v)) ? '--' : (Number(v) / 1e6).toFixed(2) + 'M';
const pct = v => v == null || !Number.isFinite(Number(v)) ? '--' : Number(v).toFixed(1) + '%';

function render(d) {
  const b = d.book || {};
  const f = d.trade_flow || {};
  $('pair').textContent = d.pair || 'SHIB/JPY';
  $('price').textContent = d.price == null ? '--' : fmt(d.price, 8) + ' JPY';
  $('updated').textContent = '最終更新 ' + new Date().toLocaleTimeString('ja-JP');

  $('bid').textContent = fmt(b.best_bid, 8);
  $('ask').textContent = fmt(b.best_ask, 8);
  $('spread').textContent = b.spread_pct == null ? '--' : Number(b.spread_pct).toFixed(3) + '%';

  const freshness = d.freshness || 'UNKNOWN';
  $('live').textContent = d.ws_connected ? '● WS LIVE' : `● ${freshness}`;
  $('live').className = 'status ' + (d.ws_connected ? 'green' : freshness === 'CAUTION' ? 'yellow' : freshness === 'LIVE' ? 'green' : '');

  const usable = d.score_usable === true;
  $('score').textContent = usable ? Number(d.score).toFixed(1) : '--';
  $('scorebar').style.width = usable ? Math.max(0, Math.min(100, Number(d.score))) + '%' : '0%';
  $('label').textContent = usable ? (d.label || '判定中') : '判定停止';
  $('label').className = 'verdict ' + (usable ? (d.score >= 58 ? 'green' : d.score <= 42 ? 'red' : 'yellow') : '');

  $('imb').textContent = pct(b.imbalance_pct);
  $('buybar').style.width = b.imbalance_pct == null ? '0%' : Math.max(0, Math.min(100, b.imbalance_pct)) + '%';
  $('imbLabel').textContent = b.imbalance_pct == null ? '--' : b.imbalance_pct >= 50 ? '買い優勢' : '売り優勢';
  $('imbLabel').className = b.imbalance_pct == null ? '' : (b.imbalance_pct >= 50 ? 'green' : 'red');
  $('wbid').textContent = qty(b.weighted_bid);
  $('wask').textContent = qty(b.weighted_ask);
  $('sell').textContent = f.sell_pct == null ? (b.imbalance_pct == null ? '--' : (100 - Number(b.imbalance_pct)).toFixed(1) + '%') : pct(f.sell_pct);

  const bids = b.bids_top10 || [];
  const asks = b.asks_top10 || [];
  $('bookRows').innerHTML = Array.from({length: 10}, (_, i) => {
    const bid = bids[i], ask = asks[i];
    return `<div class="bookrow"><span class="green">${bid ? qty(bid.amount) : '--'}</span><span>${bid || ask ? fmt(bid?.price ?? ask?.price, 8) : '--'}</span><span class="red">${ask ? qty(ask.amount) : '--'}</span></div>`;
  }).join('');

  const notes = [];
  if (!b.ready) notes.push('✕ BID/ASKが不整合のため板を無効化');
  if (b.price_in_book === false) notes.push('⚠ 現在価格がBID/ASKの範囲外のため判定停止');
  else if (b.price_in_book === true) notes.push('✓ 現在価格とBID/ASKは整合');
  if (freshness !== 'LIVE') notes.push('⚠ 板鮮度：' + freshness);
  if (!d.ws_connected) notes.push('⚠ WebSocket未接続：RESTで継続取得');
  if (d.last_error) notes.push('⚠ ' + d.last_error);
  if (!notes.length) notes.push('✓ 板・価格データ正常');
  $('alerts').innerHTML = notes.map(x => `<div class="alert">${x}</div>`).join('');

  $('detail').textContent =
`API ${d.version || '--'}
データソース ${d.source || '--'}
板 ${b.ready ? 'READY' : 'INVALID'} / ${b.bid_levels || 0} bid / ${b.ask_levels || 0} ask
TOP10 bid ${b.bids_top10?.length || 0} / ask ${b.asks_top10?.length || 0}
現在価格 ${d.price ?? '--'}
最良買い ${b.best_bid ?? '--'}
最良売り ${b.best_ask ?? '--'}
価格整合 ${b.price_in_book == null ? '--' : b.price_in_book ? 'OK' : 'NG'}
WS ${d.ws_connected ? 'CONNECTED' : 'DISCONNECTED'}
更新経過 ${d.snapshot_age_sec == null ? '--' : Number(d.snapshot_age_sec).toFixed(1) + '秒'}`;

  if (d.score_components && usable) {
    $('components').innerHTML = Object.entries(d.score_components).map(([k, v]) => `<p>${k}<b>${Number(v).toFixed(1)}</b></p>`).join('');
  } else {
    $('components').innerHTML = '<span class="red">価格または板の整合性が確認できないため判定停止</span>';
  }
}

function connect() {
  if (ws) { try { ws.close(); } catch (_) {} }
  const api = ($('apiUrl').value || DEFAULT).replace(/\/$/, '');
  const base = api.replace(/^http/, 'ws');
  try { ws = new WebSocket(base + '/ws'); } catch (_) { scheduleReconnect(); return; }

  ws.onopen = () => {
    $('live').textContent = '● WS CONNECTED';
    $('live').className = 'status green';
  };
  ws.onmessage = e => { try { render(JSON.parse(e.data)); } catch (_) {} };
  ws.onclose = () => {
    $('live').textContent = '● RECONNECTING';
    $('live').className = 'status yellow';
    scheduleReconnect();
  };
  ws.onerror = () => { try { ws.close(); } catch (_) {} };
}

function scheduleReconnect() {
  clearTimeout(reconnectTimer);
  reconnectTimer = setTimeout(connect, 3000);
}

connect();
