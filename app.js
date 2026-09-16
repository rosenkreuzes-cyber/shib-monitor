const DEFAULT='https://shib-monitor-api.onrender.com';
let ws=null,reconnectTimer=null,lastTs=null;

const $=id=>document.getElementById(id);
const num=v=>v==null?null:Number(v);
const fmtPrice=v=>{v=num(v);return v==null||!Number.isFinite(v)?'--':v.toFixed(8).replace(/0+$/,'').replace(/\.$/,'')};
const fmtQty=v=>{v=num(v);return v==null||!Number.isFinite(v)?'--':Intl.NumberFormat('ja-JP',{notation:'compact',maximumFractionDigits:2}).format(v)};
const pct=v=>v==null||!Number.isFinite(Number(v))?'--':Number(v).toFixed(1)+'%';
const clamp=(v,a,b)=>Math.max(a,Math.min(b,v));

function metrics(d){
  const bids=Array.isArray(d.bids)?d.bids:[];
  const asks=Array.isArray(d.asks)?d.asks:[];
  const bidN=bids.reduce((s,x)=>s+(num(x.notional)||0),0);
  const askN=asks.reduce((s,x)=>s+(num(x.notional)||0),0);
  const total=bidN+askN;
  const buyPct=total?bidN/total*100:null;
  const imbalance=buyPct;
  const spread=d.best_bid!=null&&d.best_ask!=null&&d.best_bid>0?((d.best_ask-d.best_bid)/d.best_bid*100):null;
  const age=d.last_data_received_ts?Math.max(0,(Date.now()-d.last_data_received_ts)/1000):null;
  const wsAge=d.last_ws_data_received_ts?Math.max(0,(Date.now()-d.last_ws_data_received_ts)/1000):null;
  const mismatch=d.price!=null&&d.best_bid!=null&&d.best_ask!=null&&(d.price<d.best_bid||d.price>d.best_ask);
  let score=null;
  if(imbalance!=null && age!=null && age<=30 && !mismatch){
    score=clamp(50+(imbalance-50)*1.2,0,100);
  }
  return {bids,asks,bidN,askN,buyPct,imbalance,spread,age,wsAge,mismatch,score};
}

function freshness(m,d){
  if(m.age==null)return ['UNKNOWN','red','停止'];
  if(d.ws_connected && !d.ws_stale && m.wsAge!=null && m.wsAge<=15)return ['LIVE','green','使用中'];
  if(m.age<=10)return ['CAUTION','yellow','注意'];
  if(m.age<=30)return ['STALE','red','抑制'];
  return ['INVALID','red','停止'];
}

function render(d){
  const m=metrics(d), fr=freshness(m,d);
  lastTs=Date.now();
  $('pair').textContent=d.pair||'SHIB/JPY';
  $('price').textContent=d.price==null?'--':fmtPrice(d.price)+' JPY';
  $('live').textContent=d.ws_connected?'● WS CONNECTED':(m.age!=null?'● REST FALLBACK':'● OFFLINE');
  $('live').className='status '+(d.ws_connected?'green':m.age!=null?'yellow':'red');
  $('updated').textContent='最終更新 '+new Date().toLocaleTimeString('ja-JP');

  $('score').textContent=m.score==null?'--':m.score.toFixed(1);
  $('scorebar').style.width=(m.score==null?0:m.score)+'%';
  let label='判定停止';
  if(m.score!=null) label=m.score>=58?'買い寄り':m.score<=42?'売り寄り':'拮抗';
  $('label').textContent=label;
  $('label').className='verdict '+(m.score==null?'red':m.score>=58?'green':m.score<=42?'red':'yellow');

  $('imb').textContent=pct(m.buyPct);
  $('sell').textContent=m.buyPct==null?'--':(100-m.buyPct).toFixed(1)+'%';
  $('buybar').style.width=(m.buyPct==null?0:m.buyPct)+'%';
  $('wbid').textContent='想定元本 ¥'+(m.bidN||0).toLocaleString('ja-JP',{maximumFractionDigits:0});
  $('wask').textContent='想定元本 ¥'+(m.askN||0).toLocaleString('ja-JP',{maximumFractionDigits:0});
  $('imbLabel').textContent=m.imbalance==null?'--':(m.imbalance>=50?'買い板側':'売り板側');
  $('imbLabel').className=m.imbalance==null?'':m.imbalance>=50?'green':'red';
  $('spread').textContent=m.spread==null?'--':m.spread.toFixed(3)+'%';
  $('bid').textContent=fmtPrice(d.best_bid);
  $('ask').textContent=fmtPrice(d.best_ask);

  $('bookRows').innerHTML=Array.from({length:10},(_,i)=>{
    const b=m.bids[i],a=m.asks[i];
    return `<div class="bookrow"><span class="green">${b?fmtQty(b.amount):'--'}</span><span>${b||a?fmtPrice(b?.price??a?.price):'--'}</span><span class="red">${a?fmtQty(a.amount):'--'}</span></div>`;
  }).join('');

  const notes=[];
  if(fr[0]==='INVALID')notes.push('✕ 板が30秒以上古いためスコア判定を停止');
  else if(fr[0]!=='LIVE')notes.push('⚠ 板鮮度：'+fr[0]);
  if(m.mismatch)notes.push('⚠ 現在価格がBID/ASKの範囲外');
  if(d.last_error)notes.push('⚠ WS: '+d.last_error);
  if(!notes.length)notes.push('✓ 板・価格データ正常');
  $('alerts').innerHTML=notes.map(x=>`<div class="alert">${x}</div>`).join('');

  $('components').innerHTML=m.score==null
    ? '<div class="alert red">鮮度または価格整合性のため判定停止</div>'
    : `<div class="alert">板バランス <b>${m.buyPct.toFixed(1)}</b> / 100</div><div class="alert">スプレッド <b>${m.spread==null?'--':m.spread.toFixed(3)+'%'}</b></div>`;

  $('detail').textContent=`API ${d.version||'--'}
データソース ${d.source||'--'}
板 ${m.bids.length} bid / ${m.asks.length} ask
最終受信 ${d.last_data_received_ts||'--'}
WS ${d.ws_connected?'CONNECTED':'DISCONNECTED'}
WS更新 ${d.last_ws_data_received_ts||'--'}
再接続 ${d.reconnect_count??0}回`;
}

function connect(){
  const raw=($('apiUrl').value||DEFAULT).trim().replace(/\/$/,'');
  localStorage.setItem('shibApiUrl',raw);
  const base=raw.replace(/^http:/,'ws:').replace(/^https:/,'wss:');
  if(ws){try{ws.close()}catch(e){}}
  ws=new WebSocket(base+'/ws');
  ws.onopen=()=>{$('live').textContent='● WS CONNECTED';$('live').className='status green'};
  ws.onmessage=e=>{try{render(JSON.parse(e.data))}catch(err){$('detail').textContent='受信データ解析エラー: '+err.message}};
  ws.onerror=()=>{try{ws.close()}catch(e){}};
  ws.onclose=()=>{ $('live').textContent='● RECONNECTING';$('live').className='status yellow'; clearTimeout(reconnectTimer); reconnectTimer=setTimeout(connect,3000); };
}

$('apiUrl').value=localStorage.getItem('shibApiUrl')||DEFAULT;
$('save').onclick=()=>connect();
connect();
setInterval(()=>{if(lastTs){const s=(Date.now()-lastTs)/1000;$('updated').textContent='最終更新 '+s.toFixed(1)+'秒前';}},500);
