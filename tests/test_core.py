import sys,time
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'backend'))
from orderbook import OrderBookEngine

def test_live_age_refreshes_on_diff():
 b=OrderBookEngine();b.load_snapshot([[100,10]],[[101,8]],update_ts=time.time());time.sleep(.02);b.apply_diff([[100,11]],[],update_ts=None)
 assert b.snapshot()['snapshot_age_sec'] < 1
 assert b.snapshot()['freshness']=='LIVE'

def test_zero_removes_level():
 b=OrderBookEngine();b.load_snapshot([[100,10]],[[101,8]]);b.apply_diff([[100,0]],[[101,0]]);assert not b.bids and not b.asks

def test_top10_and_weighted():
 b=OrderBookEngine(max_levels=100);b.load_snapshot([[100-i,10+i] for i in range(12)],[[101+i,8+i] for i in range(12)])
 s=b.snapshot();assert len(s['bids_top10'])==10 and len(s['asks_top10'])==10;assert s['weighted_bid']>0 and s['weighted_ask']>0

if __name__=='__main__':
 test_live_age_refreshes_on_diff();test_zero_removes_level();test_top10_and_weighted();print('v5.1 core tests: OK')
