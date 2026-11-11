import ast,json,re,sys
from pathlib import Path
from collections import Counter
import numpy as np,pandas as pd
R=Path('/home/htpc/backtrader');S=R/'bt-strategy-test/overnight-ah/research/spy_threshold_study_20261104';sys.path.insert(0,str(S));from evaluate_run import evaluate
out=R/'bt-core/out'; runs={'base':out/'overnight_ah/OvernightAH/simulation_9b14126fcbe74c309dd6a9ae5c3debfd','research':out/'overnight_ah_spy_research/OvernightAHSpyThreshold/simulation_c9a7e76fb84c47a38a3d2431f2c468df'}
logs={'base':R/'out/watchtower-calculations/9b14126f-cbe7-4c30-9dd6-a9ae5c3debfd.log','research':R/'out/watchtower-calculations/c9a7e76f-b84c-47a3-8a3d-2431f2c468df.log'}
semis=set('NVDA,AMD,AVGO,MU,ASML,MRVL,ARM,AMAT,LRCX,KLAC,MCHP,ADI,TXN,ON,INTC,GFS'.split(',')); static=set('NVDA,AVGO,MU,AMD,MSTR,CEG,ASML,MRVL,ARM,MELI'.split(','))
ret={}; result={}
for name,p in runs.items():
 r=pd.read_csv(p/'returns.csv',parse_dates=['index']).set_index('index')['return'].sort_index();assert not r.index.has_duplicates and r.notna().all() and (r>-1).all();ret[name]=r
 trades=pd.DataFrame(json.loads((p/'trades.json').read_text()));trades['open_date']=pd.to_datetime(trades.open_datetime).dt.normalize();trades['close_date']=pd.to_datetime(trades.close_datetime).dt.normalize();trades['notional']=trades['value'].abs();stocks=trades[~trades.asset.isin(['SPY','QQQ','SQQQ'])].copy();eq=200000*(1+r).cumprod().shift(1,fill_value=1);stocks['entry_equity']=stocks.open_date.map(eq);stocks['exposure_proxy']=stocks.notional/stocks.entry_equity;stocks=stocks[stocks.entry_equity.notna()];contrib=stocks.groupby('asset').exposure_proxy.sum();share=contrib/contrib.sum();daily=stocks.groupby('open_date').asset.nunique();raw=stocks.groupby('asset').notional.sum();
 m=evaluate(p,'2005-01-01','2020-12-31'); a={k:m[k] for k in ['total_return','annualized_log_return','max_drawdown','volatility_annual','max_drawdown_duration_sessions','sharpe_daily','sortino_daily','trade_count','stress_total_return_approx_roundtrip_bps']};a['cagr_252']=float(np.expm1(m['annualized_log_return']));a['var95']=-float(r.quantile(.05));a['active_sessions']=int(len(daily));a['avg_daily_stock_count']=float(daily.mean());a['median_daily_stock_count']=float(daily.median());a['unique_stocks']=int(stocks.asset.nunique());a['stock_trades']=len(stocks);a['missing_entry_equity']=int(trades[~trades.asset.isin(['SPY','QQQ','SQQQ'])].open_date.map(eq).isna().sum());a['top10_normalized_entry']=share.sort_values(ascending=False).head(12).to_dict();a['top3_share']=float(share.nlargest(3).sum());a['top5_share']=float(share.nlargest(5).sum());a['semis_share_normalized']=float(share[share.index.isin(semis)].sum());a['static_share_normalized']=float(share[share.index.isin(static)].sum());a['effective_symbols']=float(1/(share**2).sum());a['gross_winrate']=float((stocks.pnl>0).mean());a['symbol_pnl_gross']=stocks.groupby('asset').pnl.sum().sort_values().to_dict();a['annual']=[]
 for y in range(2005,2021):
  z=r[str(y)];ts=stocks[stocks.open_date.dt.year==y];c=ts.groupby('asset').exposure_proxy.sum();w=c/c.sum() if c.sum() else c
  a['annual'].append({'year':y,'return':float(np.expm1(np.log1p(z).sum())),'max_dd':float(((1+z).cumprod()/np.maximum(1,(1+z).cumprod().cummax())-1).min()),'symbols':int(ts.asset.nunique()),'trade_count':len(ts),'top3_share':float(w.nlargest(3).sum()) if len(w) else None,'semis_share':float(w[w.index.isin(semis)].sum()) if len(w) else None})
 a['final_equity']=float(200000*(1+r).prod());a['trades_by_symbol']=stocks.asset.value_counts().to_dict();result[name]=a
assert ret['base'].index.equals(ret['research'].index);diff=np.log1p(ret['research'])-np.log1p(ret['base']);rng=np.random.default_rng(20261105);n=len(diff);arr=diff.to_numpy();k=int(np.ceil(n/21));boot=np.empty(2000)
for i in range(2000):
 starts=rng.integers(0,n-21+1,size=k);idx=(starts[:,None]+np.arange(21)).reshape(-1)[:n];boot[i]=arr[idx].mean()*252
result['paired']={'days':n,'ann_log_excess':float(diff.mean()*252),'block21_ci95':np.quantile(boot,[.025,.975]).tolist(),'daily_returns_equal_pct':float((abs(ret['base']-ret['research'])<1e-12).mean()),'positive_years':sum(x['return']>y['return'] for x,y in zip(result['research']['annual'],result['base']['annual']))}
months={}; calls=0
rx=re.compile(r'research SPY selector month=(\d{4}-\d\d-\d\d) metric=(\w+) lookback=(\d+) selected=(.*?) audit=(.*)$')
for line in logs['research'].open():
 m=rx.search(line)
 if not m:continue
 calls+=1
 if m[1] not in months:
  selected=ast.literal_eval(m[4]);audit=ast.literal_eval(m[5]);syms=audit.get('symbols',{});months[m[1]]={'selected':selected,'n':len(selected),'eligible':sum('value' in v for v in syms.values()),'above':sum(bool(v.get('accepted')) for v in syms.values()),'reason':audit.get('reason'),'spy':audit.get('spy')}
counts=Counter()
for line in logs['base'].open():
 m=re.search(r'Monthly regime_switch universe (\d{4}-\d\d): regime=(\w+) selected=(\d+)',line)
 if m:counts[m[2]]+=1
valid=[v for v in months.values() if v['reason'] is None];size=[v['n'] for v in valid];above=[v['above'] for v in valid];elig=[v['eligible'] for v in valid]
result['selection']={'research_selector_calls':calls,'fallback_months_logged':len(months),'first_log':next(iter(months.items())) if months else None,'insufficient_months':sum(v['reason'] is not None for v in months.values()),'fallback_valid_months':len(valid),'selected_min_median_max':[min(size),float(np.median(size)),max(size)] if size else [],'above_min_median_max':[min(above),float(np.median(above)),max(above)] if above else [],'eligible_min_median_max':[min(elig),float(np.median(elig)),max(elig)] if elig else [],'baseline_switch_log_calls':dict(counts),'fallback_monthly_counts':{k:{z:v[z] for z in ('n','above','eligible','reason')} for k,v in months.items()}}
Path(__file__).with_name('analisi.json').write_text(json.dumps(result,indent=2,allow_nan=False)+'\n');print(json.dumps({k:v for k,v in result.items() if k in ('paired','selection')},indent=2)[:2000]);print('results saved')
