import ast,json,re
from collections import Counter
from pathlib import Path
import numpy as np,pandas as pd
R=Path('/home/htpc/backtrader');O=R/'bt-core/out';P=Path(__file__).parent
runs={'base':O/'overnight_ah_multifactor/OvernightAHMultiFactorSemisSwitch/simulation_50c70a6cdfa64adbbe815c18c9f7819b','research':O/'overnight_ah_spy_research/MultiFactorSpyThreshold/simulation_0f328a4fefba450386bce6f7a072c4e5'}
logs={'base':R/'out/watchtower-calculations/50c70a6c-dfa6-4adb-be81-5c18c9f7819b.log','research':R/'out/watchtower-calculations/0f328a4f-efba-4503-86bc-e6f7a072c4e5.log'}
semis=set('NVDA,AMD,AVGO,MU,ASML,MRVL,ARM,AMAT,LRCX,KLAC,MCHP,ADI,TXN,ON,INTC,GFS'.split(',')); static=set('NVDA,AVGO,MU,AMD,MSTR,CEG,ASML,MRVL,ARM,MELI'.split(','))
returns={};result={}
for name,p in runs.items():
 r=pd.read_csv(p/'returns.csv',parse_dates=['index']).set_index('index')['return'].sort_index();assert r.notna().all() and not r.index.has_duplicates and (r>-1).all();returns[name]=r
 trades=pd.DataFrame(json.loads((p/'trades.json').read_text()));trades['open_date']=pd.to_datetime(trades.open_datetime).dt.normalize();trades['close_date']=pd.to_datetime(trades.close_datetime).dt.normalize();trades['notional']=trades['value'].abs();stocks=trades[~trades.asset.isin(['SPY','QQQ','SQQQ'])].copy();eq=200000*(1+r).cumprod().shift(1,fill_value=1);stocks['entry_equity']=stocks.open_date.map(eq);stocks['normalized']=stocks.notional/stocks.entry_equity;missing=int(stocks.entry_equity.isna().sum());stocks=stocks[stocks.entry_equity.notna()];weight=stocks.groupby('asset').normalized.sum();weight=weight/weight.sum();day=stocks.groupby('open_date').asset.nunique()
 def metrics(x):
  w=(1+x).cumprod();dd=w/np.maximum(1,w.cummax())-1;dur=(dd<-1e-12).groupby((dd>=-1e-12).cumsum()).cumsum();vol=x.std(ddof=1)*np.sqrt(252);down=np.sqrt(np.minimum(x.to_numpy(),0).dot(np.minimum(x.to_numpy(),0))/len(x));return {'n':len(x),'gain':float(w.iloc[-1]-1),'cagr':float(np.expm1(np.log1p(x).mean()*252)),'ann_log':float(np.log1p(x).mean()*252),'dd':float(dd.min()),'dd_duration':int(dur.max()),'vol':float(vol),'sharpe':float(x.mean()*252/vol),'sortino':float(x.mean()*np.sqrt(252)/down),'var95':float(-x.quantile(.05))}
 z=metrics(r);z.update({'stock_trades':len(stocks),'closed_trades':len(trades),'unique':int(stocks.asset.nunique()),'missing_equity':missing,'mean_daily_count':float(day.mean()),'median_daily_count':float(day.median()),'days_active':len(day),'weights':weight.sort_values(ascending=False).head(12).to_dict(),'top3':float(weight.nlargest(3).sum()),'top5':float(weight.nlargest(5).sum()),'semis':float(weight[weight.index.isin(semis)].sum()),'static':float(weight[weight.index.isin(static)].sum()),'effective':float(1/(weight**2).sum()),'gross_win_rate':float((stocks.pnl>0).mean()),'first_trade':str(stocks.open_date.min().date()),'last_trade':str(stocks.open_date.max().date()),'final_equity':float(200000*(1+r).prod()),'years':[],'periods':{}})
 for year in range(2000,2027):
  x=r[str(year)];t=stocks[stocks.open_date.dt.year==year];w=t.groupby('asset').normalized.sum();w=w/w.sum() if len(w) and w.sum() else w
  z['years'].append({'year':year,'gain':metrics(x)['gain'],'dd':metrics(x)['dd'],'unique':int(t.asset.nunique()),'top3':float(w.nlargest(3).sum()) if len(w) else None})
 for label,start,end in [('2000_2009','2000-01-01','2009-12-31'),('2010_2019','2010-01-01','2019-12-31'),('2020_2026','2020-01-01','2026-11-02'),('2026','2026-01-01','2026-11-02')]:z['periods'][label]=metrics(r.loc[start:end])
 for n in (21,63,126):z[f'trailing_{n}']=metrics(r.tail(n))['gain']
 result[name]=z
assert returns['base'].index.equals(returns['research'].index);diff=np.log1p(returns['research'])-np.log1p(returns['base']);rng=np.random.default_rng(20261105);n=len(diff);a=diff.to_numpy();k=int(np.ceil(n/21));boot=np.empty(2000)
for i in range(2000):
 starts=rng.integers(0,n-20,size=k);idx=(starts[:,None]+np.arange(21)).reshape(-1)[:n];boot[i]=a[idx].mean()*252
result['paired']={'n':n,'corr':float(returns['base'].corr(returns['research'])),'ann_log_diff':float(diff.mean()*252),'bootstrap_ci95':np.quantile(boot,[.025,.975]).tolist(),'years_research_wins':sum(x['gain']>y['gain'] for x,y in zip(result['research']['years'],result['base']['years']))}
regime=Counter();months={};calls=0;missing=0;rx=re.compile(r'research MF SPY selector month=(\d{4}-\d\d-\d\d) spy=(\S+) scores=(\{.*?\}) selected=(\[.*\])')
for line in logs['base'].open():
 m=re.search(r'Monthly regime_switch universe (\d{4}-\d\d): regime=(\w+) selected=(\d+)',line)
 if m:regime[m[2]]+=1
for line in logs['research'].open():
 if 'SPY panel missing or ineligible' in line:missing+=1
 m=rx.search(line)
 if not m:continue
 calls+=1
 if m[1] not in months:
  scores=ast.literal_eval(m[3]);spy=float(m[2]);sel=ast.literal_eval(m[4]);months[m[1]]={'eligible':len(scores)-1,'above':sum(v>spy for k,v in scores.items() if k!='SPY'),'selected':len(sel)}
get=lambda key:[v[key] for v in months.values()];summary=lambda x:[int(min(x)),float(np.median(x)),int(max(x))] if x else []
result['selection']={'base_switch_calls':dict(regime),'research_calls':calls,'research_months':len(months),'eligible_min_median_max':summary(get('eligible')),'above_min_median_max':summary(get('above')),'selected_min_median_max':summary(get('selected')),'empty_months':sum(x['selected']==0 for x in months.values()),'capped_months':sum(x['above']>50 for x in months.values()),'missing_spy_panel_logged_calls':missing,'months':months}
(P/'analisi.json').write_text(json.dumps(result,indent=2,allow_nan=False)+'\n')
for name in ('base','research'):
 z=result[name]; print(name,{k:z[k] for k in ('gain','cagr','dd','dd_duration','vol','sharpe','sortino','unique','stock_trades','top3','top5','semis','effective','mean_daily_count','first_trade','final_equity','trailing_21','trailing_63')})
print('paired',result['paired']);print('selection',{k:v for k,v in result['selection'].items() if k!='months'})
