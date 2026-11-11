"""Retrospective diagnostic only: logged fallback lists versus future overnight outcomes."""
from pathlib import Path
import ast,hashlib,json,re
import numpy as np,pandas as pd
R=Path('/home/htpc/backtrader');P=Path(__file__).parent;D=R/'config-common/data/d/yahoo_adj';log=R/'out/watchtower-calculations/c9a7e76f-b84c-47a3-8a3d-2431f2c468df.log';base_result=R/'bt-core/out/overnight_ah/OvernightAH/simulation_9b14126fcbe74c309dd6a9ae5c3debfd/results.json'
r=next(iter(json.load(base_result.open()).values()));static=r['param_monthly_universe_static_symbols'].split(',');rx=re.compile(r'research SPY selector month=(\d{4}-\d\d-\d\d) metric=(\w+) lookback=(\d+) selected=(.*?) audit=(.*)$');months={}
for line in log.open():
 m=rx.search(line)
 if not m or m[1] in months:continue
 assert m[2]=='win_ratio' and int(m[3])==21
 info=ast.literal_eval(m[5]);months[m[1]]={'selection':ast.literal_eval(m[4]),'audit':info}
files={};sha={}
def load(s):
 if s in files:return files[s]
 f=D/(s+'.parquet')
 if not f.is_file():files[s]=None;return None
 sha[s]=hashlib.sha256(f.read_bytes()).hexdigest();t=pd.read_parquet(f,columns=['Date','Open','Close','Volume']);t['Date']=pd.to_datetime(t.Date,utc=True).dt.tz_convert(None).dt.normalize();t=t.set_index('Date').sort_index();assert not t.index.has_duplicates
 t=t.loc['2005-01-01':'2021-01-31'];files[s]=t;return t
spy=load('SPY');calendar=spy.index;records=[];excluded=[]
for month,entry in sorted(months.items()):
 audit=entry['audit'];
 if 'symbols' not in audit:excluded.append({'month':month,'reason':audit.get('reason','missing_audit')});continue
 start=pd.Timestamp(month); nextmonth=start+pd.offsets.MonthBegin(1);this=calendar[(calendar>=start)&(calendar<nextmonth)];future=calendar[calendar>=nextmonth]
 if len(this)<2 or not len(future):excluded.append({'month':month,'reason':'calendar_incomplete'});continue
 # Decisions are made at each close, so start on the second session's open;
 # the last overnight ends at the first open of the following month.
 opens=this[1:].append(future[:1]); closes=this
 assert len(opens)==len(closes)
 candidate=[s for s,v in audit['symbols'].items() if 'value' in v];scores={};win={}
 for s in candidate:
  t=load(s)
  if t is None:continue
  pre=t.reindex(closes);post=t.reindex(opens)
  if pre[['Close','Volume']].isna().any().any() or post[['Open','Volume']].isna().any().any():continue
  if (pre['Close']<=0).any() or (post['Open']<=0).any() or (pre.Volume<=0).any() or (post.Volume<=0).any():continue
  x=post.Open.to_numpy()/pre.Close.to_numpy()-1
  if not np.isfinite(x).all() or (x<=-1).any():continue
  scores[s]=float(np.expm1(np.log1p(x).sum()));win[s]=float((x>0).mean())
 if len(scores)<3:excluded.append({'month':month,'reason':'future_common_coverage','n':len(scores)});continue
 rank=sorted(scores,key=lambda s:(-scores[s],s)); lookup={s:(len(rank)-1-rank.index(s))/(len(rank)-1) for s in rank}
 research=[s for s in entry['selection'] if s in scores];baseline=[s for s in static if s in scores]
 past=pd.Series({s:audit['symbols'][s]['value'] for s in scores},dtype=float)
 future_gain=pd.Series(scores,dtype=float);future_win=pd.Series(win,dtype=float)
 gain_corr=past.corr(future_gain,method='spearman');win_corr=past.corr(future_win,method='spearman')
 row={'month':month,'eligible':len(candidate),'fully_covered':len(scores),'excluded_missing_future':len(candidate)-len(scores),'base_list':baseline,'research_list':research,'oracle_top10':rank[:10],
      'spearman_previous_win_to_future_gain':float(gain_corr) if np.isfinite(gain_corr) else None,
      'spearman_previous_win_to_future_win':float(win_corr) if np.isfinite(win_corr) else None}
 for K in (3,5):
  oracle=rank[:K];row[f'oracle_{K}']=float(np.mean([scores[s] for s in oracle]))
  for name,ls in [('base',baseline),('research',research)]:
   chosen=ls[:K]
   if len(chosen)<K:
    row[f'{name}_{K}']=None;continue
   row[f'{name}_{K}']={'names':chosen,'gain':float(np.mean([scores[s] for s in chosen])),
     'win':float(np.mean([win[s] for s in chosen])),'percentile':float(np.mean([lookup[s] for s in chosen])),
     'overlap':len(set(chosen)&set(oracle)),'regret':float(np.mean([scores[s] for s in oracle])-np.mean([scores[s] for s in chosen]))}
 records.append(row)
summary={}
for K in (3,5):
 paired=[r for r in records if r[f'base_{K}'] is not None and r[f'research_{K}'] is not None]
 vals={}
 for name in ('base','research'):
  v=[r[f'{name}_{K}'] for r in paired]
  vals[name]={key:float(np.mean([x[key] for x in v])) for key in ('gain','win','percentile','overlap','regret')}
 dif=[r[f'research_{K}']['gain']-r[f'base_{K}']['gain'] for r in paired]
 vals['n']=len(paired);vals['research_wins']=sum(x>1e-12 for x in dif);vals['base_wins']=sum(x< -1e-12 for x in dif);vals['equal']=len(dif)-vals['research_wins']-vals['base_wins'];vals['oracle_gain']=float(np.mean([r[f'oracle_{K}'] for r in paired])); vals['by_year']={str(y):{'n':len(v),'base_gain':float(np.mean([x[f'base_{K}']['gain'] for x in v])),'research_gain':float(np.mean([x[f'research_{K}']['gain'] for x in v])),'oracle_gain':float(np.mean([x[f'oracle_{K}'] for x in v]))} for y in range(2005,2021) if (v:=[x for x in paired if x['month'].startswith(str(y))])}
 summary[str(K)]=vals
corr_summary={}
for field in ('spearman_previous_win_to_future_gain','spearman_previous_win_to_future_win'):
 c=[r[field] for r in records if r[field] is not None]
 corr_summary[field]={'n':len(c),'mean':float(np.mean(c)),'median':float(np.median(c)),
                      'positive_months':sum(x>0 for x in c)}
output={'method':'future monthly close-to-next-open compounded per symbol; equal-weight arithmetic mean of per-symbol compounded returns; no execution/costs','input':'current parquet Yahoo adjusted, not frozen run-specific; selected symbol price spot-check required', 'months_logged':len(months),'months_usable_universe':len(records),'excluded':excluded,'records':records,'summary':summary,'cross_sectional_correlations':corr_summary,'manifest':sha,'limitations':['fallback branch only','only ex-ante research-eligible symbols with full subsequent coverage','coverage conditioned on later availability','capital sizing, actual signals, tradability, risk gates and execution ignored','selected names first K from logged lists, not necessarily actual executed orders','historical universe is today-selected, survivorship bias']}
(P/'analisi.json').write_text(json.dumps(output,indent=2,allow_nan=False)+'\n');print(json.dumps({'months_logged':len(months),'usable':len(records),'excluded':excluded,'summary':summary},indent=2)[:6500])
