from pathlib import Path
import json,random
import pandas as pd
R=Path('/home/htpc/backtrader');runs={'base':R/'bt-core/out/overnight_ah/OvernightAH/simulation_9b14126fcbe74c309dd6a9ae5c3debfd/trades.json','research':R/'bt-core/out/overnight_ah_spy_research/OvernightAHSpyThreshold/simulation_c9a7e76fb84c47a38a3d2431f2c468df/trades.json'};result={}
for name,path in runs.items():
 ts=json.load(path.open());ts=[x for x in ts if x['asset'] not in ('SQQQ','QQQ','SPY')];samples=random.Random(42).sample(ts,120);cache={};errors=[];unmatched=[]
 for trade in samples:
  sym=trade['asset'];src=R/f'config-common/data/d/yahoo_adj/{sym}.parquet'
  if not src.exists():unmatched.append(sym);continue
  if sym not in cache:
   d=pd.read_parquet(src);d['key']=pd.to_datetime(d.Date,utc=True).dt.strftime('%Y-%m-%d');cache[sym]=d.set_index('key')
  day=trade['open_datetime'][:10]
  if day not in cache[sym].index:unmatched.append(f'{sym}:{day}');continue
  errors.append(abs(float(cache[sym].loc[day]['Close'])/float(trade['price'])-1))
 result[name]={'sample':len(samples),'matched':len(errors),'unmatched':len(unmatched),'max_relative_difference':max(errors,default=None),'within_1e-5':sum(x<1e-5 for x in errors),'seed':42,'scope':'entry price versus current parquet close; not proof of all prices or exact historical vintage'}
Path(__file__).with_name('verifica_prezzi.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result))
