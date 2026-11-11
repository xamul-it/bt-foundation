"""Bounded offline reconciliation of registry and result artifacts. No logs/trades read."""
import argparse
from collections import Counter
from datetime import datetime
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import time

from .classification import strategy_families
from .config import Settings
from .lifecycle import atomic_json, read_json, SearchLifecycle, ServiceUnavailable
from .run_catalog import RUN_INDEX

SKIP = {'node_modules', '.venv', 'vendor', 'models', 'runtime', 'data', 'datasets',
        'dump', 'quotes', 'quote_logs', 'quote_logs_live', 'quote_logs_live_probe',
        'quote_logs_live_debug', 'quote_logs_live_overnight', 'recordings', 'cache',
        'frozen_development', 'snapshot', '__pycache__', 'backup', 'dist'}
CONFIG_KEYS = {'cash','mode','amount','broker','ticker','todate','fromdate','provider','strategy',
               'benchmark','stratargs','strategy_params','timeframe','compression'}


def require_metadata_memory():
    available=int(next(line.split()[1] for line in Path('/proc/meminfo').read_text().splitlines()
                       if line.startswith('MemAvailable:')))*1024
    if available<3*1024**3:
        raise ServiceUnavailable('RAM disponibile inferiore a 3 GiB: aggiornamento del catalogo rimandato')


def clean(value):
    if isinstance(value, dict):
        return {k: clean(v) for k,v in value.items() if not re.search(r'password|secret|token|api.?key|credential|account', k, re.I)}
    if isinstance(value, list): return [clean(v) for v in value]
    if isinstance(value, float) and not __import__('math').isfinite(value): return None
    if isinstance(value, (str, int, float, bool)) or value is None: return value
    return str(value)


def iso(value):
    if not value: return None
    value = str(value)
    for fmt in ['%Y-%m-%d', '%d/%m/%Y']:
        try: return datetime.strptime(value[:10], fmt).date().isoformat()
        except ValueError: pass
    return None


def row_from_result(value):
    if isinstance(value, list): return next((r for r in value if isinstance(r,dict) and ('input_strat' in r or 'strategy' in r)), None)
    if not isinstance(value, dict): return None
    if value.get('input_strat') or isinstance(value.get('strategy'), str): return value
    return next((r for r in value.values() if isinstance(r,dict) and (r.get('input_strat') or isinstance(r.get('strategy'),str))), None)


def infer_strategy(path):
    parts=path.parts
    for index in range(len(parts)-1,-1,-1):
        if parts[index]=='out' and index+1<len(parts)-1:
            if index+2<len(parts)-1 and parts[index+1][0].islower() and parts[index+2][0].isupper():
                return parts[index+1]+'.'+parts[index+2]
            return parts[index+1]
    return 'unknown'


def result_rows(value):
    if isinstance(value,list):
        return [row for item in value for row in result_rows(item)]
    if not isinstance(value,dict):return []
    if value.get('input_strat') or isinstance(value.get('strategy'),str):return [value]
    return [item for item in value.values() if isinstance(item,dict) and (item.get('input_strat') or isinstance(item.get('strategy'),str))]


def parse_results(text):
    # Old files contain concatenated JSON values or Python-style None/True/False.
    # Normalize tokens outside strings only; never execute source content.
    decoder=json.JSONDecoder();values=[];position=0;method='json'
    pattern=r'("(?:\\.|[^"\\])*")|\b(None|True|False)\b'
    normalized=re.sub(pattern,lambda m:m[1] if m[1] is not None else {'None':'null','True':'true','False':'false'}[m[2]],text)
    if normalized!=text:method='legacy_literal_tokens'
    while position<len(normalized):
        if normalized[position].isspace():position+=1;continue
        try: value,end=decoder.raw_decode(normalized,position)
        except json.JSONDecodeError:
            if values:return values,method+' + valid_prefix_unparsed_tail'
            raise
        values.append(value);position=end
    if len(values)>1:method+=' + concatenated_json'
    return values,method


def result_record(row, fallback_id):
    strategy = str(row.get('input_strat') or row.get('strategy') or 'unknown')
    run_id = str(row.get('input_id') or fallback_id)
    params = clean({k[6:]:v for k,v in row.items() if k.startswith('param_') and k != 'param_outpath'})
    configuration = clean({k[6:]:v for k,v in row.items() if k.startswith('input_') and k[6:] in CONFIG_KEYS})
    metrics = {k: row[k] for k in ['PNL', 'PNL_money', 'Sharpe', 'SQN', 'trades', 'TimeReturn'] if k in row}
    families = strategy_families(strategy)
    return {'run_id': run_id, 'strategy': strategy, 'strategy_family': families[0] if families else strategy,
            'status': 'output_present', 'mode': 'live' if row.get('input_live') else str(row.get('input_mode') or 'unknown'),
            'start': iso(row.get('input_fromdate') or row.get('fromdate')), 'end': iso(row.get('input_todate') or row.get('todate')),
            'artifact_inputs': clean({k[6:]:v for k,v in row.items() if k.startswith('input_') and k[6:] in CONFIG_KEYS|{'commission','slippage','margin_leverage','margin_rate','broker_coc','broker_coo','slip_open','slip_match','slip_limit','slip_out'}}),
            'resolved_parameters': params, 'requested_configurations': [configuration] if configuration else [],
            'parameter_state': 'artifact_param_fields' if params else 'requested_only_or_unknown',
            'metrics': clean(metrics), 'metric_units': {k: 'producer_native_unverified' for k in metrics},
            'metrics_source': 'results artifact; raw producer labels, no recomputation',
            'producer_provenance': clean(row.get('provenance', {})), 'job_ids': [], 'sources': []}


def database_rows():
    # Environment is supplied by the operator, or read privately from the running API.
    env = dict(os.environ)
    if not any(env.get(k) for k in ['LIVE_EVENTS_DB_DSN','WATCHTOWER_DB_DSN','DATABASE_URL']):
        import subprocess
        pid = subprocess.check_output(['systemctl','--user','show','bt-api@prod.service','-p','MainPID','--value'], text=True).strip()
        env.update(dict(x.split('=',1) for x in Path('/proc/'+pid+'/environ').read_bytes().decode().split('\0') if '=' in x))
    dsn = next(env[k] for k in ['LIVE_EVENTS_DB_DSN','WATCHTOWER_DB_DSN','DATABASE_URL'] if env.get(k))
    try: import psycopg as pg
    except ImportError: import psycopg2 as pg
    with pg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute("SELECT j.id,j.kind,j.status,j.request_snapshot,j.result,j.created_at,j.updated_at,m.performance,m.computed_at FROM watchtower_calculation_jobs j LEFT JOIN backtesting_metrics_cache m ON j.id=m.job_id WHERE j.kind <> 'market_data_refresh' ORDER BY j.created_at")
        rows = cur.fetchall()
        cur.execute('SELECT run_id,strategy,status,params,args,metadata,outpath,updated_at,run_type FROM runs WHERE NOT is_deleted')
        legacy = cur.fetchall()
    return rows, legacy


def candidates(settings, coverage):
    roots = [settings.workspace / 'out', settings.workspace / 'bt-strategy-test', settings.workspace / 'research']
    visited = set(); count = 0
    for root in roots:
        actual = root.resolve()
        if not actual.is_dir(): continue
        stack = [(actual, 0)]
        while stack:
            folder, depth = stack.pop()
            if folder in visited: continue
            visited.add(folder); count += 1
            if count > 30000: raise RuntimeError('Directory budget exceeded; no catalog published')
            try: entries = list(os.scandir(folder))
            except OSError as exc:
                coverage['directory_errors'].append({'path':str(folder),'error':str(exc)}); continue
            canonical = any(e.name == 'results.json' and e.is_file(follow_symlinks=False) for e in entries)
            result_files = [e for e in entries if e.is_file(follow_symlinks=False) and (e.name == 'results.json' or re.fullmatch(r'results-.*\.json',e.name))]
            # In a run directory canonical results.json is current; snapshots remain counted as versions.
            if canonical:
                chosen = result_files
                coverage['snapshot_alias_files'] += sum(e.name != 'results.json' for e in result_files)
            else: chosen = result_files
            for entry in chosen:
                yield Path(entry.path)
            if depth >= 20:
                coverage['depth_bound_directories'].append(str(folder)); continue
            for entry in entries:
                if entry.is_dir(follow_symlinks=False) and not entry.name.startswith('.') and entry.name not in SKIP and not entry.name.startswith('rag_poc_'):
                    # Known run directories contain output only; no need to descend into ticker archives.
                    stack.append((Path(entry.path),depth+1))
        coverage['roots'].append(str(actual))
    coverage['directories_visited'] = count


def card(row):
    return '\n'.join([f"Run {row['run_id']} · {row['strategy']} · {row['strategy_family']}",
       f"Mode {row.get('mode')} status {row.get('status')} period {row.get('start')} {row.get('end')}",
       'Resolved parameters: ' + json.dumps(row.get('resolved_parameters',{}),ensure_ascii=False,sort_keys=True)[:9000],
       'Requested: ' + json.dumps(row.get('requested_configurations',[]),ensure_ascii=False,sort_keys=True)[:3000],
       'Metrics (source units): ' + json.dumps(row.get('metrics',{}),ensure_ascii=False,sort_keys=True),
       'Studies: ' + ' '.join(row.get('study_documents',[]))[:2000]])


def ingest(settings=None):
    require_metadata_memory()
    settings = settings or Settings(); settings.runtime.mkdir(parents=True,exist_ok=True)
    coverage = {'started_at':time.time(),'roots':[],'directory_errors':[],'result_errors':[],
                'depth_bound_directories':[],'snapshot_alias_files':0,'not_trading_results':0,
                'archive_candidates':0,'registry_jobs':0,'legacy_registry_rows':0,'parser_warnings':[],'empty_result_files':0}
    records = {}; priority = {}; cached_sources = {}
    old_path = settings.runtime / 'runs.sqlite'
    if old_path.exists():
        with sqlite3.connect(f'file:{old_path}?mode=ro',uri=True) as old:
            for path,mtime,size,payload in old.execute('SELECT path,mtime,size,payload FROM sources'):
                cached_sources[path] = (mtime,size,payload)
    sources = []
    def merge(row, source_priority=0):
        if 'MultiFactor' in strategy_families(row['strategy']):row['strategy_family']='MultiFactor'
        fingerprint=hashlib.sha256(json.dumps(row.get('resolved_parameters',{}),sort_keys=True,ensure_ascii=False).encode()).hexdigest()
        row['parameter_fingerprint']=fingerprint
        inputs={k:v for k,v in row.get('artifact_inputs',{}).items() if k not in {'strategy','fromdate','todate','stratargs','strategy_params'}}
        configuration_fingerprint=hashlib.sha256(json.dumps({'parameters':row.get('resolved_parameters',{}),'inputs':inputs,'start':row.get('start'),'end':row.get('end')},sort_keys=True,ensure_ascii=False).encode()).hexdigest()
        row['configuration_fingerprint']=configuration_fingerprint
        identity = hashlib.sha256((row['strategy']+'\0'+row['run_id']+'\0'+configuration_fingerprint).encode()).hexdigest()
        row['id'] = identity
        previous = records.get(identity)
        if previous:
            combined_sources = previous.get('sources',[]) + row.get('sources',[])
            jobs = sorted(set(previous.get('job_ids',[]) + row.get('job_ids',[])))
            requested = previous.get('requested_configurations',[]) + row.get('requested_configurations',[])
            if source_priority >= priority[identity]:
                keep_status = previous.get('registry_status')
                if keep_status: row.update(status=keep_status,registry_status=keep_status)
                row.update(sources=combined_sources,job_ids=jobs,requested_configurations=requested)
            else:
                previous.update(sources=combined_sources,job_ids=jobs,requested_configurations=requested)
                row = previous
        records[identity] = row; priority[identity] = max(source_priority,priority.get(identity,-1))
    jobs, legacy = database_rows()
    coverage.update(registry_jobs=len(jobs),legacy_registry_rows=len(legacy))
    for job_id,kind,status,request,result,created,updated,metrics,computed in jobs:
        request = request or {}; result = result or {}; config=request.get('configuration') or {}
        block = row_from_result(result.get('results',{})) or {}
        row = result_record(block, result.get('run_id') or 'job:'+str(job_id))
        row['strategy'] = str(result.get('strategy') or config.get('strategy') or row['strategy'])
        family = strategy_families(row['strategy']); row['strategy_family'] = family[0] if family else row['strategy']
        row.update(status=status,registry_status=status,kind=kind,job_ids=[str(job_id)],updated_at=str(updated),
                   requested_configurations=[clean({k:v for k,v in config.items() if k in CONFIG_KEYS})],
                   sources=[{'kind':'postgres_job','id':str(job_id),'updated_at':str(updated)}])
        if metrics:
            row['cached_metrics'] = clean(metrics)
            row['cached_metrics_computed_at'] = str(computed)
        merge(row,1)
    for run_id,strategy,status,params,args,meta,outpath,updated,mode in legacy:
        row=result_record({'strategy':strategy,'input_id':run_id,'input_mode':mode},run_id)
        row.update(status=status,registry_status=status,updated_at=str(updated),resolved_parameters=clean(params or {}),
                   parameter_state='registry_params',sources=[{'kind':'postgres_runs','id':run_id,'updated_at':str(updated)}])
        merge(row,1)
    for n,path in enumerate(candidates(settings,coverage),1):
        coverage['archive_candidates'] += 1
        try:
            stat=path.stat(); old=cached_sources.get(str(path))
            if old and old[:2]==(stat.st_mtime_ns,stat.st_size) and json.loads(old[2]).get('schema_version')==3:
                parsed=json.loads(old[2])
            else:
                if stat.st_size>2*1024**2: raise ValueError('Result file exceeds 2 MiB limit')
                raw=path.read_bytes(); values,parser=parse_results(raw.decode('utf-8'))
                blocks=[row for value in values for row in result_rows(value)]
                if not blocks:
                    if all(value in ({},[]) for value in values):
                        strategy=infer_strategy(path)
                        stub=result_record({'strategy':strategy,'input_mode':'unknown'},'empty_result:'+str(path.relative_to(settings.workspace/'out') if path.is_relative_to(settings.workspace/'out') else path))
                        stub.update(status='empty_result',parameter_state='no_result_content')
                        blocks=None;empty_records=[stub]
                    else:
                        coverage['not_trading_results'] += 1; continue
                else:empty_records=None
                parsed={'schema_version':3,'records':empty_records if empty_records is not None else [result_record(block,path.parent.name if path.name=='results.json' else path.stem) for block in blocks],
                        'source_sha256':hashlib.sha256(raw).hexdigest(),'parser':parser}
            coverage['empty_result_files'] += sum(item.get('status')=='empty_result' for item in parsed['records'])
            if 'unparsed' in parsed.get('parser',''):coverage['parser_warnings'].append({'path':str(path),'parser':parsed['parser']})
            sources.append((str(path),stat.st_mtime_ns,stat.st_size,json.dumps(parsed,ensure_ascii=False)))
            for number,item in enumerate(parsed['records']):
                row=dict(item);row['sources']=[{'kind':'result_artifact','path':str(path),'sha256':parsed['source_sha256'],'mtime_ns':stat.st_mtime_ns,'size':stat.st_size,'record_index':number,'parser':parsed['parser']}]
                row['updated_at']=datetime.fromtimestamp(stat.st_mtime).isoformat()
                merge(row,(3 if path.name=='results.json' else 2)+stat.st_mtime/1e10)
        except (OSError,ValueError,TypeError) as exc:
            coverage['result_errors'].append({'path':str(path),'error':str(exc)})
            stub=result_record({'strategy':path.parent.name,'input_mode':'unknown'},'unreadable:'+path.stem)
            stub.update(status='unreadable_result',parse_error=str(exc),updated_at='',sources=[{'kind':'unreadable_result','path':str(path)}])
            merge(stub,0)
        if n%250==0:
            require_metadata_memory()
            print(json.dumps({'phase':'results','files':n,'runs':len(records),'errors':len(coverage['result_errors'])}),flush=True)
    # An unresolved registry receipt can join one unambiguous artifact variant.
    groups={}
    for identity,row in records.items(): groups.setdefault((row['strategy'],row['run_id']),[]).append(identity)
    for identities in groups.values():
        artifacts=[i for i in identities if records[i].get('resolved_parameters')]
        unresolved=[i for i in identities if not records[i].get('resolved_parameters') and records[i].get('job_ids')]
        if len(artifacts)==1:
            target=records[artifacts[0]]
            for identity in unresolved:
                original=records.pop(identity)
                target['job_ids']=sorted(set(target['job_ids']+original['job_ids']))
                target['sources']+=original['sources']
                target['requested_configurations']+=original['requested_configurations']
                target.update(status=original['status'],registry_status=original['status'])
    if coverage['depth_bound_directories']:
        raise RuntimeError('Depth budget reached; catalog not published: inspect coverage')
    # Cached metrics remain authoritative and explicit about producer units.
    for row in records.values():
        row['sources']=list({json.dumps(v,sort_keys=True):v for v in row['sources']}.values())
        row['source_count']=len(row['sources'])
        # Recover per-job cache even when the canonical artifact won the merge.
        jobset=set(row['job_ids'])
        matched=[j for j in jobs if str(j[0]) in jobset and j[7]]
        if matched:
            metric=matched[-1];row['metrics']=clean(metric[7]);row['metrics_source']='backtesting_metrics_cache:'+str(metric[0]);row['cached_metrics_computed_at']=str(metric[8])
            row['metric_units']={k:('percent' if k.endswith('_pct') else 'count' if k in {'trades','sessions'} else 'currency' if k.endswith('_equity') else 'ratio') for k in row['metrics']}
        row.setdefault('updated_at','');row['study_documents']=[]
    state=read_json(settings.state_path,{'documents':{}})
    by_run_id={}
    for row in records.values():
        if len(row['run_id'])>=8: by_run_id.setdefault(row['run_id'],[]).append(row)
    for path,doc in state['documents'].items():
        if doc.get('catalog_kind')=='study_card': continue
        full=(settings.workspace/path).resolve()
        if full.is_relative_to(settings.workspace) and full.is_file():
            tokens=set(re.findall(r'[A-Za-z0-9][A-Za-z0-9_.:-]{7,}',full.read_text()))
            for identity in tokens & by_run_id.keys():
                for row in by_run_id[identity]: row['study_documents'].append(path)
    coverage.update(runs=len(records),completed_at=time.time(),resolved_parameters=sum(bool(r.get('resolved_parameters')) for r in records.values()),
                    metrics_cached=sum(str(r.get('metrics_source','')).startswith('backtesting_metrics_cache:') for r in records.values()),
                    statuses=dict(Counter(r['status'] for r in records.values())))
    # Publish atomically after successful bounded discovery; originals are never changed.
    temp=settings.runtime/('runs.sqlite.'+str(os.getpid())+'.tmp')
    with sqlite3.connect(temp) as db:
        db.execute('CREATE TABLE runs (id TEXT PRIMARY KEY,run_id TEXT,strategy TEXT,strategy_family TEXT,status TEXT,mode TEXT,start TEXT,end TEXT,updated_at TEXT,payload TEXT,card TEXT,hash TEXT)')
        db.execute('CREATE TABLE sources (path TEXT PRIMARY KEY,mtime INTEGER,size INTEGER,payload TEXT)')
        db.execute('CREATE TABLE meta (key TEXT PRIMARY KEY,value TEXT)')
        db.execute('CREATE VIRTUAL TABLE run_text USING fts5(id UNINDEXED,text,tokenize="unicode61 tokenchars _")')
        for row in records.values():
            text=card(row);h=hashlib.sha256(text.encode()).hexdigest()
            db.execute('INSERT INTO runs VALUES (?,?,?,?,?,?,?,?,?,?,?,?)',tuple(row.get(k) for k in ['id','run_id','strategy','strategy_family','status','mode','start','end','updated_at'])+(json.dumps(row,ensure_ascii=False),text,h))
            db.execute('INSERT INTO run_text VALUES (?,?)',(row['id'],text))
        for field in ['run_id','strategy','strategy_family','status','mode']:
            db.execute('CREATE INDEX runs_'+field+' ON runs ('+field+')')
        db.execute('CREATE INDEX runs_recent ON runs (updated_at DESC,id)')
        db.executemany('INSERT INTO sources VALUES (?,?,?,?)',sources)
        db.execute('INSERT INTO meta VALUES (?,?)',('coverage',json.dumps(coverage)))
    temp.replace(old_path);atomic_json(settings.runtime/'run-coverage.json',coverage)
    print(json.dumps(coverage),flush=True)
    return coverage


def index(settings=None):
    settings=settings or Settings()
    with sqlite3.connect(f'file:{settings.runtime / "runs.sqlite"}?mode=ro',uri=True) as metadata:
        current_hashes=dict(metadata.execute('SELECT id,hash FROM runs'))
    prior_hashes=read_json(settings.runtime/'run-index-state.json',{})
    if current_hashes and current_hashes==prior_hashes:
        print(json.dumps({'indexed_runs':len(current_hashes),'changed':0,'removed':0,'search_woken':False}),flush=True)
        return
    from .engine import KnowledgeEngine
    import requests
    # Use the existing normalized model/hash cache; do not create a second model.
    import sys
    sys.path.insert(0,str(settings.data/'phase2'))
    from knowledge import embed
    engine=KnowledgeEngine(settings)
    with SearchLifecycle(settings).use():
        engine.embed('verifica modello del catalogo run')
        response=requests.get(settings.endpoint+'/'+RUN_INDEX,timeout=20)
        created=response.status_code==404
        if created:
            properties={k:{'type':'keyword'} for k in ['id','run_id','strategy','strategy_family','status','mode','parameters','hash']}
            properties.update(text={'type':'text'},vector={'type':'knn_vector','dimension':768,'method':{'name':'hnsw','engine':'lucene','space_type':'cosinesimil'}})
            response=requests.put(settings.endpoint+'/'+RUN_INDEX,json={'settings':{'number_of_shards':1,'number_of_replicas':0,'index.knn':True},'mappings':{'dynamic':False,'properties':properties}},timeout=30)
        response.raise_for_status()
        with sqlite3.connect(f'file:{settings.runtime / "runs.sqlite"}?mode=ro',uri=True) as db:
            rows=db.execute('SELECT payload,card,hash FROM runs').fetchall()
        last={} if created else read_json(settings.runtime/'run-index-state.json',{})
        changed=[(json.loads(payload),text,h) for payload,text,h in rows if last.get(json.loads(payload)['id'])!=h]
        for offset in range(0,len(changed),16):
            batch=changed[offset:offset+16]; vectors=embed([v[1] for v in batch],'g311');body=[]
            for (row,text,h),vector in zip(batch,vectors):
                doc={k:row.get(k) for k in ['id','run_id','strategy','strategy_family','status','mode']}
                doc.update(parameters=list(row.get('resolved_parameters',{})),hash=h,text=text,vector=vector)
                body.extend([json.dumps({'index':{'_id':row['id']}}),json.dumps(doc)])
            r=requests.post(settings.endpoint+'/'+RUN_INDEX+'/_bulk',data='\n'.join(body)+'\n',headers={'Content-Type':'application/x-ndjson'},timeout=60);r.raise_for_status()
            if r.json().get('errors'):raise RuntimeError('Run bulk failed; index state not promoted')
            if offset%160==0: print(json.dumps({'phase':'embeddings','done':offset+len(batch),'changed':len(changed)}),flush=True)
        current={json.loads(p)['id']:h for p,t,h in rows}
        removed=set(last)-set(current)
        removed_list=sorted(removed)
        for offset in range(0,len(removed_list),500):
            body='\n'.join(json.dumps({'delete':{'_id':key}}) for key in removed_list[offset:offset+500])+'\n'
            r=requests.post(settings.endpoint+'/'+RUN_INDEX+'/_bulk',data=body,headers={'Content-Type':'application/x-ndjson'},timeout=30);r.raise_for_status()
            if r.json().get('errors'):raise RuntimeError('Run deletion bulk failed')
        r=requests.post(settings.endpoint+'/'+RUN_INDEX+'/_refresh',timeout=20);r.raise_for_status()
        atomic_json(settings.runtime/'run-index-state.json',current)
        print(json.dumps({'indexed_runs':len(current),'changed':len(changed),'removed':len(removed)}),flush=True)


def refresh(settings=None):
    import fcntl
    settings=settings or Settings();settings.runtime.mkdir(parents=True,exist_ok=True)
    with (settings.runtime/'run-refresh.lock').open('a') as lock:
        try:fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        except BlockingIOError:return
        state={'state':'running','pid':os.getpid(),'started_at':time.time(),'phase':'metadata'}
        atomic_json(settings.runtime/'run-refresh.json',state)
        try:
            coverage=ingest(settings)
            state.update(phase='index',runs=coverage['runs']);atomic_json(settings.runtime/'run-refresh.json',state)
            index(settings)
            state.update(state='completed',finished_at=time.time(),phase='done')
        except Exception as exc:
            state.update(state='failed',finished_at=time.time(),error=str(exc));raise
        finally:atomic_json(settings.runtime/'run-refresh.json',state)


if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('action',choices=['ingest','index','refresh']);args=parser.parse_args()
    {'ingest':ingest,'index':index,'refresh':refresh}[args.action]()
