import json
import pytest
from pathlib import Path

from bt_knowledge.classification import enrich_metadata
from bt_knowledge.engine import KnowledgeEngine, digest
from bt_knowledge.run_ingest import ingest, parse_results
from bt_knowledge.run_catalog import RunCatalog


def test_document_role_does_not_assign_scientific_status():
    meta={'title':'HMA Strategy - Complete Analysis Summary','doc_type':'unknown','status':'unknown','evidence':[]}
    result=enrich_metadata('bt-strategy-test/HMA/COMPLETE_ANALYSIS_SUMMARY.md','# HMA Strategy - Complete Analysis Summary',meta)
    assert result['doc_type']=='report' and result['status']=='unknown'
    assert result['strategy_families']==['HMA']
    assert any(e['field']=='doc_type' for e in result['evidence'])
    card=enrich_metadata('cards/id.md','# Scheda',dict(meta,catalog_kind='study_card',study_id='bt-strategy-test/RMA/study_20261020'))
    assert card['doc_type']=='study_card'
    assert card['strategy_families']==['HMA']  # explicit title context is retained


def test_rma_report_mentions_hma_without_becoming_an_hma_study():
    result=enrich_metadata('bt-strategy-test/RMA/study_20261020/reports/FINAL_REPORT.md',
        '# RMAStrategy assessment\nHMA and KAMA compared.',{'title':'RMAStrategy assessment','strategies':['HMA','RMAStrategy'],'evidence':[]})
    assert result['strategy_families']==['RMA']
    assert result['strategies']==['HMA','RMAStrategy']


def test_full_document_references_preserve_relative_root_and_line_links(corpus):
    settings,state=corpus
    text='# Cooldown\n`docs/b.md:2`\n[relative](b.md#rma)\n' + 'line\n'*200
    (settings.workspace/'docs/a.md').write_text(text)
    state['documents']['docs/a.md']['sha256']=digest(text.encode())
    settings.state_path.write_text(json.dumps(state))
    value=KnowledgeEngine(settings).open_document('docs/a.md')
    assert len(value['text'].splitlines())>160
    assert value['references']['docs/b.md:2']['path']=='docs/b.md'
    assert value['references']['docs/b.md:2']['line']==2
    assert value['references']['b.md#rma']['anchor']=='rma'
    assert value['references']['b.md#rma']['path']=='docs/b.md'


def test_legacy_parser_never_changes_tokens_inside_strings():
    rows,method=parse_results('{"v":None,"text":"True None"}\n{"v":False}')
    assert rows==[{'v':None,'text':'True None'},{'v':False}]
    assert 'legacy' in method
    rows,method=parse_results('{"strategy":"weekly.RMAStrategy"}garbage')
    assert rows==[{'strategy':'weekly.RMAStrategy'}]
    assert 'unparsed_tail' in method


def test_catalog_keeps_parameter_variants_separate_and_excludes_secrets(corpus,monkeypatch):
    settings,_=corpus
    files=[]
    for index,threshold in enumerate([2,3]):
        path=settings.workspace/f'result{index}.json'
        path.write_text(json.dumps({'input_id':'same_run_id','strategy':'overnight_ah.OvernightAH',
            'param_post_up_cooldown_days':threshold,'param_secret_key':'never-store',
            'input_fromdate':'2020-01-01','input_todate':'2021-12-31'}))
        files.append(path)
    monkeypatch.setattr('bt_knowledge.run_ingest.database_rows',lambda:([],[]))
    monkeypatch.setattr('bt_knowledge.run_ingest.candidates',lambda settings,coverage: iter(files))
    result=ingest(settings)
    assert result['runs']==2
    catalog=RunCatalog(settings)
    rows=catalog.find({'run_id':'same_run_id'})['runs']
    assert len({r['id'] for r in rows})==2
    assert catalog.find({'param.post_up_cooldown_days':{'gte':3}})['total']==1
    assert catalog.find({'strategy_family':'OvernightAH'},query='post_up_cooldown_days')['total']==2
    for row in rows:
        full=catalog.get(row['id'])
        assert 'never-store' not in json.dumps(full)
        assert full['parameter_state']=='artifact_param_fields'


def test_empty_result_is_retained_without_invented_parameters_or_success(corpus,monkeypatch):
    settings,_=corpus
    path=settings.workspace/'empty.json';path.write_text('{}')
    monkeypatch.setattr('bt_knowledge.run_ingest.database_rows',lambda:([],[]))
    monkeypatch.setattr('bt_knowledge.run_ingest.candidates',lambda settings,coverage: iter([path]))
    coverage=ingest(settings)
    assert coverage['empty_result_files']==1 and coverage['not_trading_results']==0
    catalog=RunCatalog(settings)
    rows=catalog.find({'status':'empty_result'})['runs']
    assert len(rows)==1
    assert catalog.get(rows[0]['id'])['resolved_parameters']=={}
    assert catalog.get(rows[0]['id'])['metrics']=={}


def test_same_id_and_parameters_with_different_periods_remain_distinct(corpus,monkeypatch):
    settings,_=corpus;files=[]
    for number,start in enumerate(['2015-01-01','2020-01-01']):
        path=settings.workspace/f'period-{number}.json'
        path.write_text(json.dumps({'input_id':'reused_id','input_strat':'weekly.RMAStrategy',
                                   'param_fast_period':30,'input_fromdate':start,'input_margin_leverage':2}))
        files.append(path)
    monkeypatch.setattr('bt_knowledge.run_ingest.database_rows',lambda:([],[]))
    monkeypatch.setattr('bt_knowledge.run_ingest.candidates',lambda settings,coverage:iter(files))
    assert ingest(settings)['runs']==2
    assert RunCatalog(settings).find({'input.margin_leverage':{'gte':2}})['total']==2


def test_unchanged_run_index_does_not_wake_search_or_embeddings(corpus,monkeypatch):
    import sqlite3
    from bt_knowledge.run_ingest import index
    from bt_knowledge.lifecycle import atomic_json
    settings,_=corpus;path=settings.workspace/'run.json'
    path.write_text(json.dumps({'input_strat':'weekly.RMAStrategy','input_id':'same','param_fast':30}))
    monkeypatch.setattr('bt_knowledge.run_ingest.database_rows',lambda:([],[]))
    monkeypatch.setattr('bt_knowledge.run_ingest.candidates',lambda settings,coverage:iter([path]))
    ingest(settings)
    with sqlite3.connect(settings.runtime/'runs.sqlite') as db:hashes=dict(db.execute('SELECT id,hash FROM runs'))
    atomic_json(settings.runtime/'run-index-state.json',hashes)
    monkeypatch.setattr('bt_knowledge.run_ingest.SearchLifecycle',lambda *args: (_ for _ in ()).throw(AssertionError('must stay asleep')))
    index(settings)
    monkeypatch.setattr('bt_knowledge.run_ingest.require_metadata_memory',lambda: (_ for _ in ()).throw(RuntimeError('RAM unavailable')))
    with pytest.raises(RuntimeError,match='RAM unavailable'):ingest(settings)
    assert RunCatalog(settings).status()['runs']==1


def test_background_refresh_receives_database_environment_and_publishes_start_state(corpus,monkeypatch):
    from unittest.mock import Mock
    from bt_knowledge.service import KnowledgeService
    from bt_knowledge.lifecycle import read_json
    service=KnowledgeService(corpus[0])
    monkeypatch.setenv('LIVE_EVENTS_DB_DSN','postgresql://test-reader@localhost/test-only')
    monkeypatch.setenv('BROKER_API_SECRET','not-for-the-reader')
    popen=Mock(return_value=Mock(pid=12345))
    monkeypatch.setattr('subprocess.Popen',popen)
    result=service.refresh_runs()
    env=popen.call_args.kwargs['env']
    assert env['LIVE_EVENTS_DB_DSN']=='postgresql://test-reader@localhost/test-only'
    assert 'BROKER_API_SECRET' not in env
    assert result['state']=='running' and result['pid']==12345
    assert read_json(service.settings.runtime/'run-refresh.json')['state']=='running'
