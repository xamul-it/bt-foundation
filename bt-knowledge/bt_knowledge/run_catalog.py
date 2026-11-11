"""Offline run metadata catalog plus shared OpenSearch semantic retrieval."""
import json
import math
import re
import sqlite3
from pathlib import Path

from .config import Settings
from .lifecycle import ServiceUnavailable

RUN_INDEX = 'bt-knowledge-runs-v1'
SCALARS = {'id', 'run_id', 'strategy', 'strategy_family', 'status', 'mode', 'parameter'}


class RunCatalog:
    def __init__(self, settings=None):
        self.settings = settings or Settings()
        self.path = self.settings.runtime / 'runs.sqlite'

    def connect(self):
        if not self.path.exists():
            raise ServiceUnavailable('Catalogo dei run non ancora acquisito')
        return sqlite3.connect(f'file:{self.path}?mode=ro', uri=True)

    def status(self):
        if not self.path.exists():
            return {'runs': 0, 'state': 'not_indexed'}
        with self.connect() as db:
            coverage = json.loads(db.execute("SELECT value FROM meta WHERE key='coverage'").fetchone()[0])
            facets = {key: [{'value': value, 'count': count} for value, count in db.execute(
                f'SELECT {key},count(*) FROM runs GROUP BY {key} ORDER BY {key}') if value]
                for key in ['strategy', 'strategy_family', 'status', 'mode']}
        return {**coverage, 'facets': facets}

    def _where(self, filters):
        if not isinstance(filters, dict):
            raise ValueError('Filtri run non validi')
        clauses, args = [], []
        for key, value in filters.items():
            if key in SCALARS:
                if not isinstance(value, str) or not value or len(value) > 1000:
                    raise ValueError('Valore filtro run non valido')
                if key == 'parameter':
                    if not re.fullmatch(r'[A-Za-z_]\w*', value): raise ValueError('Parametro non valido')
                    clauses.append("json_type(payload, ?) IS NOT NULL")
                    args.append('$.resolved_parameters.' + value)
                else:
                    clauses.append(key + ' = ?'); args.append(value)
            elif key in {'start', 'end'}:
                values = value if isinstance(value, dict) else {'eq': value}
                for op, target in values.items():
                    if op not in {'eq', 'gt', 'gte', 'lt', 'lte'} or not isinstance(target, str): raise ValueError('Periodo run non valido')
                    from datetime import date
                    date.fromisoformat(target)
                    clauses.append(key + ' ' + {'eq': '=', 'gt': '>', 'gte': '>=', 'lt': '<', 'lte': '<='}[op] + ' ?'); args.append(target)
            elif key.startswith('param.') or key.startswith('metric.') or key.startswith('input.'):
                name = key.split('.', 1)[1]
                if not re.fullmatch(r'[A-Za-z_]\w*', name): raise ValueError('Nome parametro o metrica non valido')
                values = value if isinstance(value, dict) else {'eq': value}
                for op, target in values.items():
                    if op not in {'eq', 'gt', 'gte', 'lt', 'lte'} or not isinstance(target, (str, int, float, bool)):
                        raise ValueError('Intervallo parametro non valido')
                    if isinstance(target, float) and not math.isfinite(target): raise ValueError('Numero non finito')
                    section = 'resolved_parameters' if key.startswith('param.') else 'artifact_inputs' if key.startswith('input.') else 'metrics'
                    clauses.append('json_extract(payload, ?) ' + {'eq': '=', 'gt': '>', 'gte': '>=', 'lt': '<', 'lte': '<='}[op] + ' ?')
                    args.extend(['$.' + section + '.' + name, target])
            else:
                raise ValueError('Filtro run non supportato: ' + key)
        return (' AND '.join(clauses) or '1=1'), args

    def find(self, filters=None, query='', limit=30, offset=0):
        if type(limit) is not int or not 1 <= limit <= 100 or type(offset) is not int or offset < 0:
            raise ValueError('Paginazione run non valida')
        if not isinstance(query, str) or len(query) > 2000: raise ValueError('Ricerca run non valida')
        where, args = self._where({} if filters is None else filters)
        if query.strip():
            terms = re.findall(r'[\w.-]+', query, re.UNICODE)
            if terms:
                phrase = ' AND '.join('"' + t.replace('"', '""') + '"' for t in terms)
                where += ' AND id IN (SELECT id FROM run_text WHERE run_text MATCH ?)'; args.append(phrase)
        with self.connect() as db:
            total = db.execute('SELECT count(*) FROM runs WHERE ' + where, args).fetchone()[0]
            rows = db.execute('SELECT payload FROM runs WHERE ' + where + ' ORDER BY updated_at DESC,id LIMIT ? OFFSET ?', [*args, limit, offset]).fetchall()
        return {'runs': [self.summary(json.loads(r[0])) for r in rows], 'total': total, 'offset': offset, 'limit': limit,
                'notice': 'Parametri effettivi solo dai campi param_* degli artefatti; richieste e metriche conservano provenienza e unità.'}

    def get(self, identity):
        if not isinstance(identity, str) or len(identity) > 1000: raise ValueError('Identità run non valida')
        with self.connect() as db:
            row = db.execute('SELECT payload FROM runs WHERE id=?', (identity,)).fetchone()
        if not row:
            from .engine import SourceNotFound
            raise SourceNotFound('Run non presente nel catalogo')
        return json.loads(row[0])

    @staticmethod
    def summary(row):
        return {key: row.get(key) for key in ['id', 'run_id', 'strategy', 'strategy_family', 'status', 'mode',
                'start', 'end', 'job_ids', 'metrics', 'metric_units', 'metrics_source', 'parameter_state', 'updated_at', 'source_count', 'study_documents']}

    def search(self, query, filters=None, mode='hybrid', limit=30, offset=0):
        if not isinstance(mode,str) or mode not in {'keyword','hybrid','semantic'}:
            raise ValueError('Modalità di ricerca run non valida')
        if mode == 'keyword' or not query:
            return self.find(filters, query, limit, offset)
        if mode not in {'hybrid','semantic'} or not isinstance(query,str) or len(query)>2000:
            raise ValueError('Modalità o ricerca run non valida')
        if type(limit) is not int or not 1<=limit<=100 or type(offset) is not int or offset<0:
            raise ValueError('Paginazione run non valida')
        where,args=self._where({} if filters is None else filters)
        with self.connect() as db:
            candidates=dict(db.execute('SELECT id,hash FROM runs WHERE '+where,args))
        if not candidates:return {'runs':[],'total':0,'limit':limit,'offset':offset}
        from .engine import KnowledgeEngine
        engine=KnowledgeEngine(self.settings)
        clauses=[{'terms':{'id':list(candidates)}}] if filters else []
        common={'size':min(300,offset+limit+100),'_source':{'excludes':['vector','text']}}
        lists=[]
        if mode=='hybrid':
            result=engine.request(RUN_INDEX,{**common,'query':{'bool':{'filter':clauses,'must':[{'multi_match':{'query':query,'fields':['text','run_id^3','strategy^2']}}]}}})
            lists.append(result['hits']['hits'])
        result=engine.request(RUN_INDEX,{**common,'query':{'knn':{'vector':{'vector':engine.embed(query),'k':min(300,offset+limit+100),'filter':{'bool':{'filter':clauses}}}}}})
        lists.append(result['hits']['hits'])
        scores={}
        for hits in lists:
            for rank,hit in enumerate(hits,1):
                row=hit['_source'];identity=row['id']
                if candidates.get(identity)!=row.get('hash'):continue
                scores[identity]=scores.get(identity,0)+1/(60+rank)
        ordered=sorted(scores,key=lambda identity:(-scores[identity],identity))
        return {'runs':[self.summary(self.get(identity)) for identity in ordered[offset:offset+limit]],
                'total':len(ordered),'limit':limit,'offset':offset,'mode':mode,'ranked_candidates':True,
                'notice':'Risultati ordinati per pertinenza, non per qualità economica. Parametri e metriche mantengono le unità delle fonti.'}
