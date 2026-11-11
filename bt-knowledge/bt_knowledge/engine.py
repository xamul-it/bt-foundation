"""Read-only retrieval and source navigation over the existing shared index."""
from collections import Counter, defaultdict
from datetime import date
import hashlib
import math
from pathlib import Path
import time

import requests

from .config import Settings
from .lifecycle import read_json, ServiceUnavailable

FIELDS = {'path', 'doc_type', 'strategies', 'parameters', 'status', 'study_id',
          'version_context', 'strategy_families', 'study_date', 'period_start', 'period_end'}
DATES = {'study_date', 'period_start', 'period_end'}


class SourceConflict(ValueError):
    pass


class SourceNotFound(LookupError):
    pass


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def validate_filters(filters):
    if not isinstance(filters, dict) or not set(filters) <= FIELDS:
        raise ValueError('Filtri non supportati')
    for key, value in filters.items():
        if isinstance(value, dict):
            if key not in DATES or not value or not set(value) <= {'gt', 'gte', 'lt', 'lte'}:
                raise ValueError('Intervallo filtro non valido')
            for item in value.values():
                if not isinstance(item, str):
                    raise ValueError('Data filtro non valida')
                date.fromisoformat(item)
        elif not isinstance(value, str) or not value or len(value) > 1000:
            raise ValueError('Valore filtro non valido')
        elif key in DATES:
            date.fromisoformat(value)
    return filters


class KnowledgeEngine:
    def __init__(self, settings=None):
        self.settings = settings or Settings()
        self.reload()

    def reload(self):
        self.state = read_json(self.settings.state_path, {'documents': {}, 'errors': {}, 'root': None})
        if self.state['root'] and Path(self.state['root']).resolve() != self.settings.workspace.resolve():
            raise ServiceUnavailable('Il catalogo appartiene a un workspace diverso')
        self.documents = self.state['documents']
        self.root = self.settings.workspace.resolve()
        self._source_cache = {}

    def _raw(self, path):
        if path not in self.documents:
            raise SourceNotFound('Fonte non presente nel catalogo')
        source = (self.root / path).resolve()
        if not source.is_relative_to(self.root) or not source.is_file():
            raise SourceConflict('Fonte rimossa o non accessibile nel workspace')
        raw = source.read_bytes()
        if digest(raw) != self.documents[path]['sha256']:
            raise SourceConflict('Fonte modificata: aggiornare il catalogo prima di consultarla')
        return raw

    def freshness(self, path):
        if path in self._source_cache:
            return self._source_cache[path]
        if path in self.state.get('errors', {}):
            result = 'failed'
        else:
            try:
                self._raw(path)
                result = 'current'
                for reference in self.documents[path].get('source_documents', []):
                    dependency = reference['path']
                    try:
                        valid = (dependency in self.documents and dependency not in self.state.get('errors', {})
                                 and digest(self._raw(dependency)) == reference['sha256'])
                    except SourceConflict:
                        valid = False
                    if not valid:
                        result = 'dependency_changed'
                        break
            except SourceConflict:
                result = 'changed_or_missing'
        self._source_cache[path] = result
        return result

    def summary(self, path):
        doc = self.documents[path]
        keys = ['path', 'title', 'doc_type', 'status', 'metadata_state', 'strategies', 'parameters',
                'study_id', 'version_context', 'catalog_kind', 'sha256', 'chunks',
                'study_date', 'period_start', 'period_end', 'strategy_families', 'strategy_scope', 'classification_reason']
        return {**{k: doc[k] for k in keys if k in doc}, 'freshness': self.freshness(path)}

    def matches(self, doc, filters):
        for key, value in filters.items():
            actual = doc.get(key)
            if isinstance(value, dict):
                if actual is None:
                    return False
                for op, target in value.items():
                    if not {'gt': actual > target, 'gte': actual >= target,
                            'lt': actual < target, 'lte': actual <= target}[op]:
                        return False
            elif (value not in actual if isinstance(actual, list) else value != actual):
                return False
        return True

    def status(self):
        self.reload()
        options = {}
        for field in sorted(FIELDS - {'path'}):
            counter = Counter()
            for doc in self.documents.values():
                value = doc.get(field, [])
                counter.update(value if isinstance(value, list) else [value])
            options[field] = [{'value': key, 'count': count} for key, count in sorted(counter.items())]
        pending = [d['path'] for d in read_json(self.settings.data / 'ingestion/inventory.json', [])
                   if d.get('ingestion') == 'pending_format_parser']
        return {'index': self.settings.index, 'documents': len(self.documents),
                'originals': sum(d.get('catalog_kind') == 'original_document' for d in self.documents.values()),
                'study_cards': sum(d.get('catalog_kind') == 'study_card' for d in self.documents.values()),
                'chunks': sum(d.get('chunks', 0) for d in self.documents.values()),
                'filters': options, 'last_sync_unix': self.state.get('last_sync_unix'),
                'sync_age_seconds': time.time() - self.state['last_sync_unix'] if self.state.get('last_sync_unix') else None,
                'errors': self.state.get('errors', {}),
                'freshness': dict(Counter(self.freshness(p) for p in self.documents)),
                'pending_formats': pending,
                'notice': 'Metadati e schede automatici da revisionare. I tag dei parametri indicano menzioni, non effetti studiati.'}

    def catalog(self, filters=None, offset=0, limit=30, query=''):
        self.reload()
        filters = validate_filters({} if filters is None else filters)
        paths = [p for p, d in self.documents.items() if self.matches(d, filters)
                 and (not query or query.casefold() in (p + ' ' + d.get('title', '')).casefold())]
        paths.sort(key=lambda p: (self.documents[p].get('title', p).casefold(), p))
        return {'documents': [self.summary(p) for p in paths[offset:offset + limit]],
                'total': len(paths), 'offset': offset, 'limit': limit}

    def studies(self):
        self.reload()
        groups = defaultdict(list)
        for path, doc in self.documents.items():
            if doc.get('study_id'):
                groups[doc['study_id']].append(path)
        return {'studies': [{'id': group, 'title': group.rsplit('/', 1)[-1],
                             'documents': sum(self.documents[p].get('catalog_kind') != 'study_card' for p in paths),
                             'card_path': next((p for p in paths if self.documents[p].get('catalog_kind') == 'study_card'), None),
                             'freshness': dict(Counter(self.freshness(p) for p in paths)),
                             'strategy_families': sorted({f for p in paths for f in self.documents[p].get('strategy_families', [])}),
                             'grouping_state': 'proposed'} for group, paths in sorted(groups.items())]}

    def study(self, study_id):
        self.reload()
        paths = [p for p, d in self.documents.items() if d.get('study_id') == study_id]
        if not paths:
            raise SourceNotFound('Studio non presente nel catalogo')
        return {'id': study_id, 'grouping_state': 'proposed', 'documents': [self.summary(p) for p in paths],
                'notice': 'Raggruppamento automatico dal percorso. Le schede contengono estratti, non una validazione scientifica.'}

    def open_source(self, path, start_line=1, line_count=100, _reload=True):
        if _reload:
            self.reload()
        if path not in self.documents:
            raise SourceNotFound('Fonte non presente nel catalogo')
        if self.freshness(path) != 'current':
            raise SourceConflict('Fonte o dipendenza modificata, rimossa o fallita: aggiornare il catalogo')
        raw = self._raw(path)
        lines = raw.decode('utf-8').splitlines()
        start = max(1, min(start_line, max(1, len(lines))))
        end = min(len(lines), start + line_count - 1)
        return {'path': path, 'sha256': self.documents[path]['sha256'], 'start_line': start,
                'end_line': end, 'total_lines': len(lines), 'text': '\n'.join(lines[start - 1:end]),
                'lines': [{'number': i + 1, 'text': lines[i]} for i in range(start - 1, end)],
                'metadata': self.summary(path), 'evidence': self.documents[path].get('evidence', [])}

    def request(self, index, body):
        if index not in {self.settings.index, 'bt-rag-poc2-runs', 'bt-knowledge-runs-v1'}:
            raise ValueError('Indice non consentito')
        try:
            response = requests.post(f'{self.settings.endpoint}/{index}/_search', json=body, timeout=20)
            response.raise_for_status()
            return response.json()
        except (requests.RequestException, ValueError) as exc:
            raise ServiceUnavailable('Ricerca temporaneamente non disponibile') from exc

    def embed(self, text):
        try:
            info_url = self.settings.embeddings.rsplit('/v1/embeddings', 1)[0] + '/info'
            info = requests.get(info_url, timeout=5)
            info.raise_for_status()
            model = info.json()
            if model.get('model_id') != self.settings.model or model.get('model_sha') != self.settings.revision:
                raise ServiceUnavailable('Modello embeddings diverso da quello usato per l’indice')
            response = requests.post(self.settings.embeddings,
                                     json={'input': [text], 'model': self.settings.model}, timeout=20)
            response.raise_for_status()
            vector = response.json()['data'][0]['embedding']
            if len(vector) != 768 or not all(isinstance(v, (int, float)) and math.isfinite(v) for v in vector):
                raise ValueError('Embedding non valido')
            norm = math.sqrt(sum(x * x for x in vector))
            if not norm:
                raise ValueError('Embedding nullo')
            return [v / norm for v in vector]
        except (requests.RequestException, ValueError, KeyError, IndexError, TypeError) as exc:
            raise ServiceUnavailable('Ricerca semantica non disponibile: verificare gli embeddings locali') from exc

    def search(self, query, filters=None, limit=5, mode='hybrid'):
        if not isinstance(query, str) or not query.strip() or len(query) > 2000:
            raise ValueError('Inserisci una ricerca tra 1 e 2000 caratteri')
        if mode not in {'hybrid', 'keyword', 'semantic'}:
            raise ValueError('Modalità di ricerca non valida')
        if type(limit) is not int or not 1 <= limit <= 20:
            raise ValueError('Numero risultati non valido (1–20)')
        self.reload()
        filters = validate_filters({} if filters is None else filters)
        clauses = [{'range': {k: v}} if isinstance(v, dict) else {'term': {k: v}} for k, v in filters.items()]
        common = {'size': 100, '_source': {'excludes': ['vector']}}
        lists = []
        if mode in {'hybrid', 'keyword'}:
            lexical = {'bool': {'filter': clauses, 'must': [{'multi_match': {'query': query,
                       'fields': ['text', 'title^2', 'heading^2', 'path_text^2']}}]}}
            lists.append(self.request(self.settings.index, {**common, 'query': lexical})['hits']['hits'])
        if mode in {'hybrid', 'semantic'}:
            knn = {'knn': {'vector': {'vector': self.embed(query), 'k': 100,
                                    'filter': {'bool': {'filter': clauses}}}}}
            lists.append(self.request(self.settings.index, {**common, 'query': knn})['hits']['hits'])
        scores, items = {}, {}
        for hits in lists:
            for rank, hit in enumerate(hits, 1):
                item = hit['_source']
                path = item['path']
                if (path not in self.documents or self.freshness(path) != 'current'
                        or item['generation'] != self.documents[path]['generation']
                        or not self.matches(self.documents[path], filters)):
                    continue
                key = hit['_id']
                scores[key] = scores.get(key, 0) + 1 / (60 + rank)
                items[key] = item
        result, seen = [], set()
        relations = read_json(self.settings.data / 'phase2/relations.json', [])
        for key in sorted(scores, key=lambda k: (-scores[k], items[k]['path'], items[k]['start_line'])):
            item = items[key]
            path = item['path']
            content_hash = self.documents[path]['sha256']
            if content_hash in seen:
                continue
            seen.add(content_hash)
            try:
                context = self.open_source(path, max(1, item['start_line'] - 3),
                                           min(160, item['end_line'] - item['start_line'] + 10), _reload=False)
            except SourceConflict:
                continue
            related = []
            for relation in relations:
                if relation['source'] == path:
                    target = relation['target']
                    related.append({**relation, 'status': self.freshness(target) if target in self.documents else 'not_indexed'})
            aliases = [p for p, d in self.documents.items() if p != path and d['sha256'] == content_hash
                       and self.freshness(p) == 'current']
            result.append({'path': path, 'score': scores[key], 'heading': item['heading'],
                           'context': context, 'metadata': self.summary(path), 'aliases': aliases, 'related': related})
            if len(result) >= limit:
                break
        return {'hits': result, 'filters': filters, 'mode': mode, 'sync_errors': self.state.get('errors', {}),
                'notice': 'Risultati documentali: nessuna risposta generata e nessun filtro inferito dalla domanda'}

    def open_document(self, path):
        source = self.open_source(path, 1, 1)
        raw = self._raw(path)
        if len(raw) > 2 * 1024**2:
            raise ValueError('Documento oltre il limite di lettura')
        text = raw.decode('utf-8')
        import re
        from urllib.parse import unquote
        candidates = set(re.findall(r'`([^`\n]+)`|\]\(([^)\s]+)', text))
        tokens = {value for pair in candidates for value in pair if value}
        tokens.update(p for p in self.documents if p in text)
        tokens.update(re.findall(r'(?:[\w.-]+/)+[\w.-]+\.md(?::\d+(?:-\d+)?)?(?:#[\w%.-]+)?', text))
        references = {}
        for token in tokens:
            value = unquote(token).split('#', 1)[0]
            match = re.match(r'^(.*?)(?::(\d+)(?:-\d+)?)?$', value)
            value = match[1]
            line = int(match[2]) if match[2] else 1
            if value.startswith(str(self.root) + '/'):
                value = value[len(str(self.root)) + 1:]
            choices = [value.lstrip('/'), str((Path(path).parent / value))]
            if '/' not in value:
                matches = [p for p in self.documents if Path(p).name == value]
                if len(matches) == 1:
                    choices.append(matches[0])
            for choice in choices:
                resolved = (self.root / choice).resolve()
                if not resolved.is_relative_to(self.root):
                    continue
                target = str(resolved.relative_to(self.root))
                if target in self.documents:
                    references[token] = {'path': target, 'line': line, 'freshness': self.freshness(target),
                                         'anchor': token.split('#', 1)[1] if '#' in token else ''}
                    break
        return {**source, 'text': text, 'references': references, 'total_lines': len(text.splitlines())}
