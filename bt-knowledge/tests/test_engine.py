import json
from unittest.mock import Mock

import pytest

from bt_knowledge.engine import KnowledgeEngine, SourceConflict, SourceNotFound, validate_filters
from bt_knowledge.service import KnowledgeService
from bt_knowledge.lifecycle import ServiceUnavailable


def hits(state):
    return [{'_id': p, '_source': {**d, 'heading': d['title'], 'start_line': 1, 'end_line': 2}}
            for p, d in state['documents'].items()]


def test_keyword_search_deduplicates_keeps_aliases_and_never_embeds(corpus, monkeypatch):
    settings, state = corpus
    engine = KnowledgeEngine(settings)
    post = Mock(return_value={'hits': {'hits': hits(state)}})
    monkeypatch.setattr(engine, 'request', post)
    monkeypatch.setattr(engine, 'embed', Mock(side_effect=AssertionError('keyword must not embed')))
    result = engine.search('cooldown', mode='keyword')
    assert [h['path'] for h in result['hits']] == ['docs/a.md', 'docs/b.md']
    assert result['hits'][0]['aliases'] == ['docs/copy.md']
    assert result['hits'][0]['context']['lines'][0] == {'number': 1, 'text': '# Cooldown'}
    assert post.call_count == 1


def test_semantic_prefilters_and_rechecks_metadata(corpus, monkeypatch):
    settings, state = corpus
    engine = KnowledgeEngine(settings)
    post = Mock(return_value={'hits': {'hits': hits(state)}})
    monkeypatch.setattr(engine, 'request', post)
    monkeypatch.setattr(engine, 'embed', lambda _: [1] + [0] * 767)
    result = engine.search('pausa ingressi', {'parameters': 'post_up_cooldown_days'}, mode='semantic')
    assert [h['path'] for h in result['hits']] == ['docs/a.md']
    query = post.call_args.args[1]['query']
    assert query['knn']['vector']['filter']['bool']['filter'] == [{'term': {'parameters': 'post_up_cooldown_days'}}]
    assert post.call_count == 1


def test_changed_sources_and_failed_generations_are_not_searchable(corpus, monkeypatch):
    settings, state = corpus
    engine = KnowledgeEngine(settings)
    rows = hits(state)
    rows[1]['_source']['generation'] = 'unpromoted'
    (settings.workspace / 'docs/a.md').write_text('changed')
    monkeypatch.setattr(engine, 'request', lambda *args: {'hits': {'hits': rows}})
    assert [h['path'] for h in engine.search('cooldown', mode='keyword')['hits']] == ['docs/b.md']
    with pytest.raises(SourceConflict):
        engine.open_source('docs/a.md')
    with pytest.raises(SourceNotFound):
        engine.open_source('../../etc/passwd')


def test_card_dependencies_are_checked_and_catalog_reports_staleness(corpus):
    settings, state = corpus
    card = state['documents']['docs/b.md']
    card.update(catalog_kind='study_card', source_documents=[{'path': 'docs/a.md', 'sha256': state['documents']['docs/a.md']['sha256']}])
    settings.state_path.write_text(json.dumps(state))
    (settings.workspace / 'docs/a.md').unlink()
    engine = KnowledgeEngine(settings)
    assert engine.status()['freshness'] == {'changed_or_missing': 1, 'current': 1, 'dependency_changed': 1}
    with pytest.raises(SourceConflict):
        engine.open_source('docs/b.md')
    assert engine.studies()['studies'][0]['documents'] == 2


def test_sources_have_total_lines_and_bounded_paging(corpus):
    service = KnowledgeService(corpus[0])
    value = service.open_source('docs/a.md', 2, 1)
    assert value['text'] == 'post_up_cooldown_days'
    assert value['start_line'] == value['end_line'] == 2
    assert value['total_lines'] == 2
    for start, count in [(0, 1), (1, 161), (True, 1), (1, -1)]:
        with pytest.raises(ValueError):
            service.open_source('docs/a.md', start, count)


@pytest.mark.parametrize('filters', [[], {'unknown': 'x'}, {'parameters': {'gt': 'x'}},
                                     {'study_date': {'gte': 'not-a-date'}}, {'strategies': ['x']}])
def test_invalid_filters_rejected(filters):
    with pytest.raises(ValueError):
        validate_filters(filters)


def test_invalid_search_never_wakes_docker(corpus, monkeypatch):
    service = KnowledgeService(corpus[0])
    monkeypatch.setattr(service.lifecycle, 'use', Mock(side_effect=AssertionError('must not start')))
    for kwargs in [{'query': ''}, {'query': 'x', 'limit': True}, {'query': 'x', 'mode': 'random'},
                   {'query': 'x', 'filters': []}, {'query': 'x', 'mode': []},
                   {'query': 'x', 'filters': {'path': {'gte': 'x'}}}]:
        with pytest.raises(ValueError):
            service.search(**kwargs)


def test_semantic_search_refuses_model_revision_different_from_index(corpus, monkeypatch):
    engine = KnowledgeEngine(corpus[0])
    response = Mock()
    response.json.return_value = {'model_id': engine.settings.model, 'model_sha': 'different-revision'}
    monkeypatch.setattr('bt_knowledge.engine.requests.get', lambda *args, **kwargs: response)
    post = Mock(side_effect=AssertionError('must not embed with another model'))
    monkeypatch.setattr('bt_knowledge.engine.requests.post', post)
    with pytest.raises(ServiceUnavailable, match='diverso'):
        engine.embed('query')
    post.assert_not_called()


def test_known_path_cannot_be_replaced_with_outside_symlink(corpus):
    settings, _ = corpus
    source = settings.workspace / 'docs/a.md'
    source.unlink()
    source.symlink_to('/etc/passwd')
    with pytest.raises(SourceConflict):
        KnowledgeEngine(settings).open_source('docs/a.md')
