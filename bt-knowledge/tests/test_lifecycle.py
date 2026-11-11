from unittest.mock import Mock

import pytest

from bt_knowledge.lifecycle import SearchLifecycle, atomic_json, read_json


def test_idle_shutdown_is_blocked_by_active_query_and_then_expires(corpus, monkeypatch):
    settings, _ = corpus
    controller = SearchLifecycle(settings)
    atomic_json(controller.path, {'owned': True, 'instance': 'same', 'last_used': 100})
    inspect = Mock(return_value={'running': True, 'instance': 'same'})
    run = Mock()
    monkeypatch.setattr(controller, 'inspect', inspect)
    monkeypatch.setattr('bt_knowledge.lifecycle.subprocess.run', run)
    with controller.locked():
        assert controller.stop_if_idle(now=200) is True
    inspect.assert_not_called()
    assert controller.stop_if_idle(now=105) is True
    inspect.assert_not_called()
    assert controller.stop_if_idle(now=111) is False
    assert run.call_args.args[0] == ['docker', 'stop', '-t', '20', 'bt-rag-poc-search-1']
    assert read_json(controller.path)['phase'] == 'stopped'


def test_operator_restart_is_never_stopped(corpus, monkeypatch):
    controller = SearchLifecycle(corpus[0])
    atomic_json(controller.path, {'owned': True, 'instance': 'old', 'last_used': 100})
    monkeypatch.setattr(controller, 'inspect', lambda: {'running': True, 'instance': 'operator-restart'})
    run = Mock()
    monkeypatch.setattr('bt_knowledge.lifecycle.subprocess.run', run)
    assert controller.stop_if_idle(now=111) is False
    run.assert_not_called()
    assert read_json(controller.path)['owned'] is False


def test_query_failure_still_refreshes_idle_deadline(corpus, monkeypatch):
    controller = SearchLifecycle(corpus[0])
    monkeypatch.setattr(controller, 'inspect', lambda: {'running': True, 'instance': 'owned'})
    monkeypatch.setattr(controller, 'ready', lambda: None)
    monkeypatch.setattr(controller, 'ensure_worker', lambda: None)
    atomic_json(controller.path, {'owned': True, 'instance': 'owned', 'last_used': 0})
    with pytest.raises(RuntimeError, match='query failed'):
        with controller.use():
            raise RuntimeError('query failed')
    assert read_json(controller.path)['last_used'] > 0
    assert read_json(controller.path)['owned'] is True


def test_existing_operator_container_is_not_adopted(corpus, monkeypatch):
    controller = SearchLifecycle(corpus[0])
    monkeypatch.setattr(controller, 'inspect', lambda: {'running': True, 'instance': 'manual'})
    monkeypatch.setattr(controller, 'ready', lambda: None)
    ensure = Mock()
    monkeypatch.setattr(controller, 'ensure_worker', ensure)
    with controller.use():
        pass
    ensure.assert_not_called()
    assert read_json(controller.path)['owned'] is False
