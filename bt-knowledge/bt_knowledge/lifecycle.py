"""Cross-process idle shutdown. A query/update holds the same lock as the reaper."""
from contextlib import contextmanager
import fcntl
import json
import os
from pathlib import Path
import subprocess
import sys
import time

import requests

from .config import Settings


class ServiceUnavailable(RuntimeError):
    pass


def read_json(path, default=None):
    try:
        return json.loads(path.read_text())
    except FileNotFoundError:
        return default


def atomic_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + f'.{os.getpid()}.tmp')
    temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2) + '\n')
    temporary.replace(path)


class SearchLifecycle:
    def __init__(self, settings=None):
        self.settings = settings or Settings()
        self.path = self.settings.runtime / 'lifecycle.json'

    @contextmanager
    def locked(self, blocking=True):
        self.settings.operation_lock.parent.mkdir(parents=True, exist_ok=True)
        with self.settings.operation_lock.open('a') as handle:
            fcntl.flock(handle, fcntl.LOCK_EX | (0 if blocking else fcntl.LOCK_NB))
            yield

    def inspect(self):
        try:
            result = subprocess.run(['docker', 'inspect', '--format', '{{json .State}}',
                                     self.settings.container], check=True, capture_output=True,
                                    text=True, timeout=10)
            value = json.loads(result.stdout)
            return {'running': value['Running'], 'instance': value['StartedAt']}
        except (OSError, ValueError, subprocess.SubprocessError) as exc:
            raise ServiceUnavailable('OpenSearch non disponibile: verificare accesso Docker e container') from exc

    def status(self):
        state = read_json(self.path, {})
        return {**state, 'idle_seconds': self.settings.idle_seconds,
                'idle_deadline': state.get('last_used', 0) + self.settings.idle_seconds if state.get('owned') else None,
                'observed_at': state.get('observed_at'), 'notice': 'Stato del ciclo gestito; non accende il motore'}

    def ready(self):
        deadline = time.monotonic() + 45
        while time.monotonic() < deadline:
            try:
                response = requests.get(self.settings.endpoint + '/_cluster/health',
                                        params={'wait_for_status': 'yellow', 'timeout': '1s'}, timeout=3)
                response.raise_for_status()
                if not response.json().get('timed_out'):
                    return
            except requests.RequestException:
                pass
            time.sleep(0.5)
        raise ServiceUnavailable('OpenSearch è ancora in avvio: riprova tra poco')

    def ensure_worker(self):
        self.settings.runtime.mkdir(parents=True, exist_ok=True)
        with (self.settings.runtime / 'idle-worker.lock').open('a') as lock:
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                return
        env = {'PATH': os.defpath, 'LANG': 'C.UTF-8',
               'PYTHONPATH': str(Path(__file__).resolve().parents[1])}
        command = [sys.executable, '-m', 'bt_knowledge.idle_worker',
                   '--runtime', str(self.settings.runtime), '--data', str(self.settings.data),
                   '--idle-seconds', str(self.settings.idle_seconds)]
        with (self.settings.runtime / 'idle-worker.log').open('ab') as log:
            subprocess.Popen(command, env=env, stdin=subprocess.DEVNULL, stdout=log,
                             stderr=log, start_new_session=True, close_fds=True)

    @contextmanager
    def use(self):
        with self.locked():
            state = read_json(self.path, {})
            observed = self.inspect()
            if not observed['running']:
                available = int(next(line.split()[1] for line in Path('/proc/meminfo').read_text().splitlines()
                                     if line.startswith('MemAvailable:'))) * 1024
                if available < 3 * 1024**3:
                    raise ServiceUnavailable('RAM disponibile inferiore a 3 GiB: avvio ricerca rimandato')
                state = {'owned': True, 'phase': 'starting', 'last_used': time.time()}
                atomic_json(self.path, state)
                try:
                    subprocess.run(['docker', 'compose', '-f', str(self.settings.compose),
                                    '--profile', 'opensearch', 'up', '-d', 'search'], check=True,
                                   capture_output=True, text=True, timeout=30)
                    observed = self.inspect()
                    state['instance'] = observed['instance']
                except (OSError, subprocess.SubprocessError) as exc:
                    state.update(phase='error', owned=False, error='Avvio OpenSearch fallito')
                    atomic_json(self.path, state)
                    raise ServiceUnavailable(state['error']) from exc
            elif state.get('instance') != observed['instance']:
                # An operator started/restarted it: we do not own its shutdown.
                state = {'owned': False, 'instance': observed['instance']}
            state.update(phase='starting', observed_at=time.time(), idle_seconds=self.settings.idle_seconds)
            atomic_json(self.path, state)
            try:
                if state.get('owned'):
                    self.ensure_worker()
                self.ready()
                state.update(phase='ready', observed_at=time.time())
                atomic_json(self.path, state)
                yield
            finally:
                state.update(last_used=time.time(), observed_at=time.time())
                atomic_json(self.path, state)
                if state.get('owned'):
                    self.ensure_worker()

    def stop_if_idle(self, now=None):
        """Returns False when the worker can exit; never interrupts a held query lock."""
        try:
            with self.locked(blocking=False):
                state = read_json(self.path, {})
                if not state.get('owned'):
                    return False
                if (time.time() if now is None else now) - state.get('last_used', 0) < state.get('idle_seconds', self.settings.idle_seconds):
                    return True
                observed = self.inspect()
                if observed['running'] and state.get('instance') == observed['instance']:
                    subprocess.run(['docker', 'stop', '-t', '20', self.settings.container],
                                   check=True, capture_output=True, timeout=30)
                    state.update(phase='stopped', owned=False)
                else:
                    state.update(phase='ready' if observed['running'] else 'stopped', owned=False)
                state['observed_at'] = time.time()
                atomic_json(self.path, state)
                return False
        except BlockingIOError:
            return True
