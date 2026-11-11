"""One facade used by the authenticated HTTP API, MCP and terminal clients."""
from .config import Settings
from .engine import KnowledgeEngine
from .lifecycle import SearchLifecycle


class KnowledgeService:
    def __init__(self, settings=None):
        self.settings = settings or Settings()
        self.lifecycle = SearchLifecycle(self.settings)

    def engine(self):
        return KnowledgeEngine(self.settings)

    def status(self):
        from .run_catalog import RunCatalog
        return {**self.engine().status(), 'service': self.lifecycle.status(), 'runs': RunCatalog(self.settings).status()}

    def search(self, query, filters=None, limit=5, mode='hybrid'):
        # Reject malformed requests without waking Docker.
        from .engine import validate_filters
        filters = validate_filters({} if filters is None else filters)
        if not isinstance(query, str) or not query.strip() or len(query) > 2000:
            raise ValueError('Inserisci una ricerca tra 1 e 2000 caratteri')
        if (type(limit) is not int or not 1 <= limit <= 20 or not isinstance(mode, str)
                or mode not in {'hybrid', 'keyword', 'semantic'}):
            raise ValueError('Modalità o numero di risultati non valido')
        with self.lifecycle.use():
            return self.engine().search(query, filters, limit, mode)

    def open_source(self, path, start_line=1, line_count=100):
        if not isinstance(path, str) or not path or len(path) > 1000:
            raise ValueError('Percorso fonte non valido')
        if type(start_line) is not int or start_line < 1 or type(line_count) is not int or not 1 <= line_count <= 160:
            raise ValueError('Intervallo righe non valido')
        return self.engine().open_source(path, start_line, line_count)

    def catalog(self, filters=None, offset=0, limit=30, query=''):
        if type(offset) is not int or offset < 0 or type(limit) is not int or not 1 <= limit <= 100:
            raise ValueError('Paginazione non valida')
        if not isinstance(query, str) or len(query) > 2000:
            raise ValueError('Testo filtro non valido')
        return self.engine().catalog(filters, offset, limit, query)

    def open_document(self, path):
        if not isinstance(path, str) or not path or len(path) > 1000:
            raise ValueError('Percorso fonte non valido')
        return self.engine().open_document(path)

    def studies(self):
        return self.engine().studies()

    def study(self, study_id):
        if not isinstance(study_id, str) or not study_id or len(study_id) > 1000:
            raise ValueError('Identificativo studio non valido')
        return self.engine().study(study_id)

    def find_runs(self, filters=None, query='', mode='keyword', limit=30, offset=0):
        from .run_catalog import RunCatalog
        catalog=RunCatalog(self.settings)
        if query and mode in {'semantic','hybrid'}:
            with self.lifecycle.use():
                return catalog.search(query, filters, mode, limit, offset)
        return catalog.search(query, filters, mode, limit, offset)

    def run(self, identity):
        from .run_catalog import RunCatalog
        return RunCatalog(self.settings).get(identity)

    def run_refresh_status(self):
        from .lifecycle import read_json
        import os
        state=read_json(self.settings.runtime/'run-refresh.json',{'state':'idle'})
        if state.get('state')=='running':
            try:os.kill(state['pid'],0)
            except (ProcessLookupError,PermissionError):state.update(state='interrupted')
        return state

    def refresh_runs(self):
        import os
        from pathlib import Path
        import subprocess
        import sys
        state=self.run_refresh_status()
        if state.get('state')=='running':return state
        self.settings.runtime.mkdir(parents=True,exist_ok=True)
        env={'PATH':os.defpath,'LANG':'C.UTF-8','PYTHONPATH':str(Path(__file__).resolve().parents[1]),
             'BT_KNOWLEDGE_DATA':str(self.settings.data),'BT_KNOWLEDGE_RUNTIME':str(self.settings.runtime),
             'BT_KNOWLEDGE_IDLE_SECONDS':str(self.settings.idle_seconds)}
        # The local reader needs the same database connection, not broker credentials.
        for key in ['LIVE_EVENTS_DB_DSN','WATCHTOWER_DB_DSN','DATABASE_URL','BT_DB_ENV_FILE',
                    'XDG_RUNTIME_DIR','DBUS_SESSION_BUS_ADDRESS']:
            if os.environ.get(key):env[key]=os.environ[key]
        with (self.settings.runtime/'run-refresh.log').open('ab') as output:
            process=subprocess.Popen([sys.executable,'-m','bt_knowledge.run_ingest','refresh'],env=env,
                stdin=subprocess.DEVNULL,stdout=output,stderr=output,start_new_session=True,close_fds=True)
        import time
        from .lifecycle import atomic_json
        state={'state':'running','phase':'starting','pid':process.pid,'started_at':time.time()}
        atomic_json(self.settings.runtime/'run-refresh.json',state)
        return state
