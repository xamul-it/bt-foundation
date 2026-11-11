import json
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from bt_knowledge.config import Settings
from bt_knowledge.engine import digest


@pytest.fixture
def corpus(tmp_path):
    settings = Settings(workspace=tmp_path, data=tmp_path / 'data', runtime=tmp_path / 'runtime', idle_seconds=10)
    documents = {}
    for path, text in [('docs/a.md', '# Cooldown\npost_up_cooldown_days\n'),
                       ('docs/copy.md', '# Cooldown\npost_up_cooldown_days\n'),
                       ('docs/b.md', '# RMA\nAlternative medie\n')]:
        source = tmp_path / path
        source.parent.mkdir(parents=True, exist_ok=True)
        source.write_text(text)
        documents[path] = {'path': path, 'title': text.splitlines()[0][2:], 'generation': 'one',
                           'sha256': digest(source.read_bytes()), 'doc_type': 'guide', 'status': 'unknown',
                           'metadata_state': 'proposed', 'catalog_kind': 'original_document',
                           'version_context': 'source_document', 'study_id': 'study-one', 'chunks': 1,
                           'parameters': ['post_up_cooldown_days'] if path != 'docs/b.md' else [],
                           'strategies': []}
    state = {'root': str(tmp_path), 'documents': documents, 'errors': {}, 'last_sync_unix': 100}
    settings.state_path.parent.mkdir(parents=True)
    settings.state_path.write_text(json.dumps(state))
    return settings, state
