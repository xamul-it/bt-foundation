from dataclasses import dataclass, field
import os
from pathlib import Path

WORKSPACE = Path(__file__).resolve().parents[2]


@dataclass(frozen=True)
class Settings:
    workspace: Path = WORKSPACE
    data: Path = field(default_factory=lambda: Path(os.getenv(
        'BT_KNOWLEDGE_DATA', WORKSPACE / 'research/rag_poc_20261109')))
    runtime: Path = field(default_factory=lambda: Path(os.getenv(
        'BT_KNOWLEDGE_RUNTIME', WORKSPACE / 'bt-knowledge/runtime')))
    endpoint: str = 'http://127.0.0.1:19200'
    embeddings: str = 'http://127.0.0.1:7092/v1/embeddings'
    model: str = 'ibm-granite/granite-embedding-311m-multilingual-r2'
    revision: str = '44399559930365213510b1ee2eb15ded83374f0e'
    index: str = 'bt-rag-poc2-workspace-g311'
    container: str = 'bt-rag-poc-search-1'
    idle_seconds: float = field(default_factory=lambda: max(1, float(os.getenv('BT_KNOWLEDGE_IDLE_SECONDS', '600'))))

    @property
    def state_path(self):
        return self.data / 'phase2/runtime' / f'opensearch-{self.index}.json'

    @property
    def operation_lock(self):
        # Also guards legacy population jobs using the existing shared lock.
        return self.data / 'phase2/runtime/workspace-service.lock'

    @property
    def compose(self):
        return self.data / 'compose.yaml'
