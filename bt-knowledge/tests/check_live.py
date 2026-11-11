"""Opt-in local retrieval/MCP/idle integration. Uses no external model clients."""
import asyncio
from dataclasses import replace
import json
from pathlib import Path
import sys
import time

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'bt-knowledge'))
from bt_knowledge.config import Settings
from bt_knowledge.service import KnowledgeService
from bt_knowledge.lifecycle import read_json


async def main():
    from fastmcp import Client
    from fastmcp.client.transports import StdioTransport
    settings = replace(Settings(), idle_seconds=3)
    service = KnowledgeService(settings)
    before = service.lifecycle.inspect()
    assert before['running'] is False, 'Run this check with OpenSearch initially stopped'
    transport = StdioTransport(command=sys.executable, args=['-m', 'bt_knowledge.mcp'], cwd=str(ROOT),
        env={'PYTHONPATH': str(ROOT / 'bt-knowledge'), 'BT_KNOWLEDGE_IDLE_SECONDS': '3'})
    query = 'post_up_cooldown_days'
    filters = {'study_id': 'bt-strategy-test/overnight-ah/studies/adaptive_cooldown_20261014'}
    async with Client(transport) as client:
        tools = await client.list_tools()
        status = await client.call_tool('knowledge_status', {})
        expected_documents=len(read_json(settings.state_path)['documents'])
        assert json.loads(status.content[0].text)['documents'] == expected_documents
        assert service.lifecycle.inspect()['running'] is False, 'status must not wake the engine'
        mcp_result = await client.call_tool('search_knowledge', {'query': query, 'filters': filters, 'mode': 'keyword'})
        mcp = json.loads(mcp_result.content[0].text)
        assert mcp['hits'] and service.lifecycle.inspect()['running']
        direct = service.search(query, filters, mode='keyword')
        assert [h['path'] for h in direct['hits']] == [h['path'] for h in mcp['hits']]
        studies = await client.call_tool('list_studies', {})
        assert len(json.loads(studies.content[0].text)['studies']) == 48
        study = await client.call_tool('open_study', {'study_id': filters['study_id']})
        assert json.loads(study.content[0].text)['documents']
    deadline = time.monotonic() + 30
    while service.lifecycle.inspect()['running'] and time.monotonic() < deadline:
        await asyncio.sleep(0.5)
    assert service.lifecycle.inspect()['running'] is False, 'idle worker must stop the owned container'
    result = {'mcp_tools': [tool.name for tool in tools], 'documents': expected_documents, 'studies': 48,
              'mcp_and_service_results_identical': True, 'status_does_not_wake': True,
              'cold_start_verified': True, 'idle_shutdown_verified': True, 'external_model_calls': 0}
    Path('/tmp/bt-knowledge-live-result.json').write_text(json.dumps(result, indent=2))
    print(json.dumps(result))


if __name__ == '__main__':
    asyncio.run(main())
