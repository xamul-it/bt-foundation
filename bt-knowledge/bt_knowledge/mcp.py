"""Shared local MCP entrypoint. No generative model calls."""
from fastmcp import FastMCP
from .service import KnowledgeService

server = FastMCP('Backtrader documentazione e studi')
service = KnowledgeService()


@server.tool
def search_knowledge(query: str, filters: dict | None = None, limit: int = 5, mode: str = 'hybrid') -> dict:
    """Search current sources. Modes: hybrid, keyword, semantic. Exact filters:
    path, doc_type, strategies, parameters, status, study_id, version_context,
    study_date, period_start, period_end. Date ranges accept gt/gte/lt/lte.
    Parameter tags are mentions, not proof that an effect was studied.
    """
    return service.search(query, filters, limit, mode)


@server.tool
def open_source(path: str, start_line: int = 1, line_count: int = 100) -> dict:
    """Read a catalogued source with original line numbers and hash. Refuses stale sources."""
    return service.open_source(path, start_line, line_count)


@server.tool
def knowledge_status() -> dict:
    """Discover corpus coverage, metadata filters, source freshness, errors and idle lifecycle."""
    return service.status()


@server.tool
def list_studies() -> dict:
    """List proposed study groups, their document counts and source freshness."""
    return service.studies()


@server.tool
def open_study(study_id: str) -> dict:
    """Navigate original documents and automatic cards belonging to a study."""
    return service.study(study_id)


@server.tool
def find_runs(filters: dict | None = None, query: str = '', mode: str = 'keyword', limit: int = 30, offset: int = 0) -> dict:
    """Search the reconciled run catalog. Filters: run_id, strategy, strategy_family,
    status, mode, start/end, parameter, param.NAME, input.NAME or metric.NAME (gt/gte/lt/lte/eq).
    Resolved parameter fields and requested configurations remain distinct.
    Modes: keyword, semantic, hybrid. Metric units are returned with every run.
    """
    return service.find_runs(filters, query, mode, limit, offset)


@server.tool
def open_run(identity: str) -> dict:
    """Read a run's parameters, metric units, registry/artifact provenance and study links."""
    return service.run(identity)


def main():
    server.run()


if __name__ == '__main__':
    main()
