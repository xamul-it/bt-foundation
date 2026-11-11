import argparse
import json
from .service import KnowledgeService


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('action', choices=['search', 'source', 'status', 'catalog', 'studies', 'study', 'runs', 'run', 'update-runs'])
    parser.add_argument('value', nargs='?', default='')
    parser.add_argument('--filters', type=json.loads, default={})
    parser.add_argument('--mode', choices=['hybrid', 'keyword', 'semantic'], default='hybrid')
    parser.add_argument('--line', type=int, default=1)
    parser.add_argument('--limit', type=int, default=5)
    args = parser.parse_args()
    service = KnowledgeService()
    if args.action == 'runs':
        result = service.find_runs(args.filters, args.value, args.mode, args.limit)
    elif args.action == 'run':
        result = service.run(args.value)
    elif args.action == 'update-runs':
        result = service.refresh_runs()
    elif args.action == 'search':
        result = service.search(args.value, args.filters, args.limit, args.mode)
    elif args.action == 'source':
        result = service.open_source(args.value, args.line)
    elif args.action == 'catalog':
        result = service.catalog(args.filters, limit=args.limit, query=args.value)
    elif args.action == 'study':
        result = service.study(args.value)
    else:
        result = getattr(service, args.action)()
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
