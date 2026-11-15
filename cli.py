"""Command line entry point: build / plan-query / fetch-query / inspect.

All paths are relative to the current repository root.  Every command prints
a single JSON object on stdout so runs can be captured as real logs.
"""

import argparse
import json
import os
import sys

from cache import BlockCache
from fetcher import FetchSession
from inspect_cmd import inspect_catalog, inspect_package, summarize
from manifest import load_manifests
from planner import load_plan, make_plan, save_plan
from replica import ReplicaResolver
from store import Store


def _write_json(obj, path=None):
    text = json.dumps(obj, indent=2, sort_keys=True)
    if path:
        os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        with open(path, 'w', encoding='utf-8') as f:
            f.write(text)
    return text


def cmd_build(args):
    specs = load_manifests(args.manifest)
    resolver = ReplicaResolver(args.replica_root)
    with Store(args.db) as st:
        build_id = st.build(
            args.stac_root, specs,
            replica_root=args.replica_root,
            verify_replicas=args.verify, resolver=resolver)
        info = st.build_info(build_id)
        info['events'] = st.events(build_id)
        info['access_log'] = resolver.access_log()
    print(_write_json(info, args.out))
    return 0


def cmd_plan(args):
    bbox = [float(x) for x in args.bbox.split(',')]
    time_interval = args.time.split('/')
    ranges = None
    if args.ranges:
        ranges = {}
        for token in args.ranges.split(';'):
            target, span = token.split('=')
            item_id, asset_key = target.split(':')
            start, end = (int(x) for x in span.split('-'))
            ranges[(item_id, asset_key)] = (start, end)
    with Store(args.db) as st:
        plan = make_plan(st, bbox, time_interval, build_id=args.build,
                         asset_ranges=ranges, plan_id=args.plan_id)
    save_plan(plan, args.out)
    print(_write_json(plan.to_dict()))
    return 0


def cmd_fetch(args):
    plan = load_plan(args.plan)
    if args.build and plan['build_id'] != args.build:
        raise SystemExit(
            f"plan pinned to {plan['build_id']}, refusing --build {args.build}")
    resolver = ReplicaResolver(args.replica_root)
    cache = BlockCache(args.cache)
    session = FetchSession(plan, resolver, cache, args.package,
                           plan_id=args.plan_id,
                           policy={'full_file_threshold': args.threshold},
                           hooks=None)
    manifest = session.run()
    print(_write_json(summarize(manifest)))
    print(json.dumps({'manifest': os.path.join(args.package, 'manifest.json'),
                      'cache_stats': cache.stats()}))
    return 0 if manifest['complete'] else 2


def cmd_inspect(args):
    if args.kind == 'catalog':
        print(_write_json(inspect_catalog(args.db, args.build)))
    else:
        print(_write_json(inspect_package(args.package, rehash=not args.no_rehash)))
    return 0


def build_parser():
    p = argparse.ArgumentParser(description='Verifiable STAC replica pipeline')
    sub = p.add_subparsers(dest='command', required=True)

    b = sub.add_parser('build', help='build an immutable SQLite catalog')
    b.add_argument('--stac-root', required=True)
    b.add_argument('--replica-root', required=True)
    b.add_argument('--manifest', nargs='+', required=True)
    b.add_argument('--db', required=True)
    b.add_argument('--verify', choices=['head', 'full'], default='full')
    b.add_argument('--out')
    b.set_defaults(func=cmd_build)

    q = sub.add_parser('plan-query', help='create a pinned query plan')
    q.add_argument('--db', required=True)
    q.add_argument('--bbox', required=True, help='w,s,e,n (w>e = dateline)')
    q.add_argument('--time', required=True, help='start/end ISO8601 UTC')
    q.add_argument('--build')
    q.add_argument('--plan-id', default='plan')
    q.add_argument('--ranges', help='item:asset=start-end;...')
    q.add_argument('--out', required=True)
    q.set_defaults(func=cmd_plan)

    f = sub.add_parser('fetch-query', help='fetch and verify a plan')
    f.add_argument('--plan', required=True)
    f.add_argument('--replica-root', required=True)
    f.add_argument('--cache', required=True)
    f.add_argument('--package', required=True)
    f.add_argument('--build')
    f.add_argument('--plan-id')
    f.add_argument('--threshold', type=float, default=0.5)
    f.set_defaults(func=cmd_fetch)

    i = sub.add_parser('inspect', help='reread catalog or package')
    i.add_argument('kind', choices=['catalog', 'package'])
    i.add_argument('--db')
    i.add_argument('--build')
    i.add_argument('--package')
    i.add_argument('--no-rehash', action='store_true')
    i.set_defaults(func=cmd_inspect)
    return p


def main(argv=None):
    args = build_parser().parse_args(argv)
    if args.command == 'inspect':
        if args.kind == 'catalog' and not args.db:
            raise SystemExit('inspect catalog requires --db')
        if args.kind == 'package' and not args.package:
            raise SystemExit('inspect package requires --package')
    return args.func(args)


if __name__ == '__main__':
    sys.exit(main())
