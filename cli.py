"""Command line entry point for the SDMX migration warehouse.

Commands (all paths relative to the current repository root):

  python cli.py init <warehouse.db> <dsd.xml> <data.xml>
  python cli.py plan-migration <warehouse.db> <new-dsd.xml> [--plan-id ID]
  python cli.py apply-migration <warehouse.db> <plan-id>
  python cli.py ingest-revision <warehouse.db> <revision.json>
  python cli.py query-version <warehouse.db> [revision-seq] [--out file.json]
  python cli.py export-compat <warehouse.db> <revision-seq> <target-dsd-seq>
  python cli.py audit <warehouse.db> [--source file ...]
"""
import argparse
import json
import sys

from warehouse import (Warehouse, MigrationConflict, PlanStale,
                       RevisionConflict, PreconditionFailed, StructuralRejection)


def main(argv=None):
    parser = argparse.ArgumentParser(prog='cli.py')
    sub = parser.add_subparsers(dest='cmd', required=True)

    p_init = sub.add_parser('init')
    p_init.add_argument('db')
    p_init.add_argument('dsd')
    p_init.add_argument('data')

    p_plan = sub.add_parser('plan-migration')
    p_plan.add_argument('db')
    p_plan.add_argument('dsd')
    p_plan.add_argument('--plan-id')

    p_apply = sub.add_parser('apply-migration')
    p_apply.add_argument('db')
    p_apply.add_argument('plan_id')

    p_ing = sub.add_parser('ingest-revision')
    p_ing.add_argument('db')
    p_ing.add_argument('request')

    p_q = sub.add_parser('query-version')
    p_q.add_argument('db')
    p_q.add_argument('revision', nargs='?', type=int)
    p_q.add_argument('--out')

    p_e = sub.add_parser('export-compat')
    p_e.add_argument('db')
    p_e.add_argument('revision', type=int)
    p_e.add_argument('target_dsd', type=int)

    p_a = sub.add_parser('audit')
    p_a.add_argument('db')
    p_a.add_argument('--source', action='append', default=[])

    args = parser.parse_args(argv)
    w = Warehouse(args.db)
    try:
        if args.cmd == 'init':
            result = w.import_xml(args.dsd, args.data)
        elif args.cmd == 'plan-migration':
            result = w.plan_migration(args.dsd, args.plan_id)
        elif args.cmd == 'apply-migration':
            result = w.apply_migration(args.plan_id)
        elif args.cmd == 'ingest-revision':
            with open(args.request, encoding='utf-8') as fh:
                request = json.load(fh)
            result = w.ingest_revision(request)
        elif args.cmd == 'query-version':
            result = w.query_version(args.revision)
            if args.out:
                with open(args.out, 'w', encoding='utf-8') as fh:
                    json.dump(result, fh, ensure_ascii=False, indent=2)
                result = {'written': args.out,
                          'observations': len(result['observations']),
                          'attributes': len(result['attributes'])}
        elif args.cmd == 'export-compat':
            result = w.export_compat(args.revision, args.target_dsd)
        else:
            result = w.audit(source_files=args.source)
        print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
        return 0
    except (MigrationConflict, PlanStale, RevisionConflict,
            PreconditionFailed, StructuralRejection) as exc:
        payload = {'error': type(exc).__name__, 'message': str(exc)}
        for attr in ('plan_id', 'findings'):
            if hasattr(exc, attr):
                payload[attr] = getattr(exc, attr)
        print(json.dumps(payload, ensure_ascii=False, indent=2, default=str),
              file=sys.stderr)
        return 2
    finally:
        w.close()


if __name__ == '__main__':
    sys.exit(main())
