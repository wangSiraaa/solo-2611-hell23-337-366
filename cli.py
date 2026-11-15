"""命令行入口：apply-revision / extract / trace / sequence / affected / audit。

示例（在仓库根目录）：
  python3 cli.py init --store .work/demo --fasta inputs/reference.fa --features inputs/features.json
  python3 cli.py apply-batch --store .work/demo --revisions inputs/large-revisions.json
  python3 cli.py extract --store .work/demo --version 1 --id f1
  python3 cli.py trace   --store .work/demo --version 1 --id f1
  python3 cli.py audit   --store .work/demo
"""
from __future__ import annotations

import argparse
import json
import sys

from sequence_store import RevisionRejected, InjectedCrash
from pipeline import RevisionPipeline


def _print(obj):
    print(json.dumps(obj, indent=2, ensure_ascii=False))


def main(argv=None):
    ap = argparse.ArgumentParser(prog="cli")
    ap.add_argument("command",
                    choices=["init", "apply-revision", "apply-batch", "extract",
                             "trace", "sequence", "locate", "affected", "audit"])
    ap.add_argument("--store", required=True)
    ap.add_argument("--fasta")
    ap.add_argument("--features")
    ap.add_argument("--base", type=int)
    ap.add_argument("--target", type=int)
    ap.add_argument("--edits")
    ap.add_argument("--revisions")
    ap.add_argument("--version", type=int)
    ap.add_argument("--id", dest="feature_id")
    ap.add_argument("--crash", choices=["after_sequence", "before_commit"])
    args = ap.parse_args(argv)

    try:
        if args.command == "init":
            from sequence_store import Store
            defs = json.loads(open(args.features).read()) if args.features else []
            Store.create(args.store, args.fasta, defs)
            p = RevisionPipeline(args.store)
            _print({"ok": True, "store": args.store,
                    "versions": p.store.versions})
            return 0

        p = RevisionPipeline(args.store)

        if args.command == "apply-revision":
            edits = json.loads(open(args.edits).read())
            _print(p.apply_revision(args.base, args.target, edits, crash=args.crash))
        elif args.command == "apply-batch":
            _print(p.apply_batch_file(args.revisions, crash=args.crash))
        elif args.command == "extract":
            _print({"version": args.version, "id": args.feature_id,
                    "sequence": p.extract(args.version, args.feature_id)})
        elif args.command == "trace":
            _print(p.trace(args.version, args.feature_id))
        elif args.command == "sequence":
            _print({"version": args.version, "sequence": p.sequence(args.version),
                    "length": len(p.sequence(args.version))})
        elif args.command == "locate":
            _print({"version": args.version, "id": args.feature_id,
                    "segments": p.locate(args.version, args.feature_id)})
        elif args.command == "affected":
            _print(p.affected(args.version))
        elif args.command == "audit":
            _print(p.audit())
        return 0
    except (RevisionRejected, InjectedCrash) as exc:
        _print({"ok": False, "error_type": type(exc).__name__,
                "errors": getattr(exc, "errors", [str(exc)])})
        return 2
    except (FileNotFoundError, FileExistsError, KeyError, ValueError) as exc:
        _print({"ok": False, "error_type": type(exc).__name__, "error": str(exc)})
        return 1


if __name__ == "__main__":
    sys.exit(main())
