"""Command line interface for the persistent annotation directory.

Usage:
  python cli.py import DIR --json inputs/graph.json [--source NAME]
  python cli.py import DIR --gff3 legacy/features.gff3 [--source NAME]
  python cli.py apply-patch DIR --patch inputs/patch.json [--base N] [--id ID]
  python cli.py query DIR --lo A --hi B [--version N]
  python cli.py inspect DIR (--entity ID [--version N] | --patch [ID] | --versions)

All results are JSON on stdout; structured failures exit non-zero with
{"error": code, "message": ...} and never mutate published tables.
"""
import argparse
import json
import sys

from annotation import load_graph_json, parse_gff3
from store import open_store, PatchError


def main(argv=None):
    p = argparse.ArgumentParser(prog="annotation-dir")
    sub = p.add_subparsers(dest="cmd", required=True)

    pi = sub.add_parser("import")
    pi.add_argument("dir")
    src = pi.add_mutually_exclusive_group(required=True)
    src.add_argument("--json", dest="json_path")
    src.add_argument("--gff3", dest="gff3_path")
    pi.add_argument("--source", default=None)

    pp = sub.add_parser("apply-patch")
    pp.add_argument("dir")
    pp.add_argument("--patch", required=True)
    pp.add_argument("--base", type=int, default=None)
    pp.add_argument("--id", dest="patch_id", default=None)

    pq = sub.add_parser("query")
    pq.add_argument("dir")
    pq.add_argument("--lo", type=int, required=True)
    pq.add_argument("--hi", type=int, required=True)
    pq.add_argument("--version", type=int, default=None)

    pin = sub.add_parser("inspect")
    pin.add_argument("dir")
    pin.add_argument("--entity", default=None)
    pin.add_argument("--patch", nargs="?", const="", default=None)
    pin.add_argument("--versions", action="store_true")
    pin.add_argument("--version", type=int, default=None)

    args = p.parse_args(argv)
    try:
        if args.cmd == "import":
            st = open_store(args.dir, create=True)
            try:
                if args.json_path:
                    entities = load_graph_json(args.json_path)
                    source = args.source or args.json_path
                    extra = None
                else:
                    parsed = parse_gff3(args.gff3_path)
                    entities = parsed["entities"]
                    source = args.source or args.gff3_path
                    extra = parsed["extra_reasons"]
                    result_extra = {"gff3_issues": parsed["issues"],
                                    "conflict_ids": parsed["conflict_ids"],
                                    "anonymous_ids": parsed["anonymous_ids"],
                                    "fasta_ignored": parsed["fasta_ignored"]}
                result = st.import_graph(entities, source, extra)
                if args.gff3_path:
                    result.update(result_extra)
            finally:
                st.close()
            print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
            return 0

        st = open_store(args.dir)
        try:
            if args.cmd == "apply-patch":
                patch = json.load(open(args.patch, encoding="utf-8"))
                result = st.apply_patch(patch, base=args.base,
                                        patch_id=args.patch_id)
            elif args.cmd == "query":
                result = {"lo": args.lo, "hi": args.hi,
                          "version": args.version or st.head_version,
                          "hits": st.query(args.lo, args.hi, args.version)}
            elif args.cmd == "inspect":
                if args.versions:
                    result = st.versions()
                elif args.entity is not None:
                    result = st.inspect_entity(args.entity, args.version)
                elif args.patch is not None:
                    pid = args.patch or None
                    result = st.inspect_patch(pid)
                else:
                    p.error("inspect needs --entity, --patch or --versions")
            else:  # pragma: no cover
                p.error("unknown command")
        finally:
            st.close()
        print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
        return 0
    except (PatchError, ValueError, FileNotFoundError) as exc:
        code = getattr(exc, "code", "input_error")
        print(json.dumps({"error": code, "message": str(exc)},
                         ensure_ascii=False), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
