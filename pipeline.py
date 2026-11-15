#!/usr/bin/env python3
"""离线片段数据管道命令行：plan / collect / verify。

用法（路径均相对当前仓库根目录）：
  python3 pipeline.py plan    --manifest inputs/r2.json --request inputs/request.json --store store/
  python3 pipeline.py collect --manifest inputs/r2.json --request inputs/request.json \
      --origin origin/ --store store/ [--strategy global|online]
  python3 pipeline.py verify  --store store/ [--publication current|pub-...] [--failure fail-...]

退出码：plan/verify 成功 0；collect 发布成功 0、不可交付 3；任何核验失败 1。
"""
import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from collector import collect, plan
from origin import Origin
from store import Store
from verify import VerifyError, verify_failure, verify_publication


def main(argv=None):
    ap = argparse.ArgumentParser(prog="pipeline",
                                 description="可验证的离线片段数据管道")
    sub = ap.add_subparsers(dest="cmd", required=True)
    for name in ("plan", "collect"):
        p = sub.add_parser(name)
        p.add_argument("--manifest", required=True)
        p.add_argument("--request", required=True)
        p.add_argument("--store", required=True)
        p.add_argument("--strategy", default="global",
                       choices=["global", "online"])
    sub.choices["collect"].add_argument("--origin", required=True)
    v = sub.add_parser("verify")
    v.add_argument("--store", required=True)
    v.add_argument("--publication", default="current")
    v.add_argument("--failure", default=None)
    args = ap.parse_args(argv)

    if args.cmd == "plan":
        out = plan(args.manifest, args.request, args.store, args.strategy)
        print(json.dumps(out, indent=2, ensure_ascii=False))
        return 0
    if args.cmd == "collect":
        out = collect(args.manifest, args.request, Origin(args.origin),
                      args.store, strategy=args.strategy)
        print(json.dumps(out, indent=2, ensure_ascii=False))
        return 0 if out["status"] == "published" else 3
    # verify
    store = Store(args.store)
    try:
        if args.failure:
            rep = verify_failure(store, args.failure)
        else:
            rep = verify_publication(store, args.publication)
    except (VerifyError, FileNotFoundError, KeyError) as exc:
        print(json.dumps({"ok": False, "error": str(exc)},
                         indent=2, ensure_ascii=False))
        return 1
    print(json.dumps(rep, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
