"""命令行入口：分片仓的迁移计划、逐分片转换与版本查询。

用法示例：
    python cli.py init /tmp/wh --src fixtures/a.parquet --src fixtures/b.parquet
    python cli.py plan /tmp/wh --precision 6 --scale 2 --rounding half_even
    python cli.py begin-read /tmp/wh --schema 1
    python cli.py migrate-shard /tmp/wh a
    python cli.py query /tmp/wh --schema 2
    python cli.py end-read /tmp/wh <reader_id>
    python cli.py status /tmp/wh

退出码：0 成功（含 overflow 拒绝这一正确结果）；2 用法/计划非法；
3 配额背压（可重试）；4 语义失败（迁移失败/schema 已退役）；
5 读者超限；6 迁移冲突；42 模拟崩溃；1 其他错误。
"""
import argparse
import json
import sys
from pathlib import Path

from warehouse import (Backpressure, CorruptCatalog, MaxReadersExceeded,
                       MigrationFailed, MigrationInProgress, PlanInvalid,
                       SchemaRetired, SimulatedCrash, Warehouse, WarehouseError)

EXIT = {
    Backpressure: 3,
    MigrationFailed: 4,
    SchemaRetired: 4,
    MaxReadersExceeded: 5,
    MigrationInProgress: 6,
    PlanInvalid: 2,
    SimulatedCrash: 42,
}


def emit(obj, fh=sys.stdout):
    fh.write(json.dumps(obj, indent=2, sort_keys=True, ensure_ascii=False) + "\n")


def cmd_init(a):
    srcs = {Path(p).stem: p for p in a.src}
    wh = Warehouse.init(a.dir, srcs)
    return wh.status()


def cmd_plan(a):
    wh = Warehouse.open(a.dir)
    v = wh.set_plan(a.precision, a.scale, a.rounding)
    return {"plan_schema_version": v}


def cmd_migrate(a):
    wh = Warehouse.open(a.dir)
    return wh.migrate_shard(a.shard, crash_at=a.crash_at)


def cmd_begin_read(a):
    wh = Warehouse.open(a.dir)
    rid = wh.begin_read(a.schema)
    return {"reader": rid, "schema_version": a.schema}


def cmd_end_read(a):
    wh = Warehouse.open(a.dir)
    return wh.end_read(a.reader)


def cmd_query(a):
    wh = Warehouse.open(a.dir)
    return wh.query(reader_id=a.reader, schema_version=a.schema,
                    use_summary=not a.no_summary)


def cmd_scan(a):
    wh = Warehouse.open(a.dir)
    return wh.scan(a.shard, schema_version=a.schema, reader_id=a.reader)


def cmd_status(a):
    return Warehouse.open(a.dir).status()


def cmd_gc(a):
    return Warehouse.open(a.dir).gc()


def cmd_recover(a):
    return Warehouse.open(a.dir).recover(clear_lock=True)


def build_parser():
    p = argparse.ArgumentParser(prog="cli.py", description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)

    s = sub.add_parser("init", help="从只读源 Parquet 建仓")
    s.add_argument("dir")
    s.add_argument("--src", action="append", required=True, help="源 Parquet（可分片多次）")
    s.set_defaults(fn=cmd_init)

    s = sub.add_parser("plan", help="设定迁移计划（目标 decimal(p,s) 与 rounding）")
    s.add_argument("dir")
    s.add_argument("--precision", type=int, required=True)
    s.add_argument("--scale", type=int, required=True)
    s.add_argument("--rounding", choices=["half_even", "toward_zero"], required=True)
    s.set_defaults(fn=cmd_plan)

    s = sub.add_parser("migrate-shard", help="迁移单个分片到 plan 目标 schema")
    s.add_argument("dir")
    s.add_argument("shard")
    s.add_argument("--crash-at", choices=["after_output_before_manifest",
                                          "after_manifest_before_old_reader_release"],
                   default=None, help="注入命名中断点（测试用）")
    s.set_defaults(fn=cmd_migrate)

    s = sub.add_parser("begin-read", help="开始一个绑定 (schema, 快照) 的读者")
    s.add_argument("dir")
    s.add_argument("--schema", type=int, required=True)
    s.set_defaults(fn=cmd_begin_read)

    s = sub.add_parser("end-read", help="结束读者并释放其钉住的文件")
    s.add_argument("dir")
    s.add_argument("reader")
    s.set_defaults(fn=cmd_end_read)

    s = sub.add_parser("query", help="按读者或一次性 schema 聚合查询")
    s.add_argument("dir")
    s.add_argument("--schema", type=int, default=None)
    s.add_argument("--reader", default=None)
    s.add_argument("--no-summary", action="store_true",
                   help="已发布分片也回读物理文件而非用预计算摘要")
    s.set_defaults(fn=cmd_query)

    s = sub.add_parser("scan", help="行级读取某分片（目标 schema 语义）")
    s.add_argument("dir")
    s.add_argument("shard")
    s.add_argument("--schema", type=int, default=None)
    s.add_argument("--reader", default=None)
    s.set_defaults(fn=cmd_scan)

    s = sub.add_parser("status", help="目录、字节计量与配额")
    s.add_argument("dir")
    s.set_defaults(fn=cmd_status)

    s = sub.add_parser("gc", help="回收无读者引用的旧版本文件")
    s.add_argument("dir")
    s.set_defaults(fn=cmd_gc)

    s = sub.add_parser("recover", help="崩溃恢复：清暂存孤儿与迁移锁")
    s.add_argument("dir")
    s.set_defaults(fn=cmd_recover)
    return p


def main(argv=None):
    args = build_parser().parse_args(argv)
    try:
        emit(args.fn(args))
        return 0
    except SimulatedCrash as e:
        emit({"error": "SimulatedCrash", "message": str(e)}, sys.stderr)
        return 42
    except WarehouseError as e:
        code = next((c for cls, c in EXIT.items() if isinstance(e, cls)), 1)
        emit({"error": type(e).__name__, "message": str(e),
              **({"retryable": True} if isinstance(e, Backpressure) else {})},
             sys.stderr)
        return code


if __name__ == "__main__":
    sys.exit(main())
