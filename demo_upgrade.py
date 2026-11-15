"""升级后的端到端演示：用 fixtures 的关键场景走一遍完整流程。

  python demo_upgrade.py

fixtures/plan.json 的计划 (8,4)->(6,2) half_even 下：
  第一部分：a 片 half_even 边界 + NULL，先/后转换查询一致（对比旧错误口径）；
  第二部分：b 片溢出整片拒绝，失败分片不给部分成功金额，老读者不受影响；
  第三部分：两个命名中断点的崩溃恢复。
基线 demo.py 保持可用，本脚本演示新能力。
"""
import json
import shutil
import tempfile
from decimal import Decimal
from pathlib import Path

from warehouse import MigrationFailed, SimulatedCrash, Warehouse

FIX = Path(__file__).parent / "fixtures"


def show(title, obj):
    print(f"\n=== {title} ===")
    print(json.dumps(obj, indent=2, ensure_ascii=False))


def part1_consistency(work):
    print("\n########## 第一部分：逐行转换语义，a 片先/后转换查询一致 ##########")
    wh = Warehouse.init(work / "wh1", {"a": FIX / "a.parquet", "safe": FIX / "safe.parquet"})
    wh.set_plan(6, 2, "half_even")
    before = wh.query(schema_version=2)
    show("迁移前 schema-2 查询（两片均未迁移，纯读时逐行转换）", before)
    naive = (Decimal("0.0150") * 2 + Decimal("3.0000")).quantize(Decimal("0.01"))
    print(f"旧错误口径 round(sum(old)) = {naive} ；新口径 sum(convert(each)) = {before['sum']}")
    r = wh.migrate_shard("a")
    show("migrate a → 发布，摘要（含逐行损失恒等式）", r["summary"])
    mid = wh.query(schema_version=2)
    wh.migrate_shard("safe")
    after = wh.query(schema_version=2)
    for k in ("sum", "count", "min", "max", "rows"):
        assert before[k] == mid[k] == after[k], k
    show("a 迁移后 / 全部迁移后：金额完全一致", {"迁移中": mid["sum"], "迁移后": after["sum"]})
    scan = wh.scan("a", schema_version=2)
    show("a 片行级结果（0.0150→0.02 half_even 进位，NULL 保留）", scan)


def part2_overflow_and_readers(work):
    print("\n########## 第二部分：b 片溢出拒绝；失败不混为部分成功；老读者看原值 ##########")
    wh = Warehouse.init(work / "wh2", {n: FIX / f"{n}.parquet" for n in ("a", "b", "safe")})
    wh.set_plan(6, 2, "half_even")
    r_old = wh.begin_read(1)
    show("老读者 begin-read(schema 1)，迁移中持续看原值", wh.query(reader_id=r_old))
    try:
        wh.query(schema_version=2)
    except MigrationFailed as e:
        print(f"迁移前 schema-2 查询即明确失败（b 读时转换溢出）：{e}")
    show("migrate a", wh.migrate_shard("a")["summary"])
    rej = wh.migrate_shard("b")
    show("migrate b → 整片拒绝（9999.9950→10000.00 超 p=6）", {
        "status": rej["status"], "overflows": rej["overflows"]})
    show("migrate safe", {"status": wh.migrate_shard("safe")["status"]})
    try:
        wh.query(schema_version=2)
    except MigrationFailed as e:
        print(f"a/safe 已迁移但 b 失败：schema-2 查询仍整体失败，不给部分金额 → {e}")
    show("老读者仍看原值（b 旧片可读）", wh.query(reader_id=r_old))
    wh.end_read(r_old)
    st = wh.status()
    show("计量：数据字节 / 配额 / 元数据单列", {
        "data_bytes": st["data_bytes"], "quota_bytes": st["quota_bytes"],
        "meta_bytes": st["meta_bytes"], "startup_bytes": st["startup_bytes"],
        "max_new_file_bytes": st["max_new_file_bytes"]})


def part3_crash_recovery(work):
    print("\n########## 第三部分：命名中断点的崩溃恢复 ##########")
    wh = Warehouse.init(work / "wh3", {"a": FIX / "a.parquet"})
    wh.set_plan(6, 2, "half_even")
    try:
        wh.migrate_shard("a", crash_at="after_output_before_manifest")
    except SimulatedCrash as e:
        print(f"注入崩溃：{e}")
    staged = [p.name for p in (work / "wh3" / "staging").iterdir()]
    print(f"崩溃瞬间：staging 遗留 {staged}，目录仍只引用 v1（无半文件）")
    wh = Warehouse.open(work / "wh3")  # 重开 = 恢复
    rec = wh.recover(clear_lock=True)
    st = wh.status()
    show("恢复后：目录无半文件引用，暂存已回收，旧片可读", {
        "recover": rec, "a_versions": list(st["shards"]["a"]["versions"]),
        "schema1_sum": wh.query(schema_version=1)["sum"]})
    show("重试迁移成功", {"status": wh.migrate_shard("a")["status"]})

    wh = Warehouse.init(work / "wh4", {"a": FIX / "a.parquet"})
    wh.set_plan(6, 2, "half_even")
    r_old = wh.begin_read(1)
    try:
        wh.migrate_shard("a", crash_at="after_manifest_before_old_reader_release")
    except SimulatedCrash as e:
        print(f"\n注入崩溃：{e}")
    wh = Warehouse.open(work / "wh4")
    st = wh.status()
    show("恢复后：新片已发布，旧片因老读者未释放而保留", {
        "a_active": st["shards"]["a"]["active"],
        "a_versions": list(st["shards"]["a"]["versions"]),
        "old_reader_sum": wh.query(reader_id=r_old)["sum"],
        "new_reader_sum": wh.query(schema_version=2)["sum"]})
    wh.end_read(r_old)
    show("老读者结束后 gc，旧片回收", wh.gc())


def main():
    work = Path(tempfile.mkdtemp(prefix="wh_demo_"))
    try:
        plan = json.loads((FIX / "plan.json").read_text())
        print(f"fixtures/plan.json: (p,s)=({plan['from']['precision']},{plan['from']['scale']})"
              f" -> ({plan['to']['precision']},{plan['to']['scale']}) {plan['rounding']}")
        part1_consistency(work)
        part2_overflow_and_readers(work)
        part3_crash_recovery(work)
    finally:
        shutil.rmtree(work, ignore_errors=True)


if __name__ == "__main__":
    main()
