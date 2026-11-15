"""升级不变量测试：half_even 边界、NULL、溢出、正负损失抵消、
先/后转换查询一致、读者协议、配额背压、崩溃恢复、合法输入族变体。"""
import json
import random
import subprocess
import sys
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from warehouse import (Backpressure, MaxReadersExceeded, MigrationFailed,
                       MigrationInProgress, PlanInvalid, SchemaRetired,
                       SimulatedCrash, Warehouse, convert_value)

FIX = Path(__file__).resolve().parents[1] / "fixtures"


# ---------------------------------------------------------------- 工具

def write_parquet(path, values, precision=8, scale=4):
    t = pa.table({
        "id": pa.array(list(range(len(values))), type=pa.int64()),
        "amount": pa.array([Decimal(v) if v is not None else None for v in values],
                           type=pa.decimal128(precision, scale)),
    })
    pq.write_table(t, path)


def make_wh(tmp_path, shards, plan=(6, 2, "half_even")):
    """shards: {name: [Decimal-str|None ...]}；返回 (wh, src_dir)"""
    src = tmp_path / "src"
    src.mkdir()
    sources = {}
    for name, values in shards.items():
        p = src / f"{name}.parquet"
        write_parquet(p, values)
        sources[name] = p
    wh = Warehouse.init(tmp_path / "wh", sources)
    if plan is not None:
        wh.set_plan(*plan)
    return wh


def fixture_wh(tmp_path, names=("a", "b", "safe")):
    return Warehouse.init(tmp_path / "wh", {n: FIX / f"{n}.parquet" for n in names})


# ---------------------------------------------------------------- 逐行转换语义

class TestConvertValue:
    @pytest.mark.parametrize("x,expected,loss", [
        ("0.0150", "0.02", "-0.0050"),   # half_even：1 为奇，进位
        ("0.0250", "0.02", "0.0050"),    # half_even 边界：2 为偶，舍去
        ("0.0350", "0.04", "-0.0050"),   # 3 为奇，进位
        ("-0.0250", "-0.02", "-0.0050"), # 负数对称
        ("12.345", "12.34", "0.005"),
        ("12.355", "12.36", "-0.005"),
        ("0.0049", "0.00", "0.0049"),    # 不足半，舍去
        ("0.0051", "0.01", "-0.0049"),   # 超过半，进位
        ("2.5", "2", "0.5"),             # scale=0 的 half_even
    ])
    def test_half_even_boundary(self, x, expected, loss):
        scale = 0 if "." not in expected else len(expected.split(".")[1])
        c, l, fits = convert_value(Decimal(x), 18, scale, "half_even")
        assert c == Decimal(expected) and l == Decimal(loss) and fits

    @pytest.mark.parametrize("x,expected", [
        ("0.0190", "0.01"), ("0.9990", "0.99"), ("-0.0190", "-0.01"),
        ("-0.9990", "-0.99"), ("0.0150", "0.01"),  # toward_zero 不看半
    ])
    def test_toward_zero(self, x, expected):
        c, l, fits = convert_value(Decimal(x), 18, 2, "toward_zero")
        assert c == Decimal(expected) and fits
        assert c + l == Decimal(x)  # 损失恒等式逐行成立

    def test_overflow_detection(self):
        c, l, fits = convert_value(Decimal("9999.9950"), 6, 2, "half_even")
        assert c == Decimal("10000.00") and not fits  # 进位后 7 位 > p=6
        _, _, fits = convert_value(Decimal("9999.9949"), 6, 2, "half_even")
        assert fits
        _, _, fits = convert_value(Decimal("999999.99"), 6, 2, "half_even")
        assert not fits  # 本身就超 p

    def test_plan_validation(self):
        from warehouse import validate_plan
        for bad in [(0, 2, "half_even"), (19, 2, "half_even"), (6, 9, "half_even"),
                    (6, -1, "half_even"), (2, 3, "half_even"), (6, 2, "floor")]:
            with pytest.raises(PlanInvalid):
                validate_plan(*bad)
        validate_plan(18, 8, "half_even")
        validate_plan(1, 0, "toward_zero")


# ---------------------------------------------------------------- fixtures 关键场景

class TestFixtureScenarios:
    def test_a_convert_each_then_sum_not_round_of_sum(self, tmp_path):
        """fixture a：sum(convert(each))=0.04，而旧口径 round(sum(old))=0.03。"""
        wh = fixture_wh(tmp_path, ("a",))
        wh.set_plan(6, 2, "half_even")
        before = wh.query(schema_version=2)  # 未迁移：读时逐行转换
        assert before["sum"] == "0.04" and before["count"] == 2 and before["rows"] == 3
        # 旧错误口径：先求和后 round
        naive = (Decimal("0.0150") * 2).quantize(Decimal("0.01"))
        assert str(naive) == "0.03" and before["sum"] != str(naive)
        wh.migrate_shard("a")
        after = wh.query(schema_version=2)  # 已迁移：物理新片摘要
        after_file = wh.query(schema_version=2, use_summary=False)
        # a 片先/后转换查询一致：金额口径完全一致（快照与取数路径除外）
        for q in (after, after_file):
            for k in ("sum", "count", "min", "max", "rows"):
                assert q[k] == before[k]
        assert after["shards"]["a"]["kind"] == "physical"
        assert before["shards"]["a"]["kind"] == "read_time"

    def test_null_preserved_in_physical_output(self, tmp_path):
        wh = fixture_wh(tmp_path, ("a",))
        wh.set_plan(6, 2, "half_even")
        wh.migrate_shard("a")
        t = pq.read_table(tmp_path / "wh" / "shards" / "a__v2.parquet")
        vals = t.column("amount").to_pylist()
        assert vals == [Decimal("0.02"), Decimal("0.02"), None]  # NULL 还是 NULL
        assert t.schema.field("amount").type == pa.decimal128(6, 2)
        assert t.column("id").to_pylist() == [0, 1, 2]  # 行 id 保留
        scan = wh.scan("a", schema_version=2)
        assert [r["amount"] for r in scan["rows"]] == ["0.02", "0.02", None]

    def test_overflow_rejects_whole_shard(self, tmp_path):
        """fixture b：9999.9950→10000.00 超 p=6，整片拒绝，行数不变，旧片可读。"""
        wh = fixture_wh(tmp_path, ("b",))
        wh.set_plan(6, 2, "half_even")
        r = wh.migrate_shard("b")
        assert r["status"] == "rejected" and r["overflow_count"] == 1
        ov = r["overflows"][0]
        assert ov["value"] == "9999.9950" and ov["converted"] == "10000.00"
        st = wh.status()
        assert st["shards"]["b"]["active"] == 1  # 仍是旧版本
        assert list(st["shards"]["b"]["versions"]) == ["1"]  # 没有部分发布
        assert st["shards"]["b"]["failed"] == ["2"]
        old = wh.query(schema_version=1)  # 旧 schema 仍可读
        assert old["sum"] == "9999.9700" and old["rows"] == 2
        with pytest.raises(MigrationFailed):  # 目标 schema 明确失败
            wh.query(schema_version=2)
        # 失败明细落盘
        failed = json.loads((tmp_path / "wh" / "meta" / "b__v2.failed.json").read_text())
        assert failed["status"] == "rejected" and len(failed["rows"]) == 2

    def test_failed_shard_never_partial_success(self, tmp_path):
        """a 成功 b 失败：目标查询必须整体失败，不得只给 a 的金额。"""
        wh = fixture_wh(tmp_path, ("a", "b"))
        wh.set_plan(6, 2, "half_even")
        assert wh.migrate_shard("a")["status"] == "published"
        assert wh.migrate_shard("b")["status"] == "rejected"
        with pytest.raises(MigrationFailed):
            wh.query(schema_version=2)
        # 放宽计划后 b 可重试成功，失败记录清除
        wh.set_plan(8, 2, "half_even")
        assert wh.migrate_shard("b")["status"] == "published"
        q = wh.query(schema_version=3)
        # a: 0.02+0.02；b: -0.0250→-0.02(half_even)、9999.9950→10000.00(p=8 可容纳)
        assert q["sum"] == "10000.02"
        assert q["count"] == 4

    def test_loss_cancellation_checked_per_row(self, tmp_path):
        """正负损失抵消（净损失 0）时仍逐行核对。"""
        wh = make_wh(tmp_path, {"c": ["0.0150", "0.0250", "-0.0150", "-0.0250"]})
        r = wh.migrate_shard("c")
        assert r["status"] == "published"
        s = r["summary"]
        # 每行损失 ±0.0050，净损失 0；但逐行记录非零
        assert Decimal(s["loss_sum"]) == 0
        assert Decimal(s["source_sum"]) == Decimal(s["sum"]) == 0
        recon = json.loads((tmp_path / "wh" / "meta" / "c__v2.recon.json").read_text())
        losses = [row["loss"] for row in recon["rows"]]
        assert sorted(losses) == ["-0.0050", "-0.0050", "0.0050", "0.0050"]
        for row in recon["rows"]:  # 逐行：old - new == loss
            assert Decimal(row["old"]) - Decimal(row["new"]) == Decimal(row["loss"])
        assert recon["totals"]["identity_holds"] is True

    def test_reconciliation_identity(self, tmp_path):
        """原精确总和 = 已转换精确总和 + 逐行损失总和。"""
        wh = fixture_wh(tmp_path, ("a",))
        wh.set_plan(6, 2, "half_even")
        r = wh.migrate_shard("a")
        s = r["summary"]
        assert Decimal(s["source_sum"]) == Decimal(s["sum"]) + Decimal(s["loss_sum"])
        assert (s["source_sum"], s["sum"], s["loss_sum"]) == ("0.0300", "0.04", "-0.0100")


# ---------------------------------------------------------------- 读者协议与一致性

class TestReaders:
    def test_old_reader_sees_original_during_migration(self, tmp_path):
        wh = fixture_wh(tmp_path, ("a", "safe"))
        wh.set_plan(6, 2, "half_even")
        r_old = wh.begin_read(1)
        before = wh.query(reader_id=r_old)
        wh.migrate_shard("a")
        wh.migrate_shard("safe")
        during = wh.query(reader_id=r_old)  # 迁移中老读者仍看原值
        assert during == before
        assert during["sum"] == "3.0300"
        assert (tmp_path / "wh" / "shards" / "a__v1.parquet").exists()  # 被钉住
        wh.end_read(r_old)
        wh.gc()
        assert not (tmp_path / "wh" / "shards" / "a__v1.parquet").exists()  # 释放后可回收
        with pytest.raises(SchemaRetired):
            wh.begin_read(1)

    def test_target_reader_mixed_shards_same_semantics(self, tmp_path):
        """新 target 读者跨已转/未转分片，口径一致：三种形态结果相同。"""
        wh = fixture_wh(tmp_path, ("a", "safe"))
        wh.set_plan(6, 2, "half_even")
        q_all_readtime = wh.query(schema_version=2)
        wh.migrate_shard("a")
        q_mixed = wh.query(schema_version=2)  # a 物理 + safe 读时
        wh.migrate_shard("safe")
        q_all_physical = wh.query(schema_version=2)
        q_all_file = wh.query(schema_version=2, use_summary=False)
        assert q_all_readtime["sum"] == q_mixed["sum"] == q_all_physical["sum"] == "3.04"
        for q in (q_mixed, q_all_physical, q_all_file):
            assert q["sum"] == q_all_readtime["sum"] and q["count"] == q_all_readtime["count"]
        assert q_mixed["shards"]["a"]["kind"] == "physical"
        assert q_mixed["shards"]["safe"]["kind"] == "read_time"

    def test_reader_snapshot_isolation(self, tmp_path):
        """迁移前开始的 target 读者钉住旧片，迁移后查询结果不变。"""
        wh = fixture_wh(tmp_path, ("a",))
        wh.set_plan(6, 2, "half_even")
        r = wh.begin_read(2)  # 解析为读时转换，钉住 a__v1
        wh.migrate_shard("a")
        wh.gc()  # a__v1 被读者钉住，不能被回收
        assert (tmp_path / "wh" / "shards" / "a__v1.parquet").exists()
        q = wh.query(reader_id=r)
        assert q["sum"] == "0.04" and q["shards"]["a"]["kind"] == "read_time"
        wh.end_read(r)
        wh.gc()
        assert not (tmp_path / "wh" / "shards" / "a__v1.parquet").exists()

    def test_max_two_readers(self, tmp_path):
        wh = fixture_wh(tmp_path, ("a",))
        wh.set_plan(6, 2, "half_even")
        r1, r2 = wh.begin_read(1), wh.begin_read(2)
        with pytest.raises(MaxReadersExceeded):
            wh.begin_read(1)
        wh.end_read(r1)
        wh.end_read(r2)

    def test_one_migration_at_a_time(self, tmp_path):
        wh = fixture_wh(tmp_path, ("a", "safe"))
        wh.set_plan(6, 2, "half_even")
        with pytest.raises(SimulatedCrash):
            wh.migrate_shard("a", crash_at="after_output_before_manifest")
        with pytest.raises(MigrationInProgress):  # 锁未释放前不能再迁
            wh.migrate_shard("safe")
        wh.recover(clear_lock=True)
        assert wh.migrate_shard("safe")["status"] == "published"


# ---------------------------------------------------------------- 崩溃恢复

class TestCrashRecovery:
    def test_after_output_before_manifest(self, tmp_path):
        """新 Parquet 写完未发布：目录不得引用半文件，遗留暂存可回收。"""
        wh = fixture_wh(tmp_path, ("a",))
        wh.set_plan(6, 2, "half_even")
        with pytest.raises(SimulatedCrash):
            wh.migrate_shard("a", crash_at="after_output_before_manifest")
        wh2 = Warehouse.open(tmp_path / "wh")  # 重新打开 = 恢复
        st = wh2.status()
        assert list(st["shards"]["a"]["versions"]) == ["1"]  # 目录无新版本
        assert st["shards"]["a"]["active"] == 1
        assert list((tmp_path / "wh" / "staging").iterdir()) == []  # 暂存已回收
        assert wh2.query(schema_version=1)["sum"] == "0.0300"  # 旧片可读
        wh2.recover(clear_lock=True)
        assert wh2.migrate_shard("a")["status"] == "published"  # 可重试
        assert wh2.query(schema_version=2)["sum"] == "0.04"

    def test_after_manifest_before_old_reader_release(self, tmp_path):
        """发布后旧读者未释放：新片已生效，旧读者仍看原值，旧片不得删除。"""
        wh = fixture_wh(tmp_path, ("a",))
        wh.set_plan(6, 2, "half_even")
        r_old = wh.begin_read(1)
        with pytest.raises(SimulatedCrash):
            wh.migrate_shard("a", crash_at="after_manifest_before_old_reader_release")
        wh2 = Warehouse.open(tmp_path / "wh")
        st = wh2.status()
        assert st["shards"]["a"]["active"] == 2  # 发布已生效
        assert (tmp_path / "wh" / "shards" / "a__v1.parquet").exists()  # 旧片保留
        assert wh2.query(reader_id=r_old)["sum"] == "0.0300"  # 老读者仍看原值
        assert wh2.query(schema_version=2)["sum"] == "0.04"  # 新读者看新语义
        wh2.end_read(r_old)
        wh2.gc()
        assert not (tmp_path / "wh" / "shards" / "a__v1.parquet").exists()

    def test_catalog_never_references_missing_files(self, tmp_path):
        wh = fixture_wh(tmp_path, ("a", "safe"))
        wh.set_plan(6, 2, "half_even")
        for shard in ("a", "safe"):
            with pytest.raises(SimulatedCrash):
                wh.migrate_shard(shard, crash_at="after_output_before_manifest")
            wh = Warehouse.open(tmp_path / "wh")
            wh.recover(clear_lock=True)
            st = wh.status()
            for sh in st["shards"].values():
                for v in sh["versions"].values():
                    assert (tmp_path / "wh" / v["file"]).exists()
            assert wh.migrate_shard(shard)["status"] == "published"


# ---------------------------------------------------------------- 配额与背压

class TestQuota:
    def test_quota_formula_and_measurement(self, tmp_path):
        wh = fixture_wh(tmp_path, ("a", "safe"))
        wh.set_plan(6, 2, "half_even")
        st = wh.status()
        assert st["startup_bytes"] == st["data_bytes"] > 0
        wh.migrate_shard("a")
        st = wh.status()
        real_new = (tmp_path / "wh" / "shards" / "a__v2.parquet").stat().st_size
        assert st["max_new_file_bytes"] == real_new  # 实际测量物理字节
        assert st["quota_bytes"] == st["startup_bytes"] + 2 * real_new
        assert st["data_bytes"] <= st["quota_bytes"]
        assert st["meta_bytes"] > 0  # 目录元数据单列统计

    def test_backpressure_retryable_and_pins_untouched(self, tmp_path):
        """老读者钉住全部旧片：第 3 片迁移应背压；不重试则不删任何文件。"""
        shards = {f"s{i}": [f"{i}.0150", "2.0000", None] for i in range(4)}
        wh = make_wh(tmp_path, shards)
        r_old = wh.begin_read(1)  # 钉住所有 v1
        ok = [wh.migrate_shard(f"s{i}")["status"] for i in range(2)]
        assert ok == ["published", "published"]
        before = {p.name for p in (tmp_path / "wh" / "shards").iterdir()}
        with pytest.raises(Backpressure) as ei:
            wh.migrate_shard("s2")
        assert ei.value.retryable and ei.value.needed_bytes > 0
        after = {p.name for p in (tmp_path / "wh" / "shards").iterdir()}
        assert before == after  # 没有删除任何文件，尤其活读者依赖
        assert list((tmp_path / "wh" / "staging").iterdir()) == []  # staging 已回收
        assert wh.query(reader_id=r_old)["sum"] == "14.0600"  # 老读者不受影响
        wh.end_read(r_old)
        # 释放后重试：发布时回收无引用旧片，迁移继续
        assert wh.migrate_shard("s2")["status"] == "published"
        assert wh.migrate_shard("s3")["status"] == "published"
        assert wh.query(schema_version=2)["sum"] == "14.08"
        assert wh.status()["data_bytes"] <= wh.status()["quota_bytes"]

    def test_no_full_double_copy(self, tmp_path):
        """无读者时逐片迁移：任意时刻总字节 ≤ 启动字节 + 2×最大新片。"""
        shards = {f"s{i}": [f"{i}.0150", "2.0000", None] for i in range(8)}
        wh = make_wh(tmp_path, shards)
        peak = wh.status()["data_bytes"]
        for i in range(8):
            assert wh.migrate_shard(f"s{i}")["status"] == "published"
            st = wh.status()
            peak = max(peak, st["data_bytes"])
            assert st["data_bytes"] <= st["quota_bytes"]
        st = wh.status()
        assert peak <= st["startup_bytes"] + 2 * st["max_new_file_bytes"]
        # 全程没有出现全量双份：峰值远小于 2×启动字节
        assert peak < 2 * st["startup_bytes"]
        wh.gc()
        st = wh.status()
        new_total = sum(v["bytes"] for sh in st["shards"].values()
                        for v in sh["versions"].values())
        assert st["data_bytes"] == new_total  # 回收后只剩新片，无旧片残留
        assert all(list(sh["versions"]) == ["2"] for sh in st["shards"].values())


# ---------------------------------------------------------------- 合法输入族变体

class TestLegalFamilyVariants:
    def test_toward_zero_plan(self, tmp_path):
        wh = make_wh(tmp_path, {"t": ["0.0190", "-0.0190", "0.9990"]},
                     plan=(6, 2, "toward_zero"))
        # 逐行：0.01 / -0.01 / 0.99 → 合计 0.99
        assert wh.query(schema_version=2)["sum"] == "0.99"
        r = wh.migrate_shard("t")
        assert r["status"] == "published"
        assert wh.query(schema_version=2)["sum"] == "0.99"
        s = r["summary"]
        assert Decimal(s["source_sum"]) == Decimal(s["sum"]) + Decimal(s["loss_sum"])

    def test_scale_up_is_lossless(self, tmp_path):
        wh = make_wh(tmp_path, {"u": ["0.0150", "-2.5000", None]}, plan=(10, 6, "half_even"))
        r = wh.migrate_shard("u")
        assert r["status"] == "published" and Decimal(r["summary"]["loss_sum"]) == 0
        assert wh.query(schema_version=2)["sum"] == "-2.485000"

    def test_scale_zero_and_max_precision(self, tmp_path):
        wh = make_wh(tmp_path, {"z": ["2.5000", "3.5000", "-0.5000"]},
                     plan=(18, 0, "half_even"))
        assert wh.migrate_shard("z")["status"] == "published"
        assert wh.query(schema_version=2)["sum"] == "6"  # 2+4+0（half_even）

    def test_empty_and_all_null_shards(self, tmp_path):
        wh = make_wh(tmp_path, {"e": [], "n": [None, None]})
        assert wh.migrate_shard("e")["status"] == "published"
        assert wh.migrate_shard("n")["status"] == "published"
        q = wh.query(schema_version=2)
        assert q["rows"] == 2 and q["count"] == 0 and q["sum"] is None

    @pytest.mark.parametrize("seed", range(6))
    def test_randomized_legal_family(self, tmp_path, seed):
        """同一合法输入族的随机变体：任意合法 plan 下不变量都成立。"""
        rng = random.Random(seed)
        n_shards = rng.randint(1, 8)
        shards = {}
        for i in range(n_shards):
            rows = []
            for _ in range(rng.randint(0, 50)):
                if rng.random() < 0.1:
                    rows.append(None)
                else:
                    rows.append(f"{rng.uniform(-9999, 9999):.4f}")
            shards[f"s{i}"] = rows
        p = rng.randint(2, 18)
        s = rng.randint(0, min(8, p))
        rounding = rng.choice(["half_even", "toward_zero"])
        wh = make_wh(tmp_path, shards, plan=(p, s, rounding))

        # 手工计算期望：逐行转换语义
        quantum = Decimal(1).scaleb(-s)
        from decimal import ROUND_DOWN, ROUND_HALF_EVEN
        mode = ROUND_HALF_EVEN if rounding == "half_even" else ROUND_DOWN
        any_overflow = False
        exp_sum = Decimal(0)
        exp_cnt = 0
        for rows in shards.values():
            for v in rows:
                if v is None:
                    continue
                c = Decimal(v).quantize(quantum, rounding=mode)
                if len(c.as_tuple().digits) > p:
                    any_overflow = True
                else:
                    exp_sum += c
                    exp_cnt += 1
        if any_overflow:
            with pytest.raises(MigrationFailed):
                wh.query(schema_version=2)
            return
        q0 = wh.query(schema_version=2)
        assert q0["count"] == exp_cnt
        assert (q0["sum"] is None) == (exp_cnt == 0)
        if exp_cnt:
            assert Decimal(q0["sum"]) == exp_sum
        # 全部迁移后查询一致，且每片对账恒等式成立
        for name in shards:
            r = wh.migrate_shard(name)
            assert r["status"] == "published"
            sm = r["summary"]
            assert Decimal(sm["source_sum"]) == Decimal(sm["sum"]) + Decimal(sm["loss_sum"])
        q1 = wh.query(schema_version=2)
        assert q1["sum"] == q0["sum"] and q1["count"] == q0["count"] and q1["rows"] == q0["rows"]
        st = wh.status()
        assert st["data_bytes"] <= st["quota_bytes"]

    def test_scale_10k_rows(self, tmp_path):
        """规模上限：单片 1 万行。"""
        rng = random.Random(11)
        rows = [f"{rng.uniform(-999, 999):.4f}" for _ in range(10_000)]
        wh = make_wh(tmp_path, {"big": rows})
        q0 = wh.query(schema_version=2)
        r = wh.migrate_shard("big")
        assert r["status"] == "published" and r["summary"]["rows"] == 10_000
        assert wh.query(schema_version=2)["sum"] == q0["sum"]


# ---------------------------------------------------------------- CLI

class TestCli:
    def run_cli(self, *args, expect=0):
        r = subprocess.run([sys.executable, "cli.py", *args],
                           capture_output=True, text=True, cwd=Path(__file__).resolve().parents[1])
        assert r.returncode == expect, f"cli {args}: rc={r.returncode} stderr={r.stderr}"
        return json.loads(r.stdout) if r.stdout else json.loads(r.stderr)

    def test_end_to_end(self, tmp_path):
        d = str(tmp_path / "wh")
        self.run_cli("init", d, "--src", str(FIX / "a.parquet"), "--src", str(FIX / "safe.parquet"))
        self.run_cli("plan", d, "--precision", "6", "--scale", "2", "--rounding", "half_even")
        r = self.run_cli("begin-read", d, "--schema", "1")
        reader = r["reader"]
        assert self.run_cli("query", d, "--reader", reader)["sum"] == "3.0300"
        assert self.run_cli("migrate-shard", d, "a")["status"] == "published"
        q = self.run_cli("query", d, "--schema", "2")
        assert q["sum"] == "3.04"
        assert self.run_cli("query", d, "--reader", reader)["sum"] == "3.0300"  # 老读者不变
        self.run_cli("end-read", d, reader)
        st = self.run_cli("status", d)
        assert st["data_bytes"] <= st["quota_bytes"]
        # 崩溃注入与恢复
        self.run_cli("migrate-shard", d, "safe", "--crash-at", "after_output_before_manifest", expect=42)
        self.run_cli("recover", d)
        assert self.run_cli("migrate-shard", d, "safe")["status"] == "published"
        assert self.run_cli("query", d, "--schema", "2")["sum"] == "3.04"

    def test_overflow_exit_ok_and_failed_query_exit_4(self, tmp_path):
        d = str(tmp_path / "wh")
        self.run_cli("init", d, "--src", str(FIX / "b.parquet"))
        self.run_cli("plan", d, "--precision", "6", "--scale", "2", "--rounding", "half_even")
        r = self.run_cli("migrate-shard", d, "b")  # 溢出拒绝是正确结果，退出码 0
        assert r["status"] == "rejected"
        self.run_cli("query", d, "--schema", "2", expect=4)  # 目标查询明确失败
