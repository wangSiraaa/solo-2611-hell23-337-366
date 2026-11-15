"""可复现场景：把 fixtures 中的真实 Arrow IPC 适配进新协商协议，
以及一个覆盖全部演进不变量的合成双生产者场景。

旧 fixtures 的 schema json 没有 field_id 概念，这里按题目语义补稳定身份：
branch id 直接沿用 branch-xxx；struct 字段身份采用 struct-evolution.json 中
给出的 field-quantity / field-note。
"""

from __future__ import annotations

import json
from pathlib import Path

import pyarrow as pa

from .encode import reencode  # noqa: F401  (保证场景走真实 IPC 重编码)
from .exchange import Exchange
from .producer import Producer
from .protocol import Envelope
from .schema import (
    KIND_INT64,
    KIND_STRUCT,
    KIND_TEXT,
    MISSING_BRANCH_ID,
    BranchSpec,
    FieldSpec,
    IncompatibleSchema,
    ProducerDescriptor,
)
from .store import ShardStore

KIND_MAP = {"int64": KIND_INT64, "text": KIND_TEXT, "struct": KIND_STRUCT, "null": "null"}


def _fixture_ipc(path: str | Path) -> bytes:
    return Path(path).read_bytes()


def descriptor_from_legacy(producer_id: str, schema_json: dict) -> ProducerDescriptor:
    """把 fixtures/*.schema.json（a/b 风格）适配为带稳定身份的 descriptor。"""
    branches = []
    for code, bid in schema_json["type_codes"].items():
        kind = schema_json["fields"][bid]
        branches.append(BranchSpec(bid, bid.split("branch-")[-1], kind))
    return ProducerDescriptor(
        producer_id=producer_id,
        schema_revision=int(schema_json["schema_revision"]),
        branch_codes={int(c): bid for c, bid in schema_json["type_codes"].items()},
        branches=tuple(branches),
    )


def envelope_from_fixture_arrow(
    producer_id: str, seq: int, arrow_path: str | Path, desc: ProducerDescriptor
) -> Envelope:
    """把旧 fixture 的真实 Arrow IPC 包装成信封（注入自描述 metadata）。

    不重新生成数据：直接读回原 Union，按原字节结构重写 IPC 并加信封头，
    以此证明系统消费的是真实 dense/sparse Union IPC。
    """
    import io

    table = pa.ipc.open_file(pa.memory_map(str(arrow_path), "r")).read_all()
    env = Envelope(
        producer_id=producer_id,
        schema_revision=desc.schema_revision,
        batch_seq=seq,
        branch_codes=dict(desc.branch_codes),
        descriptor=desc,
        ipc=b"",
    )
    schema = table.schema.with_metadata(
        {**(table.schema.metadata or {}), b"x-unionx-envelope": env.metadata_bytes()}
    )
    sink = pa.BufferOutputStream()
    with pa.ipc.new_file(sink, schema) as w:
        w.write_table(table)
    env.ipc = sink.getvalue().to_pybytes()
    return env


def run_fixture_schedule(fixtures_dir: str | Path, store_root: str | Path) -> Exchange:
    """按 fixtures/schedule.json 复现：P1(a) -> pause -> P2(b) -> accept rev2 -> resume。

    关键断言点：b 批在 rev2 被接受前只能停在屏障里；接受后按 branch_id 身份重编码，
    数值 20 必须落在 number 而不是 text（旧 receiver 的错误在此被纠正）。
    """
    fx = Path(fixtures_dir)
    schedule = json.loads((fx / "schedule.json").read_text())
    ex = Exchange(ShardStore(store_root))
    paused = False
    held: list[Envelope] = []
    seq_counter: dict[str, int] = {}

    for step in schedule:
        if "producer" in step:
            pid = step["producer"]
            seq_counter.setdefault(pid, 0)
            seq_counter[pid] += 1
            schema = json.loads((fx / step["schema"]).read_text())
            desc = descriptor_from_legacy(pid, schema)
            if not ex.is_accepted(pid, desc.schema_revision):
                ex.propose(desc)
                # schedule 里 rev1 直接接受；rev2 等显式 accept_schema_revision_2
                if desc.schema_revision == 1:
                    ex.accept(pid, 1)
            env = envelope_from_fixture_arrow(pid, seq_counter[pid], fx / step["batch"], desc)
            if paused:
                held.append(env)
            else:
                res = ex.submit(env)
                if hasattr(res, "reason"):
                    raise RuntimeError(f"fixture 场景不应背压: {res}")
        elif step.get("receiver") == "pause":
            paused = True
        elif step.get("receiver") == "accept_schema_revision_2":
            # 接受 P2 的 rev2（pause 期间到达的 b 批正在屏障中等待）
            ex.accept("P2", 2)
        elif step.get("receiver") == "resume":
            paused = False
            for env in held:
                res = ex.submit(env)
                if hasattr(res, "reason"):
                    raise RuntimeError(f"resume 后不应背压: {res}")
            held.clear()
    ex.close()
    return ex


# ---------------------------------------------------------------------------
# 合成场景：struct 演进、field 改名/同名重建、missing vs child-null、typecode 重用
# ---------------------------------------------------------------------------

def build_struct_descriptors() -> dict:
    """返回 P1 的 v1/v2、P2 的 v1/v2 及一个"同名重建"非法 descriptor。"""
    v1 = ProducerDescriptor(
        "P1", 1,
        {1: "branch-record", 2: MISSING_BRANCH_ID},
        (
            BranchSpec("branch-record", "record", KIND_STRUCT, (
                FieldSpec("field-quantity", "qty", KIND_INT64, nullable=False),
            )),
            BranchSpec(MISSING_BRANCH_ID, "missing", "null"),
        ),
    )
    v2 = ProducerDescriptor(
        "P1", 2,
        {5: "branch-record", 9: MISSING_BRANCH_ID},  # typecode 完全重排
        (
            BranchSpec("branch-record", "renamed_record", KIND_STRUCT, (
                # 同 field_id 改名 qty->count；新增 nullable note
                FieldSpec("field-quantity", "count", KIND_INT64, nullable=False),
                FieldSpec("field-note", "note", KIND_TEXT, nullable=True),
            )),
            BranchSpec(MISSING_BRANCH_ID, "missing", "null"),
        ),
    )
    # 非法 A：field-quantity 被删除，新字段 field-new-quantity 占用其旧显示名 'qty'
    # —— 典型的"删除后同名重建"，必须拒绝（不能与旧值合并）
    v2_bad_reuse = ProducerDescriptor(
        "P1", 2,
        {5: "branch-record", 9: MISSING_BRANCH_ID},
        (
            BranchSpec("branch-record", "record", KIND_STRUCT, (
                FieldSpec("field-new-quantity", "qty", KIND_INT64, nullable=True),
                FieldSpec("field-note", "note", KIND_TEXT, nullable=True),
            )),
            BranchSpec(MISSING_BRANCH_ID, "missing", "null"),
        ),
    )
    # 非法 B：类型不兼容（field-quantity int64 -> text）
    v2_bad_kind = ProducerDescriptor(
        "P1", 2,
        {5: "branch-record", 9: MISSING_BRANCH_ID},
        (
            BranchSpec("branch-record", "record", KIND_STRUCT, (
                FieldSpec("field-quantity", "qty", KIND_TEXT, nullable=False),
            )),
            BranchSpec(MISSING_BRANCH_ID, "missing", "null"),
        ),
    )
    # 合法的删除+重建：旧 field-quantity 删除，新字段使用全新 field_id 且不占用旧显示名
    v2_recreate = ProducerDescriptor(
        "P1", 2,
        {5: "branch-record", 9: MISSING_BRANCH_ID},
        (
            BranchSpec("branch-record", "record", KIND_STRUCT, (
                FieldSpec("field-new-quantity", "amount", KIND_INT64, nullable=True),
            )),
            BranchSpec(MISSING_BRANCH_ID, "missing", "null"),
        ),
    )
    # P2：同 branch_id / field_id 身份（跨 producer 共享），但用自己的局部 code
    p2_v1 = ProducerDescriptor(
        "P2", 1,
        {42: "branch-record", 43: MISSING_BRANCH_ID},
        (
            BranchSpec("branch-record", "record", KIND_STRUCT, (
                FieldSpec("field-quantity", "qty", KIND_INT64, nullable=False),
            )),
            BranchSpec(MISSING_BRANCH_ID, "missing", "null"),
        ),
    )
    return {
        "v1": v1, "v2": v2,
        "v2_bad_reuse": v2_bad_reuse,
        "v2_bad_kind": v2_bad_kind,
        "v2_recreate": v2_recreate,
        "p2_v1": p2_v1,
    }


def run_struct_evolution(store_root: str | Path, *, mode_p1: str = "sparse") -> Exchange:
    """完整演进场景：稀疏 v1 -> 屏障 -> 稠密 v2，含 missing 与 child-null 区分。"""
    ds = build_struct_descriptors()
    ex = Exchange(ShardStore(store_root))

    ex.propose(ds["v1"])
    ex.accept("P1", 1)
    p1_v1 = Producer("P1", ds["v1"], mode=mode_p1)
    # 与 struct-v1 fixture 同形：record / missing / record(child NULL)
    b1 = p1_v1.build_batch(
        [
            {"branch_id": "branch-record", "value": {"field-quantity": 5}},
            {"branch_id": MISSING_BRANCH_ID, "value": None},
            {"branch_id": "branch-record", "value": None},  # child NULL，分支仍存在
        ]
    )
    ex.submit(b1)

    # P2 同身份不同局部 code，交错到达
    ex.propose(ds["p2_v1"])
    ex.accept("P2", 1)
    p2 = Producer("P2", ds["p2_v1"], mode="dense")
    b2 = p2.build_batch(
        [{"branch_id": "branch-record", "value": {"field-quantity": 77}}]
    )
    ex.submit(b2)

    # rev2 提案先到、数据后到：数据必须在屏障里等待，不能按 rev1 布局解码
    ex.propose(ds["v2"])
    p1_v2 = Producer("P1", ds["v2"], mode="dense")
    b3 = p1_v2.build_batch(
        [
            {
                "branch_id": "branch-record",
                "value": {"field-quantity": 7, "field-note": "added"},
            }
        ],
        seq=2,
    )
    ack_wait = ex.submit(b3)
    assert ack_wait.committed is False, "rev2 未接受前不得提交"
    ex.accept("P1", 2)  # 屏障放行
    ex.close()
    return ex
