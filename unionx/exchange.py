"""两生产者 -> 单 receiver 的本地协商交换协调器。

阶段（题目第 3 点）：
  1. propose()  提案进入 pending，做结构/演进/同 revision 指纹校验，不合即拒；
  2. accept()   receiver 显式接受，descriptor 并入 canonical（canonical 只增不改）；
  3. submit()   数据批提交；目标 revision 未接受 => 进屏障缓存，绝不用旧布局解码。

背压：每 producer 最多 2 批、全局最多 3 批常驻；超限显式返回 Backpressure，
消息不保留、不给提交确认。已关闭落盘文件不计入内存预算。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

from .encode import ReencodedBatch, reencode
from .protocol import Ack, Backpressure, Envelope, ProtocolError
from .schema import (
    LIMITS,
    CanonicalSchema,
    IncompatibleSchema,
    ProducerDescriptor,
)
from .store import ShardStore


@dataclass
class _ProducerState:
    pending: Optional[ProducerDescriptor] = None
    accepted: dict[int, ProducerDescriptor] = field(default_factory=dict)
    last_submitted_seq: int = 0
    last_committed_seq: int = 0
    resident: int = 0
    total_committed: int = 0
    # branch_id -> 该 struct 历史上出现过的全部显示名（跨 revision 累积，永不释放）
    # 用来区分"同 field_id 改名"与"删字段后同名重建"：名字是不可复用资源
    field_name_history: dict[str, set[str]] = field(default_factory=dict)


class Exchange:
    def __init__(self, store: ShardStore):
        self.store = store
        self.canonical = CanonicalSchema()
        self._producers: dict[str, _ProducerState] = {}
        self._queue: list[Envelope] = []          # 屏障/待提交队列（到达序）
        self._total_batches = 0
        self.failures: list[dict] = []
        # 内存记账（字节；已落盘关闭文件不计）
        self.bytes_queued_input = 0
        self.bytes_conversion_buffer = 0
        self.bytes_output_resident = 0
        self.peak_resident_bytes = 0
        self.peak_per_producer_batches = 0
        self.peak_global_batches = 0

    # ---------------- 阶段 1/2：schema 提案与接受 ----------------

    def _state(self, producer_id: str) -> _ProducerState:
        if producer_id not in self._producers:
            if len(self._producers) >= LIMITS["max_producers"]:
                raise ProtocolError(
                    f"生产者数量超过上限 {LIMITS['max_producers']}"
                )
            self._producers[producer_id] = _ProducerState()
        return self._producers[producer_id]

    def propose(self, desc: ProducerDescriptor) -> Ack:
        desc.validate()
        st = self._state(desc.producer_id)
        # 同一时刻只允许一个待接受提案；新 revision 必须邻接当前状态，
        # 防止"跳过未接受的 revision"绕过演进检查
        if st.pending is not None:
            if st.pending.schema_revision == desc.schema_revision:
                if st.pending.fingerprint() != desc.fingerprint():
                    raise IncompatibleSchema(
                        f"{desc.producer_id} 同 revision {desc.schema_revision} "
                        "出现不同 schema 提案，整体拒绝"
                    )
                return Ack(
                    desc.producer_id, desc.schema_revision, 0, False,
                    "identical proposal already pending",
                )
            raise IncompatibleSchema(
                f"{desc.producer_id} 已有待接受的 revision "
                f"{st.pending.schema_revision}，不能先提 {desc.schema_revision}"
            )
        # 同 revision 不同 schema => 整体拒绝
        if desc.schema_revision in st.accepted:
            if st.accepted[desc.schema_revision].fingerprint() != desc.fingerprint():
                raise IncompatibleSchema(
                    f"{desc.producer_id} revision {desc.schema_revision} "
                    "已接受过不同 schema，整体拒绝"
                )
            return Ack(desc.producer_id, desc.schema_revision, 0, False, "already accepted")
        # 首个提案可以从任意 revision 开始（该 producer 的基线即此 revision）；
        # 此后 revision 必须严格邻接
        if st.accepted:
            expected = max(st.accepted) + 1
            if desc.schema_revision != expected:
                raise IncompatibleSchema(
                    f"{desc.producer_id} revision 必须邻接: 期望 {expected}，"
                    f"收到 {desc.schema_revision}"
                )
            last_rev = max(st.accepted)
            prev = st.accepted[last_rev]
            desc.check_evolution(prev)
            self._check_name_reuse(st, prev, desc)
        st.pending = desc
        return Ack(
            desc.producer_id, desc.schema_revision, 0, False,
            f"proposal pending，等待 receiver 显式接受",
        )

    @staticmethod
    def _check_name_reuse(
        st: "_ProducerState", prev: ProducerDescriptor, desc: ProducerDescriptor
    ) -> None:
        """同名重建检测：显示名一旦被 field_id A 用过，不得再被 field_id B 使用。

        descriptor 相邻比较分不清"同 ID 改名"和"删除后用新 ID 同名重建"，
        因此 receiver 维护跨全部 revision 的名字历史作为裁决依据。
        """
        for nb in desc.branches:
            if nb.kind != "struct":
                continue
            history = st.field_name_history.setdefault(nb.branch_id, set())
            old_branch = prev.branch(nb.branch_id)
            old_owner = {f.name: f.field_id for f in (old_branch.fields if old_branch else ())}
            for nf in nb.fields:
                # 当前 descriptor 内同名字段属于哪个旧身份
                if nf.name in old_owner and old_owner[nf.name] != nf.field_id:
                    raise IncompatibleSchema(
                        f"{desc.producer_id}/{nb.branch_id}: 显示名 {nf.name!r} 原属 "
                        f"{old_owner[nf.name]}，不得由新 field_id {nf.field_id} 同名重建"
                    )
                # 更早 revision 已退役的名字
                if nf.name in history:
                    # 若该名字当前仍由同一 field_id 持有则是合法延续
                    if old_owner.get(nf.name) != nf.field_id:
                        raise IncompatibleSchema(
                            f"{desc.producer_id}/{nb.branch_id}: 显示名 {nf.name!r} "
                            f"在历史 revision 已被其他字段使用，同名重建必须换名"
                        )

    def accept(self, producer_id: str, schema_revision: int) -> CanonicalSchema:
        st = self._state(producer_id)
        if st.pending is None or st.pending.schema_revision != schema_revision:
            raise IncompatibleSchema(
                f"{producer_id} 没有待接受的 revision {schema_revision}"
            )
        desc = st.pending
        # 跨 producer 身份合并；冲突 => 拒绝提案，canonical 不变
        try:
            new_canonical = self.canonical.merge(desc)
        except IncompatibleSchema:
            st.pending = None
            raise
        self.canonical = new_canonical
        st.accepted[schema_revision] = desc
        # 累积该 producer 的 struct 字段显示名历史（名字永不释放）
        for b in desc.branches:
            if b.kind == "struct":
                st.field_name_history.setdefault(b.branch_id, set()).update(
                    f.name for f in b.fields
                )
        st.pending = None
        self._pump()  # 接受后尝试放行屏障中可解码的批；失败已登记在 self.failures
        return self.canonical

    def is_accepted(self, producer_id: str, schema_revision: int) -> bool:
        st = self._producers.get(producer_id)
        return bool(st and schema_revision in st.accepted)

    def reject(self, producer_id: str, schema_revision: int) -> None:
        st = self._state(producer_id)
        if st.pending and st.pending.schema_revision == schema_revision:
            st.pending = None

    # ---------------- 阶段 3：数据提交 ----------------

    def submit(self, env: Envelope) -> Ack | Backpressure:
        st = self._state(env.producer_id)
        desc = env.descriptor
        # 信封 descriptor 必须与该 producer 已提案/接受的同 revision schema 完全一致
        known = st.accepted.get(env.schema_revision)
        if known is None and (
            st.pending is None or st.pending.schema_revision != env.schema_revision
        ):
            raise ProtocolError(
                f"{env.producer_id} revision {env.schema_revision} 未经提案，批被拒绝"
            )
        ref = known or st.pending
        if ref.fingerprint() != desc.fingerprint():
            raise IncompatibleSchema(
                f"{env.producer_id} 批携带的 schema 与提案指纹不一致，整体拒绝"
            )
        if env.branch_codes != desc.branch_codes:
            raise IncompatibleSchema("批内局部 type-code 映射与 descriptor 不一致")
        # 每 producer seq 连续
        if env.batch_seq != st.last_submitted_seq + 1:
            raise IncompatibleSchema(
                f"{env.producer_id} seq 不连续: 期望 {st.last_submitted_seq + 1}，"
                f"收到 {env.batch_seq}"
            )
        if self._total_batches + 1 > LIMITS["max_batches"]:
            raise IncompatibleSchema(
                f"全局批数超过上限 {LIMITS['max_batches']}"
            )
        # 背压：按常驻批数（输入 IPC 计入内存预算）
        if (
            st.resident >= LIMITS["max_resident_per_producer"]
            or len(self._queue) >= LIMITS["max_resident_global"]
        ):
            bp = Backpressure(
                env.producer_id,
                env.batch_seq,
                "resident buffer full（每producer<=2批、全局<=3批）",
                st.resident,
                len(self._queue),
            )
            return bp  # 显式背压：不保留、不确认

        st.last_submitted_seq = env.batch_seq
        st.resident += 1
        self._queue.append(env)
        self.bytes_queued_input += len(env.ipc)
        self._touch_peak()
        committed_keys, failed = self._pump()
        # 本次提交的批若被整体拒绝，向调用方抛出（failures 已留档，队列已清理）
        for failed_env, exc in failed:
            if failed_env is env:
                raise exc
        committed = (env.producer_id, env.batch_seq) in committed_keys
        return Ack(
            env.producer_id,
            env.schema_revision,
            env.batch_seq,
            committed=committed,
            detail="committed" if committed else "awaiting schema acceptance（屏障缓存）",
        )

    # ---------------- 内部：屏障放行与原子提交 ----------------

    def _touch_peak(self) -> None:
        total = (
            self.bytes_queued_input
            + self.bytes_conversion_buffer
            + self.bytes_output_resident
        )
        self.peak_resident_bytes = max(self.peak_resident_bytes, total)
        self.peak_global_batches = max(self.peak_global_batches, len(self._queue))
        self.peak_per_producer_batches = max(
            self.peak_per_producer_batches,
            max((s.resident for s in self._producers.values()), default=0),
        )

    def _pump(self) -> tuple[set[tuple[str, int]], list[tuple[Envelope, "IncompatibleSchema"]]]:
        """放行所有 revision 已接受、且同 producer 没有更早批挡在前面的信封。

        跨 producer 不做全序：P1 等 schema 时 P2 的已接受批照常提交；
        同一 producer 内部严格按 seq，不允许越界。
        解码失败的批被原子移除并记录，不阻塞其他流。
        """
        blocked_producers: set[str] = set()
        remaining: list[Envelope] = []
        committed_keys: set[tuple[str, int]] = set()
        failed: list[tuple[Envelope, IncompatibleSchema]] = []
        for env in self._queue:
            st = self._producers[env.producer_id]
            if env.producer_id in blocked_producers:
                remaining.append(env)
                continue
            if env.schema_revision not in st.accepted:
                blocked_producers.add(env.producer_id)
                remaining.append(env)
                continue
            try:
                self._commit_one(env, st)
            except IncompatibleSchema as exc:
                # 失败批已在 _commit_one 内回滚 seq/resident 并登记 failures；
                # 它绝不能留在队列里被后续 pump 重复处理，也不阻塞同 producer 的下一批
                failed.append((env, exc))
                continue
            committed_keys.add((env.producer_id, env.batch_seq))
        self._queue = remaining
        self.bytes_queued_input = sum(len(e.ipc) for e in self._queue)
        self._touch_peak()
        return committed_keys, failed

    def _commit_one(self, env: Envelope, st: _ProducerState) -> None:
        try:
            reencoded: ReencodedBatch = self._reencode_accounted(env)
        except (IncompatibleSchema, ProtocolError) as exc:
            # 原子拒绝：该批没有任何部分进入输出；
            # 回滚 seq 占用与常驻计数，使生产者可用同一 seq 重投修正后的批
            st.resident -= 1
            st.last_submitted_seq -= 1
            self.failures.append(
                {
                    "producer_id": env.producer_id,
                    "batch_seq": env.batch_seq,
                    "schema_revision": env.schema_revision,
                    "reason": str(exc),
                }
            )
            raise IncompatibleSchema(
                f"批 {env.producer_id}#{env.batch_seq} 被整体拒绝: {exc}"
            ) from exc
        # 落盘（绑定当前 canonical 快照）；append 本身失败也不会留下半批
        self.bytes_output_resident = reencoded.table.nbytes
        self._touch_peak()
        self.store.append(
            reencoded.table,
            self.canonical,
            env.producer_id,
            env.batch_seq,
            env.schema_revision,
        )
        self.bytes_output_resident = 0
        self.bytes_conversion_buffer = 0
        st.resident -= 1
        st.last_committed_seq = env.batch_seq
        st.total_committed += 1
        self._total_batches += 1
        self._touch_peak()

    def _reencode_accounted(self, env: Envelope) -> ReencodedBatch:
        # 披露：额外一个 <=1000 行的转换缓冲（to_pylist），计入预算
        reencoded = reencode(env, self.canonical)
        self.bytes_conversion_buffer = reencoded.conversion_buffer_bytes
        self._touch_peak()
        return reencoded

    # ---------------- 收尾与观测 ----------------

    def close(self) -> None:
        if self._queue:
            raise ProtocolError(
                f"仍有 {len(self._queue)} 个屏障缓存批未决，不能 close"
            )
        self.store.close()

    def stats(self) -> dict:
        return {
            "canonical_revision": self.canonical.revision,
            "canonical_codes": self.canonical.code,
            "queued_batches": len(self._queue),
            "queued_per_producer": {
                pid: s.resident for pid, s in self._producers.items()
            },
            "committed_total": self._total_batches,
            "failures": list(self.failures),
            "memory": {
                "queued_input_bytes": self.bytes_queued_input,
                "conversion_buffer_bytes": self.bytes_conversion_buffer,
                "output_resident_bytes": self.bytes_output_resident,
                "current_resident_bytes": self.bytes_queued_input
                + self.bytes_conversion_buffer
                + self.bytes_output_resident,
                "peak_resident_bytes": self.peak_resident_bytes,
                "note": "已关闭落盘分片不计入；另允许一个<=1000行 conversion_buffer 转换缓冲（已披露并计入）",
            },
            "peaks": {
                "per_producer_batches": self.peak_per_producer_batches,
                "global_batches": self.peak_global_batches,
            },
        }
