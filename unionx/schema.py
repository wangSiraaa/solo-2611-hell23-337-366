"""稳定身份、schema descriptor、兼容性裁决与 canonical schema。

身份规则（题目硬约束）：
* branch_id 在两个 producer 间属于同一全局命名空间；
* field_id 在同一 branch_id 下跨 producer 共享身份；
* 名字只是显示名：改名不换身份，同名不同 ID 是不同字段；
* type-code 只是某 producer 某 revision 的局部编号，可任意选择且随 revision 变化；
* 已删字段的同名重建必须分配新 field_id，旧 field_id 永久退役；
* 逻辑缺席用全局保留分支 MISSING_BRANCH_ID（null payload）显式表示，
  与"分支存在但 child 值为 NULL"严格区分。
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from typing import Optional

# 输入与验收边界
LIMITS = {
    "max_producers": 2,
    "max_branches": 16,
    "max_struct_fields": 8,
    "max_rows_per_batch": 1000,
    "max_batches": 100,
    "max_resident_per_producer": 2,
    "max_resident_global": 3,
}

# 全局保留：逻辑缺席分支。payload 只能是 null。
MISSING_BRANCH_ID = "branch-missing"
KIND_INT64 = "int64"
KIND_TEXT = "text"
KIND_STRUCT = "struct"
KIND_NULL = "null"
_VALID_KINDS = {KIND_INT64, KIND_TEXT, KIND_STRUCT, KIND_NULL}


class IncompatibleSchema(Exception):
    """任何违反协商/兼容规则的情况。该异常意味着整体拒绝，不得有部分数据进入输出。"""


@dataclass(frozen=True)
class FieldSpec:
    field_id: str
    name: str
    kind: str  # int64 / text
    nullable: bool

    def to_dict(self) -> dict:
        return {
            "field_id": self.field_id,
            "name": self.name,
            "kind": self.kind,
            "nullable": self.nullable,
        }

    @staticmethod
    def from_dict(d: dict) -> "FieldSpec":
        return FieldSpec(
            field_id=str(d["field_id"]),
            name=str(d["name"]),
            kind=str(d["kind"]),
            nullable=bool(d["nullable"]),
        )


@dataclass(frozen=True)
class BranchSpec:
    branch_id: str
    name: str
    kind: str  # int64 / text / struct / null
    fields: tuple[FieldSpec, ...] = ()

    def field_by_id(self, field_id: str) -> Optional[FieldSpec]:
        for f in self.fields:
            if f.field_id == field_id:
                return f
        return None

    def to_dict(self) -> dict:
        return {
            "branch_id": self.branch_id,
            "name": self.name,
            "kind": self.kind,
            "fields": [f.to_dict() for f in self.fields],
        }

    @staticmethod
    def from_dict(d: dict) -> "BranchSpec":
        return BranchSpec(
            branch_id=str(d["branch_id"]),
            name=str(d["name"]),
            kind=str(d["kind"]),
            fields=tuple(FieldSpec.from_dict(f) for f in d.get("fields", [])),
        )


@dataclass(frozen=True)
class ProducerDescriptor:
    """一个 producer 某个 schema_revision 的完整、自描述提案。"""

    producer_id: str
    schema_revision: int
    # 局部 type-code -> branch_id
    branch_codes: dict[int, str]
    branches: tuple[BranchSpec, ...]

    def branch(self, branch_id: str) -> Optional[BranchSpec]:
        for b in self.branches:
            if b.branch_id == branch_id:
                return b
        return None

    def fingerprint(self) -> str:
        """结构指纹：只取决于身份与类型，不含显示名。

        同一 producer 同一 revision 但指纹不同 => 整体拒绝。
        """
        payload = {
            "producer_id": self.producer_id,
            "schema_revision": self.schema_revision,
            "branch_codes": {str(k): v for k, v in sorted(self.branch_codes.items())},
            "branches": [
                {
                    "branch_id": b.branch_id,
                    "kind": b.kind,
                    "fields": [
                        [f.field_id, f.kind, f.nullable] for f in b.fields
                    ],
                }
                for b in sorted(self.branches, key=lambda b: b.branch_id)
            ],
        }
        h = hashlib.sha256(json.dumps(payload, sort_keys=True).encode("utf-8"))
        return h.hexdigest()[:16]

    def to_dict(self) -> dict:
        return {
            "producer_id": self.producer_id,
            "schema_revision": self.schema_revision,
            "branch_codes": {str(k): v for k, v in self.branch_codes.items()},
            "branches": [b.to_dict() for b in self.branches],
        }

    @staticmethod
    def from_dict(d: dict) -> "ProducerDescriptor":
        branches = tuple(BranchSpec.from_dict(b) for b in d["branches"])
        return ProducerDescriptor(
            producer_id=str(d["producer_id"]),
            schema_revision=int(d["schema_revision"]),
            branch_codes={int(k): str(v) for k, v in d["branch_codes"].items()},
            branches=branches,
        )

    def validate(self) -> None:
        """结构性自洽校验（不涉及历史演进）。"""
        if self.schema_revision < 1:
            raise IncompatibleSchema("schema_revision 必须 >= 1")
        if len(self.branches) > LIMITS["max_branches"]:
            raise IncompatibleSchema(
                f"分支数 {len(self.branches)} 超过上限 {LIMITS['max_branches']}"
            )
        ids = [b.branch_id for b in self.branches]
        if len(set(ids)) != len(ids):
            raise IncompatibleSchema("descriptor 内 branch_id 重复")
        if set(self.branch_codes.values()) != set(ids):
            raise IncompatibleSchema("branch_codes 的 branch_id 集合必须与 branches 一致")
        if len(set(self.branch_codes)) != len(self.branch_codes):
            raise IncompatibleSchema("局部 type-code 必须互不相同")
        for b in self.branches:
            if b.kind not in _VALID_KINDS:
                raise IncompatibleSchema(f"分支 {b.branch_id} 类型非法: {b.kind}")
            if b.branch_id == MISSING_BRANCH_ID and b.kind != KIND_NULL:
                raise IncompatibleSchema(
                    f"保留分支 {MISSING_BRANCH_ID} 必须是 null payload"
                )
            if b.kind == KIND_NULL and b.branch_id != MISSING_BRANCH_ID:
                raise IncompatibleSchema(
                    f"null payload 只允许出现在保留分支 {MISSING_BRANCH_ID}"
                )
            if b.kind == KIND_STRUCT:
                if not b.fields:
                    raise IncompatibleSchema(f"struct 分支 {b.branch_id} 至少需要一个字段")
                if len(b.fields) > LIMITS["max_struct_fields"]:
                    raise IncompatibleSchema(
                        f"struct {b.branch_id} 字段数超过 {LIMITS['max_struct_fields']}"
                    )
                fids = [f.field_id for f in b.fields]
                if len(set(fids)) != len(fids):
                    raise IncompatibleSchema(f"struct {b.branch_id} 内 field_id 重复")
                names = [f.name for f in b.fields]
                if len(set(names)) != len(names):
                    raise IncompatibleSchema(f"struct {b.branch_id} 内字段名重复")
                for f in b.fields:
                    if f.kind not in (KIND_INT64, KIND_TEXT):
                        raise IncompatibleSchema(
                            f"struct 字段仅支持 int64/text，{f.field_id} 为 {f.kind}"
                        )
            elif b.fields:
                raise IncompatibleSchema(f"非 struct 分支 {b.branch_id} 不能携带字段")

    def check_evolution(self, prev: "ProducerDescriptor") -> None:
        """同一 producer 从 prev 演进到 self 的兼容性裁决。"""
        if self.schema_revision <= prev.schema_revision:
            raise IncompatibleSchema(
                f"revision 必须递增: {prev.schema_revision} -> {self.schema_revision}"
            )
        prev_branch_ids = {b.branch_id for b in prev.branches}
        for bid in prev_branch_ids:
            # MISSING 是 receiver 保留的全局逻辑缺席分支，producer 可在后续 revision
            # 不再声明（canonical 侧一旦出现即永久保留），不算删除数据分支
            if bid == MISSING_BRANCH_ID:
                continue
            if bid not in {b.branch_id for b in self.branches}:
                raise IncompatibleSchema(f"不允许删除已有分支 {bid}（只能新增）")
        for b in self.branches:
            old = prev.branch(b.branch_id)
            if old is None:
                continue  # 新分支，允许
            if b.kind != old.kind:
                raise IncompatibleSchema(
                    f"分支 {b.branch_id} payload 类型不可变: {old.kind} -> {b.kind}"
                )
            if b.kind != KIND_STRUCT:
                continue
            old_ids = {f.field_id for f in old.fields}
            new_ids = {f.field_id for f in b.fields}
            # 字段身份与类型
            for nf in b.fields:
                of = old.field_by_id(nf.field_id)
                if of is None:
                    # 新字段必须 nullable
                    if not nf.nullable:
                        raise IncompatibleSchema(
                            f"新增字段 {nf.field_id} 必须是 nullable"
                        )
                    if nf.kind not in (KIND_INT64, KIND_TEXT):
                        raise IncompatibleSchema(f"新字段 {nf.field_id} 类型非法")
                    continue
                if of.kind != nf.kind:
                    raise IncompatibleSchema(
                        f"字段 {nf.field_id} 类型不可变: {of.kind} -> {nf.kind}"
                    )
                if of.nullable and not nf.nullable:
                    raise IncompatibleSchema(
                        f"字段 {nf.field_id} 不允许从 nullable 收紧为非空"
                    )
            # 退役 field_id 不得在相邻 descriptor 中复活
            retired = old_ids - new_ids
            if retired & new_ids:
                raise IncompatibleSchema(
                    f"已删除 field_id 不得复活: {sorted(retired & new_ids)}"
                )
            # 注意：仅凭相邻 descriptor 无法区分"同 ID 改名"与"删后同名重建"，
            # 后者由 Exchange._check_name_reuse 依据跨 revision 名字历史裁决。
            _ = retired
            # 显示名冲突：同一 revision 内同一 struct 不允许两个字段同名（已在 validate 覆盖）；
            # 改名（同 ID 不同名）与同名不同 ID（不同身份）均合法。


@dataclass
class CanonicalSchema:
    """receiver 自己的全局 canonical 视图，按身份合并两个 producer。

    * canonical type-code 是 receiver 私有且永久稳定的整数（branch 首次出现的顺序号），
      与任何 producer 的局部 code 无关；
    * branch 只增不删；struct 字段按首次出现顺序追加，类型与可空性随身份冻结；
    * 显示名变化不产生新 revision、不重写旧片（读时按身份对齐）；
    * 每次身份/类型结构增长产生新的 canonical_revision；纯改名不增长。
    """

    revision: int = 0
    branches: list[BranchSpec] = field(default_factory=list)
    # branch_id -> canonical code
    _code: dict[str, int] = field(default_factory=dict)
    # branch_id -> {field_id -> 字段首次出现时的 canonical 序号}
    _field_order: dict[str, dict[str, int]] = field(default_factory=dict)

    @property
    def code(self) -> dict[str, int]:
        return dict(self._code)

    def branch_ids(self) -> list[str]:
        return [b.branch_id for b in self.branches]

    def get_branch(self, branch_id: str) -> Optional[BranchSpec]:
        for b in self.branches:
            if b.branch_id == branch_id:
                return b
        return None

    def canonical_code(self, branch_id: str) -> int:
        return self._code[branch_id]

    def fingerprint(self) -> str:
        payload = [
            {
                "branch_id": b.branch_id,
                "code": self._code[b.branch_id],
                "kind": b.kind,
                "fields": [[f.field_id, f.kind, f.nullable] for f in b.fields],
            }
            for b in self.branches
        ]
        return hashlib.sha256(
            json.dumps(payload, sort_keys=True).encode("utf-8")
        ).hexdigest()[:16]

    def to_dict(self) -> dict:
        return {
            "canonical_revision": self.revision,
            "fingerprint": self.fingerprint(),
            "code": self._code,
            "branches": [b.to_dict() for b in self.branches],
        }

    @staticmethod
    def from_dict(d: dict) -> "CanonicalSchema":
        cs = CanonicalSchema(
            revision=int(d["canonical_revision"]),
            branches=[BranchSpec.from_dict(b) for b in d["branches"]],
        )
        cs._code = {k: int(v) for k, v in d["code"].items()}
        for b in cs.branches:
            cs._field_order[b.branch_id] = {
                f.field_id: i for i, f in enumerate(b.fields)
            }
        return cs

    def merge(self, desc: ProducerDescriptor) -> "CanonicalSchema":
        """把一个已接受的 descriptor 合并进来，返回新快照（不改 self）。

        跨 producer 规则：同 branch_id 必须同 payload kind；同 (branch_id, field_id)
        必须同 kind；可空性取并集（任一 producer 声明 nullable 即 nullable）。
        """
        desc.validate()
        rev = self.revision
        branches = [BranchSpec(b.branch_id, b.name, b.kind, b.fields) for b in self.branches]
        code = dict(self._code)
        field_order = {k: dict(v) for k, v in self._field_order.items()}
        next_code = max(code.values(), default=-1) + 1

        for nb in desc.branches:
            existing = next((b for b in branches if b.branch_id == nb.branch_id), None)
            if existing is None:
                # 新全局分支
                if len(branches) >= LIMITS["max_branches"]:
                    raise IncompatibleSchema(
                        f"canonical 分支数超过上限 {LIMITS['max_branches']}"
                    )
                code[nb.branch_id] = next_code
                next_code += 1
                field_order[nb.branch_id] = {
                    f.field_id: i for i, f in enumerate(nb.fields)
                }
                branches.append(
                    BranchSpec(nb.branch_id, nb.name, nb.kind, nb.fields)
                )
                rev += 1
                continue
            if existing.kind != nb.kind:
                raise IncompatibleSchema(
                    f"跨 producer 分支 {nb.branch_id} 类型冲突: "
                    f"{existing.kind} vs {nb.kind}"
                )
            changed = False
            if nb.kind != KIND_STRUCT:
                continue
            old_fields = list(existing.fields)
            old_by_id = {f.field_id: f for f in old_fields}
            order = field_order.setdefault(nb.branch_id, {})
            for nf in nb.fields:
                of = old_by_id.get(nf.field_id)
                if of is None:
                    # 新字段身份，追加
                    if len(old_fields) >= LIMITS["max_struct_fields"]:
                        raise IncompatibleSchema(
                            f"struct {nb.branch_id} 字段数超过上限"
                        )
                    order[nf.field_id] = len(old_fields)
                    old_fields.append(nf)
                    changed = True
                else:
                    if of.kind != nf.kind:
                        raise IncompatibleSchema(
                            f"跨 producer 字段 {nb.branch_id}/{nf.field_id} 类型冲突"
                        )
                    nullable = of.nullable or nf.nullable
                    if nullable != of.nullable:
                        old_by_id[nf.field_id] = FieldSpec(
                            of.field_id, of.name, of.kind, True
                        )
                        changed = True
            # 保持 canonical 字段顺序（按首次出现序号）
            old_fields.sort(key=lambda f: order[f.field_id])
            new_branch = BranchSpec(
                existing.branch_id, existing.name, existing.kind, tuple(old_fields)
            )
            branches = [new_branch if b.branch_id == nb.branch_id else b for b in branches]
            if changed:
                rev += 1
        merged = CanonicalSchema(revision=rev, branches=branches)
        merged._code = code
        merged._field_order = field_order
        return merged
