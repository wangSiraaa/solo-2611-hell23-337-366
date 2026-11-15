"""管线串联层：规范输入 -> 版本化 store -> apply-revision / extract / trace / audit。

旧 run(root, edits) 接口保留用于 baseline 回归对比（旧实现仍会复现坐标漂移）；
新实现以 RevisionPipeline 暴露完整的版本化加工能力，全部结果可从磁盘重读。
"""
from __future__ import annotations

import json
from pathlib import Path

from sequence_store import Store, RevisionRejected, load, apply
import features as legacy_features


# --------------------------------------- 旧管线（baseline，保留以复现缺陷）
def run(root, edits):
    root = Path(root)
    seq = apply(load(root / "reference.fa"), edits)
    old_features = json.load(open(root / "features.json"))
    return {"sequence": seq,
            "features": {f["id"]: legacy_features.extract(seq, f)
                         for f in old_features}}


# --------------------------------------- 新版本化管线
class RevisionPipeline:
    def __init__(self, store_dir, root=None, feature_table=None):
        """root + feature_table 提供时建库；否则打开已有 store。"""
        store_dir = Path(store_dir)
        if root is not None:
            root = Path(root)
            if feature_table is None:
                feature_table = root / "features.json"
            defs = json.loads(Path(feature_table).read_text())
            self.store = Store.create(store_dir, root / "reference.fa", defs)
        else:
            self.store = Store.open(store_dir)
        self.engine = self.store.feature_engine()

    # -- apply-revision
    def apply_revision(self, base_version, target_version, edits, crash=None):
        """原子发布；RevisionRejected 表示整批拒绝、磁盘无任何变更。"""
        return self.store.apply_revision(
            base_version, target_version, edits, crash=crash)

    def apply_batch_file(self, revisions_path, crash=None):
        """消费 large-revisions.json 风格的批量修订描述。"""
        revisions = json.loads(Path(revisions_path).read_text())
        results = []
        for rev in revisions:
            results.append(self.apply_revision(
                rev["base_version"], rev["target_version"], rev["edits"],
                crash=crash))
        return results

    # -- extract / trace
    def extract(self, version, feature_id):
        return self.engine.extract(version, feature_id)

    def trace(self, version, feature_id):
        return self.engine.trace(version, feature_id)

    def locate(self, version, feature_id):
        return self.engine.locate(version, feature_id)

    def sequence(self, version):
        return self.store.sequence(version)

    def affected(self, version):
        return self.engine.affected(version)

    # -- 审计
    def audit(self):
        return self.store.audit()


def reopen(store_dir) -> RevisionPipeline:
    """模拟独立进程：只从磁盘重新打开，验证可重读性。"""
    return RevisionPipeline(store_dir)
