"""测试公共工具：临时存储与双生产者 descriptor 构造。"""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from unionx.schema import (
    KIND_INT64,
    KIND_STRUCT,
    KIND_TEXT,
    MISSING_BRANCH_ID,
    BranchSpec,
    FieldSpec,
    ProducerDescriptor,
)


class ExchangeTestCase(unittest.TestCase):
    def setUp(self):
        self._td = tempfile.TemporaryDirectory()
        self.store_root = Path(self._td.name) / "store"

    def tearDown(self):
        self._td.cleanup()

    def descriptors_simple(self):
        """同 branch 身份、不同局部 typecode 的两个 producer（a/b fixture 同族）。"""
        d1 = ProducerDescriptor(
            "P1", 1, {3: "branch-number", 7: "branch-text"},
            (
                BranchSpec("branch-number", "number", KIND_INT64),
                BranchSpec("branch-text", "text", KIND_TEXT),
            ),
        )
        d2 = ProducerDescriptor(
            "P2", 1, {7: "branch-number", 3: "branch-text"},
            (
                BranchSpec("branch-number", "number", KIND_INT64),
                BranchSpec("branch-text", "text", KIND_TEXT),
            ),
        )
        return d1, d2

    def struct_descriptors(self):
        v1 = ProducerDescriptor(
            "P1", 1, {1: "branch-record", 2: MISSING_BRANCH_ID},
            (
                BranchSpec("branch-record", "record", KIND_STRUCT, (
                    FieldSpec("field-quantity", "qty", KIND_INT64, nullable=False),
                )),
                BranchSpec(MISSING_BRANCH_ID, "missing", "null"),
            ),
        )
        v2 = ProducerDescriptor(
            "P1", 2, {5: "branch-record", 9: MISSING_BRANCH_ID},
            (
                BranchSpec("branch-record", "renamed_record", KIND_STRUCT, (
                    FieldSpec("field-quantity", "count", KIND_INT64, nullable=False),
                    FieldSpec("field-note", "note", KIND_TEXT, nullable=True),
                )),
                BranchSpec(MISSING_BRANCH_ID, "missing", "null"),
            ),
        )
        p2 = ProducerDescriptor(
            "P2", 1, {42: "branch-record", 43: MISSING_BRANCH_ID},
            (
                BranchSpec("branch-record", "record", KIND_STRUCT, (
                    FieldSpec("field-quantity", "qty", KIND_INT64, nullable=False),
                )),
                BranchSpec(MISSING_BRANCH_ID, "missing", "null"),
            ),
        )
        return v1, v2, p2
