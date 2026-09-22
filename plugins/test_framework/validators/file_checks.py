"""File-level validators: GCS arrival and row counts."""
from __future__ import annotations

import csv
import os

from .base import BaseValidator
from ..models import TestStatus


class GCSFileExistsValidator(BaseValidator):
    """TC type: gcs_file_exists -- the trigger file actually landed."""

    type_name = "gcs_file_exists"

    def validate(self):
        if self.ctx.gcs_client is None:
            return self._result(TestStatus.SKIPPED, "no GCS client in this context")
        bucket, key = self.tc["bucket"], self.tc["key"]
        exists = self.ctx.gcs_client.exists(bucket, key)
        if exists:
            return self._result(TestStatus.PASSED, f"gs://{bucket}/{key} exists")
        return self._result(TestStatus.FAILED, f"gs://{bucket}/{key} NOT FOUND")


class RowCountValidator(BaseValidator):
    """TC type: row_count -- compare counts from csv / table / literal sources.

    spec: {kind: csv, path: ...} | {kind: table, schema: ..., table: ...}
          | {kind: literal, value: N}
    tolerance: 0 | 5 | "1%"  (absolute rows, or percent of source)
    """

    type_name = "row_count"

    def _count_spec(self, spec) -> int:
        kind = spec["kind"]
        if kind == "csv":
            path = spec["path"]
            if not os.path.isabs(path):
                base = self.ctx.params.get("_base_dir", "")
                path = os.path.join(base, path)
            with open(path, newline="", encoding="utf-8") as f:
                return sum(1 for _ in csv.reader(f)) - 1  # minus header
        if kind == "table":
            return self.ctx.count(spec["schema"], spec["table"])
        if kind == "literal":
            return int(spec["value"])
        raise ValueError(f"unknown count kind: {kind!r}")

    def validate(self):
        src = self._count_spec(self.tc["source"])
        tgt = self._count_spec(self.tc["target"])
        diff = abs(src - tgt)
        tol = self.tc.get("tolerance", 0)
        if isinstance(tol, str) and tol.strip().endswith("%"):
            pct = float(tol.strip().rstrip("%"))
            ok = (src == 0 and tgt == 0) or (src and diff / src * 100 <= pct)
            tol_desc = f"{pct}%"
        else:
            ok = diff <= int(tol)
            tol_desc = f"{tol} rows"
        details = {"source_count": src, "target_count": tgt, "diff": diff, "tolerance": tol_desc}
        if ok:
            return self._result(TestStatus.PASSED, f"counts match ({src} vs {tgt}, tolerance {tol_desc})", details)
        return self._result(TestStatus.FAILED, f"count mismatch: source={src} target={tgt} diff={diff} > {tol_desc}", details)
