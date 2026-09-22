"""Data-quality validators: nulls, uniqueness, referential integrity."""
from __future__ import annotations

from .base import BaseValidator
from ..models import TestStatus


class NotNullValidator(BaseValidator):
    """TC type: not_null -- listed columns must contain zero NULLs."""

    type_name = "not_null"

    def validate(self):
        schema, table = self.tc["schema"], self.tc["table"]
        tref = self.ctx.table_ref(schema, table)
        bad = {}
        for col in self.tc["columns"]:
            n = self.ctx.count(schema, table, where=f'"{col}" IS NULL')
            if n:
                bad[col] = n
        if bad:
            return self._result(TestStatus.FAILED, f"NULLs found: {bad}",
                                {"null_counts": bad, "table": tref})
        return self._result(TestStatus.PASSED,
                            f"no NULLs in {self.tc['columns']} ({tref})")


class UniqueValidator(BaseValidator):
    """TC type: unique -- column set must be unique; reports duplicate keys."""

    type_name = "unique"

    def validate(self):
        schema, table = self.tc["schema"], self.tc["table"]
        cols = self.tc["columns"]
        tref = self.ctx.table_ref(schema, table)
        collist = ", ".join(f'"{c}"' for c in cols)
        dups = self.ctx.fetchall(
            f"SELECT {collist}, COUNT(*) AS n FROM {tref} "
            f"GROUP BY {collist} HAVING COUNT(*) > 1 LIMIT 20"
        )
        if dups:
            return self._result(
                TestStatus.FAILED,
                f"{len(dups)} duplicate key(s) on ({', '.join(cols)})",
                {"sample_duplicates": dups, "table": tref},
            )
        return self._result(TestStatus.PASSED, f"({', '.join(cols)}) is unique in {tref}")


class ReferentialIntegrityValidator(BaseValidator):
    """TC type: referential_integrity -- no orphans vs the reference table."""

    type_name = "referential_integrity"

    def validate(self):
        schema, table = self.tc["schema"], self.tc["table"]
        col = self.tc["column"]
        rref = self.ctx.table_ref(self.tc["ref_schema"], self.tc["ref_table"])
        rcol = self.tc["ref_column"]
        tref = self.ctx.table_ref(schema, table)
        row = self.ctx.fetchone(
            f'SELECT COUNT(*) AS c FROM {tref} t LEFT JOIN {rref} r '
            f'ON t."{col}" = r."{rcol}" WHERE r."{rcol}" IS NULL '
            f'AND t."{col}" IS NOT NULL'
        )
        orphans = int(row["c"]) if row else 0
        details = {"orphans": orphans, "table": tref,
                   "reference": f"{self.tc['ref_schema']}.{self.tc['ref_table']}"}
        if orphans:
            sample = self.ctx.fetchall(
                f'SELECT DISTINCT t."{col}" AS orphan FROM {tref} t '
                f'LEFT JOIN {rref} r ON t."{col}" = r."{rcol}" '
                f'WHERE r."{rcol}" IS NULL AND t."{col}" IS NOT NULL LIMIT 20'
            )
            details["sample_orphans"] = [r["orphan"] for r in sample]
            return self._result(TestStatus.FAILED,
                                f"{orphans} orphan value(s) in {tref}.{col}", details)
        return self._result(TestStatus.PASSED,
                            f"all {tref}.{col} values exist in reference", details)
