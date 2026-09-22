"""Schema-contract validator."""
from __future__ import annotations

from .base import BaseValidator
from ..models import TestStatus

# postgres reports "character varying"; contracts usually say "varchar"
_TYPE_ALIASES = {
    "CHARACTER VARYING": "VARCHAR",
    "CHARACTER": "CHAR",
    "DOUBLE PRECISION": "DOUBLE",
    "TIMESTAMP WITHOUT TIME ZONE": "TIMESTAMP",
    "TIMESTAMP WITH TIME ZONE": "TIMESTAMPTZ",
}


def _norm(t: str) -> str:
    t = (t or "").upper().strip()
    # strip length params: VARCHAR(50) -> VARCHAR
    t = t.split("(")[0].strip()
    return _TYPE_ALIASES.get(t, t)


class SchemaValidator(BaseValidator):
    """TC type: schema_match -- table matches the column name/type contract."""

    type_name = "schema_match"

    def validate(self):
        schema, table = self.tc["schema"], self.tc["table"]
        case_sensitive = self.tc.get("case_sensitive_names", False)
        expected = self.tc["columns"]

        actual_cols = self.ctx.columns(schema, table)
        if not actual_cols:
            return self._result(TestStatus.ERROR, f"table {schema}.{table} not found or has no columns")

        def key(n: str) -> str:
            return n if case_sensitive else n.lower()

        actual = {key(c["name"]): _norm(c["type"]) for c in actual_cols}
        missing, mismatched = [], []
        for col in expected:
            name, want = key(col["name"]), _norm(col["type"])
            if name not in actual:
                missing.append(col["name"])
            elif actual[name] != want:
                mismatched.append({"column": col["name"], "expected": want, "actual": actual[name]})

        details = {"expected": len(expected), "actual": len(actual_cols),
                   "missing": missing, "type_mismatches": mismatched}
        if missing or mismatched:
            return self._result(TestStatus.FAILED,
                                f"schema drift: {len(missing)} missing, {len(mismatched)} type mismatches",
                                details)
        return self._result(TestStatus.PASSED,
                            f"schema matches contract ({len(expected)} columns)", details)
