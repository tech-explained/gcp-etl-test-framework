"""SCD Type 4 consistency validator (current table + history table).

Checks:
  1. every business key in the current table exists in the history table
  2. the current row matches the LATEST history version on compare_columns
  3. no orphan history keys (unless allow_orphan_history: true, e.g. deletes)
"""
from __future__ import annotations

from .base import BaseValidator
from .scd2 import _parse_date
from ..models import TestStatus


class SCD4Validator(BaseValidator):
    type_name = "scd4_consistency"

    def validate(self):
        cur_cfg, hist_cfg = self.tc["current"], self.tc["history"]
        bk = self.tc["business_key"]
        compare = self.tc.get("compare_columns", [])
        hist_from = self.tc.get("history_eff_from")
        allow_orphans = self.tc.get("allow_orphan_history", False)

        cur_tref = self.ctx.table_ref(cur_cfg["schema"], cur_cfg["table"])
        hist_tref = self.ctx.table_ref(hist_cfg["schema"], hist_cfg["table"])

        cur_rows = self.ctx.fetchall(f"SELECT * FROM {cur_tref}")
        hist_rows = self.ctx.fetchall(f"SELECT * FROM {hist_tref}")

        def key_of(r):
            return tuple(r.get(c) for c in bk)

        cur = {key_of(r): r for r in cur_rows}
        hist_latest = {}
        hist_keys = set()
        for r in hist_rows:
            k = key_of(r)
            hist_keys.add(k)
            d = _parse_date(r.get(hist_from)) if hist_from else None
            prev = hist_latest.get(k)
            prev_d = _parse_date(prev.get(hist_from)) if (prev and hist_from) else None
            if prev is None or (d and (prev_d is None or d >= prev_d)):
                hist_latest[k] = r

        issues = []
        for k, crow in cur.items():
            label = "/".join(str(x) for x in k)
            hrow = hist_latest.get(k)
            if hrow is None:
                issues.append(f"key {label}: in current table but missing from history")
                continue
            for c in compare:
                if crow.get(c) != hrow.get(c):
                    issues.append(
                        f"key {label}: current.{c}={crow.get(c)!r} != "
                        f"latest_history.{c}={hrow.get(c)!r}"
                    )
        if not allow_orphans:
            for k in sorted(hist_keys - set(cur.keys()), key=str):
                label = "/".join(str(x) for x in k)
                issues.append(f"key {label}: in history but missing from current table")

        details = {"current_rows": len(cur_rows), "history_rows": len(hist_rows),
                   "issues": issues[:25], "issue_count": len(issues)}
        if issues:
            return self._result(TestStatus.FAILED,
                                f"SCD4 violations: {len(issues)}", details)
        return self._result(
            TestStatus.PASSED,
            f"SCD4 consistent: {len(cur)} current keys all match latest history "
            f"on {len(compare)} columns",
            details,
        )
