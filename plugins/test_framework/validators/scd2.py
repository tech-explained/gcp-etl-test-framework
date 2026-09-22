"""SCD Type 2 integrity validator.

Checks, per business key, on a table with (eff_from, eff_to, is_current):
  1. exactly one row flagged current
  2. eff_from <= eff_to on every version
  3. no overlapping effective periods between consecutive versions
  4. versions are contiguous (next.eff_from == prev.eff_to + 1 day) -- optional
  5. the current version carries the open-ended sentinel (e.g. 9999-12-31)
"""
from __future__ import annotations

from collections import defaultdict
from datetime import datetime
from typing import Optional

from .base import BaseValidator
from ..models import TestStatus

_DATE_FORMATS = ("%Y-%m-%d", "%Y/%m/%d", "%Y-%m-%d %H:%M:%S", "%m/%d/%Y")


def _parse_date(v) -> Optional["datetime.date"]:
    if v is None:
        return None
    if isinstance(v, datetime):
        return v.date()
    s = str(v).strip()
    if not s:
        return None
    for fmt in _DATE_FORMATS:
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            pass
    try:
        return datetime.fromisoformat(s).date()
    except ValueError:
        return None


def _is_current(v) -> bool:
    return str(v).strip().lower() in ("1", "true", "t", "y", "yes")


class SCD2Validator(BaseValidator):
    type_name = "scd2_integrity"

    def validate(self):
        schema, table = self.tc["schema"], self.tc["table"]
        bk = self.tc["business_key"]
        f_from, f_to, f_cur = self.tc["eff_from"], self.tc["eff_to"], self.tc["is_current"]
        open_ended = self.tc.get("open_ended", "9999-12-31")
        check_contiguity = self.tc.get("check_contiguity", True)

        order = ", ".join([f'"{c}"' for c in bk] + [f'"{f_from}"'])
        rows = self.ctx.fetchall(
            f"SELECT * FROM {self.ctx.table_ref(schema, table)} ORDER BY {order}"
        )
        groups: dict = defaultdict(list)
        for r in rows:
            groups[tuple(r.get(c) for c in bk)].append(r)

        issues = []
        for key, vers in groups.items():
            label = "/".join(str(k) for k in key)
            currents = [v for v in vers if _is_current(v.get(f_cur))]
            if len(currents) != 1:
                issues.append(f"key {label}: {len(currents)} current rows (expected 1)")
            for v in vers:
                d1, d2 = _parse_date(v.get(f_from)), _parse_date(v.get(f_to))
                if d1 and d2 and d1 > d2:
                    issues.append(f"key {label}: eff_from {d1} > eff_to {d2}")
            for prev, nxt in zip(vers, vers[1:]):
                p_to, n_from = _parse_date(prev.get(f_to)), _parse_date(nxt.get(f_from))
                if p_to and n_from and p_to >= n_from:
                    issues.append(
                        f"key {label}: overlapping versions "
                        f"({prev.get(f_from)}->{prev.get(f_to)} overlaps {nxt.get(f_from)}->{nxt.get(f_to)})"
                    )
                elif check_contiguity and p_to and n_from and (n_from - p_to).days != 1:
                    issues.append(
                        f"key {label}: gap between versions "
                        f"({p_to} -> {n_from}, expected 1 day)"
                    )
            if len(currents) == 1:
                cur_to = currents[0].get(f_to)
                if open_ended in (None, "null"):
                    sentinel_ok = cur_to is None
                else:
                    sentinel_ok = cur_to is not None and str(cur_to).startswith(str(open_ended))
                if not sentinel_ok:
                    issues.append(
                        f"key {label}: current version eff_to={cur_to!r}, "
                        f"expected open-ended sentinel {open_ended!r}"
                    )

        details = {"keys_checked": len(groups), "rows_checked": len(rows),
                   "issues": issues[:25], "issue_count": len(issues)}
        if issues:
            return self._result(TestStatus.FAILED,
                                f"SCD2 violations: {len(issues)} (checked {len(groups)} keys)", details)
        return self._result(TestStatus.PASSED,
                            f"SCD2 clean: {len(groups)} keys, {len(rows)} versions", details)
