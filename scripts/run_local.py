#!/usr/bin/env python3
"""Local end-to-end demo of the metadata-driven ETL test framework.

Simulates what Dataflow would produce (staging load -> curated SCD2 ->
consumption current + history) in sqlite, then executes every test case from
config/sample/hr_employee_test.yaml through the real validator registry.

    python scripts/run_local.py                 # all green
    python scripts/run_local.py --break-scd2    # inject overlapping SCD2 version -> TC05 fails

No GCP, no Airflow, no Postgres needed.
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import sqlite3
import sys

ROOT = os.path.join(os.path.dirname(__file__), "..")
sys.path.insert(0, os.path.join(ROOT, "plugins"))

from test_framework.context import ValidationContext  # noqa: E402
from test_framework.metadata_loader import load_metadata  # noqa: E402
from test_framework.models import TestRunSummary, TestStatus  # noqa: E402
from test_framework.validators import get_validator  # noqa: E402

META = os.path.join(ROOT, "config", "sample", "hr_employee_test.yaml")
PREFIX = "t"  # -> t_staging / t_curated / t_consumption like "{{ params.test_schema }}_..."

EMP_COLS = ["employee_id", "first_name", "last_name", "email", "department",
            "job_title", "location", "hire_date", "employment_status"]


def tname(schema: str, table: str) -> str:
    return f"{PREFIX}_{schema}_{table}"


def build_db(break_scd2: bool = False) -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    cur = conn.cursor()

    # ---- staging: straight load of the RAS csv ---------------------------
    # hire_date is DATE per the contract (TC03); everything else VARCHAR.
    stage_cols = [f'"{c}" DATE' if c == "hire_date" else f'"{c}" VARCHAR'
                  for c in EMP_COLS]
    cur.execute(f"CREATE TABLE \"{tname('staging', 'stg_employee')}\" ("
                + ", ".join(stage_cols) + ", \"load_ts\" VARCHAR)")

    csv_path = os.path.join(ROOT, "config", "sample", "test_data",
                            "workday_ras_report_sample.csv")
    with open(csv_path, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    cur.executemany(
        f"INSERT INTO \"{tname('staging', 'stg_employee')}\" VALUES ({','.join('?' * (len(EMP_COLS) + 1))})",
        [[r[c] for c in EMP_COLS] + ["2026-09-22"] for r in rows],
    )

    # ---- curated: SCD2. Everyone gets v1; E003/E007 get a dept change ----
    scd2 = tname("curated", "cur_employee_scd2")
    cur.execute(f"CREATE TABLE \"{scd2}\" ("
                + ", ".join(f'"{c}" VARCHAR' for c in EMP_COLS)
                + ', "eff_start_date" VARCHAR, "eff_end_date" VARCHAR, "is_current" INTEGER)')
    changes = {"E003": ("Product", "Senior Data Engineer"),
               "E007": ("FP&A", "Finance Manager")}
    versions = []
    for r in rows:
        base = [r[c] for c in EMP_COLS]
        versions.append(base + ["2024-01-01", "9999-12-31", 1])
    for r in rows:
        if r["employee_id"] in changes:
            dept, title = changes[r["employee_id"]]
            # expire v1
            for v in versions:
                if v[0] == r["employee_id"] and v[-1] == 1:
                    v[-2], v[-1] = "2026-09-20", 0
            base = [r[c] for c in EMP_COLS]
            base[4], base[5] = dept, title  # department, job_title
            versions.append(base + ["2026-09-21", "9999-12-31", 1])
    if break_scd2:
        # overlapping rogue version for E003 -> TC05 must fail
        r = next(x for x in rows if x["employee_id"] == "E003")
        base = [r[c] for c in EMP_COLS]
        versions.append(base + ["2026-09-15", "2026-09-25", 0])
    cur.executemany(f"INSERT INTO \"{scd2}\" VALUES ({','.join('?' * (len(EMP_COLS) + 3))})",
                    versions)

    # ---- consumption: current snapshot + full history (SCD4) --------------
    cur_t, hist_t = tname("consumption", "cons_employee"), tname("consumption", "cons_employee_history")
    cur.execute(f"CREATE TABLE \"{cur_t}\" (" + ", ".join(f'"{c}" VARCHAR' for c in EMP_COLS) + ")")
    cur.execute(f"CREATE TABLE \"{hist_t}\" ("
                + ", ".join(f'"{c}" VARCHAR' for c in EMP_COLS)
                + ', "eff_start_date" VARCHAR, "eff_end_date" VARCHAR, "is_current" INTEGER)')
    cur.executemany(f"INSERT INTO \"{hist_t}\" VALUES ({','.join('?' * (len(EMP_COLS) + 3))})",
                    versions)
    latest = {}
    for v in versions:
        k = v[0]
        if k not in latest or v[-3] >= latest[k][-3]:
            latest[k] = v
    cur.executemany(f"INSERT INTO \"{cur_t}\" VALUES ({','.join('?' * len(EMP_COLS))})",
                    [v[:len(EMP_COLS)] for v in latest.values()])

    # ---- dim_department for the FK check ---------------------------------
    dim_t = tname("consumption", "dim_department")
    cur.execute(f"CREATE TABLE \"{dim_t}\" (\"department_name\" VARCHAR)")
    depts = sorted({r["department"] for r in rows} | {c[0] for c in changes.values()})
    cur.executemany(f"INSERT INTO \"{dim_t}\" VALUES (?)", [(d,) for d in depts])

    conn.commit()
    return conn


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--break-scd2", action="store_true",
                    help="inject an overlapping SCD2 version so TC05 fails")
    args = ap.parse_args()

    params = {"test_run_id": "local-demo", "test_schema": PREFIX,
              "test_prefix": "etl-tests/local-demo"}
    variables = {"gcs_bucket": "demo-bucket", "gcp_project": "demo",
                 "dataflow_region": "us-central1",
                 "dataflow_template_path": "gs://demo/t.json",
                 "pg_host": "localhost", "pg_database": "hrdb",
                 "pg_conn_id": "postgres_hr"}
    meta = load_metadata(META, params=params, variables=variables)
    meta["_base_dir"] = os.path.join(ROOT, "config", "sample")

    conn = build_db(break_scd2=args.break_scd2)
    ctx = ValidationContext(conn, dialect="sqlite",
                            pipeline=meta["pipeline"],
                            params={**params, "_base_dir": meta["_base_dir"]})
    results = []
    for tc in meta["test_cases"]:
        try:
            results.append(get_validator(tc["type"])(tc, ctx).validate())
        except Exception as exc:
            from test_framework.models import TestResult
            results.append(TestResult(test_id=tc["id"], name=tc["name"],
                                      validator=tc["type"], status=TestStatus.ERROR,
                                      message=f"validator crashed: {exc}"))
    summary = TestRunSummary(run_id="local-demo",
                             pipeline=meta["pipeline"]["name"], results=results)

    print(f"\nrun: {summary.run_id}  pipeline: {summary.pipeline}")
    print("-" * 78)
    for r in summary.results:
        mark = {"PASSED": "PASS", "FAILED": "FAIL",
                "SKIPPED": "SKIP", "ERROR": "ERROR"}[r.status.value]
        print(f"[{mark}] {r.test_id} {r.name}")
        if r.status != TestStatus.PASSED:
            print(f"       {r.message}")
    print("-" * 78)
    print(f"{summary.passed_count} passed, {summary.failed_count} failed, "
          f"{summary.skipped_count} skipped -> {'OK' if summary.ok else 'NOT OK'}")
    print("\nfull JSON:\n" + json.dumps(summary.to_dict(), indent=2, default=str))
    return 0 if summary.ok else 1


if __name__ == "__main__":
    sys.exit(main())
