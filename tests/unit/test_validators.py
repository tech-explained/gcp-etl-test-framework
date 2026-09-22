"""Unit tests for the validator library (sqlite, no GCP/Airflow needed)."""
from __future__ import annotations

import os
import sqlite3
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "plugins"))

from test_framework.context import ValidationContext
from test_framework.models import TestStatus
from test_framework.validators import get_validator

SCH = "t"


def make_ctx():
    conn = sqlite3.connect(":memory:")
    return ValidationContext(conn, dialect="sqlite", params={"_base_dir": ""}), conn


def run(tc_type, tc, ctx):
    return get_validator(tc_type)({**tc, "type": tc_type, "id": tc.get("id", "T1"),
                                   "name": tc.get("name", "t")}, ctx).validate()


def execsql(conn, sql, args=()):
    conn.execute(sql, args)
    conn.commit()


# ------------------------------------------------------------- row_count

def test_row_count_table_vs_table_pass():
    ctx, conn = make_ctx()
    execsql(conn, 'CREATE TABLE "t_a" (id TEXT)')
    execsql(conn, 'CREATE TABLE "t_b" (id TEXT)')
    execsql(conn, 'INSERT INTO "t_a" VALUES (\'x\'),(\'y\')')
    execsql(conn, 'INSERT INTO "t_b" VALUES (\'x\'),(\'y\')')
    r = run("row_count", {"source": {"kind": "table", "schema": SCH, "table": "a"},
                          "target": {"kind": "table", "schema": SCH, "table": "b"}},
            ctx)
    assert r.status == TestStatus.PASSED


def test_row_count_mismatch_fail():
    ctx, conn = make_ctx()
    execsql(conn, 'CREATE TABLE "t_a" (id TEXT)')
    execsql(conn, 'CREATE TABLE "t_b" (id TEXT)')
    execsql(conn, 'INSERT INTO "t_a" VALUES (\'x\'),(\'y\'),(\'z\')')
    execsql(conn, 'INSERT INTO "t_b" VALUES (\'x\')')
    r = run("row_count", {"source": {"kind": "table", "schema": SCH, "table": "a"},
                          "target": {"kind": "table", "schema": SCH, "table": "b"}},
            ctx)
    assert r.status == TestStatus.FAILED
    assert r.details["diff"] == 2


def test_row_count_percent_tolerance():
    ctx, conn = make_ctx()
    execsql(conn, 'CREATE TABLE "t_a" (id TEXT)')
    execsql(conn, 'CREATE TABLE "t_b" (id TEXT)')
    for i in range(100):
        execsql(conn, 'INSERT INTO "t_a" VALUES (?)', (f"r{i}",))
    for i in range(99):
        execsql(conn, 'INSERT INTO "t_b" VALUES (?)', (f"r{i}",))
    r = run("row_count", {"source": {"kind": "table", "schema": SCH, "table": "a"},
                          "target": {"kind": "table", "schema": SCH, "table": "b"},
                          "tolerance": "1%"},
            ctx)
    assert r.status == TestStatus.PASSED


# ---------------------------------------------------------- data quality

def test_not_null_fail_and_unique_fail():
    ctx, conn = make_ctx()
    execsql(conn, 'CREATE TABLE "t_emp" (id TEXT, dept TEXT)')
    execsql(conn, "INSERT INTO \"t_emp\" VALUES ('E1','Eng'),('E1','Eng'),(NULL,'HR')")
    r = run("not_null", {"schema": SCH, "table": "emp", "columns": ["id"]}, ctx)
    assert r.status == TestStatus.FAILED and r.details["null_counts"] == {"id": 1}
    r = run("unique", {"schema": SCH, "table": "emp", "columns": ["id"]}, ctx)
    assert r.status == TestStatus.FAILED


def test_referential_integrity():
    ctx, conn = make_ctx()
    execsql(conn, 'CREATE TABLE "t_fact" (id TEXT, dept TEXT)')
    execsql(conn, 'CREATE TABLE "t_dim" (dept_name TEXT)')
    execsql(conn, "INSERT INTO \"t_fact\" VALUES ('1','Eng'),('2','Nope')")
    execsql(conn, "INSERT INTO \"t_dim\" VALUES ('Eng')")
    r = run("referential_integrity",
            {"schema": SCH, "table": "fact", "column": "dept",
             "ref_schema": SCH, "ref_table": "dim", "ref_column": "dept_name"}, ctx)
    assert r.status == TestStatus.FAILED and r.details["orphans"] == 1


# ------------------------------------------------------------------ scd2

SCD2_TC = {"schema": SCH, "table": "scd2", "business_key": ["eid"],
           "eff_from": "eff_from", "eff_to": "eff_to", "is_current": "cur",
           "open_ended": "9999-12-31", "check_contiguity": True}


def load_scd2(conn, rows):
    execsql(conn, 'CREATE TABLE "t_scd2" (eid TEXT, val TEXT, eff_from TEXT, eff_to TEXT, cur INTEGER)')
    for r in rows:
        execsql(conn, 'INSERT INTO "t_scd2" VALUES (?,?,?,?,?)', r)
    conn.commit()


def test_scd2_clean_pass():
    ctx, conn = make_ctx()
    load_scd2(conn, [
        ("E1", "a", "2024-01-01", "2024-06-30", 0),
        ("E1", "b", "2024-07-01", "9999-12-31", 1),
        ("E2", "a", "2024-01-01", "9999-12-31", 1),
    ])
    assert run("scd2_integrity", SCD2_TC, ctx).status == TestStatus.PASSED


def test_scd2_overlap_fail():
    ctx, conn = make_ctx()
    load_scd2(conn, [
        ("E1", "a", "2024-01-01", "2024-08-15", 0),
        ("E1", "b", "2024-07-01", "9999-12-31", 1),  # overlaps previous
    ])
    r = run("scd2_integrity", SCD2_TC, ctx)
    assert r.status == TestStatus.FAILED
    assert any("overlapping" in i for i in r.details["issues"])


def test_scd2_two_current_fail():
    ctx, conn = make_ctx()
    load_scd2(conn, [
        ("E1", "a", "2024-01-01", "9999-12-31", 1),
        ("E1", "b", "2024-07-01", "9999-12-31", 1),
    ])
    r = run("scd2_integrity", SCD2_TC, ctx)
    assert r.status == TestStatus.FAILED
    assert any("current rows" in i for i in r.details["issues"])


def test_scd2_gap_fail():
    ctx, conn = make_ctx()
    load_scd2(conn, [
        ("E1", "a", "2024-01-01", "2024-06-30", 0),
        ("E1", "b", "2024-07-05", "9999-12-31", 1),  # 4-day gap
    ])
    r = run("scd2_integrity", SCD2_TC, ctx)
    assert r.status == TestStatus.FAILED
    assert any("gap" in i for i in r.details["issues"])


# ------------------------------------------------------------------ scd4

SCD4_TC = {"current": {"schema": SCH, "table": "cur"},
           "history": {"schema": SCH, "table": "hist"},
           "business_key": ["eid"], "history_eff_from": "eff_from",
           "compare_columns": ["val"]}


def test_scd4_pass_and_mismatch():
    ctx, conn = make_ctx()
    execsql(conn, 'CREATE TABLE "t_cur" (eid TEXT, val TEXT)')
    execsql(conn, 'CREATE TABLE "t_hist" (eid TEXT, val TEXT, eff_from TEXT)')
    execsql(conn, "INSERT INTO \"t_cur\" VALUES ('E1','b'),('E2','a')")
    execsql(conn, "INSERT INTO \"t_hist\" VALUES ('E1','a','2024-01-01'),('E1','b','2024-07-01'),('E2','a','2024-01-01')")
    assert run("scd4_consistency", SCD4_TC, ctx).status == TestStatus.PASSED
    execsql(conn, "UPDATE \"t_cur\" SET val='zzz' WHERE eid='E1'")
    r = run("scd4_consistency", SCD4_TC, ctx)
    assert r.status == TestStatus.FAILED


def test_unknown_validator_type():
    with pytest.raises(ValueError):
        get_validator("nope_not_real")
