"""Unit tests for metadata loading / templating."""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "plugins"))

from test_framework.metadata_loader import load_metadata, render_templates, validate_metadata

SAMPLE = os.path.join(os.path.dirname(__file__), "..", "..",
                      "config", "sample", "hr_employee_test.yaml")


def test_render_templates():
    obj = {"a": "{{ params.x }}", "b": "{{ var.value.y }}", "c": "{{ params.missing }}"}
    out = render_templates(obj, {"x": "1", "test_run_id": "r"}, {"y": "2"})
    assert out == {"a": "1", "b": "2", "c": "{{ params.missing }}"}


def test_load_sample_metadata_renders():
    meta = load_metadata(
        SAMPLE,
        params={"test_run_id": "r1", "test_schema": "t", "test_prefix": "etl-tests/r1"},
        variables={"gcs_bucket": "bkt", "gcp_project": "p", "dataflow_region": "r",
                   "dataflow_template_path": "gs://t/t.json", "pg_host": "h",
                   "pg_database": "d", "pg_conn_id": "pg"},
    )
    assert meta["pipeline"]["name"] == "hr-ras-to-postgres"
    assert meta["pipeline"]["postgres"]["schemas"]["staging"] == "t_staging"
    assert meta["test_data"]["gcs_key"] == "etl-tests/r1/workday_ras_report.csv"
    assert len(meta["test_cases"]) == 9
    validate_metadata(meta)


def test_load_metadata_rejects_bad():
    try:
        validate_metadata({"version": "1.0"})
    except ValueError:
        return
    raise AssertionError("expected ValueError")
