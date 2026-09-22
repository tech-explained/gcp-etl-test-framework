"""Metadata-driven end-to-end ETL test orchestration (Cloud Composer).

Deploys to the Composer bucket as:
  dags/etl_test_orchestration.py          <- this file   (repo: dags/)
  data/etl-test-config/<pipeline>.yaml   <- metadata    (repo: config/)
  data/etl-test-config/test_data/...     <- test files (repo: config/.../test_data/)
  plugins/test_framework/                <- validators  (repo: plugins/)

Flow:
  setup -> upload_test_file -> trigger_dataflow -> wait_dataflow
        -> run_validations -> publish_results -> teardown

Trigger with DAG params, e.g.:
  {"metadata_file": "hr_employee_test.yaml",
   "test_run_id": "ras-2026-09-22-01",
   "test_schema": "t_ras01"}
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.configuration import conf
from airflow.exceptions import AirflowException
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.operators.dataflow import (
    DataflowStartFlexTemplateOperator,
)
from airflow.providers.google.cloud.sensors.dataflow import DataflowJobStatusSensor
from airflow.providers.postgres.hooks.postgres import PostgresHook

from test_framework.context import ValidationContext
from test_framework.metadata_loader import load_metadata
from test_framework.models import TestResult, TestRunSummary, TestStatus
from test_framework.validators import get_validator

# ---------------------------------------------------------------- helpers

_VAR_KEYS = (
    "gcs_bucket",
    "gcp_project",
    "dataflow_region",
    "dataflow_template_path",
    "pg_host",
    "pg_database",
    "pg_conn_id",
)


def _bucket_root() -> str:
    """Directory that contains dags/, plugins/, data/ in the Composer bucket."""
    return os.path.dirname(conf.get("core", "dags_folder"))


def _meta_path(context=None) -> str:
    if context is not None:
        fname = (context["params"] or {}).get("metadata_file") or "hr_employee_test.yaml"
    else:  # DAG parse time
        fname = Variable.get("etl_test_metadata_file", default_var="hr_employee_test.yaml")
    p = os.path.join(_bucket_root(), "data", "etl-test-config", fname)
    if not os.path.exists(p):  # local / repo-layout fallback
        p = os.path.join(os.path.dirname(__file__), "..", "config", "sample", fname)
    return p


def _variables() -> dict:
    return {k: Variable.get(k, default_var="") for k in _VAR_KEYS}


def _run_id(context) -> str:
    return (context["params"] or {}).get("test_run_id") or context["run_id"]


def _runtime_params(context) -> dict:
    params = dict(context["params"] or {})
    params["test_run_id"] = _run_id(context)
    params.setdefault("test_schema", "t")
    params["test_prefix"] = f"etl-tests/{params['test_run_id']}"
    return params


def _load_meta(context=None):
    """(metadata, base_dir). At parse time context=None: var.value resolved,
    {{ params.* }} templates left intact for runtime Jinja rendering."""
    path = _meta_path(context)
    params = _runtime_params(context) if context is not None else {}
    meta = load_metadata(path, params=params, variables=_variables())
    meta["_base_dir"] = os.path.dirname(path)
    return meta


def _pg_hook(meta) -> PostgresHook:
    return PostgresHook(postgres_conn_id=meta["pipeline"]["postgres"]["conn_id"])


# ---------------------------------------------------------------- callables

def _setup(**context):
    meta = _load_meta(context)
    hook = _pg_hook(meta)
    schemas = list(meta["pipeline"]["postgres"]["schemas"].values())
    schemas.append(meta["pipeline"]["postgres"].get("results_schema", "etl_test_audit"))
    for s in schemas:
        hook.run(f'CREATE SCHEMA IF NOT EXISTS "{s}"')
    print(f"setup: ensured schemas {schemas}")


def _upload_test_file(**context):
    meta = _load_meta(context)
    local = os.path.join(meta["_base_dir"], meta["test_data"]["source_file"])
    bucket = meta["pipeline"]["gcs"]["bucket"]
    key = meta["test_data"]["gcs_key"]
    GCSHook().upload(bucket_name=bucket, object_name=key, filename=local)
    print(f"uploaded {local} -> gs://{bucket}/{key}")


def _run_validations(**context):
    meta = _load_meta(context)
    params = _runtime_params(context)
    hook = _pg_hook(meta)
    conn = hook.get_conn()
    try:
        ctx = ValidationContext(
            conn,
            dialect="postgres",
            gcs_client=GCSHook(),
            pipeline=meta["pipeline"],
            params={**params, "_base_dir": meta["_base_dir"]},
        )
        results = []
        for tc in meta["test_cases"]:
            try:
                results.append(get_validator(tc["type"])(tc, ctx).validate())
            except Exception as exc:  # validator bug != pipeline bug; record it
                results.append(TestResult(
                    test_id=tc["id"], name=tc.get("name", tc["id"]),
                    validator=tc["type"], status=TestStatus.ERROR,
                    message=f"validator crashed: {exc}",
                ))
        summary = TestRunSummary(
            run_id=params["test_run_id"],
            pipeline=meta["pipeline"]["name"],
            results=results,
        )
    finally:
        conn.close()

    _persist_results(hook, meta, summary)
    context["ti"].xcom_push(key="summary", value=json.dumps(summary.to_dict()))
    print(json.dumps(summary.to_dict(), indent=2, default=str))
    if meta["pipeline"].get("fail_on_failure", True) and not summary.ok:
        raise AirflowException(
            f"{summary.failed_count} test(s) failed -- see etl_test_audit.test_results "
            f"run_id={summary.run_id}"
        )


def _persist_results(hook: PostgresHook, meta, summary: TestRunSummary):
    schema = meta["pipeline"]["postgres"].get("results_schema", "etl_test_audit")
    hook.run(
        f'CREATE TABLE IF NOT EXISTS "{schema}"."test_results" ('
        "run_id TEXT, pipeline TEXT, test_id TEXT, name TEXT, validator TEXT, "
        "status TEXT, message TEXT, details TEXT, "
        "checked_at TIMESTAMPTZ DEFAULT now())"
    )
    for r in summary.results:
        hook.run(
            f'INSERT INTO "{schema}"."test_results" '
            "(run_id, pipeline, test_id, name, validator, status, message, details) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
            [summary.run_id, summary.pipeline, r.test_id, r.name, r.validator,
             r.status.value, r.message, json.dumps(r.details, default=str)],
        )


def _publish_results(**context):
    meta = _load_meta(context)
    summary_json = context["ti"].xcom_pull(task_ids="run_validations", key="summary")
    bucket = meta["pipeline"]["gcs"]["bucket"]
    key = f"etl-tests/{_run_id(context)}/test-results.json"
    GCSHook().upload(bucket_name=bucket, object_name=key, data=summary_json or "{}")
    print(f"published results -> gs://{bucket}/{key}")


def _teardown(**context):
    meta = _load_meta(context)
    if not meta["pipeline"].get("cleanup_after_run", True):
        print("teardown: cleanup_after_run=false, keeping test schemas")
        return
    hook = _pg_hook(meta)
    for s in reversed(list(meta["pipeline"]["postgres"]["schemas"].values())):
        hook.run(f'DROP SCHEMA IF EXISTS "{s}" CASCADE')
    print("teardown: dropped test schemas")


# ------------------------------------------------------------------- DAG

with DAG(
    dag_id="etl_test_orchestration",
    description="Metadata-driven end-to-end test: GCS -> Dataflow -> Postgres (HR migration)",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    params={
        "metadata_file": "hr_employee_test.yaml",
        "test_run_id": None,  # defaults to the Airflow run_id
        "test_schema": "t",   # prefix for <prefix>_staging/_curated/_consumption
    },
    default_args={
        "owner": "data-eng",
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
    },
    tags=["etl-testing", "hr-migration"],
) as dag:

    _raw = _load_meta()  # parse time: {{ params.* }} stays templated for Jinja
    _df = _raw["pipeline"]["dataflow"]

    setup = PythonOperator(task_id="setup", python_callable=_setup)
    upload_test_file = PythonOperator(task_id="upload_test_file", python_callable=_upload_test_file)

    # NOTE: {{ params.* }} placeholders inside body are rendered by Airflow at runtime.
    trigger_dataflow = DataflowStartFlexTemplateOperator(
        task_id="trigger_dataflow",
        project_id=_df["project"],
        location=_df["region"],
        body={
            "launchParameter": {
                "containerSpecGcsPath": _df["template_path"],
                "jobName": "etl-test-{{ ts_nodash }}",
                "parameters": _df["parameters"],
                "environment": {"tempLocation": _df["temp_location"]},
            }
        },
    )

    wait_dataflow = DataflowJobStatusSensor(
        task_id="wait_dataflow",
        project_id=_df["project"],
        location=_df["region"],
        job_id="{{ ti.xcom_pull(task_ids='trigger_dataflow') }}",
        expected_statuses={"JOB_STATE_DONE"},
        poke_interval=60,
        timeout=60 * 60,
    )

    run_validations = PythonOperator(task_id="run_validations", python_callable=_run_validations)
    publish_results = PythonOperator(task_id="publish_results", python_callable=_publish_results)
    teardown = PythonOperator(
        task_id="teardown", python_callable=_teardown, trigger_rule="all_done"
    )

    (setup >> upload_test_file >> trigger_dataflow >> wait_dataflow
     >> run_validations >> publish_results >> teardown)
