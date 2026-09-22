# GCP ETL Test Framework

A **metadata-driven, generic test framework** for ETL pipelines on Google Cloud,
orchestrated with **Cloud Composer (Airflow)** and written in **Python**.

It was designed for the HR data-exchange migration (on-prem PCF → GCP), phase 1 batch:
`Workday RAS report → GlobalScape → GCS → Dataflow → PostgreSQL`
(staging → curated → consumption, SCD Type 2 / Type 4) — but every pipeline-specific
detail lives in a **YAML metadata file**, so the same DAG and validators test *any*
GCS → Dataflow → Postgres pipeline.

## Design review (what changed from the first sketch)

The original idea — *drop file in GCS → trigger Dataflow → validate tables from
pre-configured test cases* — is sound. These refinements were applied:

1. **Test isolation, not prod tables.** The framework never tests production schemas.
   Every run gets a unique `test_run_id`; Dataflow is launched with a
   `targetSchemaPrefix` parameter and the DAG creates `<prefix>_staging`,
   `<prefix>_curated`, `<prefix>_consumption` schemas, then drops them in teardown.
2. **Trigger with parameters, wait properly.** The Dataflow Flex Template is launched
   with test-specific parameters (test GCS prefix, test schema prefix) and the DAG
   waits on a `DataflowJobStatusSensor` (job id via XCom) instead of sleeps.
3. **Layered test types.** Metadata supports four layers, matching the schema layers:
   - *file*: did the file land in GCS? (`gcs_file_exists`)
   - *load*: source ↔ staging counts, schema contract (`row_count`, `schema_match`)
   - *transform*: SCD2 / SCD4 integrity across staging → curated → consumption
     (`scd2_integrity`, `scd4_consistency`)
   - *quality*: nulls, uniqueness, referential integrity
     (`not_null`, `unique`, `referential_integrity`)
4. **Results are persisted, failures fail the DAG.** Every run writes to
   `etl_test_audit.test_results` in Postgres and uploads a JSON report to GCS.
   `fail_on_failure: true` raises so Composer alerting fires.
5. **Validators are Airflow-free.** They run against a `ValidationContext`
   (DBAPI connection + optional GCS client), so they unit-test with sqlite and run
   unchanged on Composer with Postgres.

## Repository layout

```
gcp-etl-test-framework/
├── dags/
│   └── etl_test_orchestration.py   # Composer DAG: setup → upload → dataflow → validate → publish → teardown
├── plugins/
│   └── test_framework/             # validator library (no Airflow imports)
│       ├── metadata_loader.py      # YAML load + {{ params.* }} / {{ var.value.* }} rendering
│       ├── context.py              # ValidationContext: DB/GCS abstraction
│       ├── models.py               # TestResult / TestRunSummary
│       └── validators/             # gcs_file_exists, row_count, schema_match, not_null,
│                                   # unique, referential_integrity, scd2_integrity, scd4_consistency
├── config/
│   ├── metadata_schema.json        # JSON Schema for the metadata YAML
│   └── sample/
│       ├── hr_employee_test.yaml   # sample pipeline: Workday RAS → Postgres HR schema
│       └── test_data/
│           └── workday_ras_report_sample.csv
├── scripts/
│   └── run_local.py                # local demo: sqlite simulates Postgres, runs all validators
└── tests/
    └── unit/                       # pytest suite (sqlite, no GCP needed)
```

## Deploying to Cloud Composer

Bucket layout:

| Repo path                  | Composer bucket path              |
|----------------------------|-----------------------------------|
| `dags/`                    | `dags/`                           |
| `plugins/test_framework/`  | `plugins/test_framework/`         |
| `config/`                  | `data/etl-test-config/`           |

`composer-requirements.txt`:

```
pyyaml
jsonschema
```

(providers `google` / `postgres` are preinstalled on Composer.)

Required Airflow Variables:

| Variable                 | Example                                          |
|--------------------------|--------------------------------------------------|
| `gcs_bucket`             | `hr-exchange-dev`                                |
| `gcp_project`            | `my-gcp-project`                                 |
| `dataflow_region`        | `us-central1`                                    |
| `dataflow_template_path` | `gs://templates/hr-ras-flex-template.json`       |
| `pg_host` / `pg_database`| `10.0.0.5` / `hrdb`                              |
| `pg_conn_id`             | `postgres_hr` (Airflow connection to Postgres)   |

Trigger the DAG with params:

```json
{ "metadata_file": "hr_employee_test.yaml", "test_run_id": "ras-2026-09-22-01", "test_schema": "t_ras01" }
```

## Writing a new pipeline test

Copy `config/sample/hr_employee_test.yaml`, point `dataflow.parameters` at your
template, list tables per layer, and declare `test_cases`. Each case is:

```yaml
- id: TC05
  name: "SCD2 integrity on curated employee dimension"
  type: scd2_integrity          # one of the registry types
  schema: "{{ params.test_schema }}_curated"
  table: cur_employee_scd2
  business_key: [employee_id]
  eff_from: eff_start_date
  eff_to: eff_end_date
  is_current: is_current
  open_ended: "9999-12-31"      # sentinel for the current version
  check_contiguity: true
```

Validator catalog:

| type                   | checks |
|------------------------|--------|
| `gcs_file_exists`      | object exists in bucket (skipped when no GCS client) |
| `row_count`            | csv / table / literal counts equal within tolerance (`0`, `5` or `"1%"`) |
| `schema_match`         | column names + normalized types vs contract |
| `not_null`             | zero NULLs in listed columns |
| `unique`               | column set unique (reports duplicates) |
| `referential_integrity`| no orphans vs reference table |
| `scd2_integrity`       | one current row per key, no overlapping periods, contiguous, open-ended sentinel on current |
| `scd4_consistency`     | current keys ⊆ history keys, current row == latest history version on compared columns |

## Local demo (no GCP needed)

```bash
pip install -r requirements.txt
python scripts/run_local.py                 # all green
python scripts/run_local.py --break-scd2    # injects an overlapping SCD2 version, watch TC05 fail
pytest tests/unit -q
```

`run_local.py` builds a sqlite database that mimics what Dataflow would produce
(staging load → curated SCD2 with two changed employees → consumption current +
history), then executes every test case from the sample metadata.
